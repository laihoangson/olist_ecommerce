"""
FastAPI backend for the Operations Command Center dashboard. Reads from
BigQuery gold tables (is_synthetic = TRUE) and the prediction tables
written by scripts/08-12 -- does NOT run any model inference itself,
just serves precomputed data. Keeps request latency low and avoids
loading model artifacts into the API process at all.

Run locally:
    uvicorn api.main:app --reload --port 8000

Deploy: see render.yaml in this same folder.
"""

import sys
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from replay import bq_writer  # noqa: E402

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Olist Operations Command Center API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten this to your actual dashboard domain before going public
    allow_methods=["GET"],
    allow_headers=["*"],
)


def gold():
    return f"{config.GCP_PROJECT_ID}.{config.BQ_GOLD_DATASET}"


def preds():
    return f"{config.GCP_PROJECT_ID}.{config.PREDICTIONS_DATASET}"


def run(sql: str) -> list:
    client = bq_writer.get_client()
    return [dict(row) for row in client.query(sql).result()]


def table_exists_or_empty(dataset_table: str) -> bool:
    try:
        client = bq_writer.get_client()
        client.get_table(dataset_table)
        return True
    except Exception:
        return False


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Section 1 — Overview
# ---------------------------------------------------------------------------
@app.get("/api/overview")
def overview():
    kpis = run(f"""
        SELECT
          (SELECT SUM(price) + SUM(freight_value) FROM `{gold()}.fct_order_items` i
             JOIN `{gold()}.fct_orders` o USING(order_id)
             WHERE o.is_synthetic = TRUE AND DATE(o.order_purchase_timestamp) = CURRENT_DATE()
          ) AS revenue_today,
          (SELECT COUNT(DISTINCT order_id) FROM `{gold()}.fct_orders`
             WHERE is_synthetic = TRUE AND DATE(order_purchase_timestamp) = CURRENT_DATE()
          ) AS orders_today,
          (SELECT SUM(price) + SUM(freight_value) FROM `{gold()}.fct_order_items` i
             JOIN `{gold()}.fct_orders` o USING(order_id)
             WHERE o.is_synthetic = TRUE AND DATE(o.order_purchase_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          ) AS revenue_yesterday,
          (SELECT COUNT(DISTINCT order_id) FROM `{gold()}.fct_orders`
             WHERE is_synthetic = TRUE AND DATE(order_purchase_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          ) AS orders_yesterday
    """)[0]

    trend = run(f"""
        SELECT date_day AS ds, order_count AS orders, total_revenue AS revenue
        FROM `{gold()}.mart_daily_revenue`
        WHERE is_synthetic = TRUE AND date_day >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        ORDER BY date_day
    """)

    forecast = []
    forecast_table = f"{preds()}.mart_revenue_forecast"
    if table_exists_or_empty(forecast_table):
        forecast = run(f"SELECT ds, yhat, yhat_lower, yhat_upper FROM `{forecast_table}` ORDER BY ds")

    by_category = run(f"""
        SELECT COALESCE(p.product_category_name_english, p.product_category_name, 'unknown') AS category,
               SUM(i.price) + SUM(i.freight_value) AS revenue, COUNT(DISTINCT i.order_id) AS orders
        FROM `{gold()}.fct_order_items` i
        JOIN `{gold()}.fct_orders` o USING(order_id)
        JOIN `{gold()}.dim_products` p ON p.product_id = i.product_id
        WHERE o.is_synthetic = TRUE
        GROUP BY category ORDER BY revenue DESC LIMIT 10
    """)

    by_state = run(f"""
        SELECT c.customer_state AS state, SUM(i.price) + SUM(i.freight_value) AS revenue,
               COUNT(DISTINCT o.order_id) AS orders
        FROM `{gold()}.fct_orders` o
        JOIN `{gold()}.fct_order_items` i USING(order_id)
        JOIN `{gold()}.dim_customers` c ON c.customer_unique_id = o.customer_unique_id
        WHERE o.is_synthetic = TRUE
        GROUP BY state ORDER BY revenue DESC
    """)

    return {"kpis": kpis, "trend": trend, "forecast": forecast, "by_category": by_category, "by_state": by_state}


# ---------------------------------------------------------------------------
# Section 2 — New orders needing attention (delivery risk / negative-review risk)
# ---------------------------------------------------------------------------
@app.get("/api/orders/delivery-risk")
def delivery_risk(tier: str = None, limit: int = 200):
    table = f"{preds()}.predictions_delivery_risk"
    if not table_exists_or_empty(table):
        raise HTTPException(404, "predictions_delivery_risk not found -- run scripts/08_score_delivery_risk.py first.")
    where = f"WHERE risk_tier = '{tier}'" if tier else ""
    rows = run(f"SELECT * FROM `{table}` {where} ORDER BY risk_score DESC LIMIT {int(limit)}")
    summary = run(f"SELECT risk_tier, COUNT(*) AS n FROM `{table}` GROUP BY risk_tier")
    return {"summary": summary, "orders": rows}


@app.get("/api/orders/negative-review-risk")
def negative_review_risk(tier: str = None, limit: int = 200):
    table = f"{preds()}.predictions_negative_review_risk"
    if not table_exists_or_empty(table):
        raise HTTPException(404, "predictions_negative_review_risk not found -- run scripts/09_score_negative_review_risk.py first.")
    where = f"WHERE risk_tier = '{tier}'" if tier else ""
    rows = run(f"SELECT * FROM `{table}` {where} ORDER BY risk_score DESC LIMIT {int(limit)}")
    summary = run(f"SELECT risk_tier, COUNT(*) AS n FROM `{table}` GROUP BY risk_tier")
    return {"summary": summary, "orders": rows}


# ---------------------------------------------------------------------------
# Section 3 — Delivery performance & review trends (actuals, daily/weekly)
# ---------------------------------------------------------------------------
@app.get("/api/delivery-performance")
def delivery_performance():
    daily = run(f"""
        SELECT DATE(order_purchase_timestamp) AS ds,
               COUNT(*) AS delivered_orders,
               SAFE_DIVIDE(COUNTIF(NOT is_delayed), COUNT(*)) AS on_time_rate,
               AVG(delivery_days) AS avg_delivery_days
        FROM `{gold()}.fct_orders`
        WHERE is_synthetic = TRUE AND order_status = 'delivered' AND is_delayed IS NOT NULL
          AND DATE(order_purchase_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        GROUP BY ds ORDER BY ds
    """)
    weekly = run(f"""
        SELECT DATE_TRUNC(DATE(order_purchase_timestamp), WEEK) AS week_start,
               COUNT(*) AS delivered_orders,
               SAFE_DIVIDE(COUNTIF(NOT is_delayed), COUNT(*)) AS on_time_rate,
               AVG(delivery_days) AS avg_delivery_days
        FROM `{gold()}.fct_orders`
        WHERE is_synthetic = TRUE AND order_status = 'delivered' AND is_delayed IS NOT NULL
        GROUP BY week_start ORDER BY week_start
    """)
    return {"daily": daily, "weekly": weekly}


@app.get("/api/reviews")
def reviews():
    daily = run(f"""
        SELECT DATE(o.order_purchase_timestamp) AS ds, AVG(r.review_score) AS avg_score,
               SAFE_DIVIDE(COUNTIF(r.review_score <= 2), COUNT(*)) AS negative_rate, COUNT(*) AS n_reviews
        FROM `{gold()}.fct_reviews` r JOIN `{gold()}.fct_orders` o USING(order_id)
        WHERE o.is_synthetic = TRUE
          AND DATE(o.order_purchase_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        GROUP BY ds ORDER BY ds
    """)
    weekly = run(f"""
        SELECT DATE_TRUNC(DATE(o.order_purchase_timestamp), WEEK) AS week_start,
               AVG(r.review_score) AS avg_score,
               SAFE_DIVIDE(COUNTIF(r.review_score <= 2), COUNT(*)) AS negative_rate, COUNT(*) AS n_reviews
        FROM `{gold()}.fct_reviews` r JOIN `{gold()}.fct_orders` o USING(order_id)
        WHERE o.is_synthetic = TRUE
        GROUP BY week_start ORDER BY week_start
    """)
    return {"daily": daily, "weekly": weekly}


# ---------------------------------------------------------------------------
# Section 4 — Customer actions (voucher / retention planning)
# ---------------------------------------------------------------------------
@app.get("/api/customers")
def customers(action: str = None, limit: int = 200):
    table = f"{preds()}.predictions_customer_actions"
    if not table_exists_or_empty(table):
        raise HTTPException(404, "predictions_customer_actions not found -- run scripts/10_daily_customer_scoring.py first.")
    where = f"WHERE suggested_action = '{action}'" if action else ""
    rows = run(f"SELECT * FROM `{table}` {where} ORDER BY predicted_incremental_value DESC LIMIT {int(limit)}")
    summary = run(f"SELECT suggested_action, COUNT(*) AS n FROM `{table}` GROUP BY suggested_action")
    return {"summary": summary, "customers": rows}


# ---------------------------------------------------------------------------
# Section 5 — Seller performance (daily/weekly)
# ---------------------------------------------------------------------------
@app.get("/api/sellers")
def sellers(segment: str = None, limit: int = 200):
    table = f"{preds()}.predictions_seller_segments"
    if not table_exists_or_empty(table):
        raise HTTPException(404, "predictions_seller_segments not found -- run scripts/11_daily_seller_scoring.py first.")
    where = f"WHERE segment = '{segment}'" if segment else ""
    rows = run(f"SELECT * FROM `{table}` {where} ORDER BY revenue_this_week DESC LIMIT {int(limit)}")
    summary = run(f"SELECT segment, COUNT(*) AS n FROM `{table}` GROUP BY segment")
    return {"summary": summary, "sellers": rows}
