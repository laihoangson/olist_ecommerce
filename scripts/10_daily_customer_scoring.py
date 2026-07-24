"""
Daily customer scoring: segment (Model 2) + predicted incremental value
(Model 6), and a rule-based `suggested_action` for retention/voucher
planning. Run once/day (not part of the 6h cadence -- see
ops_dashboard_plan.md).

Scores live customers (is_synthetic = TRUE). `suggested_action` uses a
PERCENTILE-based rule (adapts to whatever the current distribution looks
like) rather than fixed dollar thresholds -- see ops_dashboard_plan.md
decision #3. Adjust the percentile cutoffs below as you see real
distributions; they're intentionally simple defaults, not tuned.

Usage:
    python scripts/10_daily_customer_scoring.py
    python scripts/10_daily_customer_scoring.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from replay import bq_writer, predictions_writer  # noqa: E402
from replay.bq_helpers import q as bq_q  # noqa: E402

ART_DIR = Path(__file__).resolve().parent.parent / "model" / "artifacts"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config.require_gcp_project()
    gold = f"{config.GCP_PROJECT_ID}.{config.BQ_GOLD_DATASET}"
    client = bq_writer.get_client()

    m2 = joblib.load(ART_DIR / "model2_rfm_segments.joblib")
    m6 = joblib.load(ART_DIR / "model6_incremental_value.joblib")

    print("Querying live customers (is_synthetic=TRUE) with recency/monetary + first-order features...")
    customers = bq_q(client, f"""
        SELECT
          c.customer_unique_id, c.customer_state,
          DATE_DIFF(CURRENT_DATE(), DATE(MAX(o.order_purchase_timestamp)), DAY) AS recency_days,
          SUM(oi.price) + SUM(oi.freight_value) AS monetary
        FROM `{gold}.dim_customers` c
        JOIN `{gold}.fct_orders` o ON o.customer_unique_id = c.customer_unique_id
        JOIN `{gold}.fct_order_items` oi ON oi.order_id = o.order_id
        WHERE c.is_synthetic = TRUE
        GROUP BY c.customer_unique_id, c.customer_state
    """)

    if customers.empty:
        print("No live customers found -- nothing to score.")
        return

    first_order_items = bq_q(client, f"""
        SELECT
          o.customer_unique_id,
          SUM(oi.price) AS first_order_price, SUM(oi.freight_value) AS first_order_freight,
          COUNT(*) AS first_order_n_items, COUNT(DISTINCT oi.seller_id) AS first_order_n_sellers,
          SUM(pr.product_weight_g) AS first_order_weight,
          ARRAY_AGG(pr.product_category_name_english ORDER BY oi.price DESC LIMIT 1)[OFFSET(0)] AS first_order_main_category
        FROM `{gold}.fct_orders` o
        JOIN (
          SELECT customer_unique_id, MIN(order_purchase_timestamp) AS first_ts
          FROM `{gold}.fct_orders` WHERE is_synthetic = TRUE GROUP BY customer_unique_id
        ) fts ON fts.customer_unique_id = o.customer_unique_id AND fts.first_ts = o.order_purchase_timestamp
        JOIN `{gold}.fct_order_items` oi ON oi.order_id = o.order_id
        JOIN `{gold}.dim_products` pr ON pr.product_id = oi.product_id
        WHERE o.is_synthetic = TRUE
        GROUP BY o.customer_unique_id
    """)

    payments = bq_q(client, f"""
        SELECT o.customer_unique_id, ANY_VALUE(p.payment_type) AS payment_type,
               ANY_VALUE(p.payment_installments) AS payment_installments
        FROM `{gold}.fct_orders` o
        JOIN (
          SELECT customer_unique_id, MIN(order_purchase_timestamp) AS first_ts
          FROM `{gold}.fct_orders` WHERE is_synthetic = TRUE GROUP BY customer_unique_id
        ) fts ON fts.customer_unique_id = o.customer_unique_id AND fts.first_ts = o.order_purchase_timestamp
        JOIN `{gold}.fct_payments` p ON p.order_id = o.order_id
        WHERE o.is_synthetic = TRUE
        GROUP BY o.customer_unique_id
    """)

    customers = customers.merge(first_order_items, on="customer_unique_id", how="left").merge(payments, on="customer_unique_id", how="left")
    customers["first_order_weight"] = customers["first_order_weight"].fillna(customers["first_order_weight"].median())
    customers["payment_installments"] = customers["payment_installments"].fillna(1)
    customers["recency_log"] = np.log1p(customers["recency_days"].clip(lower=0))
    customers["monetary_log"] = np.log1p(customers["monetary"].clip(lower=0))

    # --- Model 2: segment ---
    Xs = m2["scaler"].transform(customers[["recency_log", "monetary_log"]])
    cluster_ids = m2["kmeans"].predict(Xs)
    customers["segment"] = [m2["cluster_labels"].get(int(c), f"cluster_{c}") for c in cluster_ids]

    # --- Model 6: predicted incremental value ---
    X6 = customers[m6["num_cols"] + m6["cat_cols"]]
    pred_log_incremental = m6["pipe"].predict(X6)
    customers["predicted_incremental_value"] = np.expm1(pred_log_incremental).clip(min=0)

    # --- suggested_action: percentile-based rule, using segment (Model 2) +
    # predicted incremental value (Model 6) only -- no repeat-purchase
    # probability model in this version (Model 6b was dropped). ---
    value_p66 = customers["predicted_incremental_value"].quantile(0.66)
    value_p33 = customers["predicted_incremental_value"].quantile(0.33)
    lapsed_segments = {"Lapsed & High-Value", "Lapsed & Low-Value"}

    def suggest(row):
        if row["segment"] in lapsed_segments and row["predicted_incremental_value"] >= value_p66:
            return "Retention voucher (lapsed, high predicted future value)"
        if row["segment"] == "Lapsed & High-Value":
            return "Win-back offer (lapsed, historically high value)"
        if row["segment"] in lapsed_segments and row["predicted_incremental_value"] <= value_p33:
            return "Low-cost re-engagement email (lapsed, low predicted value)"
        return "No action / monitor"

    customers["suggested_action"] = customers.apply(suggest, axis=1)

    out = customers[["customer_unique_id", "customer_state", "segment",
                      "predicted_incremental_value", "suggested_action"]]

    print(f"Scored {len(out):,} live customers.")
    print(out["suggested_action"].value_counts())

    if args.dry_run:
        print(out.head(20).to_string(index=False))
        return

    predictions_writer.write_full_rescore(out, config.PREDICTIONS_DATASET, "predictions_customer_actions")


if __name__ == "__main__":
    main()
