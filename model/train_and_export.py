"""
Trains the models needed for live scoring and saves them as .joblib
artifacts under model/artifacts/. Run this ONCE (or whenever you want to
retrain on more historical data) -- NOT part of the 6h/daily/weekly live
cadence. The scoring scripts (scripts/08-12) load these artifacts; they
never train from scratch.

Uses the same feature definitions as model_v4_improved.ipynb, but with
hyperparameters already fixed (sourced from that notebook's Optuna search)
rather than re-running the search here -- re-tuning belongs in the
notebook/exploration phase, not in a script you might re-run routinely.
If you retrain after a meaningfully larger historical window, consider
re-running the Optuna search in the notebook first and updating the
constants below.

Trains on `is_synthetic = FALSE` only (the real historical data) -- same
as the notebook. Models 4/5's "prior-rate" features are recomputed fresh
at SCORING time from current data (including live is_synthetic = TRUE
orders) by scripts/08 and scripts/09 -- what's saved here is only the
fitted sklearn Pipeline (preprocessing + model), not any point-in-time
feature values, since those go stale the moment new orders arrive.

Usage:
    python model/train_and_export.py
"""

import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from replay import bq_writer  # noqa: E402
from replay.bq_helpers import q as _q  # noqa: E402

from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.cluster import KMeans
from xgboost import XGBClassifier, XGBRegressor

ARTIFACTS_DIR = Path(__file__).resolve().parent / "artifacts"
ARTIFACTS_DIR.mkdir(exist_ok=True)

GOLD = None


def q(sql):
    return _q(bq_writer.get_client(), sql)


def make_pipe(model, num_cols, cat_cols):
    prep = ColumnTransformer([
        ("num", Pipeline([("impute", SimpleImputer(strategy="median")),
                           ("scale", StandardScaler())]), num_cols),
        ("cat", Pipeline([("impute", SimpleImputer(strategy="constant", fill_value="missing")),
                           ("ohe", OneHotEncoder(handle_unknown="ignore"))]), cat_cols),
    ])
    return Pipeline([("prep", prep), ("model", model)])


def expanding_prior_rate(df, group_col, value_col, time_col="order_purchase_timestamp"):
    """Leakage-free expanding mean -- see model_v4_improved.ipynb Setup for
    the full docstring/rationale. Only used here for training features;
    scoring uses a simpler current-snapshot version (see scripts/08-10)."""
    df = df.sort_values([group_col, time_col])
    rate = df.groupby(group_col)[value_col].transform(lambda s: s.shift(1).expanding().mean())
    global_expanding = (
        df.sort_values(time_col)[value_col].shift(1).expanding().mean().reindex(df.index)
    )
    return rate.fillna(global_expanding).fillna(df[value_col].mean())


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


# ---------------------------------------------------------------------------
# Model 2 -- RFM segmentation
# ---------------------------------------------------------------------------
def train_model2():
    print("=== Model 2: Customer Segmentation (RFM) ===")
    rfm = q(f"""
        SELECT customer_unique_id, recency, monetary, recency_log, monetary_log
        FROM `{GOLD}.mart_customer_rfm` WHERE is_synthetic = FALSE
    """)
    scaler = StandardScaler().fit(rfm[["recency_log", "monetary_log"]])
    X = scaler.transform(rfm[["recency_log", "monetary_log"]])
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10).fit(X)
    rfm["cluster"] = kmeans.labels_

    # Human-readable labels from each cluster's ORIGINAL-scale centroid
    # characteristics (not the standardized ones) -- used by
    # scripts/10_daily_customer_scoring.py so cluster ids aren't shown as
    # bare integers in the dashboard/API.
    profile = rfm.groupby("cluster").agg(avg_recency=("recency", "mean"), avg_monetary=("monetary", "mean"))
    recency_mid = profile["avg_recency"].median()
    monetary_mid = profile["avg_monetary"].median()
    cluster_labels = {}
    for cid, row in profile.iterrows():
        recent = row["avg_recency"] <= recency_mid
        high_value = row["avg_monetary"] >= monetary_mid
        if recent and high_value:
            label = "Recent & High-Value"
        elif recent and not high_value:
            label = "Recent & Modest-Spend"
        elif not recent and high_value:
            label = "Lapsed & High-Value"
        else:
            label = "Lapsed & Low-Value"
        cluster_labels[int(cid)] = label
    print(f"  Cluster labels (by original-scale centroid): {cluster_labels}")

    joblib.dump({"scaler": scaler, "kmeans": kmeans, "cluster_labels": cluster_labels},
                ARTIFACTS_DIR / "model2_rfm_segments.joblib")
    print(f"  Saved model2_rfm_segments.joblib ({len(rfm):,} customers trained on)")


# ---------------------------------------------------------------------------
# Model 3 -- Seller segmentation
# ---------------------------------------------------------------------------
def train_model3():
    print("=== Model 3: Seller Segmentation ===")
    seller = q(f"""
        SELECT
          i.seller_id, COUNT(DISTINCT i.order_id) AS n_orders, SUM(i.price) AS revenue,
          AVG(CAST(o.is_delayed AS INT64)) AS delay_rate_raw, AVG(rv.review_score) AS review_raw
        FROM `{GOLD}.fct_order_items` i
        JOIN `{GOLD}.fct_orders` o USING(order_id)
        LEFT JOIN `{GOLD}.fct_reviews` rv USING(order_id)
        WHERE o.is_synthetic = FALSE
        GROUP BY seller_id
    """)
    global_delay = (seller["delay_rate_raw"] * seller["n_orders"]).sum() / seller["n_orders"].sum()
    global_review = (seller["review_raw"] * seller["n_orders"]).sum() / seller["n_orders"].sum()
    K_SHRINK = 10
    seller["delay_rate"] = (seller["delay_rate_raw"] * seller["n_orders"] + global_delay * K_SHRINK) / (seller["n_orders"] + K_SHRINK)
    seller["review_score"] = (seller["review_raw"] * seller["n_orders"] + global_review * K_SHRINK) / (seller["n_orders"] + K_SHRINK)
    seller["revenue_log"] = np.log1p(seller["revenue"])
    seller["n_orders_log"] = np.log1p(seller["n_orders"])

    feat_cols = ["revenue_log", "n_orders_log", "delay_rate", "review_score"]
    X_raw = seller[feat_cols].fillna(seller[feat_cols].median())
    scaler = StandardScaler().fit(X_raw)
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10).fit(scaler.transform(X_raw))
    seller["cluster"] = kmeans.labels_

    # Human-readable labels from each cluster's original-scale centroid
    profile = seller.groupby("cluster").agg(
        n_orders=("n_orders", "mean"), revenue=("revenue", "mean"),
        delay_rate=("delay_rate", "mean"), review_score=("review_score", "mean"))
    revenue_mid = profile["revenue"].median()
    cluster_labels = {}
    for cid, row in profile.iterrows():
        if row["review_score"] < 3.9 or row["delay_rate"] > 0.12:
            label = "At-Risk Seller"
        elif row["revenue"] >= revenue_mid:
            label = "Top Seller"
        else:
            label = "Small/Low-Volume Seller"
        cluster_labels[int(cid)] = label
    print(f"  Cluster labels (by original-scale centroid): {cluster_labels}")

    joblib.dump({
        "scaler": scaler, "kmeans": kmeans, "feat_cols": feat_cols, "cluster_labels": cluster_labels,
        "global_delay": global_delay, "global_review": global_review, "k_shrink": K_SHRINK,
    }, ARTIFACTS_DIR / "model3_seller_segments.joblib")
    print(f"  Saved model3_seller_segments.joblib ({len(seller):,} sellers trained on)")


# ---------------------------------------------------------------------------
# Model 4 -- Delivery delay prediction
# ---------------------------------------------------------------------------
def train_model4():
    print("=== Model 4: Delivery Delay Prediction ===")
    delivery = q(f"""
        SELECT
          o.order_id, o.order_purchase_timestamp, o.order_estimated_delivery_date, o.is_delayed,
          c.customer_state, c.customer_zip_code_prefix,
          i.seller_id, i.seller_state, i.seller_zip_code_prefix,
          i.main_category, i.price, i.freight_value, i.n_items,
          i.total_weight_g, i.total_volume_cm3, i.n_sellers,
          p.payment_type, p.payment_installments
        FROM `{GOLD}.fct_orders` o
        JOIN `{GOLD}.dim_customers` c ON c.customer_unique_id = o.customer_unique_id
        JOIN (
          SELECT oi.order_id, oi.seller_id, s.seller_state, s.seller_zip_code_prefix,
                 pr.product_category_name_english AS main_category,
                 SUM(oi.price) AS price, SUM(oi.freight_value) AS freight_value,
                 COUNT(*) AS n_items, SUM(pr.product_weight_g) AS total_weight_g,
                 SUM(pr.product_length_cm * pr.product_height_cm * pr.product_width_cm) AS total_volume_cm3,
                 COUNT(DISTINCT oi.seller_id) AS n_sellers
          FROM `{GOLD}.fct_order_items` oi
          JOIN `{GOLD}.dim_sellers` s ON s.seller_id = oi.seller_id
          JOIN `{GOLD}.dim_products` pr ON pr.product_id = oi.product_id
          GROUP BY oi.order_id, oi.seller_id, s.seller_state, s.seller_zip_code_prefix, main_category
          QUALIFY ROW_NUMBER() OVER (PARTITION BY oi.order_id ORDER BY SUM(oi.price) DESC) = 1
        ) i USING(order_id)
        JOIN (
          SELECT order_id, payment_type, payment_installments
          FROM `{GOLD}.fct_payments`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY payment_value DESC) = 1
        ) p USING(order_id)
        WHERE o.is_synthetic = FALSE AND o.order_status = 'delivered'
    """)

    numeric_cols = ["price", "freight_value", "total_weight_g", "total_volume_cm3", "payment_installments"]
    delivery[numeric_cols] = delivery[numeric_cols].astype(float)
    delivery["customer_zip_code_prefix"] = delivery["customer_zip_code_prefix"].astype(str).str.zfill(5)
    delivery["seller_zip_code_prefix"] = delivery["seller_zip_code_prefix"].astype(str).str.zfill(5)

    geo = q(f"SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng FROM `{GOLD}.dim_geolocation`")
    geo[["geolocation_lat", "geolocation_lng"]] = geo[["geolocation_lat", "geolocation_lng"]].astype(float)
    geo["geolocation_zip_code_prefix"] = geo["geolocation_zip_code_prefix"].astype(str).str.zfill(5)
    geo_idx = geo.set_index("geolocation_zip_code_prefix")

    delivery["cust_lat"] = delivery["customer_zip_code_prefix"].map(geo_idx["geolocation_lat"])
    delivery["cust_lng"] = delivery["customer_zip_code_prefix"].map(geo_idx["geolocation_lng"])
    delivery["sell_lat"] = delivery["seller_zip_code_prefix"].map(geo_idx["geolocation_lat"])
    delivery["sell_lng"] = delivery["seller_zip_code_prefix"].map(geo_idx["geolocation_lng"])
    delivery["seller_customer_distance_km"] = haversine(delivery["cust_lat"], delivery["cust_lng"], delivery["sell_lat"], delivery["sell_lng"])

    delivery["purchase_day_of_week"] = delivery["order_purchase_timestamp"].dt.dayofweek
    delivery["purchase_month"] = delivery["order_purchase_timestamp"].dt.month
    delivery["is_holiday_season"] = delivery["purchase_month"].isin([11, 12]).astype(int)
    delivery["freight_ratio"] = delivery["freight_value"] / delivery["price"].replace(0, np.nan)
    delivery["promised_delivery_days"] = (delivery["order_estimated_delivery_date"] - delivery["order_purchase_timestamp"]).dt.total_seconds() / 86400.0
    delivery["is_delayed_num"] = delivery["is_delayed"].astype(float)
    delivery = delivery.dropna(subset=["is_delayed_num"]).reset_index(drop=True)

    delivery = delivery.sort_values(["seller_id", "order_purchase_timestamp"])
    delivery["seller_ewma_delay_rate"] = delivery.groupby("seller_id")["is_delayed_num"].transform(lambda s: s.shift(1).ewm(alpha=0.3, adjust=False).mean())
    delivery["seller_ewma_delay_rate"] = delivery["seller_ewma_delay_rate"].fillna(delivery["is_delayed_num"].mean())

    delivery["category_prior_delay_rate"] = expanding_prior_rate(delivery, "main_category", "is_delayed_num")
    delivery["customer_state_prior_delay_rate"] = expanding_prior_rate(delivery, "customer_state", "is_delayed_num")

    delivery = delivery.sort_values(["main_category", "order_purchase_timestamp"])
    delivery["category_avg_promised_prior"] = delivery.groupby("main_category")["promised_delivery_days"].transform(lambda s: s.shift(1).expanding().mean())
    delivery["category_avg_promised_prior"] = delivery["category_avg_promised_prior"].fillna(delivery["promised_delivery_days"].mean())
    delivery["promised_vs_category_typical"] = delivery["promised_delivery_days"] - delivery["category_avg_promised_prior"]
    delivery["distance_x_holiday_season"] = delivery["seller_customer_distance_km"] * delivery["is_holiday_season"]

    delivery["state_pair"] = delivery["seller_state"] + "_" + delivery["customer_state"]
    delivery = delivery.sort_values("order_purchase_timestamp")
    delivery["_global_prior_mean"] = delivery["is_delayed_num"].shift(1).expanding().mean().fillna(delivery["is_delayed_num"].mean())
    delivery = delivery.sort_values(["state_pair", "order_purchase_timestamp"])
    _grp = delivery.groupby("state_pair")["is_delayed_num"]
    _count_prior = _grp.transform(lambda s: s.shift(1).expanding().count()).fillna(0)
    _mean_prior = _grp.transform(lambda s: s.shift(1).expanding().mean()).fillna(delivery["_global_prior_mean"])
    K_SHRINK_ROUTE = 15
    delivery["seller_state_to_customer_state"] = (_mean_prior * _count_prior + delivery["_global_prior_mean"] * K_SHRINK_ROUTE) / (_count_prior + K_SHRINK_ROUTE)
    delivery = delivery.sort_values("order_purchase_timestamp")

    num_cols = ["price", "freight_value", "n_items", "payment_installments",
                "purchase_day_of_week", "purchase_month", "promised_delivery_days",
                "seller_customer_distance_km", "total_weight_g", "total_volume_cm3",
                "n_sellers", "is_holiday_season", "freight_ratio",
                "seller_ewma_delay_rate", "category_prior_delay_rate",
                "customer_state_prior_delay_rate", "promised_vs_category_typical",
                "distance_x_holiday_season", "seller_state_to_customer_state"]
    cat_cols = ["main_category", "payment_type", "seller_state"]
    y = delivery["is_delayed_num"]
    X = delivery[num_cols + cat_cols]
    n_pos, n_neg = y.sum(), len(y) - y.sum()

    # Hyperparameters sourced from model_v4_improved.ipynb's Optuna search.
    xgb_model = XGBClassifier(
        n_estimators=700, max_depth=7, learning_rate=0.0172, subsample=0.9239,
        colsample_bytree=0.6167, min_child_weight=8, gamma=0.356,
        scale_pos_weight=n_neg / n_pos, eval_metric="aucpr", random_state=42,
    )
    pipe = make_pipe(xgb_model, num_cols, cat_cols).fit(X, y)

    # Global fallback priors -- used by scripts/08 for brand-new sellers/categories/
    # states/routes with no history yet at scoring time.
    global_delay_rate = float(y.mean())

    joblib.dump({
        "pipe": pipe, "num_cols": num_cols, "cat_cols": cat_cols,
        "global_delay_rate": global_delay_rate, "k_shrink_route": K_SHRINK_ROUTE,
    }, ARTIFACTS_DIR / "model4_delivery_risk.joblib")
    print(f"  Saved model4_delivery_risk.joblib ({len(delivery):,} delivered orders trained on)")


# ---------------------------------------------------------------------------
# Model 5 -- Negative review risk
# ---------------------------------------------------------------------------
def train_model5():
    print("=== Model 5: Negative Review Risk ===")
    review = q(f"""
        SELECT
          o.order_id, o.order_purchase_timestamp, o.order_estimated_delivery_date,
          o.order_delivered_customer_date, o.delivery_days, o.is_delayed,
          i.seller_id, i.main_category, i.price, i.freight_value, i.n_items,
          i.total_weight_g, i.total_volume_cm3, i.n_sellers,
          p.payment_type, rv.review_score
        FROM `{GOLD}.fct_orders` o
        JOIN (
          SELECT oi.order_id, oi.seller_id,
                 pr.product_category_name_english AS main_category,
                 SUM(oi.price) AS price, SUM(oi.freight_value) AS freight_value,
                 COUNT(*) AS n_items, SUM(pr.product_weight_g) AS total_weight_g,
                 SUM(pr.product_length_cm * pr.product_height_cm * pr.product_width_cm) AS total_volume_cm3,
                 COUNT(DISTINCT oi.seller_id) AS n_sellers
          FROM `{GOLD}.fct_order_items` oi
          JOIN `{GOLD}.dim_products` pr ON pr.product_id = oi.product_id
          GROUP BY oi.order_id, oi.seller_id, main_category
          QUALIFY ROW_NUMBER() OVER (PARTITION BY oi.order_id ORDER BY SUM(oi.price) DESC) = 1
        ) i USING(order_id)
        JOIN (
          SELECT order_id, payment_type FROM `{GOLD}.fct_payments`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY payment_value DESC) = 1
        ) p USING(order_id)
        JOIN `{GOLD}.fct_reviews` rv USING(order_id)
        WHERE o.is_synthetic = FALSE
    """)
    numeric_cols = ["price", "freight_value", "total_weight_g", "total_volume_cm3", "review_score"]
    review[numeric_cols] = review[numeric_cols].astype(float)
    review["negative_review"] = (review["review_score"] <= 2).astype(int)
    review["is_delayed_num"] = review["is_delayed"].astype(float)
    review["delivery_days"] = review["delivery_days"].astype(float).fillna(review["delivery_days"].astype(float).median())
    review["promised_delivery_days"] = (review["order_estimated_delivery_date"] - review["order_purchase_timestamp"]).dt.total_seconds() / 86400.0
    review["delay_magnitude_days"] = (review["order_delivered_customer_date"] - review["order_estimated_delivery_date"]).dt.total_seconds() / 86400.0
    review["delay_magnitude_days"] = review["delay_magnitude_days"].clip(lower=0).fillna(0)
    review = review.dropna(subset=["negative_review"]).reset_index(drop=True)

    review = review.sort_values(["seller_id", "order_purchase_timestamp"])
    review["seller_ewma_delay_rate"] = review.groupby("seller_id")["is_delayed_num"].transform(lambda s: s.shift(1).ewm(alpha=0.3, adjust=False).mean())
    review["seller_ewma_delay_rate"] = review["seller_ewma_delay_rate"].fillna(review["is_delayed_num"].mean())
    review = review.sort_values("order_purchase_timestamp")
    review["is_delayed_x_price"] = review["is_delayed_num"] * review["price"]

    num_cols = ["price", "freight_value", "n_items", "delivery_days", "promised_delivery_days",
                "is_delayed_num", "delay_magnitude_days", "seller_ewma_delay_rate", "n_sellers",
                "total_weight_g", "total_volume_cm3", "is_delayed_x_price"]
    cat_cols = ["main_category", "payment_type"]
    y = review["negative_review"]
    X = review[num_cols + cat_cols]
    n_pos, n_neg = y.sum(), len(y) - y.sum()

    xgb_model = XGBClassifier(
        n_estimators=450, max_depth=4, learning_rate=0.0380, subsample=0.9447,
        colsample_bytree=0.8400, min_child_weight=3,
        scale_pos_weight=n_neg / n_pos, eval_metric="aucpr", random_state=42,
    )
    pipe = make_pipe(xgb_model, num_cols, cat_cols).fit(X, y)

    joblib.dump({"pipe": pipe, "num_cols": num_cols, "cat_cols": cat_cols},
                ARTIFACTS_DIR / "model5_negative_review_risk.joblib")
    print(f"  Saved model5_negative_review_risk.joblib ({len(review):,} reviewed orders trained on)")


# ---------------------------------------------------------------------------
# Model 6 -- Incremental customer value
# ---------------------------------------------------------------------------
def train_model6():
    print("=== Model 6: Incremental Customer Value ===")
    hvc = q(f"""
        SELECT
          c.customer_unique_id, c.customer_state,
          fo.first_order_price, fo.first_order_freight, fo.first_order_n_items,
          fo.first_order_n_sellers, fo.first_order_weight, fo.first_order_main_category,
          fo.payment_type, fo.payment_installments, t.total_clv
        FROM `{GOLD}.dim_customers` c
        JOIN (
          SELECT
            o.customer_unique_id, oi.order_id AS first_order_id,
            SUM(oi.price) AS first_order_price, SUM(oi.freight_value) AS first_order_freight,
            COUNT(*) AS first_order_n_items, COUNT(DISTINCT oi.seller_id) AS first_order_n_sellers,
            SUM(pr.product_weight_g) AS first_order_weight,
            ARRAY_AGG(pr.product_category_name_english ORDER BY oi.price DESC LIMIT 1)[OFFSET(0)] AS first_order_main_category,
            ANY_VALUE(p.payment_type) AS payment_type, ANY_VALUE(p.payment_installments) AS payment_installments
          FROM `{GOLD}.fct_orders` o
          JOIN (
            SELECT customer_unique_id, MIN(order_purchase_timestamp) AS first_ts
            FROM `{GOLD}.fct_orders` WHERE is_synthetic = FALSE GROUP BY customer_unique_id
          ) fts ON fts.customer_unique_id = o.customer_unique_id AND fts.first_ts = o.order_purchase_timestamp
          JOIN `{GOLD}.fct_order_items` oi ON oi.order_id = o.order_id
          JOIN `{GOLD}.dim_products` pr ON pr.product_id = oi.product_id
          LEFT JOIN `{GOLD}.fct_payments` p ON p.order_id = o.order_id
          WHERE o.is_synthetic = FALSE
          GROUP BY o.customer_unique_id, first_order_id
        ) fo ON fo.customer_unique_id = c.customer_unique_id
        JOIN (
          SELECT o.customer_unique_id, SUM(oi.price) + SUM(oi.freight_value) AS total_clv
          FROM `{GOLD}.fct_orders` o JOIN `{GOLD}.fct_order_items` oi ON oi.order_id = o.order_id
          WHERE o.is_synthetic = FALSE GROUP BY o.customer_unique_id
        ) t ON t.customer_unique_id = c.customer_unique_id
        WHERE c.is_synthetic = FALSE
    """)
    hvc = hvc.dropna(subset=["total_clv", "first_order_price"]).reset_index(drop=True)
    # BUG FIX: total_clv = SUM(price)+SUM(freight_value) across ALL orders, but the
    # old line below only subtracted first_order_price (not first_order_freight) --
    # so for a one-time customer, incremental_clv worked out to just their first
    # order's OWN freight, which is > 0 for almost everyone regardless of whether
    # they ever repeat-purchased. That's exactly the kind of near-tautology this
    # reframe (see model_v4_improved.ipynb changelog) was supposed to escape.
    hvc["incremental_clv"] = (hvc["total_clv"] - hvc["first_order_price"] - hvc["first_order_freight"]).clip(lower=0)
    hvc["log_incremental_clv"] = np.log1p(hvc["incremental_clv"])
    hvc["first_order_weight"] = hvc["first_order_weight"].fillna(hvc["first_order_weight"].median())
    hvc["payment_installments"] = hvc["payment_installments"].fillna(1)

    num_cols_6 = ["first_order_price", "first_order_freight", "first_order_n_items",
                  "first_order_n_sellers", "first_order_weight", "payment_installments"]
    cat_cols_6 = ["first_order_main_category", "payment_type", "customer_state"]
    reg = XGBRegressor(n_estimators=300, max_depth=5, learning_rate=0.05, random_state=42)
    pipe6 = make_pipe(reg, num_cols_6, cat_cols_6).fit(hvc[num_cols_6 + cat_cols_6], hvc["log_incremental_clv"])
    joblib.dump({"pipe": pipe6, "num_cols": num_cols_6, "cat_cols": cat_cols_6},
                ARTIFACTS_DIR / "model6_incremental_value.joblib")
    print(f"  Saved model6_incremental_value.joblib ({len(hvc):,} customers trained on)")


def main():
    global GOLD
    config.require_gcp_project()
    GOLD = f"{config.GCP_PROJECT_ID}.{config.BQ_GOLD_DATASET}"

    train_model2()
    train_model3()
    train_model4()
    train_model5()
    train_model6()

    print(f"\nAll artifacts saved to {ARTIFACTS_DIR}/")
    print("Re-run this script whenever you want to retrain on a larger historical window.")


if __name__ == "__main__":
    main()
