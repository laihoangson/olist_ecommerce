"""
Scores delivery-delay risk for every currently OPEN order (is_synthetic =
TRUE, not yet delivered or canceled) using the Model 4 artifact from
model/train_and_export.py. Run every 6h, right after 06_live_transform.py.

Re-scores ALL open orders every run (not just newly-opened ones) --
risk changes over time even for an order untouched since last run (e.g.
its estimated delivery date getting closer with no carrier scan yet is
itself informative). Writes a full snapshot to
`{PREDICTIONS_DATASET}.predictions_delivery_risk` (WRITE_TRUNCATE).

Feature values that were "leakage-free expanding, per training row" in
model/train_and_export.py are, at scoring time, simplified to a single
CURRENT snapshot per entity (seller/category/state/route) computed from
ALL delivered orders so far (historical + live) -- there's no "future" to
leak into at inference time, so a plain aggregate is the correct
equivalent, not a simplification that loses correctness.

Usage:
    python scripts/08_score_delivery_risk.py
    python scripts/08_score_delivery_risk.py --dry-run   # print instead of writing to BQ
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

ARTIFACT_PATH = Path(__file__).resolve().parent.parent / "model" / "artifacts" / "model4_delivery_risk.joblib"

ITEMS_SUBQUERY = """
    SELECT oi.order_id, oi.seller_id, s.seller_state, s.seller_zip_code_prefix,
           pr.product_category_name_english AS main_category,
           SUM(oi.price) AS price, SUM(oi.freight_value) AS freight_value,
           COUNT(*) AS n_items, SUM(pr.product_weight_g) AS total_weight_g,
           SUM(pr.product_length_cm * pr.product_height_cm * pr.product_width_cm) AS total_volume_cm3,
           COUNT(DISTINCT oi.seller_id) AS n_sellers
    FROM `{gold}.fct_order_items` oi
    JOIN `{gold}.dim_sellers` s ON s.seller_id = oi.seller_id
    JOIN `{gold}.dim_products` pr ON pr.product_id = oi.product_id
    GROUP BY oi.order_id, oi.seller_id, s.seller_state, s.seller_zip_code_prefix, main_category
    QUALIFY ROW_NUMBER() OVER (PARTITION BY oi.order_id ORDER BY SUM(oi.price) DESC) = 1
"""

PAYMENTS_SUBQUERY = """
    SELECT order_id, payment_type, payment_installments
    FROM `{gold}.fct_payments`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY payment_value DESC) = 1
"""


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


def build_reference_stats(gold, client, k_shrink_route):
    """Current-snapshot delay-rate priors from ALL delivered orders so far
    (historical + live) -- the scoring-time equivalent of Model 4's
    leakage-free expanding features."""
    ref = bq_q(client, f"""
        SELECT
          o.order_id, o.order_purchase_timestamp, o.order_estimated_delivery_date, o.is_delayed,
          c.customer_state, i.seller_id, i.seller_state, i.main_category
        FROM `{gold}.fct_orders` o
        JOIN `{gold}.dim_customers` c ON c.customer_unique_id = o.customer_unique_id
        JOIN ({ITEMS_SUBQUERY.format(gold=gold)}) i USING(order_id)
        WHERE o.order_status = 'delivered'
    """)

    ref["is_delayed_num"] = ref["is_delayed"].astype(float)
    ref["promised_delivery_days"] = (ref["order_estimated_delivery_date"] - ref["order_purchase_timestamp"]).dt.total_seconds() / 86400.0

    global_delay_rate = float(ref["is_delayed_num"].mean())

    ref = ref.sort_values(["seller_id", "order_purchase_timestamp"])
    seller_ewma = ref.groupby("seller_id")["is_delayed_num"].transform(lambda s: s.shift(1).ewm(alpha=0.3, adjust=False).mean())
    # take each seller's LAST (most current) ewma value as their present-day rate
    ref["_seller_ewma_tmp"] = seller_ewma
    seller_current_rate = ref.groupby("seller_id")["_seller_ewma_tmp"].last().to_dict()

    category_current_rate = ref.groupby("main_category")["is_delayed_num"].mean().to_dict()
    state_current_rate = ref.groupby("customer_state")["is_delayed_num"].mean().to_dict()
    category_avg_promised = ref.groupby("main_category")["promised_delivery_days"].mean().to_dict()

    ref["state_pair"] = ref["seller_state"] + "_" + ref["customer_state"]
    route_stats = ref.groupby("state_pair")["is_delayed_num"].agg(["mean", "count"])
    route_current_rate = (
        (route_stats["mean"] * route_stats["count"] + global_delay_rate * k_shrink_route)
        / (route_stats["count"] + k_shrink_route)
    ).to_dict()

    return {
        "global_delay_rate": global_delay_rate,
        "seller_current_rate": seller_current_rate,
        "category_current_rate": category_current_rate,
        "state_current_rate": state_current_rate,
        "category_avg_promised": category_avg_promised,
        "route_current_rate": route_current_rate,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config.require_gcp_project()
    gold = f"{config.GCP_PROJECT_ID}.{config.BQ_GOLD_DATASET}"
    client = bq_writer.get_client()

    if not ARTIFACT_PATH.exists():
        print(f"[ERROR] {ARTIFACT_PATH} not found. Run model/train_and_export.py first.")
        sys.exit(1)
    artifact = joblib.load(ARTIFACT_PATH)
    pipe, num_cols, cat_cols = artifact["pipe"], artifact["num_cols"], artifact["cat_cols"]

    print("Building current-snapshot reference stats (all delivered orders so far)...")
    ref_stats = build_reference_stats(gold, client, artifact["k_shrink_route"])

    print("Querying currently open orders (is_synthetic=TRUE, not delivered/canceled)...")
    open_orders = bq_q(client, f"""
        SELECT
          o.order_id, o.order_purchase_timestamp, o.order_estimated_delivery_date,
          c.customer_state, c.customer_zip_code_prefix,
          i.seller_id, i.seller_state, i.seller_zip_code_prefix,
          i.main_category, i.price, i.freight_value, i.n_items,
          i.total_weight_g, i.total_volume_cm3, i.n_sellers,
          p.payment_type, p.payment_installments
        FROM `{gold}.fct_orders` o
        JOIN `{gold}.dim_customers` c ON c.customer_unique_id = o.customer_unique_id
        JOIN ({ITEMS_SUBQUERY.format(gold=gold)}) i USING(order_id)
        JOIN ({PAYMENTS_SUBQUERY.format(gold=gold)}) p USING(order_id)
        WHERE o.is_synthetic = TRUE AND o.order_status NOT IN ('delivered', 'canceled')
    """)

    if open_orders.empty:
        print("No open orders to score this run.")
        return

    numeric_cols = ["price", "freight_value", "total_weight_g", "total_volume_cm3", "payment_installments"]
    open_orders[numeric_cols] = open_orders[numeric_cols].astype(float)
    open_orders["customer_zip_code_prefix"] = open_orders["customer_zip_code_prefix"].astype(str).str.zfill(5)
    open_orders["seller_zip_code_prefix"] = open_orders["seller_zip_code_prefix"].astype(str).str.zfill(5)

    geo = bq_q(client, f"SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng FROM `{gold}.dim_geolocation`")
    geo[["geolocation_lat", "geolocation_lng"]] = geo[["geolocation_lat", "geolocation_lng"]].astype(float)
    geo["geolocation_zip_code_prefix"] = geo["geolocation_zip_code_prefix"].astype(str).str.zfill(5)
    geo_idx = geo.set_index("geolocation_zip_code_prefix")

    open_orders["cust_lat"] = open_orders["customer_zip_code_prefix"].map(geo_idx["geolocation_lat"])
    open_orders["cust_lng"] = open_orders["customer_zip_code_prefix"].map(geo_idx["geolocation_lng"])
    open_orders["sell_lat"] = open_orders["seller_zip_code_prefix"].map(geo_idx["geolocation_lat"])
    open_orders["sell_lng"] = open_orders["seller_zip_code_prefix"].map(geo_idx["geolocation_lng"])
    open_orders["seller_customer_distance_km"] = haversine(open_orders["cust_lat"], open_orders["cust_lng"], open_orders["sell_lat"], open_orders["sell_lng"])

    open_orders["purchase_day_of_week"] = open_orders["order_purchase_timestamp"].dt.dayofweek
    open_orders["purchase_month"] = open_orders["order_purchase_timestamp"].dt.month
    open_orders["is_holiday_season"] = open_orders["purchase_month"].isin([11, 12]).astype(int)
    open_orders["freight_ratio"] = open_orders["freight_value"] / open_orders["price"].replace(0, np.nan)
    open_orders["promised_delivery_days"] = (open_orders["order_estimated_delivery_date"] - open_orders["order_purchase_timestamp"]).dt.total_seconds() / 86400.0

    g = ref_stats["global_delay_rate"]
    open_orders["seller_ewma_delay_rate"] = open_orders["seller_id"].map(ref_stats["seller_current_rate"]).fillna(g)
    open_orders["category_prior_delay_rate"] = open_orders["main_category"].map(ref_stats["category_current_rate"]).fillna(g)
    open_orders["customer_state_prior_delay_rate"] = open_orders["customer_state"].map(ref_stats["state_current_rate"]).fillna(g)
    cat_avg_promised = open_orders["main_category"].map(ref_stats["category_avg_promised"]).fillna(open_orders["promised_delivery_days"].mean())
    open_orders["promised_vs_category_typical"] = open_orders["promised_delivery_days"] - cat_avg_promised
    open_orders["distance_x_holiday_season"] = open_orders["seller_customer_distance_km"] * open_orders["is_holiday_season"]
    open_orders["state_pair"] = open_orders["seller_state"] + "_" + open_orders["customer_state"]
    open_orders["seller_state_to_customer_state"] = open_orders["state_pair"].map(ref_stats["route_current_rate"]).fillna(g)

    X = open_orders[num_cols + cat_cols]
    risk_score = pipe.predict_proba(X)[:, 1]

    # Percentile-based tiers within this batch (see ops_dashboard_plan.md decision #3) --
    # adapts automatically to whatever the current risk distribution looks like,
    # rather than a fixed probability cutoff that may rarely fire given the ~8% base rate.
    tiers = pd.qcut(pd.Series(risk_score).rank(method="first"), [0, 0.5, 0.8, 0.95, 1.0],
                     labels=["Low", "Medium", "High", "Critical"])

    out = pd.DataFrame({
        "order_id": open_orders["order_id"],
        "seller_id": open_orders["seller_id"],
        "customer_state": open_orders["customer_state"],
        "main_category": open_orders["main_category"],
        "risk_score": risk_score,
        "risk_tier": tiers.astype(str),
        "promised_delivery_days": open_orders["promised_delivery_days"],
    })

    print(f"Scored {len(out):,} open orders. Tier distribution:")
    print(out["risk_tier"].value_counts())

    if args.dry_run:
        print(out.head(20).to_string(index=False))
        return

    predictions_writer.write_full_rescore(out, config.PREDICTIONS_DATASET, "predictions_delivery_risk")


if __name__ == "__main__":
    main()
