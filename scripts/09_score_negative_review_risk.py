"""
Scores negative-review risk for delivered orders that don't have a review
yet, using the Model 5 artifact. Run every 6h, right after
06_live_transform.py (and typically after scripts/08).

Unlike Model 4, this order's OWN delivery outcome (is_delayed,
delivery_days, delay_magnitude_days) is already known once it's delivered
-- no leakage concern computing those directly from the order itself. Only
`seller_ewma_delay_rate` needs a cross-order snapshot (that seller's
current track record), computed the same way as scripts/08.

Usage:
    python scripts/09_score_negative_review_risk.py
    python scripts/09_score_negative_review_risk.py --dry-run
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

ARTIFACT_PATH = Path(__file__).resolve().parent.parent / "model" / "artifacts" / "model5_negative_review_risk.joblib"

ITEMS_SUBQUERY = """
    SELECT oi.order_id, oi.seller_id,
           pr.product_category_name_english AS main_category,
           SUM(oi.price) AS price, SUM(oi.freight_value) AS freight_value,
           COUNT(*) AS n_items, SUM(pr.product_weight_g) AS total_weight_g,
           SUM(pr.product_length_cm * pr.product_height_cm * pr.product_width_cm) AS total_volume_cm3,
           COUNT(DISTINCT oi.seller_id) AS n_sellers
    FROM `{gold}.fct_order_items` oi
    JOIN `{gold}.dim_products` pr ON pr.product_id = oi.product_id
    GROUP BY oi.order_id, oi.seller_id, main_category
    QUALIFY ROW_NUMBER() OVER (PARTITION BY oi.order_id ORDER BY SUM(oi.price) DESC) = 1
"""

PAYMENTS_SUBQUERY = """
    SELECT order_id, payment_type
    FROM `{gold}.fct_payments`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY payment_value DESC) = 1
"""


def build_seller_current_ewma(gold, client):
    ref = bq_q(client, f"""
        SELECT o.order_id, o.order_purchase_timestamp, o.is_delayed, i.seller_id
        FROM `{gold}.fct_orders` o
        JOIN ({ITEMS_SUBQUERY.format(gold=gold)}) i USING(order_id)
        WHERE o.order_status = 'delivered'
    """)
    ref["is_delayed_num"] = ref["is_delayed"].astype(float)
    global_rate = float(ref["is_delayed_num"].mean())
    ref = ref.sort_values(["seller_id", "order_purchase_timestamp"])
    ref["_ewma"] = ref.groupby("seller_id")["is_delayed_num"].transform(lambda s: s.shift(1).ewm(alpha=0.3, adjust=False).mean())
    seller_current_rate = ref.groupby("seller_id")["_ewma"].last().to_dict()
    return seller_current_rate, global_rate


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

    print("Building seller current-EWMA delay rate (all delivered orders so far)...")
    seller_current_rate, global_rate = build_seller_current_ewma(gold, client)

    print("Querying delivered orders awaiting review (is_synthetic=TRUE)...")
    pending = bq_q(client, f"""
        SELECT
          o.order_id, o.order_purchase_timestamp, o.order_estimated_delivery_date,
          o.order_delivered_customer_date, o.delivery_days, o.is_delayed,
          i.seller_id, i.main_category, i.price, i.freight_value, i.n_items,
          i.total_weight_g, i.total_volume_cm3, i.n_sellers, p.payment_type
        FROM `{gold}.fct_orders` o
        JOIN ({ITEMS_SUBQUERY.format(gold=gold)}) i USING(order_id)
        JOIN ({PAYMENTS_SUBQUERY.format(gold=gold)}) p USING(order_id)
        LEFT JOIN `{gold}.fct_reviews` rv USING(order_id)
        WHERE o.is_synthetic = TRUE AND o.order_status = 'delivered' AND rv.order_id IS NULL
    """)

    if pending.empty:
        print("No delivered-awaiting-review orders to score this run.")
        return

    numeric_cols = ["price", "freight_value", "total_weight_g", "total_volume_cm3"]
    pending[numeric_cols] = pending[numeric_cols].astype(float)
    pending["is_delayed_num"] = pending["is_delayed"].astype(float)
    pending["delivery_days"] = pending["delivery_days"].astype(float).fillna(pending["delivery_days"].astype(float).median())
    pending["promised_delivery_days"] = (pending["order_estimated_delivery_date"] - pending["order_purchase_timestamp"]).dt.total_seconds() / 86400.0
    pending["delay_magnitude_days"] = (pending["order_delivered_customer_date"] - pending["order_estimated_delivery_date"]).dt.total_seconds() / 86400.0
    pending["delay_magnitude_days"] = pending["delay_magnitude_days"].clip(lower=0).fillna(0)
    pending["seller_ewma_delay_rate"] = pending["seller_id"].map(seller_current_rate).fillna(global_rate)
    pending["is_delayed_x_price"] = pending["is_delayed_num"] * pending["price"]

    X = pending[num_cols + cat_cols]
    risk_score = pipe.predict_proba(X)[:, 1]

    tiers = pd.qcut(pd.Series(risk_score).rank(method="first"), [0, 0.5, 0.8, 0.95, 1.0],
                     labels=["Low", "Medium", "High", "Critical"])

    out = pd.DataFrame({
        "order_id": pending["order_id"],
        "seller_id": pending["seller_id"],
        "main_category": pending["main_category"],
        "risk_score": risk_score,
        "risk_tier": tiers.astype(str),
        "was_delayed": pending["is_delayed"],
    })

    print(f"Scored {len(out):,} delivered-awaiting-review orders. Tier distribution:")
    print(out["risk_tier"].value_counts())

    if args.dry_run:
        print(out.head(20).to_string(index=False))
        return

    predictions_writer.write_full_rescore(out, config.PREDICTIONS_DATASET, "predictions_negative_review_risk")


if __name__ == "__main__":
    main()
