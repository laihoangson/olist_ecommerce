"""
Daily seller scoring: segment (Model 3, using this seller's lifetime
live-data stats) plus today/this-week operational metrics (orders,
revenue, delay rate, review score). Run once/day.

Usage:
    python scripts/11_daily_seller_scoring.py
    python scripts/11_daily_seller_scoring.py --dry-run
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

    m3 = joblib.load(ART_DIR / "model3_seller_segments.joblib")

    print("Querying live seller stats (lifetime, today, this week)...")
    seller_lifetime = bq_q(client, f"""
        SELECT
          i.seller_id,
          COUNT(DISTINCT i.order_id) AS n_orders,
          SUM(i.price) AS revenue,
          AVG(CAST(o.is_delayed AS INT64)) AS delay_rate_raw,
          AVG(rv.review_score) AS review_raw
        FROM `{gold}.fct_order_items` i
        JOIN `{gold}.fct_orders` o USING(order_id)
        LEFT JOIN `{gold}.fct_reviews` rv USING(order_id)
        WHERE o.is_synthetic = TRUE
        GROUP BY seller_id
    """)

    if seller_lifetime.empty:
        print("No live seller activity found -- nothing to score.")
        return

    seller_lifetime["delay_rate"] = (
        (seller_lifetime["delay_rate_raw"] * seller_lifetime["n_orders"] + m3["global_delay"] * m3["k_shrink"])
        / (seller_lifetime["n_orders"] + m3["k_shrink"])
    )
    seller_lifetime["review_score"] = (
        (seller_lifetime["review_raw"] * seller_lifetime["n_orders"] + m3["global_review"] * m3["k_shrink"])
        / (seller_lifetime["n_orders"] + m3["k_shrink"])
    )
    seller_lifetime["revenue_log"] = np.log1p(seller_lifetime["revenue"])
    seller_lifetime["n_orders_log"] = np.log1p(seller_lifetime["n_orders"])

    X_raw = seller_lifetime[m3["feat_cols"]].fillna(seller_lifetime[m3["feat_cols"]].median())
    Xs = m3["scaler"].transform(X_raw)
    cluster_ids = m3["kmeans"].predict(Xs)
    seller_lifetime["segment"] = [m3["cluster_labels"].get(int(c), f"cluster_{c}") for c in cluster_ids]

    print("Querying today/this-week operational metrics...")
    window_metrics = bq_q(client, f"""
        SELECT
          i.seller_id,
          COUNTIF(DATE(o.order_purchase_timestamp) = CURRENT_DATE()) AS orders_today,
          SUM(IF(DATE(o.order_purchase_timestamp) = CURRENT_DATE(), i.price, 0)) AS revenue_today,
          AVG(IF(DATE(o.order_purchase_timestamp) = CURRENT_DATE(), CAST(o.is_delayed AS INT64), NULL)) AS delay_rate_today,
          COUNTIF(DATE(o.order_purchase_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AS orders_this_week,
          SUM(IF(DATE(o.order_purchase_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY), i.price, 0)) AS revenue_this_week,
          AVG(IF(DATE(o.order_purchase_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY), CAST(o.is_delayed AS INT64), NULL)) AS delay_rate_this_week,
          AVG(IF(DATE(o.order_purchase_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY), rv.review_score, NULL)) AS review_score_this_week
        FROM `{gold}.fct_order_items` i
        JOIN `{gold}.fct_orders` o USING(order_id)
        LEFT JOIN `{gold}.fct_reviews` rv USING(order_id)
        WHERE o.is_synthetic = TRUE
        GROUP BY seller_id
    """)

    out = seller_lifetime[["seller_id", "segment", "n_orders", "revenue", "delay_rate", "review_score"]].merge(
        window_metrics, on="seller_id", how="left"
    )
    out = out.rename(columns={"n_orders": "lifetime_orders", "revenue": "lifetime_revenue",
                               "delay_rate": "lifetime_delay_rate", "review_score": "lifetime_review_score"})

    print(f"Scored {len(out):,} sellers with live activity.")
    print(out["segment"].value_counts())

    if args.dry_run:
        print(out.head(20).to_string(index=False))
        return

    predictions_writer.write_full_rescore(out, config.PREDICTIONS_DATASET, "predictions_seller_segments")


if __name__ == "__main__":
    main()
