"""
One-time copy of the real Olist data into `olist_bronze`, tagged
is_synthetic=false. This is straight passthrough — no replay logic — because
historical data is used as-is per the project plan (Power BI + ML training
source).

Products, sellers, and geolocation are catalog/reference data: synthetic
orders reuse these SAME product_id/seller_id values (no new synthetic
products or sellers are invented), so those three tables only ever have the
is_synthetic=false historical copy. This is a deliberate simplification —
documented in README.md — since generating a fake product catalog adds
complexity without adding realism.

Usage:
    python scripts/03_load_historical_to_bronze.py            # writes to BigQuery
    python scripts/03_load_historical_to_bronze.py --dry-run  # local only, no BigQuery
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402


def load_csv(table_name: str) -> pd.DataFrame:
    path = config.DATA_DIR / config.RAW_FILES[table_name]
    if not path.exists():
        print(f"[ERROR] {path} not found. Copy the 9 Olist CSVs into data/.")
        sys.exit(1)
    parse_cols = config.TIMESTAMP_COLUMNS.get(table_name)
    return pd.read_csv(path, low_memory=False, parse_dates=parse_cols)


TABLE_PLAN = [
    # (source csv key, bronze table name, key columns for MERGE)
    ("olist_customers_dataset", "bronze_customers", ["customer_id"]),
    ("olist_orders_dataset", "bronze_orders", ["order_id"]),
    ("olist_order_items_dataset", "bronze_order_items", ["order_id", "order_item_id"]),
    ("olist_order_payments_dataset", "bronze_order_payments", ["order_id", "payment_sequential"]),
    ("olist_order_reviews_dataset", "bronze_order_reviews", ["review_id"]),
    ("olist_products_dataset", "bronze_products", ["product_id"]),
    ("olist_sellers_dataset", "bronze_sellers", ["seller_id"]),
    ("olist_geolocation_dataset", "bronze_geolocation", ["geolocation_zip_code_prefix"]),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                         help="Validate + tag locally, skip BigQuery entirely.")
    args = parser.parse_args()

    if not args.dry_run:
        from replay import bq_writer

    for source_key, bronze_table, keys in TABLE_PLAN:
        print(f"Loading {source_key} ...")
        df = load_csv(source_key)

        # geolocation has no natural single-row key; dedup on zip prefix
        # (keep first) so it merges cleanly.
        if bronze_table == "bronze_geolocation":
            df = df.drop_duplicates(subset=["geolocation_zip_code_prefix"], keep="first")

        df["is_synthetic"] = False

        if args.dry_run:
            out_path = config.PROJECT_ROOT / "outputs_dry_run"
            out_path.mkdir(exist_ok=True)
            df.to_csv(out_path / f"{bronze_table}.csv", index=False)
            print(f"  [dry-run] {len(df):,} rows -> outputs_dry_run/{bronze_table}.csv")
        else:
            # bq_writer has no upsert/MERGE (BQ sandbox rejects DML) — it only
            # does idempotent load jobs keyed by batch_id. Historical load is
            # one-time and one batch per table, so batch_id = table name is
            # stable across re-runs and lets write_table's own
            # already-loaded-skip logic handle retries.
            bq_writer.write_table(
                df,
                config.BQ_BRONZE_DATASET,
                bronze_table,
                batch_id=f"historical_{bronze_table}",
            )

    print("\nHistorical -> bronze load complete.")


if __name__ == "__main__":
    main()