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

FREE-TIER / NO-BILLING SAFE: writes go through bq_writer.write_table(), which
only uses load jobs + SELECT checks (no MERGE/DML, no streaming insert) — see
bq_writer.py's docstring. All rows from this one-time load are tagged with
batch_id="historical-load", so a re-run is automatically skipped per table
instead of duplicating rows (same skip-if-already-loaded mechanism the
backfill/live-ingest scripts use).

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
    # (source csv key, bronze table name)
    ("olist_customers_dataset", "bronze_customers"),
    ("olist_orders_dataset", "bronze_orders"),
    ("olist_order_items_dataset", "bronze_order_items"),
    ("olist_order_payments_dataset", "bronze_order_payments"),
    ("olist_order_reviews_dataset", "bronze_order_reviews"),
    ("olist_products_dataset", "bronze_products"),
    ("olist_sellers_dataset", "bronze_sellers"),
    ("olist_geolocation_dataset", "bronze_geolocation"),
    # Added for Phase 2: needed by dbt's dim_products to resolve English
    # category names. Was missing from Phase 1 — no is_synthetic concept
    # applies here (pure lookup table), but we tag it False for consistency.
    ("product_category_name_translation", "bronze_product_category_translation"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                         help="Validate + tag locally, skip BigQuery entirely.")
    args = parser.parse_args()

    if not args.dry_run:
        from replay import bq_writer

    for source_key, bronze_table in TABLE_PLAN:
        print(f"Loading {source_key} ...")
        df = load_csv(source_key)

        # geolocation has no natural single-row key; dedup on zip prefix
        # (keep first) so there's exactly one row per prefix.
        if bronze_table == "bronze_geolocation":
            df = df.drop_duplicates(subset=["geolocation_zip_code_prefix"], keep="first")

        # bronze_orders needs a source_template_date column to match the
        # schema that synthetic batches will later append (replay_engine
        # tags every synthetic order with the historical day it was
        # resampled from). Adding it here — NULL for all historical rows —
        # means the column exists from the very first load, so Phase 2's
        # dbt models can reference it directly without needing any
        # schema-introspection workaround.
        if bronze_table == "bronze_orders":
            df["source_template_date"] = pd.NaT

        df["is_synthetic"] = False
        batch_id = f"historical_{bronze_table}"

        if args.dry_run:
            out_path = config.PROJECT_ROOT / "outputs_dry_run"
            out_path.mkdir(exist_ok=True)
            tagged = df.copy()
            tagged["batch_id"] = batch_id
            tagged.to_csv(out_path / f"{bronze_table}.csv", index=False)
            print(f"  [dry-run] {len(tagged):,} rows -> outputs_dry_run/{bronze_table}.csv")
        else:
            # bq_writer has no upsert/MERGE (BQ sandbox rejects DML) — it only
            # does idempotent load jobs keyed by batch_id. Historical load is
            # one-time and one batch per table, so batch_id = table name is
            # stable across re-runs and lets write_table's own
            # already-loaded-skip logic handle retries.
            bq_writer.write_table(df, config.BQ_BRONZE_DATASET, bronze_table, batch_id)

    print("\nHistorical -> bronze load complete.")


if __name__ == "__main__":
    main()
