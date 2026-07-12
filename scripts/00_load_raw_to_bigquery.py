"""
One-time, immutable load of the 9 Olist CSV files into BigQuery `olist_raw`.

Design intent (per project plan):
- olist_raw is loaded ONCE and never mutated afterwards. It exists purely as
  a lookup source for the Phase 1 replay script.
- Because of that, this script refuses to overwrite a table that already
  contains data unless --force is passed.
- Schema is auto-detected by BigQuery from the CSV headers/values, since raw
  layer intentionally does not enforce types yet (that happens in silver).

Usage:
    python scripts/00_load_raw_to_bigquery.py
    python scripts/00_load_raw_to_bigquery.py --force
    python scripts/00_load_raw_to_bigquery.py --dry-run
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402


def validate_local_csvs() -> dict:
    """Check that every expected CSV exists and can be read; return row counts."""
    row_counts = {}
    missing = []

    for table_name, filename in config.RAW_FILES.items():
        path = config.DATA_DIR / filename
        if not path.exists():
            missing.append(filename)
            continue
        # Read only to validate + count rows; keep memory bounded by reading
        # in chunks for the larger files (order_items, reviews, geolocation).
        row_count = 0
        for chunk in pd.read_csv(path, chunksize=100_000, low_memory=False):
            row_count += len(chunk)
        row_counts[table_name] = row_count
        print(f"  [OK] {filename}: {row_count:,} rows")

    if missing:
        print("\n[ERROR] Missing CSV files in data/:")
        for f in missing:
            print(f"  - {f}")
        print(
            "\nDownload the dataset from "
            "https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce "
            "and unzip all CSVs into the data/ folder."
        )
        sys.exit(1)

    return row_counts


def table_has_rows(client, table_ref) -> bool:
    """Return True if the table exists and has at least one row."""
    from google.cloud.exceptions import NotFound

    try:
        table = client.get_table(table_ref)
    except NotFound:
        return False
    return table.num_rows > 0


def load_to_bigquery(row_counts: dict, force: bool) -> None:
    from google.cloud import bigquery

    if not config.GCP_PROJECT_ID:
        print(
            "[ERROR] GCP_PROJECT_ID environment variable is not set. "
            "See README.md for setup steps."
        )
        sys.exit(1)

    client = bigquery.Client(project=config.GCP_PROJECT_ID)

    dataset_ref = bigquery.DatasetReference(config.GCP_PROJECT_ID, config.BQ_RAW_DATASET)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {config.BQ_RAW_DATASET} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = config.BQ_LOCATION
        client.create_dataset(dataset)
        print(f"Created dataset {config.BQ_RAW_DATASET} in {config.BQ_LOCATION}.")

    for table_name, filename in config.RAW_FILES.items():
        path = config.DATA_DIR / filename
        table_id = f"{config.GCP_PROJECT_ID}.{config.BQ_RAW_DATASET}.{table_name}"

        if table_has_rows(client, table_id) and not force:
            print(
                f"[SKIP] {table_name} already has data. "
                f"olist_raw is immutable by design — pass --force to reload."
            )
            continue

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            encoding="UTF-8",
            quote_character='"',
            allow_quoted_newlines=True,
            field_delimiter=",",
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if force
                else bigquery.WriteDisposition.WRITE_EMPTY
            ),
        )

        print(f"Loading {filename} -> {table_id} ...")
        with open(path, "rb") as f:
            load_job = client.load_table_from_file(f, table_id, job_config=job_config)
        load_job.result()  # wait for completion, raises on error

        table = client.get_table(table_id)
        expected = row_counts[table_name]
        print(f"  [DONE] {table.num_rows:,} rows loaded (local CSV had {expected:,} rows).")
        if table.num_rows != expected:
            print(
                "  [WARN] Row count mismatch between local CSV and BigQuery table. "
                "Investigate before proceeding to Phase 1."
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing BigQuery tables (breaks the immutability assumption).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate local CSVs only; do not touch BigQuery.",
    )
    args = parser.parse_args()

    print("Validating local CSV files...")
    row_counts = validate_local_csvs()

    if args.dry_run:
        print("\n[dry-run] CSVs are valid. Skipping BigQuery load.")
        return

    load_to_bigquery(row_counts, force=args.force)
    print("\nolist_raw load complete.")


if __name__ == "__main__":
    main()
