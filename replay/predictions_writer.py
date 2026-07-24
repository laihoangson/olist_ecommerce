"""
Writer for model-prediction tables (delivery risk, negative-review risk,
customer actions, seller segments, revenue forecast).

Unlike bq_writer.write_table() (append + batch_id, used for bronze) or
06_live_transform.py's run_transform_query() (append, used for silver/gold),
every prediction table here is a FULL RESCORE each run: open orders get
re-scored from scratch every 6h (risk changes over time -- see
ops_dashboard_plan.md), customers/sellers get re-scored daily, forecast gets
re-fit weekly. So the write pattern here is simply WRITE_TRUNCATE via a load
job (like scripts/07_sync_supabase_to_bigquery.py's sync_table) -- no
batch_id, no append, no idempotency logic needed: each run's output IS the
complete, current state of that table.
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402
from replay import bq_writer  # noqa: E402


def write_full_rescore(df: pd.DataFrame, dataset: str, table: str):
    """Overwrites `table` with exactly the rows in df. Adds a `scored_at`
    UTC timestamp column so the API/dashboard can show "last updated"."""
    from google.cloud import bigquery

    if df is None or df.empty:
        print(f"[predictions_writer] {table}: nothing to write (0 rows) -- "
              f"skipping so a transient empty batch doesn't wipe the table.")
        return

    df = df.copy()
    df["scored_at"] = datetime.now(timezone.utc)

    client = bq_writer.get_client()
    bq_writer.ensure_dataset(dataset)
    table_id = f"{config.GCP_PROJECT_ID}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print(f"[predictions_writer] Wrote {len(df):,} rows to {table_id} (WRITE_TRUNCATE).")
