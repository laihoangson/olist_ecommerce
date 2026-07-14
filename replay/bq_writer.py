"""
BigQuery helpers used by both the backfill and live-ingest scripts.
FREE-TIER / NO-BILLING SAFE — see notes below.

BigQuery sandbox mode (no billing account attached) rejects ALL DML queries
(MERGE, UPDATE, DELETE). This module never issues DML. It only uses:
  - load jobs (WRITE_EMPTY to bootstrap a new table, WRITE_APPEND to add
    rows to an existing one) — batch loads are not DML and work fine
    without billing.
  - SELECT queries to check what's already loaded — SELECT is DQL, not
    DML, also fine without billing.
  - a load job (not a streaming insert) for the pipeline_state checkpoint
    row, since streaming inserts have their own billing requirements.

Idempotency model: every batch (backfill day or live 6h window) gets a
unique `batch_id`. write_table() tags every row with that batch_id and,
for tables that already exist, checks whether that batch_id was already
written before appending — so a retried run never duplicates a batch.
This is "skip if already loaded", not a per-row MERGE — sufficient here
because rows are never edited in place after being written.

Schema evolution: tables created by the Phase 0/historical load (or by an
older version of this pipeline) don't have a `batch_id` column at all.
write_table() uses ALLOW_FIELD_ADDITION on append so BigQuery adds the
column automatically (existing rows get NULL for it) instead of erroring
with "Unrecognized name: batch_id". The batch-id-already-loaded check
first confirms the column exists before querying it, for the same reason.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config  # noqa: E402

_client = None


def get_client():
    global _client
    if _client is None:
        from google.cloud import bigquery
        config.require_gcp_project()
        _client = bigquery.Client(project=config.GCP_PROJECT_ID)
    return _client


def ensure_dataset(dataset_id: str):
    from google.cloud import bigquery

    client = get_client()
    ref = bigquery.DatasetReference(config.GCP_PROJECT_ID, dataset_id)
    try:
        client.get_dataset(ref)
    except Exception:
        ds = bigquery.Dataset(ref)
        ds.location = config.BQ_LOCATION
        client.create_dataset(ds)
        print(f"[bq_writer] Created dataset {dataset_id} in {config.BQ_LOCATION}.")


def table_exists(table_id: str) -> bool:
    from google.cloud.exceptions import NotFound

    client = get_client()
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False


def has_column(table_id: str, column_name: str) -> bool:
    """Check the table's CURRENT schema for a column, without assuming it
    exists — needed because older/historical tables may predate columns
    like batch_id."""
    client = get_client()
    table = client.get_table(table_id)
    return any(field.name == column_name for field in table.schema)


def already_loaded_batch_ids(table_id: str, batch_ids: list) -> set:
    """SELECT-only check (no DML) for which of these batch_ids already have
    rows in the table. Returns empty set (nothing to skip) if the table has
    no batch_id column yet — that just means no batch-tagged rows exist
    there so far, e.g. an older historical-only table."""
    if not batch_ids:
        return set()
    if not has_column(table_id, "batch_id"):
        return set()

    from google.cloud import bigquery

    client = get_client()
    query = f"""
    SELECT DISTINCT batch_id
    FROM `{table_id}`
    WHERE batch_id IN UNNEST(@batch_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("batch_ids", "STRING", batch_ids)]
    )
    rows = client.query(query, job_config=job_config).result()
    return {r["batch_id"] for r in rows}


def write_table(df: pd.DataFrame, dataset: str, table: str, batch_id: str):
    """Bootstrap via load job if the table doesn't exist yet, otherwise
    append via a load job with schema evolution enabled — skipping the
    write entirely if this batch_id was already loaded. No MERGE, no
    streaming insert, no DML of any kind."""
    from google.cloud import bigquery

    if df is None or df.empty:
        print(f"[bq_writer] {table}: nothing to write (empty batch).")
        return

    df = df.copy()
    df["batch_id"] = batch_id

    client = get_client()
    ensure_dataset(dataset)
    table_id = f"{config.GCP_PROJECT_ID}.{dataset}.{table}"

    if not table_exists(table_id):
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_EMPTY", autodetect=True)
        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        print(f"[bq_writer] Bootstrapped {table_id} with {len(df):,} rows.")
        return

    if already_loaded_batch_ids(table_id, [batch_id]):
        print(f"[bq_writer] {table}: batch {batch_id} already loaded, skipping.")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print(f"[bq_writer] Appended {len(df):,} rows into {table_id}.")


def run_transform_query(sql: str, dataset: str, table: str, query_parameters=None):
    """Runs `sql` as a BigQuery query job that writes its results directly
    into `dataset.table`, server-side — used by scripts/06_live_transform.py
    for the silver/gold bronze->silver->gold live cadence (Python replaces
    dbt for this cadence; see live.md).

    This is NOT a DML statement. A destination-table query job is the same
    underlying mechanism as `CREATE TABLE AS SELECT` (a job, not an
    INSERT/MERGE), so it works without a billing account attached, exactly
    like write_table()'s load jobs. The difference from write_table() is
    that the SELECT/transform happens entirely inside BigQuery — nothing is
    pulled into Python — and `sql` is expected to already filter its source
    tables down to just the new batch (via a `batch_id IN UNNEST(@batch_ids)`
    predicate), so only a small slice of bytes is scanned per run instead of
    the whole growing table. total_bytes_processed is logged so the quota
    cost of every step is visible.
    """
    from google.cloud import bigquery

    client = get_client()
    ensure_dataset(dataset)
    table_id = f"{config.GCP_PROJECT_ID}.{dataset}.{table}"
    exists = table_exists(table_id)

    job_config = bigquery.QueryJobConfig(
        destination=table_id,
        write_disposition="WRITE_APPEND" if exists else "WRITE_EMPTY",
        query_parameters=query_parameters or [],
    )
    if exists:
        # Same rationale as write_table(): older/historical tables (built by
        # the initial dbt run) may not have every column this query
        # produces, so let BigQuery add columns instead of erroring.
        job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

    query_job = client.query(sql, job_config=job_config)
    query_job.result()
    bytes_processed = query_job.total_bytes_processed or 0
    print(
        f"[bq_writer] {table}: appended via query job "
        f"({bytes_processed / 1e9:.4f} GB processed)."
    )
    return bytes_processed


def dry_run_query(sql: str, query_parameters=None) -> int:
    """Validates `sql` and returns the bytes it WOULD process, without
    running it or writing anything. Used by --dry-run in
    scripts/06_live_transform.py."""
    from google.cloud import bigquery

    client = get_client()
    job_config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
        query_parameters=query_parameters or [],
    )
    query_job = client.query(sql, job_config=job_config)
    return query_job.total_bytes_processed or 0


STATE_TABLE = "pipeline_state"


def ensure_state_table():
    from google.cloud import bigquery

    client = get_client()
    ensure_dataset(config.BQ_BRONZE_DATASET)
    table_id = f"{config.GCP_PROJECT_ID}.{config.BQ_BRONZE_DATASET}.{STATE_TABLE}"
    if table_exists(table_id):
        return table_id

    schema = [
        bigquery.SchemaField("pipeline_name", "STRING"),
        bigquery.SchemaField("last_processed_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    print(f"[bq_writer] Created state table {table_id}.")
    return table_id


def get_state(pipeline_name: str):
    from google.cloud import bigquery

    table_id = ensure_state_table()
    client = get_client()
    query = f"""
    SELECT last_processed_at
    FROM `{table_id}`
    WHERE pipeline_name = @pipeline_name
    ORDER BY updated_at DESC
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("pipeline_name", "STRING", pipeline_name)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return None
    return rows[0]["last_processed_at"]


def set_state(pipeline_name: str, last_processed_at: datetime):
    """Writes the checkpoint via a load job (not a streaming insert) —
    streaming inserts (tabledata.insertAll) have their own billing
    requirements independent of DML, so we avoid them too."""
    table_id = ensure_state_table()
    client = get_client()
    from google.cloud import bigquery

    row_df = pd.DataFrame([{
        "pipeline_name": pipeline_name,
        "last_processed_at": last_processed_at,
        "updated_at": datetime.now(timezone.utc),
    }])
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(row_df, table_id, job_config=job_config).result()


def fetch_synthetic_customer_pool(limit: int = 5000) -> list:
    """Sample existing synthetic customers from bronze so a fresh process
    (e.g. the live-ingest cron, which runs as a brand-new process every 6h)
    can still pick repeat customers consistently with past batches."""
    table_id = f"{config.GCP_PROJECT_ID}.{config.BQ_BRONZE_DATASET}.bronze_customers"
    if not table_exists(table_id):
        return []

    client = get_client()
    query = f"""
    SELECT customer_id, customer_unique_id, customer_zip_code_prefix,
           customer_city, customer_state, TRUE AS is_synthetic
    FROM `{table_id}`
    WHERE is_synthetic = TRUE
    LIMIT {int(limit)}
    """
    rows = list(client.query(query).result())
    return [dict(r) for r in rows]