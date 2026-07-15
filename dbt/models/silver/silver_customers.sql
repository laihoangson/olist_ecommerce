{{ config(materialized='table') }}

-- Cleans + dedupes bronze_customers. No business logic here (that's gold).
-- Dedup exists as a defensive measure: bronze is append-only (BigQuery
-- sandbox can't do MERGE without billing — see bq_writer.py), so this
-- guards against any accidental duplicate row for the same customer_id.

with source as (
    select * from {{ source('bronze', 'bronze_customers') }}
),

deduped as (
    select
        *,
        row_number() over (partition by customer_id order by batch_id desc) as rn
    from source
)

select
    customer_id,
    customer_unique_id,
    cast(customer_zip_code_prefix as string) as customer_zip_code_prefix,
    trim(lower(customer_city)) as customer_city,
    upper(trim(customer_state)) as customer_state,
    is_synthetic,
    -- Kept even though dbt itself has no use for it: scripts/06_live_transform.py
    -- appends live batches into this table and relies on batch_id being
    -- present to check per-step idempotency (bq_writer.already_loaded_batch_ids).
    -- If this column is dropped from dbt's SELECT, a full `dbt run` (e.g. the
    -- periodic reconciliation in live.md #5) silently wipes it via
    -- CREATE OR REPLACE TABLE AS SELECT, which then makes 06's idempotency
    -- check see the column as missing and treat every batch as "not yet
    -- loaded" — see live.md for the full incident writeup.
    batch_id
from deduped
where rn = 1