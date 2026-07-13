{{ config(materialized='table') }}

-- Cleans + dedupes bronze_orders. Timestamps are cast explicitly to
-- TIMESTAMP because different load batches (historical CSV load vs
-- synthetic replay batches) can be autodetected as DATETIME vs TIMESTAMP by
-- BigQuery depending on the source data — casting here makes every
-- downstream consumer see one consistent type regardless of which batch a
-- row came from.
--
-- No is_delayed / delivery_days here — those are business logic and belong
-- in gold's fct_orders per the project plan.
--
-- source_template_date: 03_load_historical_to_bronze.py adds this column
-- (NULL) on historical rows so the schema matches synthetic rows from the
-- start. If you're working off an OLDER bronze_orders table that predates
-- that fix, BigQuery's ALLOW_FIELD_ADDITION (see bq_writer.write_table)
-- adds the column automatically the first time 04_backfill.py appends a
-- synthetic batch — either way, by the time dbt runs (after backfill has
-- run at least once) this column exists.

with source as (
    select * from {{ source('bronze', 'bronze_orders') }}
),

deduped as (
    select
        *,
        row_number() over (partition by order_id order by batch_id desc) as rn
    from source
)

select
    order_id,
    customer_id,
    order_status,
    cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp,
    cast(order_approved_at as timestamp) as order_approved_at,
    cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
    cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
    cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,
    is_synthetic,
    batch_id,
    source_template_date
from deduped
where rn = 1
