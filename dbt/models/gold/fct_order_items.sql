{{ config(materialized='table') }}

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    is_synthetic,
    -- See the matching comment in silver_customers.sql: required for
    -- scripts/06_live_transform.py's per-step idempotency check to survive
    -- a full dbt rebuild.
    batch_id
from {{ ref('silver_order_items') }}