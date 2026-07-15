{{ config(materialized='table') }}

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    is_synthetic,
    -- See the matching comment in silver_customers.sql.
    batch_id
from {{ ref('silver_order_payments') }}