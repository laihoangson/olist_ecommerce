{{ config(materialized='table') }}

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    is_synthetic
from {{ ref('silver_order_payments') }}
