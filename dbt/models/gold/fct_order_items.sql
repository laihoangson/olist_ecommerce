{{ config(materialized='table') }}

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    is_synthetic
from {{ ref('silver_order_items') }}
