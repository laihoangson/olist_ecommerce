{{ config(materialized='table') }}

with source as (
    select * from {{ source('bronze', 'bronze_order_items') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by order_id, order_item_id order by batch_id desc
        ) as rn
    from source
)

select
    order_id,
    cast(order_item_id as int64) as order_item_id,
    product_id,
    seller_id,
    cast(shipping_limit_date as timestamp) as shipping_limit_date,
    cast(price as numeric) as price,
    cast(freight_value as numeric) as freight_value,
    is_synthetic,
    batch_id
from deduped
where rn = 1
