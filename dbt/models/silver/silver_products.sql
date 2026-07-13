{{ config(materialized='table') }}

with products as (
    select * from {{ source('bronze', 'bronze_products') }}
),

translation as (
    select * from {{ source('bronze', 'bronze_product_category_translation') }}
),

deduped as (
    select
        *,
        row_number() over (partition by product_id order by batch_id desc) as rn
    from products
)

select
    p.product_id,
    p.product_category_name,
    coalesce(t.product_category_name_english, p.product_category_name) as product_category_name_english,
    cast(p.product_weight_g as numeric) as product_weight_g,
    cast(p.product_length_cm as numeric) as product_length_cm,
    cast(p.product_height_cm as numeric) as product_height_cm,
    cast(p.product_width_cm as numeric) as product_width_cm,
    cast(p.product_photos_qty as int64) as product_photos_qty,
    p.is_synthetic
from deduped p
left join translation t
    on p.product_category_name = t.product_category_name
where p.rn = 1
