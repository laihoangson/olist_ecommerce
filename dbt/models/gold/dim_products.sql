{{ config(materialized='table') }}

select
    product_id,
    product_category_name,
    product_category_name_english,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_photos_qty,
    is_synthetic
from {{ ref('silver_products') }}
