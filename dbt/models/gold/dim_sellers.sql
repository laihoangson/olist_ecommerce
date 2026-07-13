{{ config(materialized='table') }}

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    is_synthetic
from {{ ref('silver_sellers') }}
