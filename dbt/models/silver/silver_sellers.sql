{{ config(materialized='table') }}

with source as (
    select * from {{ source('bronze', 'bronze_sellers') }}
),

deduped as (
    select
        *,
        row_number() over (partition by seller_id order by batch_id desc) as rn
    from source
)

select
    seller_id,
    cast(seller_zip_code_prefix as string) as seller_zip_code_prefix,
    trim(lower(seller_city)) as seller_city,
    upper(trim(seller_state)) as seller_state,
    is_synthetic
from deduped
where rn = 1
