{{ config(materialized='table') }}

with source as (
    select * from {{ source('bronze', 'bronze_order_payments') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by order_id, payment_sequential order by batch_id desc
        ) as rn
    from source
)

select
    order_id,
    cast(payment_sequential as int64) as payment_sequential,
    payment_type,
    cast(payment_installments as int64) as payment_installments,
    cast(payment_value as numeric) as payment_value,
    is_synthetic,
    batch_id
from deduped
where rn = 1
