{{ config(materialized='table') }}

-- The real Olist review data occasionally has more than one review row for
-- the same order_id (customer re-reviews). We keep review_id as the grain
-- (dedup on that), not order_id — an order legitimately CAN have multiple
-- reviews over time; gold's fct_reviews decides how to roll that up if
-- needed.

with source as (
    select * from {{ source('bronze', 'bronze_order_reviews') }}
),

deduped as (
    select
        *,
        row_number() over (partition by review_id order by batch_id desc) as rn
    from source
)

select
    review_id,
    order_id,
    cast(review_score as int64) as review_score,
    review_comment_title,
    review_comment_message,
    cast(review_creation_date as timestamp) as review_creation_date,
    cast(review_answer_timestamp as timestamp) as review_answer_timestamp,
    is_synthetic,
    batch_id
from deduped
where rn = 1
