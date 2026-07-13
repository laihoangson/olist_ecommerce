{{ config(materialized='table') }}

select
    review_id,
    order_id,
    review_score,
    review_creation_date,
    review_answer_timestamp,
    timestamp_diff(review_answer_timestamp, review_creation_date, hour) / 24.0
        as review_response_days,
    is_synthetic
from {{ ref('silver_order_reviews') }}
