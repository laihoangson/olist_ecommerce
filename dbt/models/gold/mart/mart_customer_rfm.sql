{{ config(materialized='table') }}

-- ML-feature-ready RFM: log-transformed Recency + Monetary only. Frequency
-- is deliberately excluded here — per plan.md, 96.88% of customers buy
-- exactly once, so Frequency is near-constant and would add no signal to
-- KMeans (it's kept in dim_customers for BI, just not here).

select
    customer_unique_id,
    recency_days as recency,
    monetary,
    ln(recency_days + 1) as recency_log,
    ln(greatest(monetary, 0) + 1) as monetary_log,
    is_synthetic
from {{ ref('dim_customers') }}
