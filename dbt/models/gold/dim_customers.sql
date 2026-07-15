{{ config(materialized='table') }}

-- Grain: 1 customer_unique_id (a real person), not customer_id (Olist gives
-- every order a fresh customer_id even for the same person). RFM here is a
-- BI-facing snapshot; mart_customer_rfm below is the ML-feature-ready
-- version (log-transformed, Frequency dropped per the KMeans design note).

with customer_profile as (
    select
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        row_number() over (partition by customer_unique_id order by customer_id) as rn
    from {{ ref('silver_customers') }}
),

orders_with_person as (
    select
        o.order_id,
        o.order_purchase_timestamp,
        o.is_synthetic,
        c.customer_unique_id
    from {{ ref('silver_orders') }} o
    left join {{ ref('silver_customers') }} c using (customer_id)
),

payments as (
    select order_id, sum(payment_value) as order_payment_value
    from {{ ref('silver_order_payments') }}
    group by order_id
),

order_value as (
    select
        o.customer_unique_id,
        o.order_id,
        o.order_purchase_timestamp,
        o.is_synthetic,
        coalesce(p.order_payment_value, 0) as order_payment_value
    from orders_with_person o
    left join payments p using (order_id)
),

reference_date as (
    -- One "as of" timestamp PER is_synthetic cohort, not one global value.
    -- Historical and synthetic orders live on two unrelated timelines
    -- (2016-2018 vs. 2024-onward) — a single global MAX() means the
    -- moment synthetic/live data starts landing in silver_orders, every
    -- historical customer's recency_days balloons to ~2800+ days (measured
    -- against a "now" from 2026, not 2018) and gets labeled 'lost'
    -- regardless of their real purchase behavior. Grouping by is_synthetic
    -- keeps each cohort's recency meaningful relative to its own timeline.
    select
        is_synthetic,
        max(order_purchase_timestamp) as as_of_timestamp
    from order_value
    group by is_synthetic
),

customer_agg as (
    select
        customer_unique_id,
        min(order_purchase_timestamp) as first_purchase_at,
        max(order_purchase_timestamp) as last_purchase_at,
        count(distinct order_id) as frequency,
        sum(order_payment_value) as monetary,
        -- A given person's orders are either all historical or all
        -- synthetic by construction (synthetic customers are brand-new
        -- entities — see replay_engine.py), so max() here just surfaces
        -- that single value, not a real aggregation across mixed types.
        max(is_synthetic) as is_synthetic
    from order_value
    group by customer_unique_id
)

select
    p.customer_unique_id,
    p.customer_zip_code_prefix,
    p.customer_city,
    p.customer_state,
    a.first_purchase_at,
    a.last_purchase_at,
    a.frequency,
    a.monetary,
    date_diff(date(r.as_of_timestamp), date(a.last_purchase_at), day) as recency_days,
    case when a.frequency > 1 then 'repeat' else 'one_time' end as customer_type,
    case
        when date_diff(date(r.as_of_timestamp), date(a.last_purchase_at), day) <= 90 then 'active'
        when date_diff(date(r.as_of_timestamp), date(a.last_purchase_at), day) <= 180 then 'at_risk'
        else 'lost'
    end as segment_label,
    a.is_synthetic
from customer_agg a
join customer_profile p on p.customer_unique_id = a.customer_unique_id and p.rn = 1
join reference_date r on r.is_synthetic = a.is_synthetic