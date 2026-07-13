{{ config(materialized='table') }}

-- Grain: 1 order. This is where business logic lives (is_delayed,
-- delivery_days) — silver_orders deliberately has none.
--
-- delivery_days / is_delayed are computed leakage-aware: they use
-- order_delivered_customer_date, which is only known AFTER delivery. Both
-- columns stay in this fact table for BI use, but per plan.md they must be
-- excluded from ML training/serving features — see
-- ml/feature_specs/inference_time_features.md (Phase 4) for the contract.

with orders as (
    select * from {{ ref('silver_orders') }}
),

customers as (
    select customer_id, customer_unique_id from {{ ref('silver_customers') }}
)

select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    case
        when o.order_status = 'delivered' and o.order_delivered_customer_date is not null
        then timestamp_diff(o.order_delivered_customer_date, o.order_purchase_timestamp, hour) / 24.0
        else null
    end as delivery_days,
    case
        when o.order_status = 'delivered'
             and o.order_delivered_customer_date is not null
             and o.order_estimated_delivery_date is not null
        then o.order_delivered_customer_date > o.order_estimated_delivery_date
        else null
    end as is_delayed,
    o.is_synthetic,
    o.source_template_date,
    o.batch_id
from orders o
left join customers c using (customer_id)
