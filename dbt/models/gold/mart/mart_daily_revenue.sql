{{ config(materialized='table') }}

-- Pre-aggregated for two consumers per the project plan: Prophet training
-- (filter is_synthetic=false) and the Power BI trend chart, which shows the
-- synthetic/live series as a dashed continuation of the real one — hence
-- is_synthetic stays as a dimension here rather than being filtered out.

with orders as (
    select order_id, date(order_purchase_timestamp) as order_date, is_synthetic
    from {{ ref('fct_orders') }}
),

items as (
    select order_id, price, freight_value
    from {{ ref('fct_order_items') }}
),

joined as (
    select
        o.order_date,
        o.is_synthetic,
        o.order_id,
        i.price,
        i.freight_value
    from orders o
    join items i using (order_id)
)

select
    order_date as date_day,
    is_synthetic,
    count(distinct order_id) as order_count,
    sum(price) as total_product_revenue,
    sum(freight_value) as total_freight_revenue,
    sum(price + freight_value) as total_revenue
from joined
group by order_date, is_synthetic
