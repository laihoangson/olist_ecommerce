{{ config(materialized='table') }}

-- Wide enough to cover real historical data (from 2016), the synthetic
-- backfill (from 2024), and years of future live-ingest data without
-- needing to touch this model again.
with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

select
    cast(date_day as date) as date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    format_date('%B', date_day) as month_name,
    extract(day from date_day) as day_of_month,
    extract(dayofweek from date_day) as day_of_week,  -- BigQuery: 1=Sun .. 7=Sat
    format_date('%A', date_day) as day_name,
    extract(dayofweek from date_day) in (1, 7) as is_weekend,
    extract(week from date_day) as week_of_year
from spine
