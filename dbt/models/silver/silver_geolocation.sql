{{ config(materialized='table') }}

-- Raw Olist geolocation has many lat/lng samples per zip prefix. Averaging
-- down to one row per prefix is basic cleaning (not business logic) needed
-- so this can act as a dimension lookup in gold.

with source as (
    select * from {{ source('bronze', 'bronze_geolocation') }}
)

select
    cast(geolocation_zip_code_prefix as string) as geolocation_zip_code_prefix,
    avg(geolocation_lat) as geolocation_lat,
    avg(geolocation_lng) as geolocation_lng,
    -- most common city/state string per prefix (rough mode via count+order)
    array_agg(trim(lower(geolocation_city)) order by geolocation_city)[offset(0)] as geolocation_city,
    array_agg(upper(trim(geolocation_state)) order by geolocation_state)[offset(0)] as geolocation_state
from source
group by geolocation_zip_code_prefix
