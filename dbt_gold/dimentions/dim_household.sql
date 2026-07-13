{{ config(
    materialized='table',
  catalog='gold_catalog',
    schema='gold_sch'
) }}

SELECT

    household_id,

    MAX(region_name) AS region_name,

    MAX(city_name) AS city_name,

    MAX(meter_type) AS meter_type,

    MAX(customer_category) AS customer_category,

    MAX(grid_zone) AS grid_zone

FROM {{ source('silver','Silver_Energy_Usage') }}

WHERE is_current = TRUE

GROUP BY household_id