{{ config(
    materialized='table',
    catalog='gold_catalog',
    schema='gold_sch',
   
) }}

SELECT DISTINCT

    grid_region,
    substation_name,
    feeder_line,
    distribution_zone,
    grid_operator

FROM {{ source('silver','Silver_Grid_Load') }}

WHERE is_current = TRUE