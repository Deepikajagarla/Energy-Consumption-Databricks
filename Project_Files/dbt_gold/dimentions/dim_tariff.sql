{{ config(
    materialized='table',
   catalog='gold_catalog',
    schema='gold_sch'
) }}

SELECT DISTINCT

    tariff_region,
    tariff_city,
    tariff_plan_type,
    billing_cycle,
    utility_provider

FROM {{ source('silver','Silver_Tariff') }}

WHERE is_current = TRUE