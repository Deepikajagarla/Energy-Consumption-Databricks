-- ==========================================================
-- Model Name : daily_energy_summary
-- Layer      : Gold
-- Purpose    : Daily Energy Consumption Summary
-- ==========================================================

{{ config(
    materialized='table',
    catalog='gold_catalog',
    schema='gold_sch'
    
) }}

SELECT

    DATE(timestamp) AS consumption_date,

    COUNT(DISTINCT household_id) AS total_households,

    SUM(CAST(energy_usage_kwh AS DOUBLE)) AS total_energy_usage,

    AVG(CAST(energy_usage_kwh AS DOUBLE)) AS average_energy_usage,

    MAX(CAST(peak_demand_kw AS DOUBLE)) AS peak_demand,

    SUM(CAST(daily_consumption_kwh AS DOUBLE)) AS total_daily_consumption

FROM {{ source('silver','Silver_Energy_Usage') }}

GROUP BY DATE(timestamp)

ORDER BY consumption_date;