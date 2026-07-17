-- ==========================================================
-- Model Name : yearly_energy_summary
-- Layer      : Gold Layer
-- Purpose    : Yearly Energy Consumption Summary
-- ==========================================================

{{ config(
    materialized='table',
    catalog='gold_catalog',
    schema='gold_sch'
) }}

SELECT

    YEAR(TO_TIMESTAMP(timestamp)) AS year,

    COUNT(DISTINCT household_id) AS total_households,

    SUM(CAST(energy_usage_kwh AS DOUBLE)) AS total_energy_usage,

    AVG(CAST(energy_usage_kwh AS DOUBLE)) AS average_energy_usage,

    MAX(CAST(peak_demand_kw AS DOUBLE)) AS highest_peak_demand,

    SUM(CAST(daily_consumption_kwh AS DOUBLE)) AS total_yearly_consumption

FROM {{ source('silver','Silver_Energy_Usage') }}

GROUP BY
    YEAR(TO_TIMESTAMP(timestamp))

ORDER BY year;