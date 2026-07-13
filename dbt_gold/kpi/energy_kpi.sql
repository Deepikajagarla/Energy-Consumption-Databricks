-- ==========================================================
-- Model Name : energy_kpi
-- Layer      : Gold Layer
-- Purpose    : Overall Energy Business KPIs
-- ==========================================================

{{ config(
    materialized='table',
   catalog='gold_catalog',
    schema='gold_sch'
) }}

SELECT

    COUNT(DISTINCT household_id) AS total_households,

    SUM(CAST(energy_usage_kwh AS DOUBLE)) AS total_energy_usage,

    AVG(CAST(energy_usage_kwh AS DOUBLE)) AS average_energy_usage,

    SUM(CAST(daily_consumption_kwh AS DOUBLE)) AS total_consumption,

    AVG(CAST(load_factor AS DOUBLE)) AS average_load_factor,

    MAX(CAST(peak_demand_kw AS DOUBLE)) AS highest_peak_demand,

    AVG(CAST(voltage_reading AS DOUBLE)) AS average_voltage,

    AVG(CAST(current_reading AS DOUBLE)) AS average_current

FROM {{ source('silver','Silver_Energy_Usage') }}