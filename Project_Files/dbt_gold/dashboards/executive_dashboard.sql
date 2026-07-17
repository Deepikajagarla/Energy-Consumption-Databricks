-- ==========================================================
-- Model Name : executive_dashboard
-- Layer      : Gold
-- Purpose    : Executive Dashboard KPIs
-- ==========================================================

{{ config(
    materialized='table',
   catalog='gold_catalog',
    schema='gold_sch'
) }}

SELECT

    /* Energy KPIs */

    COUNT(DISTINCT e.household_id) AS total_households,

    SUM(CAST(e.energy_usage_kwh AS DOUBLE)) AS total_energy_usage,

    AVG(CAST(e.energy_usage_kwh AS DOUBLE)) AS average_energy_usage,

    SUM(CAST(e.daily_consumption_kwh AS DOUBLE)) AS total_consumption,

    MAX(CAST(e.peak_demand_kw AS DOUBLE)) AS highest_peak_demand,

    /* Weather KPIs */

    AVG(CAST(w.temperature_celsius AS DOUBLE)) AS average_temperature,

    AVG(CAST(w.humidity_percent AS DOUBLE)) AS average_humidity,

    AVG(CAST(w.rainfall_mm AS DOUBLE)) AS average_rainfall,

    /* Device KPIs */

    AVG(CAST(d.efficiency_ratio AS DOUBLE)) AS average_device_efficiency,

    AVG(CAST(d.runtime_hours AS DOUBLE)) AS average_runtime,

    /* Billing KPIs */

    SUM(CAST(t.monthly_bill AS DOUBLE)) AS total_monthly_bill,

    AVG(CAST(t.unit_rate AS DOUBLE)) AS average_unit_rate

FROM {{ source('silver','Silver_Energy_Usage') }} e

LEFT JOIN {{ source('silver','Silver_Weather') }} w
ON e.household_id = w.household_id

LEFT JOIN {{ source('silver','Silver_Device_Metrics') }} d
ON e.household_id = d.household_id

LEFT JOIN {{ source('silver','Silver_Tariff') }} t
ON e.household_id = t.household_id