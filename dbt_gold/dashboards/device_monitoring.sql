-- ==========================================================
-- Model Name : device_monitoring
-- Layer      : Gold
-- Purpose    : Device Monitoring Dashboard
-- ==========================================================

{{ config(
    materialized='table',
   catalog='gold_catalog',
    schema='gold_sch'
) }}

SELECT

    d.device_category,

    d.device_brand,

    d.device_model,

    d.maintenance_status,

    COUNT(DISTINCT d.household_id) AS total_households,

    AVG(CAST(d.runtime_hours AS DOUBLE)) AS avg_runtime_hours,

    AVG(CAST(d.device_power_kw AS DOUBLE)) AS avg_device_power,

    AVG(CAST(d.efficiency_ratio AS DOUBLE)) AS avg_efficiency,

    SUM(CAST(d.energy_draw_kwh AS DOUBLE)) AS total_energy_draw,

    SUM(CAST(e.energy_usage_kwh AS DOUBLE)) AS total_energy_usage,

    MAX(CAST(e.peak_demand_kw AS DOUBLE)) AS highest_peak_demand

FROM {{ source('silver','Silver_Device_Metrics') }} d

JOIN {{ source('silver','Silver_Energy_Usage') }} e

ON d.household_id = e.household_id

GROUP BY

    d.device_category,
    d.device_brand,
    d.device_model,
    d.maintenance_status