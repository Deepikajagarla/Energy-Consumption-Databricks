-- ==========================================================
-- Model Name : weather_impact
-- Layer      : Gold
-- Purpose    : Weather Impact Dashboard
-- ==========================================================

{{ config(
    materialized='table',
  catalog='gold_catalog',
    schema='gold_sch'
) }}

SELECT

    w.weather_region,

    w.weather_city,

    w.climate_zone,

    AVG(CAST(w.temperature_celsius AS DOUBLE)) AS avg_temperature,

    AVG(CAST(w.humidity_percent AS DOUBLE)) AS avg_humidity,

    AVG(CAST(w.rainfall_mm AS DOUBLE)) AS avg_rainfall,

    AVG(CAST(w.wind_speed_kmh AS DOUBLE)) AS avg_wind_speed,

    AVG(CAST(w.solar_radiation AS DOUBLE)) AS avg_solar_radiation,

    SUM(CAST(e.energy_usage_kwh AS DOUBLE)) AS total_energy_usage,

    AVG(CAST(e.energy_usage_kwh AS DOUBLE)) AS avg_energy_usage,

    MAX(CAST(e.peak_demand_kw AS DOUBLE)) AS highest_peak_demand,

    COUNT(DISTINCT e.household_id) AS total_households

FROM {{ source('silver','Silver_Weather') }} w

JOIN {{ source('silver','Silver_Energy_Usage') }} e

ON w.household_id = e.household_id

GROUP BY

    w.weather_region,
    w.weather_city,
    w.climate_zone