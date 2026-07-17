-- ==========================================================
-- Model Name : dim_weather
-- Layer      : Gold
-- Purpose    : Create Weather Dimension Table
-- Source     : Silver_Weather
-- ==========================================================

{{ config(
    materialized='table',
    catalog='gold_catalog',
    schema='gold_sch'
) }}

SELECT DISTINCT

    household_id,
    weather_region,
    weather_city,
    weather_station,
    climate_zone,
    condition_type

FROM {{ source('silver','Silver_Weather') }}