-- ==========================================================
-- Model Name : fact_energy_usage
-- Layer      : Gold
-- Purpose    : Create Fact Energy Usage Table
-- Source     : Silver Layer Tables
-- ==========================================================

{{ config(
    materialized='table',
    catalog='gold_catalog',
    schema='gold_sch',
   unique_key=['household_id','timestamp']
) }}

SELECT

    e.household_id,

    -- Household Information
    e.region_name,
    e.city_name,
    e.meter_type,
    e.customer_category,
    e.grid_zone,

    -- Energy Measures
    e.voltage_reading,
    e.current_reading,
    e.active_power_kw,
    e.reactive_power_kvar,
    e.energy_usage_kwh,
    e.frequency_hz,
    e.load_factor,
    e.peak_demand_kw,
    e.offpeak_demand_kw,
    e.daily_consumption_kwh,
    e.timestamp,

    -- Weather Information
    w.temperature_celsius,
    w.humidity_percent,
    w.wind_speed_kmh,
    w.rainfall_mm,
    w.pressure_hpa,
    w.solar_radiation,

    -- Grid Information
    g.grid_load_kw,
    g.transformer_load,
    g.line_loss_percent,
    g.demand_forecast_kw,
    g.reserve_margin,

    -- Tariff Information
    t.unit_rate,
    t.peak_rate,
    t.offpeak_rate,
    t.monthly_bill,

    -- Device Information
    d.device_power_kw,
    d.runtime_hours,
    d.energy_draw_kwh,
    d.efficiency_ratio

FROM {{ source('silver','Silver_Energy_Usage') }} e

LEFT JOIN {{ source('silver','Silver_Weather') }} w
ON e.household_id = w.household_id

LEFT JOIN {{ source('silver','Silver_Grid_Load') }} g
ON e.household_id = g.household_id

LEFT JOIN {{ source('silver','Silver_Tariff') }} t
ON e.household_id = t.household_id

LEFT JOIN {{ source('silver','Silver_Device_Metrics') }} d
ON e.household_id = d.household_id




