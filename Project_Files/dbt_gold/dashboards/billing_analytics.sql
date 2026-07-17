-- ==========================================================
-- Model Name : billing_analytics
-- Layer      : Gold
-- Purpose    : Billing Analytics Dashboard
-- ==========================================================

{{ config(
    materialized='table',
    catalog='gold_catalog',
    schema='gold_sch'
) }}

SELECT

    t.tariff_region,

    t.tariff_city,

    t.tariff_plan_type,

    t.utility_provider,

    e.customer_category,

    COUNT(DISTINCT t.household_id) AS total_households,

    AVG(CAST(t.unit_rate AS DOUBLE)) AS avg_unit_rate,

    AVG(CAST(t.peak_rate AS DOUBLE)) AS avg_peak_rate,

    AVG(CAST(t.offpeak_rate AS DOUBLE)) AS avg_offpeak_rate,

    AVG(CAST(t.fixed_charge AS DOUBLE)) AS avg_fixed_charge,

    AVG(CAST(t.tax_amount AS DOUBLE)) AS avg_tax,

    AVG(CAST(t.subsidy_amount AS DOUBLE)) AS avg_subsidy,

    SUM(CAST(t.monthly_bill AS DOUBLE)) AS total_monthly_bill,

    SUM(CAST(t.billing_units AS DOUBLE)) AS total_billing_units,

    SUM(CAST(e.energy_usage_kwh AS DOUBLE)) AS total_energy_usage

FROM {{ source('silver','Silver_Tariff') }} t

JOIN {{ source('silver','Silver_Energy_Usage') }} e

ON t.household_id = e.household_id

GROUP BY

    t.tariff_region,
    t.tariff_city,
    t.tariff_plan_type,
    t.utility_provider,
    e.customer_category