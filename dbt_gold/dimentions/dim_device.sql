{{ config(
    materialized='table',
    catalog='gold_catalog',
    schema='gold_sch'
) }}

SELECT DISTINCT

    device_category,
    device_brand,
    device_model,
    maintenance_status,
    installation_region

FROM {{ source('silver','Silver_Device_Metrics') }}

WHERE is_current = TRUE