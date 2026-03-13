{{ config(
    materialized='table',
    schema='BRONZE'
) }}

SELECT
    *,
    CURRENT_TIMESTAMP AS ingestion_timestamp
FROM {{ source('raw','product_ext') }}