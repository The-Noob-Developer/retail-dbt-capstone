SELECT
    *,
    CURRENT_TIMESTAMP AS ingestion_timestamp
FROM {{ source('raw','customer_ext') }}