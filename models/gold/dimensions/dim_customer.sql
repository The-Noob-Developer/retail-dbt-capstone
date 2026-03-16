{{ config(
    materialized='table',
    schema='gold'
) }}

select

    -- Surrogate Key
    row_number() over(order by customer_id) as customer_key,

    -- Business Key
    customer_id,

    -- Basic Info
    full_name,
    email,
    phone,

    -- Address Details
    street,
    city,
    state,
    country,
    zip_code,
    full_address,

    -- Demographic Information
    customer_age,
    occupation,
    income_bracket,

    -- Segment
    customer_segment,

    -- Registration
    registration_date,

    -- SCD tracking fields
    -- last_modified_date as effective_from,
    -- null as effective_to,
    -- true as is_current

from {{ ref('stg_customer') }}