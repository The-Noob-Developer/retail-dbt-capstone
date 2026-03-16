{{ config(
    materialized='table',
    schema='gold'
) }}

with ranked_store as (

select
    *,
    row_number() over(
        partition by store_id
        order by last_modified_date desc
    ) as rn

from {{ ref('stg_store') }}

)

select

    row_number() over(order by store_id) as store_key,

    store_id,
    store_name,

    street,
    city,
    state,
    country,
    zip_code,

    region,
    store_type,

    opening_date,

    store_size_category

from ranked_store
where rn = 1