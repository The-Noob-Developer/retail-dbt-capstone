{{ config(
    materialized='table',
    schema='gold'
) }}


with ranked_suppliers as (

select
    *,
    row_number() over (
        partition by supplier_id
        order by last_modified_date desc
    ) as rn

from {{ ref('stg_supplier') }}

)

select

    row_number() over(order by supplier_id) as supplier_key,

    supplier_id,

    supplier_name,

    supplier_type,

    contact_person,
    email,
    phone,

    payment_terms,

    website,
    address

from ranked_suppliers
where rn = 1