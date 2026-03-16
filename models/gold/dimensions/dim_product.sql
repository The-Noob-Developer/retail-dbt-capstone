{{ config(
    materialized='table',
    schema='gold'
) }}

with ranked_products as (

select
    *,
    row_number() over (
        partition by product_id
        order by last_modified_date desc
    ) as rn

from {{ ref('stg_product') }}

)

select

    row_number() over(order by product_id) as product_key,

    product_id,
    name as product_name,

    category,
    subcategory,
    brand,

    color,
    size,

    unit_price,
    cost_price,

    supplier_id as supplier_information

from ranked_products
where rn = 1