{{ config(
    materialized='table',
    schema='gold'
) }}

select
    p.product_name,
    sum(f.inventory_value) as total_inventory_value
from {{ ref('fact_inventory') }} f
join {{ ref('dim_product') }} p
    on f.product_key = p.product_key
group by
    p.product_name
order by
    total_inventory_value desc