{{ config(
    materialized='table',
    schema='gold'
) }}

select
    p.product_name,
    sum(f.quantity_sold) as total_units_sold,
    sum(f.total_sales_amount) as total_sales
from {{ ref('fact_sales') }} f
join {{ ref('dim_product') }} p
    on f.product_key = p.product_key
group by
    p.product_name
order by
    total_units_sold desc