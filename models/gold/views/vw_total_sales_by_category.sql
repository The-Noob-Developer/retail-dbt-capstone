{{ config(
    materialized='table',
    schema='gold'
) }}

select
    p.subcategory,
    sum(f.total_sales_amount) as total_sales
from {{ ref('fact_sales') }} f
join {{ ref('dim_product') }} p
    on f.product_key = p.product_key
group by
    p.subcategory
order by
    total_sales desc