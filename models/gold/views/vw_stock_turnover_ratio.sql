{{ config(
    materialized='table',
    schema='gold'
) }}

select
    p.product_name,
    avg(f.stock_turnover_ratio) as stock_turnover_ratio
from {{ ref('fact_inventory') }} f
join {{ ref('dim_product') }} p
    on f.product_key = p.product_key
group by
    p.product_name
order by
    stock_turnover_ratio desc