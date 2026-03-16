{{ config(
    materialized='table',
    schema='gold'
) }}

select
    p.product_name,
    sum(f.sold_quantity) as total_units_sold,
    case
        when sum(f.sold_quantity) > 100 then 'Fast Moving'
        else 'Slow Moving'
    end as movement_category
from {{ ref('fact_inventory') }} f
join {{ ref('dim_product') }} p
    on f.product_key = p.product_key
group by
    p.product_name
order by
    total_units_sold desc