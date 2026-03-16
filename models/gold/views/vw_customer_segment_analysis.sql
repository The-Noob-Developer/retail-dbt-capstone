{{ config(
    materialized='table',
    schema='gold'
) }}

select
    c.customer_segment,
    count(distinct f.customer_key) as total_customers,
    sum(f.total_sales_amount) as total_sales,
    avg(f.total_sales_amount) as avg_purchase_value
from {{ ref('fact_sales') }} f
join {{ ref('dim_customer') }} c
    on f.customer_key = c.customer_key
group by
    c.customer_segment
order by
    total_sales desc