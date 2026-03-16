{{ config(
    materialized='table',
    schema='gold'
) }}

select
    c.customer_id,
    c.full_name,
    sum(f.total_sales_amount) as lifetime_value
from {{ ref('fact_sales') }} f
join {{ ref('dim_customer') }} c
    on f.customer_key = c.customer_key
group by
    c.customer_id,
    c.full_name
order by
    lifetime_value desc