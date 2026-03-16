{{ config(
    materialized='table',
    schema='gold'
) }}

select
    e.tenure,
    avg(f.total_sales_amount) as avg_sales
from {{ ref('fact_sales') }} f
join {{ ref('dim_employee') }} e
    on f.employee_key = e.employee_key
group by
    e.tenure
order by
    avg_sales desc