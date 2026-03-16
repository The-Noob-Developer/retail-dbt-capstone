{{ config(
    materialized='table',
    schema='gold'
) }}

select
    e.role,
    sum(f.total_sales_amount) as total_sales
from {{ ref('fact_sales') }} f
join {{ ref('dim_employee') }} e
    on f.employee_key = e.employee_key
group by
    e.role
order by
    total_sales desc