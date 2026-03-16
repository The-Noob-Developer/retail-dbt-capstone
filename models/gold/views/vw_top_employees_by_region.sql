{{ config(
    materialized='table',
    schema='gold'
) }}

select
    s.region,
    e.full_name,
    sum(f.total_sales_amount) as employee_sales
from {{ ref('fact_sales') }} f
join {{ ref('dim_employee') }} e
    on f.employee_key = e.employee_key
join {{ ref('dim_store') }} s
    on f.store_key = s.store_key
group by
    s.region,
    e.full_name
order by
    employee_sales desc