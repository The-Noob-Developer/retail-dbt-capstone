{{ config(
    materialized='table',
    schema='gold'
) }}

select
    d.year,
    d.month,
    s.region,
    sum(f.total_sales_amount) as total_sales
from {{ ref('fact_sales') }} f
join {{ ref('dim_store') }} s
    on f.store_key = s.store_key
join {{ ref('dim_date') }} d
    on f.date_key = d.date_key
group by
    d.year,
    d.month,
    s.region
order by
    d.year,
    d.month