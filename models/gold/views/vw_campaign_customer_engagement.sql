{{ config(
    materialized='table',
    schema='gold'
) }}

select
    c.campaign_id,
    count(distinct f.customer_key) as engaged_customers,
    sum(f.total_sales_amount) as campaign_sales
from {{ ref('fact_sales') }} f
join {{ ref('dim_marketing_campaign') }} c
    on f.campaign_key = c.campaign_key
group by
    c.campaign_id
order by
    campaign_sales desc