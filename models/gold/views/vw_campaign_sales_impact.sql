{{ config(
    materialized='table',
    schema='gold'
) }}

select
    c.campaign_id,
    avg(c.roi) as campaign_roi,
    sum(f.total_sales_amount) as influenced_sales
from {{ ref('fact_sales') }} f
join {{ ref('dim_marketing_campaign') }} c
    on f.campaign_key = c.campaign_key
group by
    c.campaign_id
order by
    influenced_sales desc