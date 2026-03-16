{{ config(
    materialized='table',
    schema='gold'
) }}

select
    c.target_audience_segment,
    avg(c.roi) as avg_roi
from {{ ref('dim_marketing_campaign') }} c
group by
    c.target_audience_segment
order by
    avg_roi desc