{{ config(
    materialized='table',
    schema='gold'
) }}

with ranked_campaigns as (

select
    *,
    row_number() over(
        partition by campaign_id
        order by last_modified_date desc
    ) as rn

from {{ ref('stg_campaign') }}

)

select

    row_number() over(order by campaign_id) as campaign_key,

    campaign_id,

    target_audience as target_audience_segment,

    budget_amount as budget,

    campaign_duration_days as duration,

    roi,

    start_date,

    end_date

from ranked_campaigns
where rn = 1