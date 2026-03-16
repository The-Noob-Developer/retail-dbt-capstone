{{ config(
    materialized='table',
    schema='gold'
) }}

with purchase_counts as (

    select
        customer_key,
        count(distinct order_id) as purchase_count
    from {{ ref('fact_sales') }}
    group by
        customer_key

)

select
    c.customer_segment,
    count(distinct case when p.purchase_count > 1 then p.customer_key end)
        / count(distinct p.customer_key) * 100 as repeat_purchase_rate
from purchase_counts p
join {{ ref('dim_customer') }} c
    on p.customer_key = c.customer_key
group by
    c.customer_segment