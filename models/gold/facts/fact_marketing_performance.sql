{{ config(
    materialized='table',
    schema='gold'
) }}

with sales as (

    select *
    from {{ ref('fact_sales') }}

),

campaign as (

    select *
    from {{ ref('dim_marketing_campaign') }}

),

date_dim as (

    select *
    from {{ ref('dim_date') }}

),

customer as (

    select *
    from {{ ref('dim_customer') }}

),

sales_with_dates as (

    select
        s.campaign_key,
        s.date_key,
        s.customer_key,
        s.total_sales_amount
    from sales s

),

campaign_sales as (

    select

        swd.campaign_key,
        swd.date_key,

        sum(swd.total_sales_amount) as total_sales_influenced,

        count(distinct swd.customer_key) as customers_reached

    from sales_with_dates swd

    group by
        swd.campaign_key,
        swd.date_key

),

new_customers as (

    select

        s.campaign_key,
        s.date_key,

        count(distinct s.customer_key) as new_customers_acquired

    from sales s

    join campaign c
        on s.campaign_key = c.campaign_key

    join date_dim d
        on s.date_key = d.date_key

    where d.full_date between c.start_date and c.end_date
    group by
        s.campaign_key,
        s.date_key

),

repeat_purchase as (

    select

        campaign_key,

        100 *
        count(distinct case when purchase_count > 1 then customer_key end)
        / nullif(count(distinct customer_key),0)
        as repeat_purchase_rate

    from (

        select
            campaign_key,
            customer_key,
            count(*) as purchase_count
        from sales
        group by campaign_key, customer_key

    )

    group by campaign_key

)

select

    cs.campaign_key,

    cs.date_key,

    cs.total_sales_influenced,

    nc.new_customers_acquired,

    rp.repeat_purchase_rate,

    c.roi

from campaign_sales cs

left join new_customers nc
    on cs.campaign_key = nc.campaign_key
    and cs.date_key = nc.date_key

left join repeat_purchase rp
    on cs.campaign_key = rp.campaign_key

left join campaign c
    on cs.campaign_key = c.campaign_key