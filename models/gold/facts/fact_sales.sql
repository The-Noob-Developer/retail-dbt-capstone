{{ config(
    materialized='table',
    schema='gold'
) }}

with orders as (

    select *
    from {{ ref('stg_orders') }}

),

joined as (

    select

        o.order_id,

        c.customer_key,
        p.product_key,
        s.store_key,
        d.date_key,
        e.employee_key,
        mc.campaign_key,

        o.quantity as quantity_sold,
        o.unit_price,

        o.discount_amount,
        o.shipping_cost,

        s.region,

        case
            when lower(o.order_source) like '%online%' then 'Online'
            else 'In-Store'
        end as sales_channel,

        c.customer_segment as customer_segment_impact,

        p.cost_price

    from orders o

    left join {{ ref('dim_customer') }} c
        on o.customer_id = c.customer_id

    left join {{ ref('dim_product') }} p
        on o.product_id = p.product_id

    left join {{ ref('dim_store') }} s
        on o.store_id = s.store_id

    left join {{ ref('dim_employee') }} e
        on o.employee_id = e.employee_id

    left join {{ ref('dim_marketing_campaign') }} mc
        on o.campaign_id = mc.campaign_id

    left join {{ ref('dim_date') }} d
        on date(o.order_date) = d.full_date

)

select

    row_number() over(order by order_id) as sales_key,

    order_id,

    customer_key,
    product_key,
    store_key,
    date_key,
    employee_key,
    campaign_key,

    quantity_sold,
    unit_price,

    -- Total Sales
    quantity_sold * unit_price as total_sales_amount,

    -- Cost
    quantity_sold * cost_price as cost_amount,

    -- Profit
    (quantity_sold * unit_price)
        - (quantity_sold * cost_price)
        - discount_amount
        - shipping_cost as profit_amount,

    discount_amount,
    shipping_cost,

    region,

    sales_channel,

    customer_segment_impact

from joined