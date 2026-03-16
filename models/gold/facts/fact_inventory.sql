{{ config(
    materialized='table',
    schema='gold'
) }}

with orders as (

    select *
    from {{ ref('stg_orders') }}

),

products as (

    select *
    from {{ ref('stg_product') }}

),

sales_agg as (

    select
        o.product_id,
        o.store_id,
        date(o.order_date) as order_date,
        sum(o.quantity) as sold_quantity
    from orders o
    group by
        o.product_id,
        o.store_id,
        date(o.order_date)

),

joined as (

    select
        p.product_id,
        p.supplier_id,
        sa.store_id,
        sa.order_date,
        sa.sold_quantity,
        p.stock_quantity,
        p.cost_price,
        p.reorder_level

    from sales_agg sa

    left join products p
        on sa.product_id = p.product_id

)

select

    row_number() over(order by dp.product_id) as inventory_key,

    dp.product_key,
    dd.date_key,
    ds.store_key,
    sup.supplier_key,

    -- beginning inventory
    j.stock_quantity as beginning_inventory,

    -- ending inventory after sales
    (j.stock_quantity - j.sold_quantity) as ending_inventory,

    -- recommended purchase quantity
    case
        when (j.stock_quantity - j.sold_quantity) < j.reorder_level
        then j.reorder_level - (j.stock_quantity - j.sold_quantity)
        else 0
    end as purchased_quantity,

    j.sold_quantity,

    -- inventory value
    (j.stock_quantity - j.sold_quantity) * j.cost_price
        as inventory_value,

    -- stock turnover ratio
    j.sold_quantity /
    nullif(
        (j.stock_quantity + (j.stock_quantity - j.sold_quantity)) / 2,
        0
    ) as stock_turnover_ratio,

    -- supplier contribution %
    100 *
    case
        when (j.stock_quantity - j.sold_quantity) < j.reorder_level
        then j.reorder_level - (j.stock_quantity - j.sold_quantity)
        else 0
    end
    /
    nullif(
        sum(
            case
                when (j.stock_quantity - j.sold_quantity) < j.reorder_level
                then j.reorder_level - (j.stock_quantity - j.sold_quantity)
                else 0
            end
        ) over (partition by sup.supplier_key),
        0
    ) as supplier_contribution_percentage

from joined j

left join {{ ref('dim_product') }} dp
    on j.product_id = dp.product_id

left join {{ ref('dim_store') }} ds
    on j.store_id = ds.store_id

left join {{ ref('dim_supplier') }} sup
    on j.supplier_id = sup.supplier_id

left join {{ ref('dim_date') }} dd
    on j.order_date = dd.full_date