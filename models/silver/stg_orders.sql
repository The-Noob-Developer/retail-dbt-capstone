{{ config(
    materialized='table',
    schema='silver'
) }}

WITH json_extracted AS (

    SELECT
        {{ trim_whitespace("f.value:order_id::STRING") }} AS order_id,
        {{ trim_whitespace("f.value:customer_id::STRING") }} AS customer_id,
        {{ trim_whitespace("f.value:employee_id::STRING") }} AS employee_id,
        {{ trim_whitespace("f.value:campaign_id::STRING") }} AS campaign_id,
        {{ trim_whitespace("f.value:store_id::STRING") }} AS store_id,

        {{ standardize_case(trim_whitespace("f.value:order_source::STRING")) }} AS order_source,
        {{ standardize_case(trim_whitespace("f.value:order_status::STRING")) }} AS order_status,
        {{ standardize_case(trim_whitespace("f.value:payment_method::STRING")) }} AS payment_method,
        {{ standardize_case(trim_whitespace("f.value:shipping_method::STRING")) }} AS shipping_method,

        {{ standardize_timestamp("f.value:created_at") }} AS created_at,
        {{ standardize_timestamp("f.value:order_date") }} AS order_date,
        {{ standardize_timestamp("f.value:shipping_date") }} AS shipping_date,
        {{ standardize_timestamp("f.value:delivery_date") }} AS delivery_date,
        {{ standardize_timestamp("f.value:estimated_delivery_date") }} AS estimated_delivery_date,

        {{ clean_currency("f.value:total_amount::STRING") }} AS total_amount,
        {{ clean_currency("f.value:discount_amount::STRING") }} AS discount_amount,
        {{ clean_currency("f.value:shipping_cost::STRING") }} AS shipping_cost,
        {{ clean_currency("f.value:tax_amount::STRING") }} AS tax_amount,

        {{ trim_whitespace("f.value:billing_address.street::STRING") }} AS billing_street,
        {{ trim_whitespace("f.value:billing_address.city::STRING") }} AS billing_city,
        {{ trim_whitespace("f.value:billing_address.state::STRING") }} AS billing_state,
        {{ trim_whitespace("f.value:billing_address.zip_code::STRING") }} AS billing_zip_code,

        {{ trim_whitespace("f.value:shipping_address.street::STRING") }} AS shipping_street,
        {{ trim_whitespace("f.value:shipping_address.city::STRING") }} AS shipping_city,
        {{ trim_whitespace("f.value:shipping_address.state::STRING") }} AS shipping_state,
        {{ trim_whitespace("f.value:shipping_address.zip_code::STRING") }} AS shipping_zip_code,

        item.value:product_id::STRING AS product_id,
        item.value:quantity::INT AS quantity,
        item.value:unit_price::FLOAT AS unit_price,
        item.value:cost_price::FLOAT AS cost_price,
        item.value:discount_amount::FLOAT AS item_discount

    FROM RETAIL_DB.BRONZE.BR_ORDERS t,
         LATERAL FLATTEN(input => t.value:orders_data) f,
         LATERAL FLATTEN(input => f.value:order_items) item

),

order_item_metrics AS (

    SELECT
        *,
        COUNT(product_id) OVER (PARTITION BY order_id) AS total_items,
        SUM(quantity) OVER (PARTITION BY order_id) AS total_quantity,
        SUM(quantity * unit_price) OVER (PARTITION BY order_id) AS items_total_amount,
        SUM(quantity * cost_price) OVER (PARTITION BY order_id) AS items_total_cost,
        SUM(item_discount) OVER (PARTITION BY order_id) AS items_total_discount
    FROM json_extracted

),

profit_metrics AS (

    SELECT
        *,

        (total_amount - items_total_cost - discount_amount - shipping_cost - tax_amount)
        AS profit_amount,

        (total_amount - items_total_cost - discount_amount - shipping_cost - tax_amount)
        / NULLIF(total_amount,0)
        AS profit_margin_percentage

    FROM order_item_metrics

),

order_time_calc AS (

    SELECT
        *,

        CASE
            WHEN DATE_PART(HOUR, order_date) BETWEEN 5 AND 11 THEN 'Morning'
            WHEN DATE_PART(HOUR, order_date) BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN DATE_PART(HOUR, order_date) BETWEEN 17 AND 21 THEN 'Evening'
            ELSE 'Night'
        END AS order_time_of_day

    FROM profit_metrics

),

date_parts AS (

    SELECT
        *,

        WEEK(order_date) AS order_week,
        MONTH(order_date) AS order_month,
        QUARTER(order_date) AS order_quarter,
        YEAR(order_date) AS order_year

    FROM order_time_calc

),

shipping_metrics AS (

    SELECT
        *,

        DATEDIFF(day, order_date, shipping_date) AS processing_days,
        DATEDIFF(day, shipping_date, delivery_date) AS shipping_days,

        CASE
            WHEN delivery_date IS NOT NULL
                 AND delivery_date <= estimated_delivery_date
            THEN 'On Time'

            WHEN delivery_date IS NOT NULL
                 AND delivery_date > estimated_delivery_date
            THEN 'Delayed'

            WHEN delivery_date IS NULL
                 AND CURRENT_DATE > estimated_delivery_date
            THEN 'Potentially Delayed'

            ELSE 'In Transit'
        END AS delivery_status

    FROM date_parts

),

deduplicated AS (

    SELECT DISTINCT *
    FROM shipping_metrics

)

SELECT *
FROM deduplicated