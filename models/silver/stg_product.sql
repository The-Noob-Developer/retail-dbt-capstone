{{ config(
    materialized='table',
    schema='silver'
) }}

WITH json_extracted AS (

    SELECT
        {{ trim_whitespace("f.value:product_id::STRING") }} AS product_id,
        {{ standardize_case(trim_whitespace("f.value:name::STRING")) }} AS name,

        {{ standardize_case(trim_whitespace("f.value:brand::STRING")) }} AS brand,
        {{ standardize_case(trim_whitespace("f.value:category::STRING")) }} AS category,
        {{ standardize_case(trim_whitespace("f.value:subcategory::STRING")) }} AS subcategory,
        {{ standardize_case(trim_whitespace("f.value:product_line::STRING")) }} AS product_line,

        {{ trim_whitespace("f.value:short_description::STRING") }} AS short_description,
        {{ trim_whitespace("f.value:technical_specs::STRING") }} AS technical_specs,

        {{ trim_whitespace("f.value:color::STRING") }} AS color,
        {{ trim_whitespace("f.value:size::STRING") }} AS size,
        {{ trim_whitespace("f.value:dimensions::STRING") }} AS dimensions,
        {{ trim_whitespace("f.value:weight::STRING") }} AS weight,

        {{ trim_whitespace("f.value:warranty_period::STRING") }} AS warranty_period,

        {{ trim_whitespace("f.value:supplier_id::STRING") }} AS supplier_id,

        {{ standardize_date("f.value:launch_date") }} AS launch_date,
        {{ standardize_date("f.value:last_modified_date") }} AS last_modified_date,

        f.value:is_featured::BOOLEAN AS is_featured,

        f.value:stock_quantity::INT AS stock_quantity,
        f.value:reorder_level::INT AS reorder_level,

        {{ clean_currency("f.value:unit_price::STRING") }} AS unit_price,
        {{ clean_currency("f.value:cost_price::STRING") }} AS cost_price

    FROM RETAIL_DB.BRONZE.BR_PRODUCT t,
         LATERAL FLATTEN(input => t.value:products_data) f

),

product_description AS (

    SELECT
        *,
        CONCAT(name, ' - ', short_description, ' - ', technical_specs)
        AS product_full_description
    FROM json_extracted

),

product_hierarchy AS (

    SELECT
        *,
        CONCAT(category, ' > ', subcategory, ' > ', product_line)
        AS product_hierarchy
    FROM product_description

),

profit_margin_calc AS (

    SELECT
        *,
        ((unit_price - cost_price) / NULLIF(unit_price,0)) * 100
        AS profit_margin_percentage
    FROM product_hierarchy

),

stock_flag AS (

    SELECT
        *,

        CASE
            WHEN stock_quantity < reorder_level
            THEN TRUE
            ELSE FALSE
        END AS low_stock_flag

    FROM profit_margin_calc

),

deduplicated AS (

    SELECT DISTINCT *
    FROM stock_flag

)

SELECT *
FROM deduplicated