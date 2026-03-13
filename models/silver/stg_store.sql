{{ config(
    materialized='table',
    schema='silver'
) }}

WITH json_extracted AS (

    SELECT
        {{ trim_whitespace("f.value:store_id::STRING") }} AS store_id,

        {{ standardize_case(trim_whitespace("f.value:store_name::STRING")) }} AS store_name,

        {{ standardize_case(trim_whitespace("f.value:store_type::STRING")) }} AS store_type,
        {{ standardize_case(trim_whitespace("f.value:region::STRING")) }} AS region,

        {{ validate_email(trim_whitespace("f.value:email::STRING")) }} AS email,

        {{ normalize_phone(remove_special_characters(trim_whitespace("f.value:phone_number::STRING"))) }} AS phone_number,

        f.value:is_active::BOOLEAN AS is_active,

        {{ standardize_date("f.value:opening_date") }} AS opening_date,
        {{ standardize_date("f.value:last_modified_date") }} AS last_modified_date,

        {{ clean_currency("f.value:monthly_rent::STRING") }} AS monthly_rent,

        {{ clean_currency("f.value:current_sales::STRING") }} AS current_sales,
        {{ clean_currency("f.value:sales_target::STRING") }} AS sales_target,

        f.value:size_sq_ft::INT AS size_sq_ft,
        f.value:employee_count::INT AS employee_count,

        {{ trim_whitespace("f.value:manager_id::STRING") }} AS manager_id,

        -- address fields
        {{ trim_whitespace("f.value:address.street::STRING") }} AS street,
        {{ trim_whitespace("f.value:address.city::STRING") }} AS city,
        {{ trim_whitespace("f.value:address.state::STRING") }} AS state,
        {{ trim_whitespace("f.value:address.country::STRING") }} AS country,
        {{ trim_whitespace("f.value:address.zip_code::STRING") }} AS zip_code,

        -- operating hours
        {{ trim_whitespace("f.value:operating_hours.weekdays::STRING") }} AS weekdays_hours,
        {{ trim_whitespace("f.value:operating_hours.weekends::STRING") }} AS weekends_hours,
        {{ trim_whitespace("f.value:operating_hours.holidays::STRING") }} AS holidays_hours,

        ARRAY_TO_STRING(f.value:services, ', ') AS services

    FROM RETAIL_DB.BRONZE.BR_STORE t,
         LATERAL FLATTEN(input => t.value:stores_data) f

),

store_size_category AS (

    SELECT
        *,

        CASE
            WHEN size_sq_ft < 5000 THEN 'Small'
            WHEN size_sq_ft BETWEEN 5000 AND 10000 THEN 'Medium'
            WHEN size_sq_ft > 10000 THEN 'Large'
        END AS store_size_category

    FROM json_extracted

),

store_age_calc AS (

    SELECT
        *,
        DATEDIFF(year, opening_date, CURRENT_DATE) AS store_age_years
    FROM store_size_category

),

performance_metrics AS (

    SELECT
        *,

        (current_sales / NULLIF(sales_target,0)) * 100
        AS sales_target_achievement_percentage,

        current_sales / NULLIF(size_sq_ft,0)
        AS revenue_per_sq_ft,

        current_sales / NULLIF(employee_count,0)
        AS employee_efficiency

    FROM store_age_calc

),

performance_flag AS (

    SELECT
        *,

        CASE
            WHEN (current_sales / NULLIF(sales_target,0)) * 100 < 90
            THEN TRUE
            ELSE FALSE
        END AS performance_issue_flag

    FROM performance_metrics

),

deduplicated AS (

    SELECT DISTINCT *
    FROM performance_flag

)

SELECT *
FROM deduplicated