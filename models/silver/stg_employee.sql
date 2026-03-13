{{ config(
    materialized='table',
    schema='silver'
) }}

WITH json_extracted AS (

    SELECT
        {{ trim_whitespace("f.value:employee_id::STRING") }} AS employee_id,

        {{ standardize_case(trim_whitespace("f.value:first_name::STRING")) }} AS first_name,
        {{ standardize_case(trim_whitespace("f.value:last_name::STRING")) }} AS last_name,

        {{ validate_email(trim_whitespace("f.value:email::STRING")) }} AS email,

        {{ normalize_phone(remove_special_characters(trim_whitespace("f.value:phone::STRING"))) }} AS phone,

        {{ standardize_date("f.value:date_of_birth") }} AS date_of_birth,
        {{ standardize_date("f.value:hire_date") }} AS hire_date,

        {{ standardize_case(trim_whitespace("f.value:department::STRING")) }} AS department,
        {{ standardize_case(trim_whitespace("f.value:education::STRING")) }} AS education,
        {{ standardize_case(trim_whitespace("f.value:employment_status::STRING")) }} AS employment_status,

        {{ standardize_case(trim_whitespace("f.value:role::STRING")) }} AS role,

        {{ trim_whitespace("f.value:manager_id::STRING") }} AS manager_id,
        {{ trim_whitespace("f.value:work_location::STRING") }} AS work_location,

        {{ clean_currency("f.value:salary::STRING") }} AS salary,

        f.value:performance_rating::FLOAT AS performance_rating,

        f.value:current_sales::FLOAT AS current_sales,
        f.value:sales_target::FLOAT AS sales_target,

        -- address fields kept as-is
        {{ trim_whitespace("f.value:address.street::STRING") }} AS street,
        {{ trim_whitespace("f.value:address.city::STRING") }} AS city,
        {{ trim_whitespace("f.value:address.state::STRING") }} AS state,
        {{ trim_whitespace("f.value:address.zip_code::STRING") }} AS zip_code,

        {{ standardize_date("f.value:last_modified_date") }} AS last_modified_date,

        ARRAY_TO_STRING(f.value:certifications, ', ') AS certifications

    FROM RETAIL_DB.BRONZE.BR_EMPLOYEE t,
         LATERAL FLATTEN(input => t.value:employees_data) f

),

name_build AS (

    SELECT
        *,
        CONCAT(first_name, ' ', last_name) AS full_name
    FROM json_extracted

),

tenure_calc AS (

    SELECT
        *,
        DATEDIFF(year, hire_date, CURRENT_DATE) AS tenure_years
    FROM name_build

),

role_standardization AS (

    SELECT
        *,

        CASE
            WHEN role ILIKE 'Sales Associate' THEN 'Associate'
            WHEN role ILIKE 'Store Manager' THEN 'Manager'
            WHEN role ILIKE 'Senior Manager' THEN 'Senior Manager'
            ELSE role
        END AS standardized_role

    FROM tenure_calc

),

performance_metrics AS (

    SELECT
        *,

        (current_sales / NULLIF(sales_target,0)) * 100
        AS target_achievement_percentage

    FROM role_standardization

),

deduplicated AS (

    SELECT DISTINCT *
    FROM performance_metrics

)

SELECT *
FROM deduplicated