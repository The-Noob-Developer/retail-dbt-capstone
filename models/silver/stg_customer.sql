{{ config(
    materialized='table',
    schema='silver'
) }}

WITH json_extracted AS (

    SELECT
        {{ trim_whitespace("f.value:customer_id::STRING") }}      AS customer_id,

        {{ standardize_case(trim_whitespace("f.value:first_name::STRING")) }}  AS first_name,
        {{ standardize_case(trim_whitespace("f.value:last_name::STRING")) }}   AS last_name,

        {{ standardize_date("f.value:birth_date") }}              AS birth_date,
        {{ standardize_date("f.value:registration_date") }}       AS registration_date,
        {{ standardize_date("f.value:last_purchase_date") }}      AS last_purchase_date,

        {{ validate_email(trim_whitespace("f.value:email::STRING")) }}   AS email,
        {{ normalize_phone(trim_whitespace("f.value:phone::STRING")) }}  AS phone,

        {{ standardize_case(trim_whitespace("f.value:occupation::STRING")) }} AS occupation,
        {{ standardize_case(trim_whitespace("f.value:loyalty_tier::STRING")) }} AS loyalty_tier,

        {{ standardize_case(trim_whitespace("f.value:income_bracket::STRING")) }} AS income_bracket,

        {{ standardize_case(trim_whitespace("f.value:preferred_payment_method::STRING")) }} AS preferred_payment_method,
        {{ standardize_case(trim_whitespace("f.value:preferred_communication::STRING")) }} AS preferred_communication,

        f.value:marketing_opt_in::BOOLEAN AS marketing_opt_in,

        f.value:total_purchases::INT      AS total_purchases,
        {{ clean_currency("f.value:total_spend::STRING") }} AS total_spend,

        -- address fields
        {{ trim_whitespace("f.value:address.street::STRING") }}  AS street,
        {{ trim_whitespace("f.value:address.city::STRING") }}    AS city,
        {{ trim_whitespace("f.value:address.state::STRING") }}   AS state,
        {{ trim_whitespace("f.value:address.country::STRING") }} AS country,
        {{ trim_whitespace("f.value:address.zip_code::STRING") }} AS zip_code,

        f.value:last_modified_date::DATE AS last_modified_date

    FROM RETAIL_DB.BRONZE.BR_CUSTOMER t,
         LATERAL FLATTEN(input => t.value:customers_data) f

),

name_build AS (

    SELECT
        *,
        CONCAT(first_name, ' ', last_name) AS full_name
    FROM json_extracted

),

age_calc AS (

    SELECT
        *,
        DATEDIFF(year, birth_date, CURRENT_DATE) AS customer_age
    FROM name_build

),

customer_segment AS (

    SELECT
        *,

        CASE
            WHEN customer_age BETWEEN 18 AND 35 THEN 'Young'
            WHEN customer_age BETWEEN 36 AND 55 THEN 'Middle-aged'
            WHEN customer_age > 55 THEN 'Senior'
            ELSE 'Unknown'
        END AS customer_segment

    FROM age_calc

),

address_standardized AS (

    SELECT
        *,
        CONCAT(street, ', ', city, ', ', state, ', ', country, ' - ', zip_code)
        AS full_address
    FROM customer_segment

),

deduplicated AS (

    SELECT DISTINCT *
    FROM address_standardized

)

SELECT *
FROM deduplicated