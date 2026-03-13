{{ config(
    materialized='table',
    schema='silver'
) }}

WITH json_extracted AS (

    SELECT
        {{ trim_whitespace("f.value:supplier_id::STRING") }} AS supplier_id,

        {{ standardize_case(trim_whitespace("f.value:supplier_name::STRING")) }} AS supplier_name,
        {{ standardize_case(trim_whitespace("f.value:supplier_type::STRING")) }} AS supplier_type,

        {{ trim_whitespace("f.value:tax_id::STRING") }} AS tax_id,

        {{ trim_whitespace("f.value:website::STRING") }} AS website,

        f.value:is_active::BOOLEAN AS is_active,

        {{ standardize_date("f.value:last_modified_date") }} AS last_modified_date,
        {{ standardize_date("f.value:last_order_date") }} AS last_order_date,

        f.value:year_established::INT AS year_established,
        f.value:lead_time_days::INT AS lead_time_days,
        f.value:minimum_order_quantity::INT AS minimum_order_quantity,

        {{ trim_whitespace("f.value:payment_terms::STRING") }} AS payment_terms,
        {{ standardize_case(trim_whitespace("f.value:preferred_carrier::STRING")) }} AS preferred_carrier,

        {{ standardize_case(trim_whitespace("f.value:credit_rating::STRING")) }} AS credit_rating,

        -- contact information
        {{ trim_whitespace("f.value:contact_information.address::STRING") }} AS address,
        {{ standardize_case(trim_whitespace("f.value:contact_information.contact_person::STRING")) }} AS contact_person,
        {{ validate_email(trim_whitespace("f.value:contact_information.email::STRING")) }} AS email,
        {{ normalize_phone(remove_special_characters(trim_whitespace("f.value:contact_information.phone::STRING"))) }} AS phone,

        -- contract details
        {{ trim_whitespace("f.value:contract_details.contract_id::STRING") }} AS contract_id,
        {{ standardize_date("f.value:contract_details.start_date") }} AS contract_start_date,
        {{ standardize_date("f.value:contract_details.end_date") }} AS contract_end_date,

        f.value:contract_details.exclusivity::BOOLEAN AS exclusivity,
        f.value:contract_details.renewal_option::BOOLEAN AS renewal_option,

        -- performance metrics
        f.value:performance_metrics.average_delay_days::FLOAT AS average_delay_days,
        f.value:performance_metrics.defect_rate::FLOAT AS defect_rate,
        f.value:performance_metrics.on_time_delivery_rate::FLOAT AS on_time_delivery_rate,
        {{ standardize_case(trim_whitespace("f.value:performance_metrics.quality_rating::STRING")) }} AS quality_rating,
        f.value:performance_metrics.response_time_hours::FLOAT AS response_time_hours,
        f.value:performance_metrics.returns_percentage::FLOAT AS returns_percentage,

        -- categories supplied
        ARRAY_TO_STRING(f.value:categories_supplied, ', ') AS categories_supplied

    FROM RETAIL_DB.BRONZE.BR_SUPPLIER t,
         LATERAL FLATTEN(input => t.value:suppliers_data) f

),

deduplicated AS (

    SELECT DISTINCT *
    FROM json_extracted

)

SELECT *
FROM deduplicated