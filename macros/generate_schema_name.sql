{% macro generate_schema_name(custom_schema_name, node) %}
    {{ custom_schema_name }}
{% endmacro %}

-- VALIDATIONS:
    -- validate_email.sql
    -- trim_whitespace.sql
    -- standardize_date.sql
    -- remove_special_characters.sql
    -- normalize_phone.sql
    -- standardize_case.sql
    -- clean_currency.sql