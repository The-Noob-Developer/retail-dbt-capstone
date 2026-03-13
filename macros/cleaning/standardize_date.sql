{% macro standardize_date(column_name) %}

CAST(
    COALESCE(
        TRY_TO_TIMESTAMP({{ column_name }}::STRING),
        TRY_TO_TIMESTAMP({{ column_name }}::STRING, 'YYYY-MM-DD'),
        TRY_TO_TIMESTAMP({{ column_name }}::STRING, 'DD-MM-YYYY'),
        TRY_TO_TIMESTAMP({{ column_name }}::STRING, 'MM-DD-YYYY')
    ) AS DATE
)

{% endmacro %}