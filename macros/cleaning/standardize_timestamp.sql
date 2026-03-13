{% macro standardize_timestamp(column_name) %}

COALESCE(
    TRY_TO_TIMESTAMP({{ column_name }}::STRING),
    TRY_TO_TIMESTAMP({{ column_name }}::STRING, 'YYYY-MM-DD HH24:MI:SS'),
    TRY_TO_TIMESTAMP({{ column_name }}::STRING, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
)

{% endmacro %}