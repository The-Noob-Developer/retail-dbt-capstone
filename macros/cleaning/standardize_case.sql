{% macro standardize_case(column_name, case_type='upper') %}

{% if case_type == 'upper' %}
UPPER({{ column_name }})
{% elif case_type == 'lower' %}
LOWER({{ column_name }})
{% else %}
{{ column_name }}
{% endif %}

{% endmacro %}