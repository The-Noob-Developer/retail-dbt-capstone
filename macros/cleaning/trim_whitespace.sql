{%macro trim_whitespace(column_name)%}
TRIM({{column_name}})
{% endmacro %}