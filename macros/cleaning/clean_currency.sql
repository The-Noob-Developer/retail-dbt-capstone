{%macro clean_currency(column_name)%}
CAST(REGEXP_REPLACE({{column_name}} , '[^0-9.]' , '') AS FLOAT)
{% endmacro%}