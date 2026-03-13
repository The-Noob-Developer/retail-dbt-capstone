{%macro remove_special_characters(column_name)%}
REGEXP_REPLACE({{column_name}} , '[^A-Za-z0-9]' , '')
{%endmacro%}