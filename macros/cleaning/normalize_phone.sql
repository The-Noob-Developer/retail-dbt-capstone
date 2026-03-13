-- {%macro normalize_phone(column_name)%}
-- CASE 
--     WHEN LENGTH(REGEXP_REPLACE({{column_name}} , '[^0-9]', '')) < 10
--         THEN NULL
--     ELSE REGEXP_REPLACE({{column_name}} , '[^0-9]', '')
-- END
-- {%endmacro%}


{% macro normalize_phone(column_name) %}
CASE 
    WHEN LENGTH(REGEXP_REPLACE({{ column_name }}, '[^0-9]', '')) < 10 THEN NULL -- If there are less digits eg: 555 1234 56X
    
    WHEN LENGTH(REGEXP_REPLACE({{ column_name }}, '[^0-9]', '')) = 11 -- If country code is there remove it
         AND LEFT(REGEXP_REPLACE({{ column_name }}, '[^0-9]', ''), 1) = '1'
        THEN SUBSTRING(REGEXP_REPLACE({{ column_name }}, '[^0-9]', ''), 2)
        
    ELSE REGEXP_REPLACE({{ column_name }}, '[^0-9]', '') -- Normal Case
END
{% endmacro %}