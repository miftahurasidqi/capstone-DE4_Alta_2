{% macro normalize_phone_number(column_name) %}
    replace(ltrim({{ column_name }}, '+'), ' ', '')
{% endmacro %}
