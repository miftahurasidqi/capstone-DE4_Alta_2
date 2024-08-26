{% macro get_distinct(column) %}
  select distinct {{ column }} from {{ this }}
{% endmacro %}
