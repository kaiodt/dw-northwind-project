{% macro null_if_invalid(col, valid_values) %}
  CASE
    WHEN {{ col }} NOT IN ({{ valid_values | join(', ') }}) THEN NULL
    ELSE {{ col }}
  END
{% endmacro %}
