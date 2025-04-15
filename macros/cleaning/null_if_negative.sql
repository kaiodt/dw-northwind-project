{% macro null_if_negative(col) %}
  CASE
    WHEN {{ col }} < 0 THEN NULL
    ELSE {{ col }}
  END
{% endmacro %}
