{% macro remove_extra_spaces(col) %}
  CASE
    WHEN TRIM({{ col }}) = '' OR TRIM({{ col }}) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM({{ col }}), ' ')
      WHERE
        LEN(value) > 0
    )
  END
{% endmacro %}
