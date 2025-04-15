{% macro titlecase_without_separators(col) %}
  CASE
    WHEN TRIM({{ col }}) = '' OR TRIM({{ col }}) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(
          UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
          ' '
        )
      FROM
        STRING_SPLIT(TRIM({{ col }}), ' ')
      WHERE
        LEN(value) > 0
    )
  END
{% endmacro %}
