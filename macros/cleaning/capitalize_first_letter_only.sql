{% macro capitalize_first_letter_only(col) %}
  CASE
    WHEN TRIM({{ col }}) = '' OR TRIM({{ col }}) = '-' THEN NULL
    ELSE UPPER(LEFT(TRIM({{ col }}), 1)) + (
      SELECT
        LOWER(STRING_AGG(value, ' '))
      FROM
        STRING_SPLIT(
          SUBSTRING(TRIM({{ col }}), 2, LEN(TRIM({{ col }}))),
          ' '
        )
      WHERE
        LEN(value) > 0
    )
  END
{% endmacro %}
