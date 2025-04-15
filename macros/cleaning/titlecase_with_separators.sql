{% macro titlecase_with_separators(col) %}
  CASE
    WHEN TRIM({{ col }}) = '' OR TRIM({{ col }}) = '-' THEN NULL
    ELSE (
      SELECT
        REPLACE(
          REPLACE(
            STRING_AGG(
              UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
              ' '
            ),
            ' - ', '-'
          ),
          ' / ', '/'
        )
      FROM
        STRING_SPLIT(
          REPLACE(
            REPLACE(
              TRIM({{ col }}),
              '-', ' - '
            ),
            '/', ' / '
          ),
          ' '
        )
      WHERE
        LEN(value) > 0
    )
  END
{% endmacro %}
