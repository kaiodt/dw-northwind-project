{% macro titlecase_country(col) %}
  CASE
    WHEN TRIM({{ col }}) = '' OR TRIM({{ col }}) = '-' THEN NULL
    WHEN UPPER(TRIM({{ col }})) IN ('USA', 'UK') THEN UPPER(TRIM({{ col }}))
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
