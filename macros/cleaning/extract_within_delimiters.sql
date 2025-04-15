{% macro extract_within_delimiters(col, delimiter) %}
  CASE
    WHEN TRIM({{ col }}) = '' OR TRIM({{ col }}) = '-' THEN NULL
    ELSE (
      SELECT
        LOWER(
          RTRIM(
            SUBSTRING(
              STRING_AGG(value, ' '),
              CHARINDEX('{{ delimiter }}', STRING_AGG(value, ' ')) + 1,
              CHARINDEX(
                '{{ delimiter }}',
                STRING_AGG(value, ' '),
                CHARINDEX('{{ delimiter }}', STRING_AGG(value, ' ')) + 1) -
                  CHARINDEX('{{ delimiter }}', STRING_AGG(value, ' ')
                )
              ),
              '{{ delimiter }}'
          )
      )
      FROM
        STRING_SPLIT(TRIM({{ col }}), ' ')
      WHERE
        LEN(value) > 0
    )
  END
{% endmacro %}
