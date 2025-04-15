

WITH source AS (
  SELECT
    category_id,
    category_name,
    description,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_PRODUCTS__categories"

  
    WHERE
      last_modified > (
        SELECT MAX(last_modified)
        FROM "DW_SILVER"."dbo"."SLV_PRODUCTS__categories"
      )
  
),

cleaned AS (
  SELECT
    category_id,
    
  CASE
    WHEN TRIM(category_name) = '' OR TRIM(category_name) = '-' THEN NULL
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
              TRIM(category_name),
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
 AS category_name,
    
  CASE
    WHEN TRIM(description) = '' OR TRIM(description) = '-' THEN NULL
    ELSE UPPER(LEFT(TRIM(description), 1)) + (
      SELECT
        LOWER(STRING_AGG(value, ' '))
      FROM
        STRING_SPLIT(
          SUBSTRING(TRIM(description), 2, LEN(TRIM(description))),
          ' '
        )
      WHERE
        LEN(value) > 0
    )
  END
 AS description,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    category_id IS NOT NULL
)

SELECT
  CAST(category_id AS SMALLINT) AS category_id,
  CAST(category_name AS VARCHAR(60)) AS category_name,
  CAST(description AS VARCHAR(100)) AS description,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  category_name IS NOT NULL;