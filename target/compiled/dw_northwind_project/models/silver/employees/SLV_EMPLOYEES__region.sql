

WITH source AS (
  SELECT
    region_id,
    region_description,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_EMPLOYEES__region"

  
    WHERE
      last_modified > (
        SELECT MAX(last_modified)
        FROM "DW_SILVER"."dbo"."SLV_EMPLOYEES__region"
      )
  
),

cleaned AS (
  SELECT
    region_id,
    
  CASE
    WHEN TRIM(region_description) = '' OR TRIM(region_description) = '-' THEN NULL
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
              TRIM(region_description),
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
 AS region_description,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    region_id IS NOT NULL
)

SELECT
  CAST(region_id AS SMALLINT) AS region_id,
  CAST(region_description AS VARCHAR(60)) AS region_description,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  region_description IS NOT NULL;