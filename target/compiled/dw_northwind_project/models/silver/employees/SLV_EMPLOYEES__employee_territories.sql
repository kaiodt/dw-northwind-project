

WITH source AS (
  SELECT
    employee_id,
    territory_id,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_EMPLOYEES__employee_territories"

  
    WHERE
      last_modified > (
        SELECT MAX(last_modified)
        FROM "DW_SILVER"."dbo"."SLV_EMPLOYEES__employee_territories"
      )
  
),

cleaned AS (
  SELECT
    employee_id,
    territory_id,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    employee_id IS NOT NULL AND
    territory_id IS NOT NULL
)

SELECT
  CAST(employee_id AS SMALLINT) AS employee_id,
  CAST(territory_id AS SMALLINT) AS territory_id,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned;