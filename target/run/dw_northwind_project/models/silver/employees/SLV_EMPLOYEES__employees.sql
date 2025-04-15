
      
  
    USE [DW_SILVER];
    USE [DW_SILVER];
    
    

    

    
    USE [DW_SILVER];
    EXEC('
        create view "dbo"."SLV_EMPLOYEES__employees__dbt_tmp_vw" as 

WITH source AS (
  SELECT
    employee_id,
    last_name,
    first_name,
    title,
    title_of_courtesy,
    birth_date,
    hire_date,
    address,
    city,
    region,
    postal_code,
    country,
    home_phone,
    extension,
    notes,
    reports_to,
    photo_path,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_EMPLOYEES__employees"

  
),

cleaned AS (
  SELECT
    employee_id,
    
  CASE
    WHEN TRIM(last_name) = '''' OR TRIM(last_name) = ''-'' THEN NULL
    ELSE (
      SELECT
        REPLACE(
          REPLACE(
            STRING_AGG(
              UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
              '' ''
            ),
            '' - '', ''-''
          ),
          '' / '', ''/''
        )
      FROM
        STRING_SPLIT(
          REPLACE(
            REPLACE(
              TRIM(last_name),
              ''-'', '' - ''
            ),
            ''/'', '' / ''
          ),
          '' ''
        )
      WHERE
        LEN(value) > 0
    )
  END
 AS last_name,
    
  CASE
    WHEN TRIM(first_name) = '''' OR TRIM(first_name) = ''-'' THEN NULL
    ELSE (
      SELECT
        REPLACE(
          REPLACE(
            STRING_AGG(
              UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
              '' ''
            ),
            '' - '', ''-''
          ),
          '' / '', ''/''
        )
      FROM
        STRING_SPLIT(
          REPLACE(
            REPLACE(
              TRIM(first_name),
              ''-'', '' - ''
            ),
            ''/'', '' / ''
          ),
          '' ''
        )
      WHERE
        LEN(value) > 0
    )
  END
 AS first_name,
    
  CASE
    WHEN TRIM(title) = '''' OR TRIM(title) = ''-'' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(
          UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
          '' ''
        )
      FROM
        STRING_SPLIT(TRIM(title), '' '')
      WHERE
        LEN(value) > 0
    )
  END
 AS title,
    
  CASE
    WHEN TRIM(title_of_courtesy) = '''' OR TRIM(title_of_courtesy) = ''-'' THEN NULL
    ELSE UPPER(LEFT(TRIM(title_of_courtesy), 1)) + (
      SELECT
        LOWER(STRING_AGG(value, '' ''))
      FROM
        STRING_SPLIT(
          SUBSTRING(TRIM(title_of_courtesy), 2, LEN(TRIM(title_of_courtesy))),
          '' ''
        )
      WHERE
        LEN(value) > 0
    )
  END
 AS title_of_courtesy,
    birth_date,
    hire_date,
    
  CASE
    WHEN TRIM(address) = '''' OR TRIM(address) = ''-'' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, '' '')
      FROM
        STRING_SPLIT(TRIM(address), '' '')
      WHERE
        LEN(value) > 0
    )
  END
 AS address,
    
  CASE
    WHEN TRIM(city) = '''' OR TRIM(city) = ''-'' THEN NULL
    ELSE (
      SELECT
        REPLACE(
          REPLACE(
            STRING_AGG(
              UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
              '' ''
            ),
            '' - '', ''-''
          ),
          '' / '', ''/''
        )
      FROM
        STRING_SPLIT(
          REPLACE(
            REPLACE(
              TRIM(city),
              ''-'', '' - ''
            ),
            ''/'', '' / ''
          ),
          '' ''
        )
      WHERE
        LEN(value) > 0
    )
  END
 AS city,
    
  CASE
    WHEN TRIM(region) = '''' OR TRIM(region) = ''-'' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, '' '')
      FROM
        STRING_SPLIT(TRIM(region), '' '')
      WHERE
        LEN(value) > 0
    )
  END
 AS region,
    
  CASE
    WHEN TRIM(postal_code) = '''' OR TRIM(postal_code) = ''-'' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, '' '')
      FROM
        STRING_SPLIT(TRIM(postal_code), '' '')
      WHERE
        LEN(value) > 0
    )
  END
 AS postal_code,
    
  CASE
    WHEN TRIM(country) = '''' OR TRIM(country) = ''-'' THEN NULL
    WHEN UPPER(TRIM(country)) IN (''USA'', ''UK'') THEN UPPER(TRIM(country))
    ELSE (
      SELECT
        REPLACE(
          REPLACE(
            STRING_AGG(
              UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
              '' ''
            ),
            '' - '', ''-''
          ),
          '' / '', ''/''
        )
      FROM
        STRING_SPLIT(
          REPLACE(
            REPLACE(
              TRIM(country),
              ''-'', '' - ''
            ),
            ''/'', '' / ''
          ),
          '' ''
        )
      WHERE
        LEN(value) > 0
    )
  END
 AS country,
    
  CASE
    WHEN TRIM(home_phone) = '''' OR TRIM(home_phone) = ''-'' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, '' '')
      FROM
        STRING_SPLIT(TRIM(home_phone), '' '')
      WHERE
        LEN(value) > 0
    )
  END
 AS home_phone,
    
  CASE
    WHEN TRIM(extension) = '''' OR TRIM(extension) = ''-'' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, '' '')
      FROM
        STRING_SPLIT(TRIM(extension), '' '')
      WHERE
        LEN(value) > 0
    )
  END
 AS extension,
    
  CASE
    WHEN TRIM(notes) = '''' OR TRIM(notes) = ''-'' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, '' '')
      FROM
        STRING_SPLIT(TRIM(notes), '' '')
      WHERE
        LEN(value) > 0
    )
  END
 AS notes,
    reports_to,
    LOWER(
  CASE
    WHEN TRIM(photo_path) = '''' OR TRIM(photo_path) = ''-'' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, '' '')
      FROM
        STRING_SPLIT(TRIM(photo_path), '' '')
      WHERE
        LEN(value) > 0
    )
  END
) AS photo_path,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    employee_id IS NOT NULL
)

SELECT
  CAST(employee_id AS SMALLINT) AS employee_id,
  CAST(last_name AS VARCHAR(20)) AS last_name,
  CAST(first_name AS VARCHAR(10)) AS first_name,
  CAST(title AS VARCHAR(30)) AS title,
  CAST(title_of_courtesy AS VARCHAR(25)) AS title_of_courtesy,
  CAST(birth_date AS DATE) AS birth_date,
  CAST(hire_date AS DATE) AS hire_date,
  CAST(address AS VARCHAR(60)) AS address,
  CAST(city AS VARCHAR(15)) AS city,
  CAST(region AS VARCHAR(15)) AS region,
  CAST(postal_code AS VARCHAR(10)) AS postal_code,
  CAST(country AS VARCHAR(15)) AS country,
  CAST(home_phone AS VARCHAR(24)) AS home_phone,
  CAST(extension AS VARCHAR(4)) AS extension,
  CAST(notes AS VARCHAR(1000)) AS notes,
  CAST(reports_to AS SMALLINT) AS reports_to,
  CAST(photo_path AS VARCHAR(255)) AS photo_path,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  last_name IS NOT NULL AND
  first_name IS NOT NULL;;
    ')

EXEC('
            SELECT * INTO "DW_SILVER"."dbo"."SLV_EMPLOYEES__employees" FROM "DW_SILVER"."dbo"."SLV_EMPLOYEES__employees__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.SLV_EMPLOYEES__employees__dbt_tmp_vw')



    
    use [DW_SILVER];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_SLV_EMPLOYEES__employees_cci'
        AND object_id=object_id('dbo_SLV_EMPLOYEES__employees')
    )
    DROP index "dbo"."SLV_EMPLOYEES__employees".dbo_SLV_EMPLOYEES__employees_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_SLV_EMPLOYEES__employees_cci
    ON "dbo"."SLV_EMPLOYEES__employees"

   


  
  