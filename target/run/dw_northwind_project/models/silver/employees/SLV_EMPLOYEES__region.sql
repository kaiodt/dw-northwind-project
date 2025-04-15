
      
  
    USE [DW_SILVER];
    USE [DW_SILVER];
    
    

    

    
    USE [DW_SILVER];
    EXEC('
        create view "dbo"."SLV_EMPLOYEES__region__dbt_tmp_vw" as 

WITH source AS (
  SELECT
    region_id,
    region_description,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_EMPLOYEES__region"

  
),

cleaned AS (
  SELECT
    region_id,
    
  CASE
    WHEN TRIM(region_description) = '''' OR TRIM(region_description) = ''-'' THEN NULL
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
              TRIM(region_description),
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
  region_description IS NOT NULL;;
    ')

EXEC('
            SELECT * INTO "DW_SILVER"."dbo"."SLV_EMPLOYEES__region" FROM "DW_SILVER"."dbo"."SLV_EMPLOYEES__region__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.SLV_EMPLOYEES__region__dbt_tmp_vw')



    
    use [DW_SILVER];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_SLV_EMPLOYEES__region_cci'
        AND object_id=object_id('dbo_SLV_EMPLOYEES__region')
    )
    DROP index "dbo"."SLV_EMPLOYEES__region".dbo_SLV_EMPLOYEES__region_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_SLV_EMPLOYEES__region_cci
    ON "dbo"."SLV_EMPLOYEES__region"

   


  
  