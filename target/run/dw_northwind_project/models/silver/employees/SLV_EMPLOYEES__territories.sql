
      
  
    USE [DW_SILVER];
    USE [DW_SILVER];
    
    

    

    
    USE [DW_SILVER];
    EXEC('
        create view "dbo"."SLV_EMPLOYEES__territories__dbt_tmp_vw" as 

WITH source AS (
  SELECT
    territory_id,
    territory_description,
    region_id,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_EMPLOYEES__territories"

  
),

cleaned AS (
  SELECT
    territory_id,
    
  CASE
    WHEN TRIM(territory_description) = '''' OR TRIM(territory_description) = ''-'' THEN NULL
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
              TRIM(territory_description),
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
 AS territory_description,
    region_id,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    territory_id IS NOT NULL
)

SELECT
  CAST(territory_id AS SMALLINT) AS territory_id,
  CAST(territory_description AS VARCHAR(60)) AS territory_description,
  CAST(region_id AS SMALLINT) AS region_id,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  territory_description IS NOT NULL;;
    ')

EXEC('
            SELECT * INTO "DW_SILVER"."dbo"."SLV_EMPLOYEES__territories" FROM "DW_SILVER"."dbo"."SLV_EMPLOYEES__territories__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.SLV_EMPLOYEES__territories__dbt_tmp_vw')



    
    use [DW_SILVER];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_SLV_EMPLOYEES__territories_cci'
        AND object_id=object_id('dbo_SLV_EMPLOYEES__territories')
    )
    DROP index "dbo"."SLV_EMPLOYEES__territories".dbo_SLV_EMPLOYEES__territories_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_SLV_EMPLOYEES__territories_cci
    ON "dbo"."SLV_EMPLOYEES__territories"

   


  
  