
      
  
    USE [DW_SILVER];
    USE [DW_SILVER];
    
    

    

    
    USE [DW_SILVER];
    EXEC('
        create view "dbo"."SLV_SHIPPERS__shippers__dbt_tmp_vw" as 

WITH source AS (
  SELECT
    shipper_id,
    company_name,
    phone,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_SHIPPERS__shippers"

  
),

cleaned AS (
  SELECT
    shipper_id,
    
  CASE
    WHEN TRIM(company_name) = '''' OR TRIM(company_name) = ''-'' THEN NULL
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
              TRIM(company_name),
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
 AS company_name,
    
  CASE
    WHEN TRIM(phone) = '''' OR TRIM(phone) = ''-'' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, '' '')
      FROM
        STRING_SPLIT(TRIM(phone), '' '')
      WHERE
        LEN(value) > 0
    )
  END
 AS phone,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    shipper_id IS NOT NULL
)

SELECT
  CAST(shipper_id AS SMALLINT) AS shipper_id,
  CAST(company_name AS VARCHAR(100)) AS company_name,
  CAST(phone AS VARCHAR(20)) AS phone,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  company_name IS NOT NULL;;
    ')

EXEC('
            SELECT * INTO "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers" FROM "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.SLV_SHIPPERS__shippers__dbt_tmp_vw')



    
    use [DW_SILVER];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_SLV_SHIPPERS__shippers_cci'
        AND object_id=object_id('dbo_SLV_SHIPPERS__shippers')
    )
    DROP index "dbo"."SLV_SHIPPERS__shippers".dbo_SLV_SHIPPERS__shippers_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_SLV_SHIPPERS__shippers_cci
    ON "dbo"."SLV_SHIPPERS__shippers"

   


  
  