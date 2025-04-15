
      
  
    USE [DW_SILVER];
    USE [DW_SILVER];
    
    

    

    
    USE [DW_SILVER];
    EXEC('
        create view "dbo"."SLV_EMPLOYEES__employee_territories__dbt_tmp_vw" as 

WITH source AS (
  SELECT
    employee_id,
    territory_id,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_EMPLOYEES__employee_territories"

  
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
  cleaned;;
    ')

EXEC('
            SELECT * INTO "DW_SILVER"."dbo"."SLV_EMPLOYEES__employee_territories" FROM "DW_SILVER"."dbo"."SLV_EMPLOYEES__employee_territories__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.SLV_EMPLOYEES__employee_territories__dbt_tmp_vw')



    
    use [DW_SILVER];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_SLV_EMPLOYEES__employee_territories_cci'
        AND object_id=object_id('dbo_SLV_EMPLOYEES__employee_territories')
    )
    DROP index "dbo"."SLV_EMPLOYEES__employee_territories".dbo_SLV_EMPLOYEES__employee_territories_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_SLV_EMPLOYEES__employee_territories_cci
    ON "dbo"."SLV_EMPLOYEES__employee_territories"

   


  
  