
      
  
    USE [DW_GOLD];
    USE [DW_GOLD];
    
    

    

    
    USE [DW_GOLD];
    EXEC('
        create view "dbo"."bridge_employees_territories__dbt_tmp_vw" as -- models/gold/bridge/bridge_employees_territories.sql


WITH employee_dim AS (
  SELECT
   employee_id,
   employee_sk
  
  FROM
    "DW_GOLD"."dbo"."dim_employees"
  
  WHERE
    is_current = 1
),

territory_dim AS (
  SELECT
    territory_id,
    territory_sk
  
  FROM
    "DW_GOLD"."dbo"."dim_territories"
  
  WHERE
    is_current = 1
),

mapped AS (
  SELECT
    et.employee_id,
    et.territory_id,
    et.last_modified,
    et.silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at
  
  FROM
    "DW_SILVER"."dbo"."SLV_EMPLOYEES__employee_territories" et

  WHERE
    et.employee_id IS NOT NULL AND
    et.territory_id IS NOT NULL
),

final AS (
  SELECT
    COALESCE(e.employee_sk, ''-2'') AS employee_sk,
    COALESCE(t.territory_sk, ''-2'') AS territory_sk,
    m.last_modified,
    m.silver_loaded_at,
    m.gold_loaded_at
  
  FROM
    mapped m
    LEFT JOIN employee_dim e
      ON m.employee_id = e.employee_id
    LEFT JOIN territory_dim t
      ON m.territory_id = t.territory_id
)

SELECT * FROM final;;
    ')

EXEC('
            SELECT * INTO "DW_GOLD"."dbo"."bridge_employees_territories" FROM "DW_GOLD"."dbo"."bridge_employees_territories__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.bridge_employees_territories__dbt_tmp_vw')



    
    use [DW_GOLD];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_bridge_employees_territories_cci'
        AND object_id=object_id('dbo_bridge_employees_territories')
    )
    DROP index "dbo"."bridge_employees_territories".dbo_bridge_employees_territories_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_bridge_employees_territories_cci
    ON "dbo"."bridge_employees_territories"

   


  
  