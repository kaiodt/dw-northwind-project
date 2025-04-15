
      
  
    USE [DW_GOLD];
    USE [DW_GOLD];
    
    

    

    
    USE [DW_GOLD];
    EXEC('
        create view "dbo"."dim_territories__dbt_tmp_vw" as -- models/gold/dim/dim_territories.sql


WITH joined_source AS (
  SELECT
    t.territory_id,
    t.territory_description,
    t.region_id,
    r.region_description,
    COALESCE(t.last_modified, r.last_modified) AS last_modified,
    COALESCE(t.silver_loaded_at, r.silver_loaded_at) AS silver_loaded_at

  FROM
    "DW_SILVER"."dbo"."SLV_EMPLOYEES__territories" t
    LEFT JOIN "DW_SILVER"."dbo"."SLV_EMPLOYEES__region" r
      ON t.region_id = r.region_id
),

staged AS (
  SELECT
    
    lower(convert(varchar(50), hashbytes(''md5'', coalesce(convert(varchar(8000), concat(coalesce(cast(territory_id as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''), ''-'', coalesce(cast(territory_description as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''), ''-'', coalesce(cast(region_id as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''), ''-'', coalesce(cast(region_description as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''))), '''')), 2))
 AS territory_sk,

    territory_id,
    COALESCE(territory_description, ''No Territory'') AS territory_description,
    region_id,
    COALESCE(region_description, ''No Region'') AS region_description,
    COALESCE(last_modified, CAST(''1900-01-01'' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST(''1900-01-01'' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    joined_source

  WHERE
    territory_id IS NOT NULL
),

existing_keys AS (
  

  SELECT
    CAST(NULL AS INT) AS territory_id
  
  WHERE
    1 = 0

  
),

staged_prepared AS (
  SELECT
    s.territory_sk,
    s.territory_id,
    s.territory_description,
    s.region_id,
    s.region_description,
    s.last_modified,
    s.silver_loaded_at,
    s.gold_loaded_at,
    CASE
      WHEN e.territory_id IS NULL THEN CAST(''1900-01-01'' AS DATETIME)
      ELSE s.gold_loaded_at
    END AS valid_from,
    CAST(''9999-12-31'' AS DATETIME) AS valid_to,
    1 AS is_current

  FROM
    staged s
    LEFT JOIN existing_keys e
      ON s.territory_id = e.territory_id
),

final_data AS (
  SELECT * FROM staged_prepared

  

  UNION ALL

  -- Technical row for records where territory_id is NULL

  SELECT
      ''-1'' AS territory_sk,
      -1 AS territory_id,
      ''Unknown Territory'' AS territory_description,
      -1 AS region_id,
      ''Unknown Region'' AS region_description,
      CAST(''1900-01-01'' AS DATETIME) AS last_modified,
      CAST(''1900-01-01'' AS DATETIME) AS silver_loaded_at,
      SYSDATETIME() AS gold_loaded_at,
      CAST(''1900-01-01'' AS DATETIME) AS valid_from,
      CAST(''9999-12-31'' AS DATETIME) AS valid_to,
      1 AS is_current

  UNION ALL

  -- Technical row for records where territory_id is not found

  SELECT
      ''-2'' AS territory_sk,
      -2 AS territory_id,
      ''Unregistered Territory'' AS territory_description,
      -1 AS region_id,
      ''Unregistered Region'' AS region_description,
      CAST(''1900-01-01'' AS DATETIME) AS last_modified,
      CAST(''1900-01-01'' AS DATETIME) AS silver_loaded_at,
      SYSDATETIME() AS gold_loaded_at,
      CAST(''1900-01-01'' AS DATETIME) AS valid_from,
      CAST(''9999-12-31'' AS DATETIME) AS valid_to,
      1 AS is_current
)

SELECT * FROM final_data;;
    ')

EXEC('
            SELECT * INTO "DW_GOLD"."dbo"."dim_territories" FROM "DW_GOLD"."dbo"."dim_territories__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.dim_territories__dbt_tmp_vw')



    
    use [DW_GOLD];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_dim_territories_cci'
        AND object_id=object_id('dbo_dim_territories')
    )
    DROP index "dbo"."dim_territories".dbo_dim_territories_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_dim_territories_cci
    ON "dbo"."dim_territories"

   


  
  