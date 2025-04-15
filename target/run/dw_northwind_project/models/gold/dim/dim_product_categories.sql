
      
  
    USE [DW_GOLD];
    USE [DW_GOLD];
    
    

    

    
    USE [DW_GOLD];
    EXEC('
        create view "dbo"."dim_product_categories__dbt_tmp_vw" as 

WITH staged AS (
  SELECT
    
    lower(convert(varchar(50), hashbytes(''md5'', coalesce(convert(varchar(8000), concat(coalesce(cast(category_id as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''), ''-'', coalesce(cast(category_name as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''))), '''')), 2))
 AS category_sk,

    category_id,
    COALESCE(category_name, ''No Category Name'') AS category_name,
    COALESCE(description, ''No Description'') AS category_description,
    COALESCE(last_modified, CAST(''1900-01-01'' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST(''1900-01-01'' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    "DW_SILVER"."dbo"."SLV_PRODUCTS__categories"
  
  WHERE
    category_id IS NOT NULL
),

existing_keys AS (
  

  SELECT
    CAST(NULL AS INT) AS category_id

  WHERE 1 = 0

  
),

staged_prepared AS (
  SELECT
    s.category_sk,
    s.category_id,
    s.category_name,
    s.category_description,
    s.last_modified,
    s.silver_loaded_at,
    s.gold_loaded_at,
    CASE
      WHEN e.category_id IS NULL THEN CAST(''1900-01-01'' AS DATETIME)
      ELSE s.gold_loaded_at
    END AS valid_from,
    CAST(''9999-12-31'' AS DATETIME) AS valid_to,
    1 AS is_current

  FROM
    staged s
    LEFT JOIN existing_keys e
      ON s.category_id = e.category_id
),

final_data AS (
  SELECT * FROM staged_prepared

  

  UNION ALL

  -- Technical row for records where category_id is NULL

  SELECT
    ''-1'' AS category_sk,
    -1 AS category_id,
    ''Unknown Category'' AS category_name,
    ''Unknown Description'' AS category_description,
    CAST(''1900-01-01'' AS DATETIME) AS last_modified,
    CAST(''1900-01-01'' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST(''1900-01-01'' AS DATETIME) AS valid_from,
    CAST(''9999-12-31'' AS DATETIME) AS valid_to,
    1 AS is_current

  UNION ALL

  -- Technical row for records where category_id is not found

  SELECT
    ''-2'' AS category_sk,
    -2 AS category_id,
    ''Unregistered Category'' AS category_name,
    ''Unknown Description'' AS category_description,
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
            SELECT * INTO "DW_GOLD"."dbo"."dim_product_categories" FROM "DW_GOLD"."dbo"."dim_product_categories__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.dim_product_categories__dbt_tmp_vw')



    
    use [DW_GOLD];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_dim_product_categories_cci'
        AND object_id=object_id('dbo_dim_product_categories')
    )
    DROP index "dbo"."dim_product_categories".dbo_dim_product_categories_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_dim_product_categories_cci
    ON "dbo"."dim_product_categories"

   


  
  