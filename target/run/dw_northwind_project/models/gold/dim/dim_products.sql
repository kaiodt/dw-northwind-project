
      
  
    USE [DW_GOLD];
    USE [DW_GOLD];
    
    

    

    
    USE [DW_GOLD];
    EXEC('
        create view "dbo"."dim_products__dbt_tmp_vw" as 

WITH staged AS (
  SELECT
    
    lower(convert(varchar(50), hashbytes(''md5'', coalesce(convert(varchar(8000), concat(coalesce(cast(product_id as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''), ''-'', coalesce(cast(unit_price as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''), ''-'', coalesce(cast(discontinued as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_''))), '''')), 2))
 AS product_sk,

    product_id,
    COALESCE(product_name, ''No Product Name'') AS product_name,
    COALESCE(quantity_per_unit, ''No Quantity Info'') AS quantity_per_unit,
    COALESCE(unit_price, -1) AS unit_price,
    COALESCE(units_in_stock, -1) AS units_in_stock,
    COALESCE(units_on_order, -1) AS units_on_order,
    COALESCE(reorder_level, -1) AS reorder_level,
    COALESCE(discontinued, -1) AS discontinued,
    last_modified,
    silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    "DW_SILVER"."dbo"."SLV_PRODUCTS__products"

  WHERE
    product_id IS NOT NULL
),

existing_keys AS (
  

  SELECT
    CAST(NULL AS INT) AS product_id

  WHERE 1 = 0

  
),

staged_prepared AS (
  SELECT
    s.product_sk,
    s.product_id,
    s.product_name,
    s.quantity_per_unit,
    s.unit_price,
    s.units_in_stock,
    s.units_on_order,
    s.reorder_level,
    s.discontinued,
    s.last_modified,
    s.silver_loaded_at,
    s.gold_loaded_at,
    CASE
      WHEN e.product_id IS NULL THEN CAST(''1900-01-01'' AS DATETIME)
      ELSE s.gold_loaded_at
    END AS valid_from,
    CAST(''9999-12-31'' AS DATETIME) AS valid_to,
    1 AS is_current

  FROM
    staged s
    LEFT JOIN existing_keys e
      ON s.product_id = e.product_id
),

final_data AS (
  SELECT * FROM staged_prepared

  

  UNION ALL

  -- Technical row for records where product_id is NULL

  SELECT
    ''-1'' AS product_sk,
    -1 AS product_id,
    ''Unknown Product'' AS product_name,
    ''Unknown Quantity'' AS quantity_per_unit,
    -1 AS unit_price,
    -1 AS units_in_stock,
    -1 AS units_on_order,
    -1 AS reorder_level,
    -1 AS discontinued,
    CAST(''1900-01-01'' AS DATETIME) AS last_modified,
    CAST(''1900-01-01'' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST(''1900-01-01'' AS DATETIME) AS valid_from,
    CAST(''9999-12-31'' AS DATETIME) AS valid_to,
    1 AS is_current

  UNION ALL

  -- Technical row for records where product_id is not found

  SELECT
    ''-2'' AS product_sk,
    -2 AS product_id,
    ''Unregistered Product'' AS product_name,
    ''Unknown Quantity'' AS quantity_per_unit,
    -1 AS unit_price,
    -1 AS units_in_stock,
    -1 AS units_on_order,
    -1 AS reorder_level,
    -1 AS discontinued,
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
            SELECT * INTO "DW_GOLD"."dbo"."dim_products" FROM "DW_GOLD"."dbo"."dim_products__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.dim_products__dbt_tmp_vw')



    
    use [DW_GOLD];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_dim_products_cci'
        AND object_id=object_id('dbo_dim_products')
    )
    DROP index "dbo"."dim_products".dbo_dim_products_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_dim_products_cci
    ON "dbo"."dim_products"

   


  
  