
      
  
    USE [DW_GOLD];
    USE [DW_GOLD];
    
    

    

    
    USE [DW_GOLD];
    EXEC('
        create view "dbo"."dim_orders__dbt_tmp_vw" as 

WITH source AS (
  SELECT
    order_id,
    COALESCE(ship_name, ''No Name'') AS ship_to_name,
    COALESCE(ship_address, ''No Address'') AS ship_to_address,
    COALESCE(ship_city, ''No City'') AS ship_to_city,
    COALESCE(ship_region, ''No Region'') AS ship_to_region,
    COALESCE(ship_postal_code, ''No Postal Code'') AS ship_to_postal_code,
    COALESCE(ship_country, ''No Country'') AS ship_to_country,
    COALESCE(freight, 0) AS freight,
    COALESCE(last_modified, CAST(''1900-01-01'' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST(''1900-01-01'' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    "DW_SILVER"."dbo"."SLV_ORDERS__orders"
  
  WHERE
    order_id IS NOT NULL
),

final AS (
  SELECT
    
    lower(convert(varchar(50), hashbytes(''md5'', coalesce(convert(varchar(8000), coalesce(cast(order_id as VARCHAR(8000)), ''_dbt_utils_surrogate_key_null_'')), '''')), 2))
 AS order_sk,
    *

  FROM
    source
)

SELECT * FROM final;;
    ')

EXEC('
            SELECT * INTO "DW_GOLD"."dbo"."dim_orders" FROM "DW_GOLD"."dbo"."dim_orders__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbo.dim_orders__dbt_tmp_vw')



    
    use [DW_GOLD];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbo_dim_orders_cci'
        AND object_id=object_id('dbo_dim_orders')
    )
    DROP index "dbo"."dim_orders".dbo_dim_orders_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbo_dim_orders_cci
    ON "dbo"."dim_orders"

   


  
  