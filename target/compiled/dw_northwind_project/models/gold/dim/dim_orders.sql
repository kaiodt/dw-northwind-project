

WITH source AS (
  SELECT
    order_id,
    COALESCE(ship_name, 'No Name') AS ship_to_name,
    COALESCE(ship_address, 'No Address') AS ship_to_address,
    COALESCE(ship_city, 'No City') AS ship_to_city,
    COALESCE(ship_region, 'No Region') AS ship_to_region,
    COALESCE(ship_postal_code, 'No Postal Code') AS ship_to_postal_code,
    COALESCE(ship_country, 'No Country') AS ship_to_country,
    COALESCE(freight, 0) AS freight,
    COALESCE(last_modified, CAST('1900-01-01' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST('1900-01-01' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    "DW_SILVER"."dbo"."SLV_ORDERS__orders"
  
  WHERE
    order_id IS NOT NULL
),

final AS (
  SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), coalesce(cast(order_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_')), '')), 2))
 AS order_sk,
    *

  FROM
    source
)

SELECT * FROM final;