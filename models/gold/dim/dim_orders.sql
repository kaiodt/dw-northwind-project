{{ config(
  materialized = 'incremental',
  unique_key = 'order_id',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns',
  database = 'DW_GOLD',
  schema = 'dbo'
) }}

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
    {{ ref('SLV_ORDERS__orders') }}
  
  WHERE
    order_id IS NOT NULL
),

final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS order_sk,
    *

  FROM
    source
)

SELECT * FROM final;
