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
  
  UNION ALL

  -- Technical row for records where order_id is NULL

  SELECT
    '-1' AS order_sk,
    -1 AS order_id,
    'Unknown Name' AS ship_to_name,
    'Unknown Address' AS ship_to_address,
    'Unknown City' AS ship_to_city,
    'Unknown Region' AS ship_to_region,
    'Unknown Postal Code' AS ship_to_postal_code,
    'Unknown Country' AS ship_to_country,
    -1 AS freight,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  UNION ALL

  -- Technical row for records where customer_id is not found

  SELECT
    '-2' AS order_sk,
    -2 AS order_id,
    'Unregistered Name' AS ship_to_name,
    'Unknown Address' AS ship_to_address,
    'Unknown City' AS ship_to_city,
    'Unknown Region' AS ship_to_region,
    'Unknown Postal Code' AS ship_to_postal_code,
    'Unknown Country' AS ship_to_country,
    -1 AS freight,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at
)

SELECT * FROM final;
