{{ config(
  materialized = 'incremental',
  unique_key = 'order_detail_id',
  database = 'DW_SILVER',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
  SELECT
    order_detail_id,
    order_id,
    product_id,
    unit_price,
    quantity,
    discount,
    last_modified

  FROM
    {{ source('db_orders', 'DB_ORDERS__order_details') }}

  {% if is_incremental() %}
    WHERE
      last_modified > (
        SELECT MAX(last_modified)
        FROM {{ this }}
      )
  {% endif %}
),

cleaned AS (
  SELECT
    order_detail_id,
    order_id,
    product_id,
    {{ null_if_negative('unit_price') }} AS unit_price,
    {{ null_if_negative('quantity') }} AS quantity,
    {{ null_if_negative('discount') }} AS discount,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    order_detail_id IS NOT NULL AND
    order_id IS NOT NULL AND
    product_id IS NOT NULL
)

SELECT
  CAST(order_detail_id AS INT) AS order_detail_id,
  CAST(order_id AS INT) AS order_id,
  CAST(product_id AS SMALLINT) AS product_id,
  CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
  CAST(quantity AS SMALLINT) AS quantity,
  CAST(discount AS DECIMAL(10, 2)) AS discount,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned;
