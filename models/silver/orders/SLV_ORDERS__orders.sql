{{ config(
  materialized = 'incremental',
  unique_key = 'order_id',
  database = 'DW_SILVER',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
  SELECT
    order_id,
    customer_id,
    employee_id,
    order_date,
    required_date,
    shipped_date,
    ship_via,
    freight,
    ship_name,
    ship_address,
    ship_city,
    ship_region,
    ship_postal_code,
    ship_country,
    last_modified

  FROM
    {{ source('db_orders', 'DB_ORDERS__orders') }}

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
    order_id,
    customer_id,
    employee_id,
    order_date,
    required_date,
    shipped_date,
    ship_via,
    {{ null_if_negative('freight') }} AS freight,
    {{ remove_extra_spaces('ship_name') }} AS ship_name,
    {{ remove_extra_spaces('ship_address') }} AS ship_address,
    {{ titlecase_with_separators('ship_city') }} AS ship_city,
    {{ remove_extra_spaces('ship_region') }} AS ship_region,
    {{ remove_extra_spaces('ship_postal_code') }} AS ship_postal_code,
    {{ titlecase_country('ship_country') }} AS ship_country,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    order_id IS NOT NULL
)

SELECT
  CAST(order_id AS INT) AS order_id,
  CAST(customer_id AS SMALLINT) AS customer_id,
  CAST(employee_id AS SMALLINT) AS employee_id,
  CAST(order_date AS DATE) AS order_date,
  CAST(required_date AS DATE) AS required_date,
  CAST(shipped_date AS DATE) AS shipped_date,
  CAST(ship_via AS SMALLINT) AS ship_via,
  CAST(freight AS DECIMAL(10, 2)) AS freight,
  CAST(ship_name AS VARCHAR(60)) AS ship_name,
  CAST(ship_address AS VARCHAR(100)) AS ship_address,
  CAST(ship_city AS VARCHAR(30)) AS ship_city,
  CAST(ship_region AS VARCHAR(20)) AS ship_region,
  CAST(ship_postal_code AS VARCHAR(10)) AS ship_postal_code,
  CAST(ship_country AS VARCHAR(20)) AS ship_country,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned;
