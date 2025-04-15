{{ config(
  materialized = 'incremental',
  unique_key = 'product_sk',
  database = 'DW_GOLD',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH staged AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key([
      'product_id',
      'unit_price',
      'discontinued'
    ]) }} AS product_sk,

    product_id,
    COALESCE(product_name, 'No Product Name') AS product_name,
    COALESCE(quantity_per_unit, 'No Quantity Info') AS quantity_per_unit,
    COALESCE(unit_price, -1) AS unit_price,
    COALESCE(units_in_stock, -1) AS units_in_stock,
    COALESCE(units_on_order, -1) AS units_on_order,
    COALESCE(reorder_level, -1) AS reorder_level,
    COALESCE(discontinued, -1) AS discontinued,
    COALESCE(last_modified, CAST('1900-01-01' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST('1900-01-01' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    {{ ref('SLV_PRODUCTS__products') }}

  WHERE
    product_id IS NOT NULL
),

existing_keys AS (
  {% if is_incremental() %}

  SELECT
    DISTINCT product_id

  FROM
    {{ this }}

  {% else %}

  SELECT
    CAST(NULL AS INT) AS product_id

  WHERE 1 = 0

  {% endif %}
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
      WHEN e.product_id IS NULL THEN CAST('1900-01-01' AS DATETIME)
      ELSE s.gold_loaded_at
    END AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  FROM
    staged s
    LEFT JOIN existing_keys e
      ON s.product_id = e.product_id
),

final_data AS (
  SELECT * FROM staged_prepared

  {% if is_incremental() %}

  UNION ALL

  SELECT
    existing.product_sk,
    existing.product_id,
    existing.product_name,
    existing.quantity_per_unit,
    existing.unit_price,
    existing.units_in_stock,
    existing.units_on_order,
    existing.reorder_level,
    existing.discontinued,
    existing.last_modified,
    existing.silver_loaded_at,
    existing.gold_loaded_at,
    existing.valid_from,
    CAST(
      DATEADD(SECOND, -1, staged_prepared.gold_loaded_at) AS DATETIME
    ) AS valid_to,
    0 AS is_current

  FROM
    {{ this }} AS existing
    INNER JOIN staged_prepared ON
      existing.product_id = staged_prepared.product_id

  WHERE
    existing.is_current = 1 AND
    {{ dbt_utils.generate_surrogate_key([
      'existing.product_id',
      'existing.unit_price',
      'existing.discontinued'
    ]) }}
    !=
    {{ dbt_utils.generate_surrogate_key([
      'staged_prepared.product_id',
      'staged_prepared.unit_price',
      'staged_prepared.discontinued'
    ]) }}

  {% endif %}

  UNION ALL

  -- Technical row for records where product_id is NULL

  SELECT
    '-1' AS product_sk,
    -1 AS product_id,
    'Unknown Product' AS product_name,
    'Unknown Quantity' AS quantity_per_unit,
    -1 AS unit_price,
    -1 AS units_in_stock,
    -1 AS units_on_order,
    -1 AS reorder_level,
    -1 AS discontinued,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  UNION ALL

  -- Technical row for records where product_id is not found

  SELECT
    '-2' AS product_sk,
    -2 AS product_id,
    'Unregistered Product' AS product_name,
    'Unknown Quantity' AS quantity_per_unit,
    -1 AS unit_price,
    -1 AS units_in_stock,
    -1 AS units_on_order,
    -1 AS reorder_level,
    -1 AS discontinued,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current
)

SELECT * FROM final_data;
