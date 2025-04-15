{{ config(
  materialized = 'incremental',
  unique_key = 'product_id',
  database = 'DW_SILVER',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
  SELECT
    product_id,
    product_name,
    supplier_id,
    category_id,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    units_on_order,
    reorder_level,
    discontinued,
    last_modified

  FROM
    {{ source('db_products', 'DB_PRODUCTS__products') }}

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
    product_id,
    {{ titlecase_with_separators('product_name') }} AS product_name,
    supplier_id,
    category_id,
    {{ remove_extra_spaces('quantity_per_unit') }} AS quantity_per_unit,
    {{ null_if_negative('unit_price') }} AS unit_price,
    {{ null_if_negative('units_in_stock') }} AS units_in_stock,
    {{ null_if_negative('units_on_order') }} AS units_on_order,
    {{ null_if_negative('reorder_level') }} AS reorder_level,
    {{ null_if_invalid('discontinued', [0, 1]) }} AS discontinued,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    product_id IS NOT NULL
)

SELECT
  CAST(product_id AS SMALLINT) AS product_id,
  CAST(product_name AS VARCHAR(100)) AS product_name,
  CAST(supplier_id AS SMALLINT) AS supplier_id,
  CAST(category_id AS SMALLINT) AS category_id,
  CAST(quantity_per_unit AS VARCHAR(30)) AS quantity_per_unit,
  CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
  CAST(units_in_stock AS SMALLINT) AS units_in_stock,
  CAST(units_on_order AS SMALLINT) AS units_on_order,
  CAST(reorder_level AS SMALLINT) AS reorder_level,
  CAST(discontinued AS TINYINT) AS discontinued,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  product_name IS NOT NULL;
