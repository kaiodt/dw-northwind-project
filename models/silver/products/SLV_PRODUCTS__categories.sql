{{ config(
  materialized = 'incremental',
  unique_key = 'category_id',
  database = 'DW_SILVER',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
  SELECT
    category_id,
    category_name,
    description,
    last_modified

  FROM
    {{ source('db_products', 'DB_PRODUCTS__categories') }}

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
    category_id,
    {{ titlecase_with_separators('category_name') }} AS category_name,
    {{ capitalize_first_letter_only('description') }} AS description,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    category_id IS NOT NULL
)

SELECT
  CAST(category_id AS SMALLINT) AS category_id,
  CAST(category_name AS VARCHAR(60)) AS category_name,
  CAST(description AS VARCHAR(100)) AS description,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  category_name IS NOT NULL;
