{{ config(
  materialized = 'incremental',
  unique_key = 'region_id',
  database = 'DW_SILVER',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
  SELECT
    region_id,
    region_description,
    last_modified

  FROM
    {{ source('db_employees', 'DB_EMPLOYEES__region') }}

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
    region_id,
    {{ titlecase_with_separators('region_description') }} AS region_description,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    region_id IS NOT NULL
)

SELECT
  CAST(region_id AS SMALLINT) AS region_id,
  CAST(region_description AS VARCHAR(60)) AS region_description,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  region_description IS NOT NULL;
