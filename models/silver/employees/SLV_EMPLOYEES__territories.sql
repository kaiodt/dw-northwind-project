{{ config(
  materialized = 'incremental',
  unique_key = 'territory_id',
  database = 'DW_SILVER',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
  SELECT
    territory_id,
    territory_description,
    region_id,
    last_modified

  FROM
    {{ source('db_employees', 'DB_EMPLOYEES__territories') }}

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
    territory_id,
    {{ titlecase_with_separators('territory_description') }} AS territory_description,
    region_id,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    territory_id IS NOT NULL
)

SELECT
  CAST(territory_id AS SMALLINT) AS territory_id,
  CAST(territory_description AS VARCHAR(60)) AS territory_description,
  CAST(region_id AS SMALLINT) AS region_id,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  territory_description IS NOT NULL;
