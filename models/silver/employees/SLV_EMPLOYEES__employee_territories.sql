{{ config(
  materialized = 'incremental',
  unique_key = ['employee_id', 'territory_id'],
  database = 'DW_SILVER',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
  SELECT
    employee_id,
    territory_id,
    last_modified

  FROM
    {{ source('db_employees', 'DB_EMPLOYEES__employee_territories') }}

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
    employee_id,
    territory_id,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    employee_id IS NOT NULL AND
    territory_id IS NOT NULL
)

SELECT
  CAST(employee_id AS SMALLINT) AS employee_id,
  CAST(territory_id AS SMALLINT) AS territory_id,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned;
