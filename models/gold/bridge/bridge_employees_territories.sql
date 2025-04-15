-- models/gold/bridge/bridge_employees_territories.sql
{{ config(
  materialized = 'incremental',
  unique_key = ['employee_sk', 'territory_sk'],
  database = 'DW_GOLD',
  schema = 'dbo',
  incremental_strategy = 'delete+insert',
  on_schema_change = 'sync_all_columns'
) }}

WITH employee_dim AS (
  SELECT
   employee_id,
   employee_sk
  
  FROM
    {{ ref('dim_employees') }}
  
  WHERE
    is_current = 1
),

territory_dim AS (
  SELECT
    territory_id,
    territory_sk
  
  FROM
    {{ ref('dim_territories') }}
  
  WHERE
    is_current = 1
),

mapped AS (
  SELECT
    et.employee_id,
    et.territory_id,
    et.last_modified,
    et.silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at
  
  FROM
    {{ ref('SLV_EMPLOYEES__employee_territories') }} et

  WHERE
    et.employee_id IS NOT NULL AND
    et.territory_id IS NOT NULL
),

final AS (
  SELECT
    COALESCE(e.employee_sk, '-2') AS employee_sk,
    COALESCE(t.territory_sk, '-2') AS territory_sk,
    m.last_modified,
    m.silver_loaded_at,
    m.gold_loaded_at
  
  FROM
    mapped m
    LEFT JOIN employee_dim e
      ON m.employee_id = e.employee_id
    LEFT JOIN territory_dim t
      ON m.territory_id = t.territory_id
)

SELECT * FROM final;
