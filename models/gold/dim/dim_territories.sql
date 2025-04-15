-- models/gold/dim/dim_territories.sql
{{ config(
  materialized = 'incremental',
  unique_key = 'territory_sk',
  database = 'DW_GOLD',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH joined_source AS (
  SELECT
    t.territory_id,
    t.territory_description,
    t.region_id,
    r.region_description,
    COALESCE(t.last_modified, r.last_modified) AS last_modified,
    COALESCE(t.silver_loaded_at, r.silver_loaded_at) AS silver_loaded_at

  FROM
    {{ ref('SLV_EMPLOYEES__territories') }} t
    LEFT JOIN {{ ref('SLV_EMPLOYEES__region') }} r
      ON t.region_id = r.region_id
),

staged AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key([
      'territory_id',
      'territory_description',
      'region_id',
      'region_description'
    ]) }} AS territory_sk,

    territory_id,
    COALESCE(territory_description, 'No Territory') AS territory_description,
    region_id,
    COALESCE(region_description, 'No Region') AS region_description,
    COALESCE(last_modified, CAST('1900-01-01' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST('1900-01-01' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    joined_source

  WHERE
    territory_id IS NOT NULL
),

existing_keys AS (
  {% if is_incremental() %}

  SELECT
    DISTINCT territory_id
  
  FROM
    {{ this }}

  {% else %}

  SELECT
    CAST(NULL AS INT) AS territory_id
  
  WHERE
    1 = 0

  {% endif %}
),

staged_prepared AS (
  SELECT
    s.territory_sk,
    s.territory_id,
    s.territory_description,
    s.region_id,
    s.region_description,
    s.last_modified,
    s.silver_loaded_at,
    s.gold_loaded_at,
    CASE
      WHEN e.territory_id IS NULL THEN CAST('1900-01-01' AS DATETIME)
      ELSE s.gold_loaded_at
    END AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  FROM
    staged s
    LEFT JOIN existing_keys e
      ON s.territory_id = e.territory_id
),

final_data AS (
  SELECT * FROM staged_prepared

  {% if is_incremental() %}

  UNION ALL

  SELECT
    existing.territory_sk,
    existing.territory_id,
    existing.territory_description,
    existing.region_id,
    existing.region_description,
    existing.last_modified,
    existing.silver_loaded_at,
    existing.gold_loaded_at,
    existing.valid_from,
    CAST(DATEADD(SECOND, -1, staged_prepared.gold_loaded_at) AS DATETIME) AS valid_to,
    0 AS is_current
  
  FROM
    {{ this }} AS existing
    INNER JOIN staged_prepared
      ON existing.territory_id = staged_prepared.territory_id

  WHERE
    existing.is_current = 1 AND
    {{ dbt_utils.generate_surrogate_key([
      'existing.territory_id',
      'existing.territory_description',
      'existing.region_id',
      'existing.region_description'
    ]) }}
    !=
    {{ dbt_utils.generate_surrogate_key([
      'staged_prepared.territory_id',
      'staged_prepared.territory_description',
      'staged_prepared.region_id',
      'staged_prepared.region_description'
    ]) }}

  {% endif %}

  UNION ALL

  -- Technical row for records where territory_id is NULL

  SELECT
      '-1' AS territory_sk,
      -1 AS territory_id,
      'Unknown Territory' AS territory_description,
      -1 AS region_id,
      'Unknown Region' AS region_description,
      CAST('1900-01-01' AS DATETIME) AS last_modified,
      CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
      SYSDATETIME() AS gold_loaded_at,
      CAST('1900-01-01' AS DATETIME) AS valid_from,
      CAST('9999-12-31' AS DATETIME) AS valid_to,
      1 AS is_current

  UNION ALL

  -- Technical row for records where territory_id is not found

  SELECT
      '-2' AS territory_sk,
      -2 AS territory_id,
      'Unregistered Territory' AS territory_description,
      -1 AS region_id,
      'Unregistered Region' AS region_description,
      CAST('1900-01-01' AS DATETIME) AS last_modified,
      CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
      SYSDATETIME() AS gold_loaded_at,
      CAST('1900-01-01' AS DATETIME) AS valid_from,
      CAST('9999-12-31' AS DATETIME) AS valid_to,
      1 AS is_current
)

SELECT * FROM final_data;
