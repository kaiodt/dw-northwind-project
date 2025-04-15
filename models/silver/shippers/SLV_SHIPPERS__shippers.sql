{{ config(
  materialized = 'incremental',
  unique_key = 'supplier_id',
  database = 'DW_SILVER',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
  SELECT
    shipper_id,
    company_name,
    phone,
    last_modified

  FROM
    {{ source('db_shippers', 'DB_SHIPPERS__shippers') }}

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
    shipper_id,
    {{ titlecase_with_separators('company_name') }} AS company_name,
    {{ remove_extra_spaces('phone') }} AS phone,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    shipper_id IS NOT NULL
)

SELECT
  CAST(shipper_id AS SMALLINT) AS shipper_id,
  CAST(company_name AS VARCHAR(100)) AS company_name,
  CAST(phone AS VARCHAR(20)) AS phone,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  company_name IS NOT NULL;
