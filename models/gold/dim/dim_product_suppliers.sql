{{ config(
  materialized = 'incremental',
  unique_key = 'supplier_sk',
  database = 'DW_GOLD',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH staged AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key([
      'supplier_id',
      'city',
      'region',
      'country'
    ]) }} AS supplier_sk,

    supplier_id,
    COALESCE(company_name, 'No Company Name') AS supplier_company_name,
    COALESCE(contact_name, 'No Contact Name') AS supplier_contact_name,
    COALESCE(contact_title, 'No Contact Title') AS supplier_contact_title,
    COALESCE(address, 'No Address') AS supplier_address,
    COALESCE(city, 'No City') AS supplier_city,
    COALESCE(region, 'No Region') AS supplier_region,
    COALESCE(postal_code, 'No Postal Code') AS supplier_postal_code,
    COALESCE(country, 'No Country') AS supplier_country,
    COALESCE(phone, 'No Phone') AS supplier_phone,
    COALESCE(fax, 'No Fax') AS supplier_fax,
    COALESCE(homepage, 'No Homepage') AS supplier_homepage,
    COALESCE(last_modified, CAST('1900-01-01' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST('1900-01-01' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    {{ ref('SLV_PRODUCTS__suppliers') }}
  
  WHERE
    supplier_id IS NOT NULL
),

existing_keys AS (
  {% if is_incremental() %}

  SELECT
    DISTINCT supplier_id

  FROM
    {{ this }}

  {% else %}

  SELECT
    CAST(NULL AS INT) AS supplier_id

  WHERE 1 = 0

  {% endif %}
),

staged_prepared AS (
  SELECT
    s.supplier_sk,
    s.supplier_id,
    s.supplier_company_name,
    s.supplier_contact_name,
    s.supplier_contact_title,
    s.supplier_address,
    s.supplier_city,
    s.supplier_region,
    s.supplier_postal_code,
    s.supplier_country,
    s.supplier_phone,
    s.supplier_fax,
    s.supplier_homepage,
    s.last_modified,
    s.silver_loaded_at,
    s.gold_loaded_at,
    CASE
      WHEN e.supplier_id IS NULL THEN CAST('1900-01-01' AS DATETIME)
      ELSE s.gold_loaded_at
    END AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  FROM
    staged s
    LEFT JOIN existing_keys e
      ON s.supplier_id = e.supplier_id
),

final_data AS (
  SELECT * FROM staged_prepared

  {% if is_incremental() %}

  UNION ALL

  SELECT
    existing.supplier_sk,
    existing.supplier_id,
    existing.supplier_company_name,
    existing.supplier_contact_name,
    existing.supplier_contact_title,
    existing.supplier_address,
    existing.supplier_city,
    existing.supplier_region,
    existing.supplier_postal_code,
    existing.supplier_country,
    existing.supplier_phone,
    existing.supplier_fax,
    existing.supplier_homepage,
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
      existing.supplier_id = staged_prepared.supplier_id
  
  WHERE
    existing.is_current = 1 AND
    {{ dbt_utils.generate_surrogate_key([
      'existing.supplier_id',
      'existing.supplier_city',
      'existing.supplier_region',
      'existing.supplier_country'
    ]) }}
    !=
    {{ dbt_utils.generate_surrogate_key([
      'staged_prepared.supplier_id',
      'staged_prepared.supplier_city',
      'staged_prepared.supplier_region',
      'staged_prepared.supplier_country'
    ]) }}

  {% endif %}

  UNION ALL

  -- Technical row for records where supplier_id is NULL

  SELECT
    '-1' AS supplier_sk,
    -1 AS supplier_id,
    'Unknown Company' AS supplier_company_name,
    'Unknown Contact' AS supplier_contact_name,
    'Unknown Title' AS supplier_contact_title,
    'Unknown Address' AS supplier_address,
    'Unknown City' AS supplier_city,
    'Unknown Region' AS supplier_region,
    'Unknown Postal' AS supplier_postal_code,
    'Unknown Country' AS supplier_country,
    'Unknown Phone' AS supplier_phone,
    'Unknown Fax' AS supplier_fax,
    'Unknown Homepage' AS supplier_homepage,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  UNION ALL

  -- Technical row for records where supplier_id is not found

  SELECT
    '-2' AS supplier_sk,
    -2 AS supplier_id,
    'Unregistered Company' AS supplier_company_name,
    'Unknown Contact' AS supplier_contact_name,
    'Unknown Title' AS supplier_contact_title,
    'Unknown Address' AS supplier_address,
    'Unknown City' AS supplier_city,
    'Unknown Region' AS supplier_region,
    'Unknown Postal' AS supplier_postal_code,
    'Unknown Country' AS supplier_country,
    'Unknown Phone' AS supplier_phone,
    'Unknown Fax' AS supplier_fax,
    'Unknown Homepage' AS supplier_homepage,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current
)

SELECT * FROM final_data;
