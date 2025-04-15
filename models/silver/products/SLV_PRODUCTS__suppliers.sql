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
    supplier_id,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    region,
    postal_code,
    country,
    phone,
    fax,
    homepage,
    last_modified

  FROM
    {{ source('db_products', 'DB_PRODUCTS__suppliers') }}

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
    supplier_id,
    {{ remove_extra_spaces('company_name') }} AS company_name,
    {{ titlecase_with_separators('contact_name') }} AS contact_name,
    {{ titlecase_without_separators('contact_title') }} AS contact_title,
    {{ remove_extra_spaces('address') }} AS address,
    {{ titlecase_with_separators('city') }} AS city,
    {{ remove_extra_spaces('region') }} AS region,
    {{ remove_extra_spaces('postal_code') }} AS postal_code,
    {{ titlecase_country('country') }} AS country,
    {{ remove_extra_spaces('phone') }} AS phone,
    {{ remove_extra_spaces('fax') }} AS fax,
    {{ extract_within_delimiters('homepage', '#') }} AS homepage,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    supplier_id IS NOT NULL
)

SELECT
  CAST(supplier_id AS SMALLINT) AS supplier_id,
  CAST(company_name AS VARCHAR(100)) AS company_name,
  CAST(contact_name AS VARCHAR(60)) AS contact_name,
  CAST(contact_title AS VARCHAR(30)) AS contact_title,
  CAST(address AS VARCHAR(100)) AS address,
  CAST(city AS VARCHAR(30)) AS city,
  CAST(region AS VARCHAR(20)) AS region,
  CAST(postal_code AS VARCHAR(10)) AS postal_code,
  CAST(country AS VARCHAR(20)) AS country,
  CAST(phone AS VARCHAR(20)) AS phone,
  CAST(fax AS VARCHAR(20)) AS fax,
  CAST(homepage AS VARCHAR(100)) AS homepage,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  company_name IS NOT NULL;
