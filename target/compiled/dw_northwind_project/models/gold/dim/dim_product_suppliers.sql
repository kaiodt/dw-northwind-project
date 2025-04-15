

WITH staged AS (
  SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(supplier_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(city as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(region as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(country as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
 AS supplier_sk,

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
    "DW_SILVER"."dbo"."SLV_PRODUCTS__suppliers"
  
  WHERE
    supplier_id IS NOT NULL
),

existing_keys AS (
  

  SELECT
    CAST(NULL AS INT) AS supplier_id

  WHERE 1 = 0

  
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