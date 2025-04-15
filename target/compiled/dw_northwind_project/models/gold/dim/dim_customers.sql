

WITH staged AS (
  SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(customer_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(city as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(region as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(country as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
 AS customer_sk,

    customer_id,
    COALESCE(company_name, 'No Company Name') AS customer_company_name,
    COALESCE(contact_name, 'No Contact Name') AS customer_contact_name,
    COALESCE(contact_title, 'No Contact Title') AS customer_contact_title,
    COALESCE(address, 'No Address') AS customer_address,
    COALESCE(city, 'No City') AS customer_city,
    COALESCE(region, 'No Region') AS customer_region,
    COALESCE(postal_code, 'No Postal Code') AS customer_postal_code,
    COALESCE(country, 'No Country') AS customer_country,
    COALESCE(phone, 'No Phone') AS customer_phone,
    COALESCE(fax, 'No Fax') AS customer_fax,
    COALESCE(last_modified, CAST('1900-01-01' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST('1900-01-01' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    "DW_SILVER"."dbo"."SLV_CUSTOMERS__customers"
  
  WHERE
    customer_id IS NOT NULL
),

existing_keys AS (
  

  SELECT
    CAST(NULL AS INT) AS customer_id

  WHERE 1 = 0

  
),

staged_prepared AS (
  SELECT
    s.customer_sk,
    s.customer_id,
    s.customer_company_name,
    s.customer_contact_name,
    s.customer_contact_title,
    s.customer_address,
    s.customer_city,
    s.customer_region,
    s.customer_postal_code,
    s.customer_country,
    s.customer_phone,
    s.customer_fax,
    s.last_modified,
    s.silver_loaded_at,
    s.gold_loaded_at,
    CASE
      WHEN e.customer_id IS NULL THEN CAST('1900-01-01' AS DATETIME)
      ELSE s.gold_loaded_at
    END AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  FROM
    staged s
    LEFT JOIN existing_keys e
      ON s.customer_id = e.customer_id
),

final_data AS (
  SELECT * FROM staged_prepared

  

  UNION ALL

  -- Technical row for records where customer_id is NULL

  SELECT
    '-1' AS customer_sk,
    -1 AS customer_id,
    'Unknown Company' AS customer_company_name,
    'Unknown Contact' AS customer_contact_name,
    'Unknown Title' AS customer_contact_title,
    'Unknown Address' AS customer_address,
    'Unknown City' AS customer_city,
    'Unknown Region' AS customer_region,
    'Unknown Postal' AS customer_postal_code,
    'Unknown Country' AS customer_country,
    'Unknown Phone' AS customer_phone,
    'Unknown Fax' AS customer_fax,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  UNION ALL

  -- Technical row for records where customer_id is not found

  SELECT
    '-2' AS customer_sk,
    -2 AS customer_id,
    'Unregistered Company' AS customer_company_name,
    'Unknown Contact' AS customer_contact_name,
    'Unknown Title' AS customer_contact_title,
    'Unknown Address' AS customer_address,
    'Unknown City' AS customer_city,
    'Unknown Region' AS customer_region,
    'Unknown Postal' AS customer_postal_code,
    'Unknown Country' AS customer_country,
    'Unknown Phone' AS customer_phone,
    'Unknown Fax' AS customer_fax,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current
)

SELECT * FROM final_data;