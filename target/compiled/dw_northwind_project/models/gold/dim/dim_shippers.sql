

WITH staged AS (
  SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), coalesce(cast(shipper_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_')), '')), 2))
 AS shipper_sk,

    shipper_id,
    COALESCE(company_name, 'No Company Name') AS shipper_company_name,
    COALESCE(phone, 'No Phone') AS shipper_phone,
    COALESCE(last_modified, CAST('1900-01-01' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST('1900-01-01' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers"
),

existing_keys AS (
  

  SELECT
    CAST(NULL AS INT) AS shipper_id
  
  WHERE
    1 = 0
  
  
),

staged_prepared AS (
  SELECT
    s.shipper_sk,
    s.shipper_id,
    s.shipper_company_name,
    s.shipper_phone,
    s.last_modified,
    s.silver_loaded_at,
    s.gold_loaded_at,
    CASE
      WHEN e.shipper_id IS NULL THEN CAST('1900-01-01' AS DATETIME)
      ELSE s.gold_loaded_at
    END AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  FROM
    staged s
    LEFT JOIN existing_keys e
      ON s.shipper_id = e.shipper_id
),

final_data AS (
  SELECT * FROM staged_prepared

  

  UNION ALL

  -- Technical row for records where shipper_id is NULL

  SELECT
    '-1' AS shipper_sk,
    -1 AS shipper_id,
    'Unknown Shipper' AS shipper_company_name,
    'Unknown Phone' AS shipper_phone,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  UNION ALL

  -- Technical row for records where shipper_id is not found

  SELECT
    '-2' AS shipper_sk,
    -2 AS shipper_id,
    'Unregistered Shipper' AS shipper_company_name,
    'Unknown Phone' AS shipper_phone,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current
)

SELECT * FROM final_data