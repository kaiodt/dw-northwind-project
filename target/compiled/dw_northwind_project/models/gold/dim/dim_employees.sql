

WITH staged AS (
  SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(employee_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(city as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(region as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(country as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(reports_to as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
 AS employee_sk,

    employee_id,
    COALESCE(last_name, 'No Last Name') AS employee_last_name,
    COALESCE(first_name, 'No First Name') AS employee_first_name,
    COALESCE(title, 'No Title') AS employee_title,
    COALESCE(title_of_courtesy, 'No Courtesy Title') AS employee_title_of_courtesy,
    COALESCE(birth_date, CAST('1900-01-01' AS DATETIME)) AS employee_birth_date,
    COALESCE(hire_date, CAST('1900-01-01' AS DATETIME)) AS employee_hire_date,
    COALESCE(address, 'No Address') AS employee_address,
    COALESCE(city, 'No City') AS employee_city,
    COALESCE(region, 'No Region') AS employee_region,
    COALESCE(postal_code, 'No Postal Code') AS employee_postal_code,
    COALESCE(country, 'No Country') AS employee_country,
    COALESCE(home_phone, 'No Phone') AS employee_home_phone,
    COALESCE(extension, 'No Extension') AS employee_extension,
    COALESCE(notes, 'No Notes') AS employee_notes,
    COALESCE(reports_to, -1) AS employee_reports_to,
    COALESCE(photo_path, 'No Photo Path') AS employee_photo_path,
    COALESCE(last_modified, CAST('1900-01-01' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST('1900-01-01' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    "DW_SILVER"."dbo"."SLV_EMPLOYEES__employees"
  
  WHERE
    employee_id IS NOT NULL
),

existing_keys AS (
  

  SELECT
    DISTINCT employee_id
  
  FROM
    "DW_GOLD"."dbo"."dim_employees"

  
),

staged_prepared AS (
  SELECT
    s.employee_sk,
    s.employee_id,
    s.employee_last_name,
    s.employee_first_name,
    s.employee_title,
    s.employee_title_of_courtesy,
    s.employee_birth_date,
    s.employee_hire_date,
    s.employee_address,
    s.employee_city,
    s.employee_region,
    s.employee_postal_code,
    s.employee_country,
    s.employee_home_phone,
    s.employee_extension,
    s.employee_notes,
    s.employee_reports_to,
    s.employee_photo_path,
    s.last_modified,
    s.silver_loaded_at,
    s.gold_loaded_at,
    CASE
      WHEN e.employee_id IS NULL THEN CAST('1900-01-01' AS DATETIME)
      ELSE s.gold_loaded_at
    END AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  FROM
    staged s
    LEFT JOIN existing_keys e
      ON s.employee_id = e.employee_id
),

final_data AS (
  SELECT * FROM staged_prepared

  

  UNION ALL

  SELECT
    existing.employee_sk,
    existing.employee_id,
    existing.employee_last_name,
    existing.employee_first_name,
    existing.employee_title,
    existing.employee_title_of_courtesy,
    existing.employee_birth_date,
    existing.employee_hire_date,
    existing.employee_address,
    existing.employee_city,
    existing.employee_region,
    existing.employee_postal_code,
    existing.employee_country,
    existing.employee_home_phone,
    existing.employee_extension,
    existing.employee_notes,
    existing.employee_reports_to,
    existing.employee_photo_path,
    existing.last_modified,
    existing.silver_loaded_at,
    existing.gold_loaded_at,
    existing.valid_from,
    CAST(DATEADD(SECOND, -1, staged_prepared.gold_loaded_at) AS DATETIME) AS valid_to,
    0 AS is_current

  FROM
    "DW_GOLD"."dbo"."dim_employees" AS existing
    INNER JOIN staged_prepared
      ON existing.employee_id = staged_prepared.employee_id
  
  WHERE
    existing.is_current = 1 AND
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(existing.employee_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(existing.city as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(existing.region as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(existing.country as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(existing.reports_to as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))

    !=
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(staged_prepared.employee_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(staged_prepared.employee_city as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(staged_prepared.employee_region as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(staged_prepared.employee_country as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(staged_prepared.employee_reports_to as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))


  

  UNION ALL

  -- Technical row for records where employee_id is NULL

  SELECT
    '-1' AS employee_sk,
    -1 AS employee_id,
    'Unknown Last Name' AS employee_last_name,
    'Unknown First Name' AS employee_first_name,
    'Unknown Title' AS employee_title,
    'Unknown Courtesy Title' AS employee_title_of_courtesy,
    CAST('1900-01-01' AS DATETIME) AS employee_birth_date,
    CAST('1900-01-01' AS DATETIME) AS employee_hire_date,
    'Unknown Address' AS employee_address,
    'Unknown City' AS employee_city,
    'Unknown Region' AS employee_region,
    'Unknown Postal Code' AS employee_postal_code,
    'Unknown Country' AS employee_country,
    'Unknown Phone' AS employee_home_phone,
    'Unknown Extension' AS employee_extension,
    'Unknown Notes' AS employee_notes,
    -1 AS employee_reports_to,
    'Unknown Photo Path' AS employee_photo_path,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  UNION ALL

  -- Technical row for records where employee_id is not found

  SELECT
    '-2' AS employee_sk,
    -2 AS employee_id,
    'Unregistered Last Name' AS employee_last_name,
    'Unregistered First Name' AS employee_first_name,
    'Unknown Title' AS employee_title,
    'Unknown Courtesy Title' AS employee_title_of_courtesy,
    CAST('1900-01-01' AS DATETIME) AS employee_birth_date,
    CAST('1900-01-01' AS DATETIME) AS employee_hire_date,
    'Unknown Address' AS employee_address,
    'Unknown City' AS employee_city,
    'Unknown Region' AS employee_region,
    'Unknown Postal Code' AS employee_postal_code,
    'Unknown Country' AS employee_country,
    'Unknown Phone' AS employee_home_phone,
    'Unknown Extension' AS employee_extension,
    'Unknown Notes' AS employee_notes,
    -1 AS employee_reports_to,
    'Unknown Photo Path' AS employee_photo_path,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current
)

SELECT * FROM final_data;