

WITH staged AS (
  SELECT
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(category_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(category_name as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))
 AS category_sk,

    category_id,
    COALESCE(category_name, 'No Category Name') AS category_name,
    COALESCE(description, 'No Description') AS category_description,
    COALESCE(last_modified, CAST('1900-01-01' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST('1900-01-01' AS DATETIME)) AS silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    "DW_SILVER"."dbo"."SLV_PRODUCTS__categories"
  
  WHERE
    category_id IS NOT NULL
),

existing_keys AS (
  

  SELECT
    DISTINCT category_id

  FROM
    "DW_GOLD"."dbo"."dim_product_categories"

  
),

staged_prepared AS (
  SELECT
    s.category_sk,
    s.category_id,
    s.category_name,
    s.category_description,
    s.last_modified,
    s.silver_loaded_at,
    s.gold_loaded_at,
    CASE
      WHEN e.category_id IS NULL THEN CAST('1900-01-01' AS DATETIME)
      ELSE s.gold_loaded_at
    END AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  FROM
    staged s
    LEFT JOIN existing_keys e
      ON s.category_id = e.category_id
),

final_data AS (
  SELECT * FROM staged_prepared

  

  UNION ALL

  SELECT
    existing.category_sk,
    existing.category_id,
    existing.category_name,
    existing.category_description,
    existing.last_modified,
    existing.silver_loaded_at,
    existing.gold_loaded_at,
    existing.valid_from,
    CAST(
      DATEADD(SECOND, -1, staged_prepared.gold_loaded_at) AS DATETIME
    ) AS valid_to,
    0 AS is_current

  FROM
    "DW_GOLD"."dbo"."dim_product_categories" AS existing
    INNER JOIN staged_prepared
      ON existing.category_id = staged_prepared.category_id

  WHERE
    existing.is_current = 1 AND
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(existing.category_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(existing.category_name as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))

    !=
    
    lower(convert(varchar(50), hashbytes('md5', coalesce(convert(varchar(8000), concat(coalesce(cast(staged_prepared.category_id as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(staged_prepared.category_name as VARCHAR(8000)), '_dbt_utils_surrogate_key_null_'))), '')), 2))


  

  UNION ALL

  -- Technical row for records where category_id is NULL

  SELECT
    '-1' AS category_sk,
    -1 AS category_id,
    'Unknown Category' AS category_name,
    'Unknown Description' AS category_description,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current

  UNION ALL

  -- Technical row for records where category_id is not found

  SELECT
    '-2' AS category_sk,
    -2 AS category_id,
    'Unregistered Category' AS category_name,
    'Unknown Description' AS category_description,
    CAST('1900-01-01' AS DATETIME) AS last_modified,
    CAST('1900-01-01' AS DATETIME) AS silver_loaded_at,
    SYSDATETIME() AS gold_loaded_at,
    CAST('1900-01-01' AS DATETIME) AS valid_from,
    CAST('9999-12-31' AS DATETIME) AS valid_to,
    1 AS is_current
)

SELECT * FROM final_data;