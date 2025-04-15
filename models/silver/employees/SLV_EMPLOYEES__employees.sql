{{ config(
  materialized = 'incremental',
  unique_key = 'employee_id',
  database = 'DW_SILVER',
  schema = 'dbo',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
  SELECT
    employee_id,
    last_name,
    first_name,
    title,
    title_of_courtesy,
    birth_date,
    hire_date,
    address,
    city,
    region,
    postal_code,
    country,
    home_phone,
    extension,
    notes,
    reports_to,
    photo_path,
    last_modified

  FROM
    {{ source('db_employees', 'DB_EMPLOYEES__employees') }}

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
    employee_id,
    {{ titlecase_with_separators('last_name') }} AS last_name,
    {{ titlecase_with_separators('first_name') }} AS first_name,
    {{ titlecase_without_separators('title') }} AS title,
    {{ capitalize_first_letter_only('title_of_courtesy') }} AS title_of_courtesy,
    birth_date,
    hire_date,
    {{ remove_extra_spaces('address') }} AS address,
    {{ titlecase_with_separators('city') }} AS city,
    {{ remove_extra_spaces('region') }} AS region,
    {{ remove_extra_spaces('postal_code') }} AS postal_code,
    {{ titlecase_country('country') }} AS country,
    {{ remove_extra_spaces('home_phone') }} AS home_phone,
    {{ remove_extra_spaces('extension') }} AS extension,
    {{ remove_extra_spaces('notes') }} AS notes,
    reports_to,
    LOWER({{ remove_extra_spaces('photo_path') }}) AS photo_path,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    employee_id IS NOT NULL
)

SELECT
  CAST(employee_id AS SMALLINT) AS employee_id,
  CAST(last_name AS VARCHAR(20)) AS last_name,
  CAST(first_name AS VARCHAR(10)) AS first_name,
  CAST(title AS VARCHAR(30)) AS title,
  CAST(title_of_courtesy AS VARCHAR(25)) AS title_of_courtesy,
  CAST(birth_date AS DATE) AS birth_date,
  CAST(hire_date AS DATE) AS hire_date,
  CAST(address AS VARCHAR(60)) AS address,
  CAST(city AS VARCHAR(15)) AS city,
  CAST(region AS VARCHAR(15)) AS region,
  CAST(postal_code AS VARCHAR(10)) AS postal_code,
  CAST(country AS VARCHAR(15)) AS country,
  CAST(home_phone AS VARCHAR(24)) AS home_phone,
  CAST(extension AS VARCHAR(4)) AS extension,
  CAST(notes AS VARCHAR(1000)) AS notes,
  CAST(reports_to AS SMALLINT) AS reports_to,
  CAST(photo_path AS VARCHAR(255)) AS photo_path,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  last_name IS NOT NULL AND
  first_name IS NOT NULL;
