

WITH source AS (
  SELECT
    customer_id,
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
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_CUSTOMERS__customers"

  
    WHERE
      last_modified > (
        SELECT MAX(last_modified)
        FROM "DW_SILVER"."dbo"."SLV_CUSTOMERS__customers"
      )
  
),

cleaned AS (
  SELECT
    customer_id,
    
  CASE
    WHEN TRIM(company_name) = '' OR TRIM(company_name) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(company_name), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS company_name,
    
  CASE
    WHEN TRIM(contact_name) = '' OR TRIM(contact_name) = '-' THEN NULL
    ELSE (
      SELECT
        REPLACE(
          REPLACE(
            STRING_AGG(
              UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
              ' '
            ),
            ' - ', '-'
          ),
          ' / ', '/'
        )
      FROM
        STRING_SPLIT(
          REPLACE(
            REPLACE(
              TRIM(contact_name),
              '-', ' - '
            ),
            '/', ' / '
          ),
          ' '
        )
      WHERE
        LEN(value) > 0
    )
  END
 AS contact_name,
    
  CASE
    WHEN TRIM(contact_title) = '' OR TRIM(contact_title) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(
          UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
          ' '
        )
      FROM
        STRING_SPLIT(TRIM(contact_title), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS contact_title,
    
  CASE
    WHEN TRIM(address) = '' OR TRIM(address) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(address), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS address,
    
  CASE
    WHEN TRIM(city) = '' OR TRIM(city) = '-' THEN NULL
    ELSE (
      SELECT
        REPLACE(
          REPLACE(
            STRING_AGG(
              UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
              ' '
            ),
            ' - ', '-'
          ),
          ' / ', '/'
        )
      FROM
        STRING_SPLIT(
          REPLACE(
            REPLACE(
              TRIM(city),
              '-', ' - '
            ),
            '/', ' / '
          ),
          ' '
        )
      WHERE
        LEN(value) > 0
    )
  END
 AS city,
    
  CASE
    WHEN TRIM(region) = '' OR TRIM(region) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(region), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS region,
    
  CASE
    WHEN TRIM(postal_code) = '' OR TRIM(postal_code) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(postal_code), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS postal_code,
    
  CASE
    WHEN TRIM(country) = '' OR TRIM(country) = '-' THEN NULL
    WHEN UPPER(TRIM(country)) IN ('USA', 'UK') THEN UPPER(TRIM(country))
    ELSE (
      SELECT
        REPLACE(
          REPLACE(
            STRING_AGG(
              UPPER(LEFT(value,1)) + LOWER(SUBSTRING(value, 2, LEN(value))),
              ' '
            ),
            ' - ', '-'
          ),
          ' / ', '/'
        )
      FROM
        STRING_SPLIT(
          REPLACE(
            REPLACE(
              TRIM(country),
              '-', ' - '
            ),
            '/', ' / '
          ),
          ' '
        )
      WHERE
        LEN(value) > 0
    )
  END
 AS country,
    
  CASE
    WHEN TRIM(phone) = '' OR TRIM(phone) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(phone), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS phone,
    
  CASE
    WHEN TRIM(fax) = '' OR TRIM(fax) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(fax), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS fax,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    customer_id IS NOT NULL
)

SELECT
  CAST(customer_id AS SMALLINT) AS customer_id,
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
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  company_name IS NOT NULL;