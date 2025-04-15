

WITH source AS (
  SELECT
    order_id,
    customer_id,
    employee_id,
    order_date,
    required_date,
    shipped_date,
    ship_via,
    freight,
    ship_name,
    ship_address,
    ship_city,
    ship_region,
    ship_postal_code,
    ship_country,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_ORDERS__orders"

  
    WHERE
      last_modified > (
        SELECT MAX(last_modified)
        FROM "DW_SILVER"."dbo"."SLV_ORDERS__orders"
      )
  
),

cleaned AS (
  SELECT
    order_id,
    customer_id,
    employee_id,
    order_date,
    required_date,
    shipped_date,
    ship_via,
    
  CASE
    WHEN freight < 0 THEN NULL
    ELSE freight
  END
 AS freight,
    
  CASE
    WHEN TRIM(ship_name) = '' OR TRIM(ship_name) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(ship_name), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS ship_name,
    
  CASE
    WHEN TRIM(ship_address) = '' OR TRIM(ship_address) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(ship_address), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS ship_address,
    
  CASE
    WHEN TRIM(ship_city) = '' OR TRIM(ship_city) = '-' THEN NULL
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
              TRIM(ship_city),
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
 AS ship_city,
    
  CASE
    WHEN TRIM(ship_region) = '' OR TRIM(ship_region) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(ship_region), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS ship_region,
    
  CASE
    WHEN TRIM(ship_postal_code) = '' OR TRIM(ship_postal_code) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(ship_postal_code), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS ship_postal_code,
    
  CASE
    WHEN TRIM(ship_country) = '' OR TRIM(ship_country) = '-' THEN NULL
    WHEN UPPER(TRIM(ship_country)) IN ('USA', 'UK') THEN UPPER(TRIM(ship_country))
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
              TRIM(ship_country),
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
 AS ship_country,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    order_id IS NOT NULL
)

SELECT
  CAST(order_id AS INT) AS order_id,
  CAST(customer_id AS SMALLINT) AS customer_id,
  CAST(employee_id AS SMALLINT) AS employee_id,
  CAST(order_date AS DATE) AS order_date,
  CAST(required_date AS DATE) AS required_date,
  CAST(shipped_date AS DATE) AS shipped_date,
  CAST(ship_via AS SMALLINT) AS ship_via,
  CAST(freight AS DECIMAL(10, 2)) AS freight,
  CAST(ship_name AS VARCHAR(60)) AS ship_name,
  CAST(ship_address AS VARCHAR(100)) AS ship_address,
  CAST(ship_city AS VARCHAR(30)) AS ship_city,
  CAST(ship_region AS VARCHAR(20)) AS ship_region,
  CAST(ship_postal_code AS VARCHAR(10)) AS ship_postal_code,
  CAST(ship_country AS VARCHAR(20)) AS ship_country,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned;