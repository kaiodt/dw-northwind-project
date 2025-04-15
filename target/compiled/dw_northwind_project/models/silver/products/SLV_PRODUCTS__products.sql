

WITH source AS (
  SELECT
    product_id,
    product_name,
    supplier_id,
    category_id,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    units_on_order,
    reorder_level,
    discontinued,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_PRODUCTS__products"

  
    WHERE
      last_modified > (
        SELECT MAX(last_modified)
        FROM "DW_SILVER"."dbo"."SLV_PRODUCTS__products"
      )
  
),

cleaned AS (
  SELECT
    product_id,
    
  CASE
    WHEN TRIM(product_name) = '' OR TRIM(product_name) = '-' THEN NULL
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
              TRIM(product_name),
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
 AS product_name,
    supplier_id,
    category_id,
    
  CASE
    WHEN TRIM(quantity_per_unit) = '' OR TRIM(quantity_per_unit) = '-' THEN NULL
    ELSE (
      SELECT
        STRING_AGG(value, ' ')
      FROM
        STRING_SPLIT(TRIM(quantity_per_unit), ' ')
      WHERE
        LEN(value) > 0
    )
  END
 AS quantity_per_unit,
    
  CASE
    WHEN unit_price < 0 THEN NULL
    ELSE unit_price
  END
 AS unit_price,
    
  CASE
    WHEN units_in_stock < 0 THEN NULL
    ELSE units_in_stock
  END
 AS units_in_stock,
    
  CASE
    WHEN units_on_order < 0 THEN NULL
    ELSE units_on_order
  END
 AS units_on_order,
    
  CASE
    WHEN reorder_level < 0 THEN NULL
    ELSE reorder_level
  END
 AS reorder_level,
    
  CASE
    WHEN discontinued NOT IN (0, 1) THEN NULL
    ELSE discontinued
  END
 AS discontinued,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    product_id IS NOT NULL
)

SELECT
  CAST(product_id AS SMALLINT) AS product_id,
  CAST(product_name AS VARCHAR(100)) AS product_name,
  CAST(supplier_id AS SMALLINT) AS supplier_id,
  CAST(category_id AS SMALLINT) AS category_id,
  CAST(quantity_per_unit AS VARCHAR(30)) AS quantity_per_unit,
  CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
  CAST(units_in_stock AS SMALLINT) AS units_in_stock,
  CAST(units_on_order AS SMALLINT) AS units_on_order,
  CAST(reorder_level AS SMALLINT) AS reorder_level,
  CAST(discontinued AS TINYINT) AS discontinued,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned

WHERE
  product_name IS NOT NULL;