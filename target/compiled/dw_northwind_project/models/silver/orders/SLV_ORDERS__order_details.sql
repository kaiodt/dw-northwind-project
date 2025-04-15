

WITH source AS (
  SELECT
    order_detail_id,
    order_id,
    product_id,
    unit_price,
    quantity,
    discount,
    last_modified

  FROM
    "DW_BRONZE"."dbo"."DB_ORDERS__order_details"

  
    WHERE
      last_modified > (
        SELECT MAX(last_modified)
        FROM "DW_SILVER"."dbo"."SLV_ORDERS__order_details"
      )
  
),

cleaned AS (
  SELECT
    order_detail_id,
    order_id,
    product_id,
    
  CASE
    WHEN unit_price < 0 THEN NULL
    ELSE unit_price
  END
 AS unit_price,
    
  CASE
    WHEN quantity < 0 THEN NULL
    ELSE quantity
  END
 AS quantity,
    
  CASE
    WHEN discount < 0 THEN NULL
    ELSE discount
  END
 AS discount,
    last_modified,
    CAST(SYSDATETIME() AS DATETIME) AS silver_loaded_at

  FROM
    source

  WHERE
    order_detail_id IS NOT NULL AND
    order_id IS NOT NULL AND
    product_id IS NOT NULL
)

SELECT
  CAST(order_detail_id AS INT) AS order_detail_id,
  CAST(order_id AS INT) AS order_id,
  CAST(product_id AS SMALLINT) AS product_id,
  CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
  CAST(quantity AS SMALLINT) AS quantity,
  CAST(discount AS DECIMAL(10, 2)) AS discount,
  CAST(last_modified AS DATETIME) AS last_modified,
  silver_loaded_at

FROM
  cleaned;