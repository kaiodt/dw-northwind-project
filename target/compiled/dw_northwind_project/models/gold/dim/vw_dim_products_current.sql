

SELECT
  product_id,
  COALESCE(product_name, 'No Product Name') AS product_name,
  COALESCE(quantity_per_unit, 'No Quantity Info') AS quantity_per_unit,
  COALESCE(unit_price, -1) AS unit_price,
  COALESCE(units_in_stock, -1) AS units_in_stock,
  COALESCE(units_on_order, -1) AS units_on_order,
  COALESCE(reorder_level, -1) AS reorder_level,
  COALESCE(discontinued, -1) AS discontinued,
  last_modified,
  silver_loaded_at,
  CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

FROM
  "DW_SILVER"."dbo"."SLV_PRODUCTS__products";