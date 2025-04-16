{{ config(
  materialized = 'incremental',
  unique_key = 'order_detail_id',
  incremental_strategy = 'merge',
  on_schema_change = 'sync_all_columns',
  database = 'DW_GOLD',
  schema = 'dbo'
) }}

WITH order_details AS (
  SELECT
    order_detail_id,
    COALESCE(order_id, -1) AS order_id,
    COALESCE(product_id, -1) AS product_id,
    unit_price,
    quantity,
    discount,
    COALESCE(last_modified, CAST('1900-01-01' AS DATETIME)) AS last_modified,
    COALESCE(silver_loaded_at, CAST('1900-01-01' AS DATETIME)) AS silver_loaded_at

  FROM
    {{ ref('SLV_ORDERS__order_details') }}

  WHERE
    order_detail_id IS NOT NULL
),

orders AS (
  SELECT
    order_id,
    COALESCE(customer_id, -1) AS customer_id,
    COALESCE(employee_id, -1) AS employee_id,
    order_date,
    required_date,
    shipped_date,
    COALESCE(ship_via, -1) AS ship_via

  FROM
    {{ ref('SLV_ORDERS__orders') }}
),

products AS (
  SELECT
    product_id,
    COALESCE(category_id, -1) AS category_id,
    COALESCE(supplier_id, -1) AS supplier_id

  FROM
    {{ ref('SLV_PRODUCTS__products') }}
),

dim_customers AS (
  SELECT
    customer_id,
    customer_sk
  
  FROM
    {{ ref('dim_customers') }}
  
  WHERE
    is_current = 1
),

dim_employees AS (
  SELECT
    employee_id,
    employee_sk
  
  FROM
    {{ ref('dim_employees') }}
  
  WHERE
    is_current = 1
),

dim_products AS (
  SELECT
    product_id,
    product_sk
  
  FROM
    {{ ref('dim_products') }}
  
  WHERE
    is_current = 1
),

dim_categories AS (
  SELECT
    category_id,
    category_sk
  
  FROM
    {{ ref('dim_product_categories') }}
  
  WHERE
    is_current = 1
),

dim_suppliers AS (
  SELECT
    supplier_id,
    supplier_sk
  
  FROM
    {{ ref('dim_product_suppliers') }}
  
  WHERE
    is_current = 1
),

dim_shippers AS (
  SELECT
    shipper_id,
    shipper_sk
  
  FROM
    {{ ref('dim_shippers') }}
  
  WHERE
    is_current = 1
),

dim_dates AS (
  SELECT
    date_day,
    date_sk
  
  FROM
    {{ ref('dim_dates') }}
),

fact_base AS (
  SELECT
    od.order_detail_id,
    COALESCE(d_orders.order_sk, '-2') AS order_sk,
    COALESCE(d_customers.customer_sk, '-2') AS customer_sk,
    COALESCE(d_employees.employee_sk, '-2') AS employee_sk,
    COALESCE(d_products.product_sk, '-2') AS product_sk,
    COALESCE(d_categories.category_sk, '-2') AS product_category_sk,
    COALESCE(d_suppliers.supplier_sk, '-2') AS product_supplier_sk,
    COALESCE(d_shippers.shipper_sk, '-2') AS shipper_sk,
    dt_order.date_sk AS order_date_sk,
    dt_required.date_sk AS required_date_sk,
    dt_shipped.date_sk AS shipped_date_sk,
    od.quantity,
    od.unit_price,
    od.discount AS unit_discount,
    od.quantity * od.unit_price AS gross_amount,
    od.quantity * od.unit_price * od.discount AS discount_amount,
    od.quantity * od.unit_price * (1 - od.discount) AS net_amount,
    od.last_modified,
    od.silver_loaded_at,
    CAST(SYSDATETIME() AS DATETIME) AS gold_loaded_at

  FROM
    order_details od
    LEFT JOIN orders o
      ON od.order_id = o.order_id
    LEFT JOIN products p
      ON od.product_id = p.product_id
    LEFT JOIN {{ ref('dim_orders') }} d_orders
      ON od.order_id = d_orders.order_id
    LEFT JOIN dim_customers d_customers
      ON o.customer_id = d_customers.customer_id
    LEFT JOIN dim_employees d_employees
      ON o.employee_id = d_employees.employee_id
    LEFT JOIN dim_products d_products
      ON od.product_id = d_products.product_id
    LEFT JOIN dim_categories d_categories
      ON p.category_id = d_categories.category_id
    LEFT JOIN dim_suppliers d_suppliers
      ON p.supplier_id = d_suppliers.supplier_id
    LEFT JOIN dim_shippers d_shippers
      ON o.ship_via = d_shippers.shipper_id
    LEFT JOIN dim_dates dt_order
      ON o.order_date = dt_order.date_day
    LEFT JOIN dim_dates dt_required
      ON o.required_date = dt_required.date_day
    LEFT JOIN dim_dates dt_shipped
      ON o.shipped_date = dt_shipped.date_day
)

SELECT * FROM fact_base;
