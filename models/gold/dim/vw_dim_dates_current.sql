{{ config(
  materialized = 'view',
  database = 'DW_GOLD',
  schema = 'dbo'
) }}

SELECT *

FROM
  {{ ref('dim_dates') }}

WHERE
  date_day <= CAST(SYSDATETIME() AS DATE);
