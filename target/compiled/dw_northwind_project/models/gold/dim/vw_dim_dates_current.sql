

SELECT *

FROM
  "DW_GOLD"."dbo"."dim_dates"

WHERE
  date_day <= CAST(SYSDATETIME() AS DATE);