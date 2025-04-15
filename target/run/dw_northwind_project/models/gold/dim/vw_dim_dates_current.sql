USE [DW_GOLD];
    
    

    

    
    USE [DW_GOLD];
    EXEC('
        create view "dbo"."vw_dim_dates_current__dbt_tmp" as 

SELECT *

FROM
  "DW_GOLD"."dbo"."dim_dates"

WHERE
  date_day <= CAST(SYSDATETIME() AS DATE);;
    ')

