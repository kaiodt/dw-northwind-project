
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_4b2db411c9e21ed4d88887331340de98]
   as 
    
    



select company_name
from "DW_SILVER"."dbo"."SLV_CUSTOMERS__customers"
where company_name is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_4b2db411c9e21ed4d88887331340de98]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_4b2db411c9e21ed4d88887331340de98]
  ;')