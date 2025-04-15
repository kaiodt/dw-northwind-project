
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_d8defe2ba601a3fe201e2b417a884f76]
   as 
    
    



select last_modified
from "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers"
where last_modified is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_d8defe2ba601a3fe201e2b417a884f76]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_d8defe2ba601a3fe201e2b417a884f76]
  ;')