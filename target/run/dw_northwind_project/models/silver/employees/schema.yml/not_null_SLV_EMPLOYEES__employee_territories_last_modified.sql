
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_ab5022db621b3e3f7ba729a42cc9e048]
   as 
    
    



select last_modified
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__employee_territories"
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

    [dbt_test__audit.testview_ab5022db621b3e3f7ba729a42cc9e048]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_ab5022db621b3e3f7ba729a42cc9e048]
  ;')