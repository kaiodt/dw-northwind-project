
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_4dc04864c2ece81fd959037828d4c8f3]
   as 
    
    



select territory_id
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__employee_territories"
where territory_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_4dc04864c2ece81fd959037828d4c8f3]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_4dc04864c2ece81fd959037828d4c8f3]
  ;')