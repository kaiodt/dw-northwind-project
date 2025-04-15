
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_766f20686f5bbf272fe760b6551e7671]
   as 
    
    



select last_modified
from "DW_SILVER"."dbo"."SLV_ORDERS__order_details"
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

    [dbt_test__audit.testview_766f20686f5bbf272fe760b6551e7671]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_766f20686f5bbf272fe760b6551e7671]
  ;')