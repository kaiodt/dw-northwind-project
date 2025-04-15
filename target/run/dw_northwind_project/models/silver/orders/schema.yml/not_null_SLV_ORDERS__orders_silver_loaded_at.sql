
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_1c76bf43783215cc21fc60d51c9cf3a8]
   as 
    
    



select silver_loaded_at
from "DW_SILVER"."dbo"."SLV_ORDERS__orders"
where silver_loaded_at is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_1c76bf43783215cc21fc60d51c9cf3a8]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_1c76bf43783215cc21fc60d51c9cf3a8]
  ;')