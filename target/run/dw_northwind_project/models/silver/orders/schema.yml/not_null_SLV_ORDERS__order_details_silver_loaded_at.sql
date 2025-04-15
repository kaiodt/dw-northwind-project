
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_b5f77734643ee2a678e02ad08c7f6ca6]
   as 
    
    



select silver_loaded_at
from "DW_SILVER"."dbo"."SLV_ORDERS__order_details"
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

    [dbt_test__audit.testview_b5f77734643ee2a678e02ad08c7f6ca6]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_b5f77734643ee2a678e02ad08c7f6ca6]
  ;')