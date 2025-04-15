
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_cde2c34226743fa071a7f370e4c97057]
   as 
    
    



select silver_loaded_at
from "DW_SILVER"."dbo"."SLV_CUSTOMERS__customers"
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

    [dbt_test__audit.testview_cde2c34226743fa071a7f370e4c97057]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_cde2c34226743fa071a7f370e4c97057]
  ;')