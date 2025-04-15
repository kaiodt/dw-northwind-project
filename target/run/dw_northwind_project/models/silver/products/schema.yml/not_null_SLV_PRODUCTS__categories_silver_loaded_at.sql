
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_edb2605a62b24ab44a0e80e9a56d985c]
   as 
    
    



select silver_loaded_at
from "DW_SILVER"."dbo"."SLV_PRODUCTS__categories"
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

    [dbt_test__audit.testview_edb2605a62b24ab44a0e80e9a56d985c]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_edb2605a62b24ab44a0e80e9a56d985c]
  ;')