
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_a79d24ffa40630e37a33f055dc87fff5]
   as 
    
    



select silver_loaded_at
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__employee_territories"
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

    [dbt_test__audit.testview_a79d24ffa40630e37a33f055dc87fff5]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_a79d24ffa40630e37a33f055dc87fff5]
  ;')