
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_336b929e3de702216f401e72341d55e3]
   as 
    
    



select last_modified
from "DW_SILVER"."dbo"."SLV_PRODUCTS__products"
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

    [dbt_test__audit.testview_336b929e3de702216f401e72341d55e3]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_336b929e3de702216f401e72341d55e3]
  ;')