
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_265874c783fd25df8287b0bc7a4ae4c2]
   as 
    
    



select category_name
from "DW_SILVER"."dbo"."SLV_PRODUCTS__categories"
where category_name is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_265874c783fd25df8287b0bc7a4ae4c2]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_265874c783fd25df8287b0bc7a4ae4c2]
  ;')