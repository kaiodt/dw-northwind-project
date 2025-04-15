
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_59fd47e0a4741a5e2630f81c566b5dfb]
   as 
    
    



select product_name
from "DW_SILVER"."dbo"."SLV_PRODUCTS__products"
where product_name is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_59fd47e0a4741a5e2630f81c566b5dfb]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_59fd47e0a4741a5e2630f81c566b5dfb]
  ;')