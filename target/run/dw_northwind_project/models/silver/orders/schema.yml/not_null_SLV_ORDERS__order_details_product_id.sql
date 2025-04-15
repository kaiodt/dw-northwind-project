
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_8b94b96199ee42707cbc7b54b5538ea1]
   as 
    
    



select product_id
from "DW_SILVER"."dbo"."SLV_ORDERS__order_details"
where product_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_8b94b96199ee42707cbc7b54b5538ea1]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_8b94b96199ee42707cbc7b54b5538ea1]
  ;')