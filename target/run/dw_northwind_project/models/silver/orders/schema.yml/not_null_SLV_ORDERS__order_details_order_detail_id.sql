
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_0d872399bd674e8b4627d7e185ec1732]
   as 
    
    



select order_detail_id
from "DW_SILVER"."dbo"."SLV_ORDERS__order_details"
where order_detail_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_0d872399bd674e8b4627d7e185ec1732]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_0d872399bd674e8b4627d7e185ec1732]
  ;')