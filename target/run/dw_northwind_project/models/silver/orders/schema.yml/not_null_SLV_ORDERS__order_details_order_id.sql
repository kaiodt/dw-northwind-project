
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_7304eaa63baa5e4c042b97228749344a]
   as 
    
    



select order_id
from "DW_SILVER"."dbo"."SLV_ORDERS__order_details"
where order_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_7304eaa63baa5e4c042b97228749344a]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_7304eaa63baa5e4c042b97228749344a]
  ;')