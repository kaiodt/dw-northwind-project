
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_5311552f5d612996e603f521623cc548]
   as 
    
    



select order_id
from "DW_SILVER"."dbo"."SLV_ORDERS__orders"
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

    [dbt_test__audit.testview_5311552f5d612996e603f521623cc548]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_5311552f5d612996e603f521623cc548]
  ;')