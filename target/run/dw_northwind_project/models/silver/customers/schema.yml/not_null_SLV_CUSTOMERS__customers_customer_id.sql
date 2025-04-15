
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_e6abb3e055ebba548d03b572318bb028]
   as 
    
    



select customer_id
from "DW_SILVER"."dbo"."SLV_CUSTOMERS__customers"
where customer_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_e6abb3e055ebba548d03b572318bb028]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_e6abb3e055ebba548d03b572318bb028]
  ;')