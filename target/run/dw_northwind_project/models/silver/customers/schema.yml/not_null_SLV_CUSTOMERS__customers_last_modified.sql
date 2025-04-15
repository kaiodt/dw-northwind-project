
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_13cff21280b1b28649d1f7569ba0b506]
   as 
    
    



select last_modified
from "DW_SILVER"."dbo"."SLV_CUSTOMERS__customers"
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

    [dbt_test__audit.testview_13cff21280b1b28649d1f7569ba0b506]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_13cff21280b1b28649d1f7569ba0b506]
  ;')