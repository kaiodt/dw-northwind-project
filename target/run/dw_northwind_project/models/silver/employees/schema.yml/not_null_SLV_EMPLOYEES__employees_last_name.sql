
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_3db718707fa93d65b627a4fdb2d9ff07]
   as 
    
    



select last_name
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__employees"
where last_name is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_3db718707fa93d65b627a4fdb2d9ff07]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_3db718707fa93d65b627a4fdb2d9ff07]
  ;')