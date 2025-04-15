
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_d9c82a1e88e8a08217d815bd26d11808]
   as 
    
    



select employee_id
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__employees"
where employee_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_d9c82a1e88e8a08217d815bd26d11808]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_d9c82a1e88e8a08217d815bd26d11808]
  ;')