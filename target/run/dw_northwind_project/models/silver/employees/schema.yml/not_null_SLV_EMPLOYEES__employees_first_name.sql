
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_8e189123600630a4571f3874adcca9c1]
   as 
    
    



select first_name
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__employees"
where first_name is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_8e189123600630a4571f3874adcca9c1]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_8e189123600630a4571f3874adcca9c1]
  ;')