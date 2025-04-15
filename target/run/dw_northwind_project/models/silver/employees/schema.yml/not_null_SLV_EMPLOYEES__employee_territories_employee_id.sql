
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_f3bfb5f07412e331b80fe064d8fc0836]
   as 
    
    



select employee_id
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__employee_territories"
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

    [dbt_test__audit.testview_f3bfb5f07412e331b80fe064d8fc0836]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_f3bfb5f07412e331b80fe064d8fc0836]
  ;')