
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_35c0ea15702755e390838afe537a5b3a]
   as 
    
    



select company_name
from "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers"
where company_name is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_35c0ea15702755e390838afe537a5b3a]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_35c0ea15702755e390838afe537a5b3a]
  ;')