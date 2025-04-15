
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_b40342eaae15975240ae106d19e11e54]
   as 
    
    



select region_description
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__region"
where region_description is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_b40342eaae15975240ae106d19e11e54]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_b40342eaae15975240ae106d19e11e54]
  ;')