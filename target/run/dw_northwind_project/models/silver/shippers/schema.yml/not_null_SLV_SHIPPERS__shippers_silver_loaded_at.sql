
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_75a8f6c1ea0347f5811b13c0c17d05a1]
   as 
    
    



select silver_loaded_at
from "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers"
where silver_loaded_at is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_75a8f6c1ea0347f5811b13c0c17d05a1]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_75a8f6c1ea0347f5811b13c0c17d05a1]
  ;')