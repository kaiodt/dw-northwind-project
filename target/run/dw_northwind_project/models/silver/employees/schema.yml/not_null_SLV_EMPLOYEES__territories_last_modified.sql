
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_5e128a4c44865f01333d3014c63aa568]
   as 
    
    



select last_modified
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__territories"
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

    [dbt_test__audit.testview_5e128a4c44865f01333d3014c63aa568]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_5e128a4c44865f01333d3014c63aa568]
  ;')