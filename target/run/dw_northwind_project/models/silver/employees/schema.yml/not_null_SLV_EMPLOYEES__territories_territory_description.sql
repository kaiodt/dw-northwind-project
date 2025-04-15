
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_285dd11316d6006b842c72db82217737]
   as 
    
    



select territory_description
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__territories"
where territory_description is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_285dd11316d6006b842c72db82217737]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_285dd11316d6006b842c72db82217737]
  ;')