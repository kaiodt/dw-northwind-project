
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_227a2eec116d0cced547ec19ac598852]
   as 
    
    



select territory_id
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__territories"
where territory_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_227a2eec116d0cced547ec19ac598852]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_227a2eec116d0cced547ec19ac598852]
  ;')