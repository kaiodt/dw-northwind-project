
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_0e0d8332a2701433c37dbf1dabb9b752]
   as 
    
    



select region_id
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__territories"
where region_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_0e0d8332a2701433c37dbf1dabb9b752]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_0e0d8332a2701433c37dbf1dabb9b752]
  ;')