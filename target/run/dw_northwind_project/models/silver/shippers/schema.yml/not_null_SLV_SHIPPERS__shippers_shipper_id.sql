
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_2bc683db98848a969771ecf212eb6521]
   as 
    
    



select shipper_id
from "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers"
where shipper_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_2bc683db98848a969771ecf212eb6521]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_2bc683db98848a969771ecf212eb6521]
  ;')