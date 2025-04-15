
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_fecc9582b740d3d47787c25c1b6b230b]
   as 
    
    



select supplier_id
from "DW_SILVER"."dbo"."SLV_PRODUCTS__suppliers"
where supplier_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_fecc9582b740d3d47787c25c1b6b230b]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_fecc9582b740d3d47787c25c1b6b230b]
  ;')