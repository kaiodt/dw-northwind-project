
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_53f7eec26da584b9bb9583bf5591b146]
   as 
    
    



select category_id
from "DW_SILVER"."dbo"."SLV_PRODUCTS__categories"
where category_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_53f7eec26da584b9bb9583bf5591b146]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_53f7eec26da584b9bb9583bf5591b146]
  ;')