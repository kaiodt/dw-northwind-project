
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_d9a608da4eac799a28a3bea1f724f4f8]
   as 
    
    



select silver_loaded_at
from "DW_SILVER"."dbo"."SLV_PRODUCTS__products"
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

    [dbt_test__audit.testview_d9a608da4eac799a28a3bea1f724f4f8]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_d9a608da4eac799a28a3bea1f724f4f8]
  ;')