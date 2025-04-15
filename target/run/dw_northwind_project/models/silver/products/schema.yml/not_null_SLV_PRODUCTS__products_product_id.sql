
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_e239d1786d2b4ab9439e1096236a16f3]
   as 
    
    



select product_id
from "DW_SILVER"."dbo"."SLV_PRODUCTS__products"
where product_id is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_e239d1786d2b4ab9439e1096236a16f3]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_e239d1786d2b4ab9439e1096236a16f3]
  ;')