
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_c0b1de96c8fe0578d077dd11668025db]
   as 
    
    



select last_modified
from "DW_SILVER"."dbo"."SLV_PRODUCTS__categories"
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

    [dbt_test__audit.testview_c0b1de96c8fe0578d077dd11668025db]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_c0b1de96c8fe0578d077dd11668025db]
  ;')