
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_991d580072631de668e7a307de673e86]
   as 
    
    



select last_modified
from "DW_SILVER"."dbo"."SLV_PRODUCTS__suppliers"
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

    [dbt_test__audit.testview_991d580072631de668e7a307de673e86]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_991d580072631de668e7a307de673e86]
  ;')