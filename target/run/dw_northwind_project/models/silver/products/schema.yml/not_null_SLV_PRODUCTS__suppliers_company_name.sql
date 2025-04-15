
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_91ae4fe256145a98a7349e5924acdba2]
   as 
    
    



select company_name
from "DW_SILVER"."dbo"."SLV_PRODUCTS__suppliers"
where company_name is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_91ae4fe256145a98a7349e5924acdba2]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_91ae4fe256145a98a7349e5924acdba2]
  ;')