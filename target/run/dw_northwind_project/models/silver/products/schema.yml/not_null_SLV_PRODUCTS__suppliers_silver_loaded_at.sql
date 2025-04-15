
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_b369290252c0c44e9e819bf17d7313e0]
   as 
    
    



select silver_loaded_at
from "DW_SILVER"."dbo"."SLV_PRODUCTS__suppliers"
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

    [dbt_test__audit.testview_b369290252c0c44e9e819bf17d7313e0]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_b369290252c0c44e9e819bf17d7313e0]
  ;')