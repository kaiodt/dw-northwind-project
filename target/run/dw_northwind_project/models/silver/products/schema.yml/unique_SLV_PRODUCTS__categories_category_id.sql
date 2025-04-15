
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_9341b751da2bdd1f3922474207ca0219]
   as 
    
    

select
    category_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_PRODUCTS__categories"
where category_id is not null
group by category_id
having count(*) > 1


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_9341b751da2bdd1f3922474207ca0219]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_9341b751da2bdd1f3922474207ca0219]
  ;')