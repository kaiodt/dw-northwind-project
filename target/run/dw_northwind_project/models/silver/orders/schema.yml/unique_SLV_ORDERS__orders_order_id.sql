
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_c3cc9858b762a196a79c90e11ebf72ee]
   as 
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_ORDERS__orders"
where order_id is not null
group by order_id
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

    [dbt_test__audit.testview_c3cc9858b762a196a79c90e11ebf72ee]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_c3cc9858b762a196a79c90e11ebf72ee]
  ;')