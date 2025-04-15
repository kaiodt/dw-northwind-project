
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_409b4925558b57c57e7701091cf86d78]
   as 
    
    

select
    order_detail_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_ORDERS__order_details"
where order_detail_id is not null
group by order_detail_id
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

    [dbt_test__audit.testview_409b4925558b57c57e7701091cf86d78]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_409b4925558b57c57e7701091cf86d78]
  ;')