
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_d34805a0d740c0272a23acd91430547f]
   as 
    
    

select
    region_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_EMPLOYEES__region"
where region_id is not null
group by region_id
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

    [dbt_test__audit.testview_d34805a0d740c0272a23acd91430547f]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_d34805a0d740c0272a23acd91430547f]
  ;')