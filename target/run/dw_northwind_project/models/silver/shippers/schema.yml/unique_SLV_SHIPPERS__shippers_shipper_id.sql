
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_fc35573d401f68ceb721cb8a86cca827]
   as 
    
    

select
    shipper_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers"
where shipper_id is not null
group by shipper_id
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

    [dbt_test__audit.testview_fc35573d401f68ceb721cb8a86cca827]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_fc35573d401f68ceb721cb8a86cca827]
  ;')