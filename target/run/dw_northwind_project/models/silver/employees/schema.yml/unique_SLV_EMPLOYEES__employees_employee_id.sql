
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_f86873245abdc60fb66d7e8451e529d8]
   as 
    
    

select
    employee_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_EMPLOYEES__employees"
where employee_id is not null
group by employee_id
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

    [dbt_test__audit.testview_f86873245abdc60fb66d7e8451e529d8]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_f86873245abdc60fb66d7e8451e529d8]
  ;')