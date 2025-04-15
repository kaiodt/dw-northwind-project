
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_65fa3f238f65f15cb2a3d4d308ed25cb]
   as 
    
    

select
    company_name as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers"
where company_name is not null
group by company_name
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

    [dbt_test__audit.testview_65fa3f238f65f15cb2a3d4d308ed25cb]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_65fa3f238f65f15cb2a3d4d308ed25cb]
  ;')