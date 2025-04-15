
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_4a3afe4ff6edb9e7a3c0bd801dd7de54]
   as 
    
    

select
    territory_description as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_EMPLOYEES__territories"
where territory_description is not null
group by territory_description
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

    [dbt_test__audit.testview_4a3afe4ff6edb9e7a3c0bd801dd7de54]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_4a3afe4ff6edb9e7a3c0bd801dd7de54]
  ;')