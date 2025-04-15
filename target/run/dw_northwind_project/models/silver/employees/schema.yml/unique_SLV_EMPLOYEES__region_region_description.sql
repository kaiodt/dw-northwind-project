
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_c3f523e905aadc2ba8facec3052bd201]
   as 
    
    

select
    region_description as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_EMPLOYEES__region"
where region_description is not null
group by region_description
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

    [dbt_test__audit.testview_c3f523e905aadc2ba8facec3052bd201]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_c3f523e905aadc2ba8facec3052bd201]
  ;')