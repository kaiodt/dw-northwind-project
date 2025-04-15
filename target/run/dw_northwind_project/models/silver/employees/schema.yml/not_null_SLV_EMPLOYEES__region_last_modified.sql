
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_a897b60fa0cba64026678a83bccbe01b]
   as 
    
    



select last_modified
from "DW_SILVER"."dbo"."SLV_EMPLOYEES__region"
where last_modified is null


;')
  select
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select  * from 

    [dbt_test__audit.testview_a897b60fa0cba64026678a83bccbe01b]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_a897b60fa0cba64026678a83bccbe01b]
  ;')