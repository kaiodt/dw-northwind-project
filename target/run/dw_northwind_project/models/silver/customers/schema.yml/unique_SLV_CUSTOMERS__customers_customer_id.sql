
  
  

  
  USE [DW_SILVER];
  EXEC('create view 

    [dbt_test__audit.testview_daf99e12be562c2ee4d78ad0b47fba96]
   as 
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_CUSTOMERS__customers"
where customer_id is not null
group by customer_id
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

    [dbt_test__audit.testview_daf99e12be562c2ee4d78ad0b47fba96]
  
  ) dbt_internal_test;

  USE [DW_SILVER];
  EXEC('drop view 

    [dbt_test__audit.testview_daf99e12be562c2ee4d78ad0b47fba96]
  ;')