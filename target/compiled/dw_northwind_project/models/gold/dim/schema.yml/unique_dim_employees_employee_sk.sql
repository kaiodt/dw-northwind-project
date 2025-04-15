
    
    

select
    employee_sk as unique_field,
    count(*) as n_records

from "DW_GOLD"."dbo"."dim_employees"
where employee_sk is not null
group by employee_sk
having count(*) > 1


