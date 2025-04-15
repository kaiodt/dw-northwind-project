
    
    

select
    employee_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_EMPLOYEES__employees"
where employee_id is not null
group by employee_id
having count(*) > 1


