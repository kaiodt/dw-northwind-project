
    
    

select
    region_description as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_EMPLOYEES__region"
where region_description is not null
group by region_description
having count(*) > 1


