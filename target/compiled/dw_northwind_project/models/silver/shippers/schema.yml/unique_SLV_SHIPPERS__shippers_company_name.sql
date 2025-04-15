
    
    

select
    company_name as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers"
where company_name is not null
group by company_name
having count(*) > 1


