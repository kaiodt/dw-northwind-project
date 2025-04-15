
    
    

select
    territory_description as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_EMPLOYEES__territories"
where territory_description is not null
group by territory_description
having count(*) > 1


