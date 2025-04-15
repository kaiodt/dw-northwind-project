
    
    

select
    territory_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_EMPLOYEES__territories"
where territory_id is not null
group by territory_id
having count(*) > 1


