
    
    

select
    territory_sk as unique_field,
    count(*) as n_records

from "DW_GOLD"."dbo"."dim_territories"
where territory_sk is not null
group by territory_sk
having count(*) > 1


