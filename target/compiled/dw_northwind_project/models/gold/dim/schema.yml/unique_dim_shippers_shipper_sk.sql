
    
    

select
    shipper_sk as unique_field,
    count(*) as n_records

from "DW_GOLD"."dbo"."dim_shippers"
where shipper_sk is not null
group by shipper_sk
having count(*) > 1


