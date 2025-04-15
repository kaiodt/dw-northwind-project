
    
    

select
    shipper_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_SHIPPERS__shippers"
where shipper_id is not null
group by shipper_id
having count(*) > 1


