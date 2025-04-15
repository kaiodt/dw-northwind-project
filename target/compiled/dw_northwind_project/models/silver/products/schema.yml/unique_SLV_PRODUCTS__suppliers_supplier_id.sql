
    
    

select
    supplier_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_PRODUCTS__suppliers"
where supplier_id is not null
group by supplier_id
having count(*) > 1


