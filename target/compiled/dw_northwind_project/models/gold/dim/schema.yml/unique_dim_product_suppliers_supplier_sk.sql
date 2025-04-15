
    
    

select
    supplier_sk as unique_field,
    count(*) as n_records

from "DW_GOLD"."dbo"."dim_product_suppliers"
where supplier_sk is not null
group by supplier_sk
having count(*) > 1


