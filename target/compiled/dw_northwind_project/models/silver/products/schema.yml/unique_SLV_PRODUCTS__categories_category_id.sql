
    
    

select
    category_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_PRODUCTS__categories"
where category_id is not null
group by category_id
having count(*) > 1


