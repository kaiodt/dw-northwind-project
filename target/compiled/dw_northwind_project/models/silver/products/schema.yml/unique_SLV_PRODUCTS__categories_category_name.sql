
    
    

select
    category_name as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_PRODUCTS__categories"
where category_name is not null
group by category_name
having count(*) > 1


