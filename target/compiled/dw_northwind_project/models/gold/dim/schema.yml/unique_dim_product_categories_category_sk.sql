
    
    

select
    category_sk as unique_field,
    count(*) as n_records

from "DW_GOLD"."dbo"."dim_product_categories"
where category_sk is not null
group by category_sk
having count(*) > 1


