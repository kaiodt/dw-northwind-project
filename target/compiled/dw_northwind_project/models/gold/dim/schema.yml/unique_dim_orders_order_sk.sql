
    
    

select
    order_sk as unique_field,
    count(*) as n_records

from "DW_GOLD"."dbo"."dim_orders"
where order_sk is not null
group by order_sk
having count(*) > 1


