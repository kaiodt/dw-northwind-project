
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "DW_SILVER"."dbo"."SLV_ORDERS__orders"
where order_id is not null
group by order_id
having count(*) > 1


