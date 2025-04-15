
    
    

select
    date_sk as unique_field,
    count(*) as n_records

from "DW_GOLD"."dbo"."dim_dates"
where date_sk is not null
group by date_sk
having count(*) > 1


