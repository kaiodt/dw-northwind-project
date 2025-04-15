
    
    

select
    date_sk as unique_field,
    count(*) as n_records

from "DW_GOLD"."dbo"."vw_dim_dates_current"
where date_sk is not null
group by date_sk
having count(*) > 1


