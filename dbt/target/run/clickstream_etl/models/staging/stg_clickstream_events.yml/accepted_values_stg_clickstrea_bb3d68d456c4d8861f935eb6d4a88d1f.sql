select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        country as value_field,
        count(*) as n_records

    from analytics_staging.stg_clickstream_events
    group by country

)

select *
from all_values
where value_field not in (
    'US','IN','GB','DE','FR','CA','AU','JP','BR','NG','ZA','BD','NZ','AE'
)



      
    ) dbt_internal_test