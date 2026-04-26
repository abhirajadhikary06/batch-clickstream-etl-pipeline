select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select country
from analytics_staging.stg_clickstream_events
where country is null



      
    ) dbt_internal_test