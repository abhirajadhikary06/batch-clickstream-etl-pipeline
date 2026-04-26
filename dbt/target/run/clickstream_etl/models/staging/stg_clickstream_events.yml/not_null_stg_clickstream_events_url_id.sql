select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select url_id
from analytics_staging.stg_clickstream_events
where url_id is null



      
    ) dbt_internal_test