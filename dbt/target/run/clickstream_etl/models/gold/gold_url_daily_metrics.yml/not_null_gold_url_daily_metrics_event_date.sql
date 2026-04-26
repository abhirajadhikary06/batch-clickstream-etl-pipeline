select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select event_date
from analytics_gold.gold_url_daily_metrics
where event_date is null



      
    ) dbt_internal_test