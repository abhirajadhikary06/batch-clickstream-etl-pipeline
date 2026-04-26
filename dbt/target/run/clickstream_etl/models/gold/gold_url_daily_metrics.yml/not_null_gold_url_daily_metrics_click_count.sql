select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select click_count
from analytics_gold.gold_url_daily_metrics
where click_count is null



      
    ) dbt_internal_test