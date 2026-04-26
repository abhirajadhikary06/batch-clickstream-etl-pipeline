
      insert into analytics_silver.silver_clickstream_events ("event_id", "event_ts", "url_id", "ip_address", "user_agent", "referrer", "country", "ingestion_ts")
    (
        select "event_id", "event_ts", "url_id", "ip_address", "user_agent", "referrer", "country", "ingestion_ts"
        from analytics_silver.silver_clickstream_events__dbt_tmp
    )


  