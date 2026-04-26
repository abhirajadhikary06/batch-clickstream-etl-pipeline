

with source_data as (
    select
        event_id,
        event_ts,
        url_id,
        ip_address,
        user_agent,
        referrer,
        country,
        ingestion_ts
    from analytics_staging.stg_clickstream_events
    where event_id is not null
),

deduped as (
    select
        event_id,
        event_ts,
        url_id,
        ip_address,
        user_agent,
        referrer,
        country,
        ingestion_ts,
        row_number() over (
            partition by event_id
            order by event_ts desc, ingestion_ts desc
        ) as rn
    from source_data
    
        where event_ts > coalesce((select max(event_ts) from analytics_silver.silver_clickstream_events), timestamp '1970-01-01 00:00:00+00')
    
)

select
    event_id,
    event_ts,
    url_id,
    ip_address,
    user_agent,
    referrer,
    country,
    ingestion_ts
from deduped
where rn = 1