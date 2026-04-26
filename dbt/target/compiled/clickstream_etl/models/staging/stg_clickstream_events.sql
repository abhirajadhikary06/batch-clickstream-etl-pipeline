select
    trim(event_id) as event_id,
    cast(event_timestamp as timestamp with time zone) as event_ts,
    cast(url_id as integer) as url_id,
    trim(ip_address) as ip_address,
    trim(user_agent) as user_agent,
    nullif(trim(referrer), '') as referrer,
    upper(trim(country)) as country,
    ingestion_ts
from raw.clickstream_events
where event_id is not null