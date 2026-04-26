{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

select
    date_trunc('day', event_ts) as event_date,
    url_id,
    country,
    count(*) as click_count,
    count(distinct ip_address) as unique_visitors,
    count(distinct user_agent) as unique_user_agents,
    max(event_ts) as last_event_ts
from {{ ref('silver_clickstream_events') }}
group by 1, 2, 3
