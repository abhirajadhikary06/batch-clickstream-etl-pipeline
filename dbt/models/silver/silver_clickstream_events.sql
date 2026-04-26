{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

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
    from {{ ref('stg_clickstream_events') }}
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
    {% if is_incremental() %}
        where event_ts > coalesce((select max(event_ts) from {{ this }}), timestamp '1970-01-01 00:00:00+00')
    {% endif %}
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
