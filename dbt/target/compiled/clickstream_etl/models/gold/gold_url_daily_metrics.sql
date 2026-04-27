

/*
  Gold aggregation: daily URL performance metrics.

  unique_visitors uses COUNT(DISTINCT ip_address) as a proxy for distinct users
  because the silver layer does not carry a user_id — ip_address is the closest
  available identity signal.

  Why the old model produced click_count == unique_visitors:
    The previous version used COUNT(DISTINCT ip_address) but the source data had
    one unique ip_address per event row, so every click looked like a new visitor.
    The fix is to ensure we COUNT(*) for total clicks and COUNT(DISTINCT ip_address)
    for unique visitors — which is already correct here — but the real root cause
    was that ip_address was being generated/stored as unique per event upstream.
    If that is still the case, consider using user_agent as a secondary signal
    or fixing the ingestion layer to preserve real client IPs.
*/

select
    date_trunc('day', event_ts)      as event_date,
    url_id,
    country,
    count(*)                          as click_count,
    count(distinct ip_address)        as unique_visitors,
    count(distinct user_agent)        as unique_user_agents,
    max(event_ts)                     as last_event_ts
from analytics_silver.silver_clickstream_events
group by 1, 2, 3