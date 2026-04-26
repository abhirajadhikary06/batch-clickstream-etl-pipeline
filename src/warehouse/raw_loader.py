from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

from src.warehouse.crate_connection import connect_to_crate


RAW_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS raw"

RAW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.clickstream_events (
    event_id TEXT,
    event_timestamp TEXT,
    url_id INTEGER,
    ip_address TEXT,
    user_agent TEXT,
    referrer TEXT,
    country TEXT,
    ingestion_ts TIMESTAMP WITH TIME ZONE
)
"""

INSERT_SQL = """
INSERT INTO raw.clickstream_events (
    event_id,
    event_timestamp,
    url_id,
    ip_address,
    user_agent,
    referrer,
    country,
    ingestion_ts
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


def ensure_raw_clickstream_table() -> None:
    with connect_to_crate() as conn:
        cursor = conn.cursor()
        cursor.execute(RAW_SCHEMA_SQL)
        cursor.execute(RAW_TABLE_SQL)


def load_events_to_raw_clickstream(events: Sequence[dict[str, Any]]) -> int:
    if not events:
        return 0

    ensure_raw_clickstream_table()
    ingestion_ts = datetime.now(timezone.utc)

    rows = [
        (
            event["event_id"],
            event["timestamp"],
            event["url_id"],
            event["ip_address"],
            event["user_agent"],
            event.get("referrer"),
            event["country"],
            ingestion_ts,
        )
        for event in events
    ]

    with connect_to_crate() as conn:
        cursor = conn.cursor()
        cursor.executemany(INSERT_SQL, rows)

    return len(rows)
