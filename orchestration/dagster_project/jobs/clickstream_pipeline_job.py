from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dagster import job, op

from src.ingestion.fetch_events import fetch_events_since
from src.ingestion.watermark_store import WatermarkStore
from src.lakehouse.bronze_writer import write_bronze_batch


@op
def ingest_to_bronze() -> str:
    watermark_store = WatermarkStore()
    watermark = watermark_store.get()

    if watermark is None:
        watermark = datetime.now(timezone.utc) - timedelta(minutes=30)

    events = fetch_events_since(watermark)
    max_event_ts = write_bronze_batch(events)

    if max_event_ts is not None:
        watermark_store.set(max_event_ts)
        return f"ingested={len(events)} watermark={max_event_ts.isoformat()}"

    return "ingested=0 watermark=unchanged"


@job
def clickstream_pipeline_job() -> None:
    ingest_to_bronze()
