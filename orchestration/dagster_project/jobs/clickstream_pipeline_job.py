from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from dagster import RetryPolicy, job, op

from src.bi.metabase_client import (
    trigger_dashboard_refresh as trigger_dashboard_refresh_api,
    validate_metabase_api as validate_metabase_api_health,
)
from src.ingestion.fetch_events import fetch_events_since
from src.ingestion.watermark_store import WatermarkStore
from src.lakehouse.bronze_writer import write_bronze_batch
from src.observability.openobserve_logger import log_pipeline_event
from src.warehouse.crate_connection import smoke_test_crate_connection
from src.warehouse.dbt_runner import run_dbt_command
from src.warehouse.raw_loader import load_events_to_raw_clickstream


@op
def ingest_to_bronze() -> Dict[str, Any]:
    watermark_store = WatermarkStore()
    watermark = watermark_store.get()

    if watermark is None:
        watermark = datetime.now(timezone.utc) - timedelta(minutes=30)

    events = fetch_events_since(watermark)
    max_event_ts = write_bronze_batch(events)

    if max_event_ts is not None:
        return {"events": events, "ingested": len(events), "watermark": max_event_ts.isoformat()}

    return {"events": [], "ingested": 0, "watermark": "unchanged"}


@op
def validate_crate_connection() -> str:
    cluster_info = smoke_test_crate_connection()
    if not cluster_info:
        raise RuntimeError("CrateDB connection test returned no cluster name")
    return cluster_info[0]


@op
def load_raw_events_to_crate(batch: Dict[str, Any], crate_cluster: str) -> Dict[str, Any]:
    _ = crate_cluster
    events: List[Dict[str, Any]] = batch["events"]
    inserted = load_events_to_raw_clickstream(events)
    return {
        "events": events,
        "ingested": batch.get("ingested", 0),
        "watermark": batch.get("watermark", "unchanged"),
        "raw_rows": inserted,
    }


@op(retry_policy=RetryPolicy(max_retries=1, delay=10), tags={"dagster/max_runtime": "300"})
def run_dbt_staging_and_tests(batch: Dict[str, Any]) -> Dict[str, Any]:
    run_dbt_command(["run", "--select", "stg_clickstream_events"])
    run_dbt_command(["test", "--select", "stg_clickstream_events"])
    return batch


@op
def run_dbt_silver_and_tests(batch: Dict[str, Any]) -> Dict[str, Any]:
    run_dbt_command(["run", "--select", "silver_clickstream_events"])
    run_dbt_command(["test", "--select", "silver_clickstream_events"])
    return batch


@op
def run_dbt_gold(batch: Dict[str, Any]) -> Dict[str, Any]:
    run_dbt_command(["run", "--select", "gold_url_daily_metrics"])
    run_dbt_command(["test", "--select", "gold_url_daily_metrics"])
    return batch


@op(retry_policy=RetryPolicy(max_retries=2, delay=10), tags={"dagster/max_runtime": "180"})
def validate_metabase_api(batch: Dict[str, Any]) -> Dict[str, Any]:
    try:
        result = validate_metabase_api_health()
        log_pipeline_event("metabase_api", "success", {"result": result})
        return batch
    except Exception as exc:
        log_pipeline_event("metabase_api", "error", {"error": str(exc)})
        raise RuntimeError("Metabase API validation failed") from exc


@op(retry_policy=RetryPolicy(max_retries=2, delay=10), tags={"dagster/max_runtime": "240"})
def trigger_dashboard_refresh(batch: Dict[str, Any]) -> Dict[str, Any]:
    try:
        result = trigger_dashboard_refresh_api()
        log_pipeline_event("metabase_refresh", "success", {"result": result})
        return batch
    except Exception as exc:
        log_pipeline_event("metabase_refresh", "error", {"error": str(exc)})
        raise RuntimeError("Metabase dashboard refresh failed") from exc


@op
def finalize_watermark(batch: Dict[str, Any]) -> str:
    watermark_value = batch.get("watermark")
    if watermark_value in (None, "unchanged"):
        return "watermark_unchanged"

    watermark_store = WatermarkStore()
    watermark_store.set(datetime.fromisoformat(watermark_value.replace("Z", "+00:00")))
    return f"watermark={watermark_value}"


@job
def clickstream_pipeline_job() -> None:
    batch = ingest_to_bronze()
    crate_cluster = validate_crate_connection()
    loaded_batch = load_raw_events_to_crate(batch, crate_cluster)
    staged_batch = run_dbt_staging_and_tests(loaded_batch)
    silver_batch = run_dbt_silver_and_tests(staged_batch)
    gold_batch = run_dbt_gold(silver_batch)
    metabase_validated_batch = validate_metabase_api(gold_batch)
    metabase_refreshed_batch = trigger_dashboard_refresh(metabase_validated_batch)
    finalize_watermark(metabase_refreshed_batch)
