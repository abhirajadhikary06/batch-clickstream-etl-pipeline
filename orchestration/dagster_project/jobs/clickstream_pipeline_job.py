from __future__ import annotations

from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Dict, List

from dagster import RetryPolicy, job, op

from src.bi.metabase_client import (
    trigger_dashboard_refresh as trigger_dashboard_refresh_api,
    validate_metabase_api as validate_metabase_api_health,
)
from src.ingestion.fetch_events import fetch_events_since
from src.ingestion.watermark_store import WatermarkStore
from src.lakehouse.bronze_writer import write_bronze_batch
from src.observability.openobserve_logger import log_stage_event
from src.observability.telemetry import emit_stage_telemetry
from src.warehouse.crate_connection import smoke_test_crate_connection
from src.warehouse.dbt_runner import run_dbt_command
from src.warehouse.raw_loader import load_events_to_raw_clickstream
from src.warehouse.supabase_sync import sync_gold_metrics_to_supabase


def _emit_stage(
    context,
    *,
    stage: str,
    status: str,
    details: dict[str, Any] | None = None,
) -> None:
    op_name = context.op.name
    job_name = context.job_name
    run_id = context.run_id

    try:
        emit_stage_telemetry(context, stage, status, details)
    except Exception as exc:
        context.log.warning("Failed to emit OTLP telemetry for %s: %s", stage, exc)

    logged = log_stage_event(
        stage=stage,
        status=status,
        op_name=op_name,
        job_name=job_name,
        run_id=run_id,
        details=details,
    )

    if not logged and status == "error":
        context.log.warning("Failed to emit observability event for %s", stage)


@op
def ingest_to_bronze(context) -> Dict[str, Any]:
    stage = "ingest_to_bronze"
    started = perf_counter()
    _emit_stage(context, stage=stage, status="start")

    try:
        watermark_store = WatermarkStore()
        watermark = watermark_store.get()

        if watermark is None:
            watermark = datetime.now(timezone.utc) - timedelta(minutes=30)

        events = fetch_events_since(watermark)
        max_event_ts = write_bronze_batch(events)

        if max_event_ts is not None:
            result = {"events": events, "ingested": len(events), "watermark": max_event_ts.isoformat()}
        else:
            result = {"events": [], "ingested": 0, "watermark": "unchanged"}

        _emit_stage(
            context,
            stage=stage,
            status="success",
            details={
                "ingested": result.get("ingested", 0),
                "watermark": result.get("watermark"),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return result
    except Exception as exc:
        _emit_stage(
            context,
            stage=stage,
            status="error",
            details={
                "error": str(exc),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        raise


@op
def validate_crate_connection(context) -> str:
    stage = "validate_crate_connection"
    started = perf_counter()
    _emit_stage(context, stage=stage, status="start")

    try:
        cluster_info = smoke_test_crate_connection()
        if not cluster_info:
            raise RuntimeError("CrateDB connection test returned no cluster name")

        result = cluster_info[0]
        _emit_stage(
            context,
            stage=stage,
            status="success",
            details={
                "cluster": result,
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return result
    except Exception as exc:
        _emit_stage(
            context,
            stage=stage,
            status="error",
            details={
                "error": str(exc),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        raise


@op
def load_raw_events_to_crate(context, batch: Dict[str, Any], crate_cluster: str) -> Dict[str, Any]:
    stage = "load_raw_events_to_crate"
    started = perf_counter()
    _emit_stage(context, stage=stage, status="start", details={"cluster": crate_cluster})

    try:
        events: List[Dict[str, Any]] = batch["events"]
        inserted = load_events_to_raw_clickstream(events)
        result = {
            "events": events,
            "ingested": batch.get("ingested", 0),
            "watermark": batch.get("watermark", "unchanged"),
            "raw_rows": inserted,
        }
        _emit_stage(
            context,
            stage=stage,
            status="success",
            details={
                "raw_rows": inserted,
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return result
    except Exception as exc:
        _emit_stage(
            context,
            stage=stage,
            status="error",
            details={
                "error": str(exc),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        raise


@op(retry_policy=RetryPolicy(max_retries=1, delay=10), tags={"dagster/max_runtime": "300"})
def run_dbt_staging_and_tests(context, batch: Dict[str, Any]) -> Dict[str, Any]:
    stage = "run_dbt_staging_and_tests"
    started = perf_counter()
    _emit_stage(context, stage=stage, status="start")

    try:
        run_dbt_command(["run", "--select", "stg_clickstream_events"])
        run_dbt_command(["test", "--select", "stg_clickstream_events"])
        _emit_stage(
            context,
            stage=stage,
            status="success",
            details={
                "model": "stg_clickstream_events",
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return batch
    except Exception as exc:
        _emit_stage(
            context,
            stage=stage,
            status="error",
            details={
                "model": "stg_clickstream_events",
                "error": str(exc),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        raise


@op
def run_dbt_silver_and_tests(context, batch: Dict[str, Any]) -> Dict[str, Any]:
    stage = "run_dbt_silver_and_tests"
    started = perf_counter()
    _emit_stage(context, stage=stage, status="start")

    try:
        run_dbt_command(["run", "--select", "silver_clickstream_events"])
        run_dbt_command(["test", "--select", "silver_clickstream_events"])
        _emit_stage(
            context,
            stage=stage,
            status="success",
            details={
                "model": "silver_clickstream_events",
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return batch
    except Exception as exc:
        _emit_stage(
            context,
            stage=stage,
            status="error",
            details={
                "model": "silver_clickstream_events",
                "error": str(exc),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        raise


@op
def run_dbt_gold(context, batch: Dict[str, Any]) -> Dict[str, Any]:
    stage = "run_dbt_gold"
    started = perf_counter()
    _emit_stage(context, stage=stage, status="start")

    try:
        run_dbt_command(["run", "--select", "gold_url_daily_metrics"])
        run_dbt_command(["test", "--select", "gold_url_daily_metrics"])
        _emit_stage(
            context,
            stage=stage,
            status="success",
            details={
                "model": "gold_url_daily_metrics",
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return batch
    except Exception as exc:
        _emit_stage(
            context,
            stage=stage,
            status="error",
            details={
                "model": "gold_url_daily_metrics",
                "error": str(exc),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        raise


@op(retry_policy=RetryPolicy(max_retries=1, delay=10), tags={"dagster/max_runtime": "240"})
def sync_gold_to_supabase(context, batch: Dict[str, Any]) -> Dict[str, Any]:
    stage = "sync_gold_to_supabase"
    started = perf_counter()
    _emit_stage(context, stage=stage, status="start")

    try:
        synced_rows = sync_gold_metrics_to_supabase()
        _emit_stage(
            context,
            stage=stage,
            status="success",
            details={
                "supabase_rows": synced_rows,
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return batch
    except Exception as exc:
        _emit_stage(
            context,
            stage=stage,
            status="error",
            details={
                "error": str(exc),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        raise RuntimeError("Supabase gold sync failed") from exc


@op(retry_policy=RetryPolicy(max_retries=2, delay=10), tags={"dagster/max_runtime": "180"})
def validate_metabase_api(context, batch: Dict[str, Any]) -> Dict[str, Any]:
    stage = "validate_metabase_api"
    started = perf_counter()
    _emit_stage(context, stage=stage, status="start")

    try:
        result = validate_metabase_api_health()
        _emit_stage(
            context,
            stage=stage,
            status="success",
            details={
                "result": result,
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return batch
    except Exception as exc:
        _emit_stage(
            context,
            stage=stage,
            status="error",
            details={
                "error": str(exc),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        raise RuntimeError("Metabase API validation failed") from exc


@op(retry_policy=RetryPolicy(max_retries=2, delay=10), tags={"dagster/max_runtime": "240"})
def trigger_dashboard_refresh(context, batch: Dict[str, Any]) -> Dict[str, Any]:
    stage = "trigger_dashboard_refresh"
    started = perf_counter()
    _emit_stage(context, stage=stage, status="start")

    try:
        result = trigger_dashboard_refresh_api()
        _emit_stage(
            context,
            stage=stage,
            status="success",
            details={
                "result": result,
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return batch
    except Exception as exc:
        _emit_stage(
            context,
            stage=stage,
            status="error",
            details={
                "error": str(exc),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        raise RuntimeError("Metabase dashboard refresh failed") from exc


@op
def finalize_watermark(context, batch: Dict[str, Any]) -> str:
    stage = "finalize_watermark"
    started = perf_counter()
    _emit_stage(context, stage=stage, status="start")

    try:
        watermark_value = batch.get("watermark")
        if watermark_value in (None, "unchanged"):
            result = "watermark_unchanged"
        else:
            watermark_store = WatermarkStore()
            watermark_store.set(datetime.fromisoformat(watermark_value.replace("Z", "+00:00")))
            result = f"watermark={watermark_value}"

        _emit_stage(
            context,
            stage=stage,
            status="success",
            details={
                "watermark_result": result,
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        return result
    except Exception as exc:
        _emit_stage(
            context,
            stage=stage,
            status="error",
            details={
                "error": str(exc),
                "duration_ms": round((perf_counter() - started) * 1000, 2),
            },
        )
        raise


@job
def clickstream_pipeline_job() -> None:
    batch = ingest_to_bronze()
    crate_cluster = validate_crate_connection()
    loaded_batch = load_raw_events_to_crate(batch, crate_cluster)
    staged_batch = run_dbt_staging_and_tests(loaded_batch)
    silver_batch = run_dbt_silver_and_tests(staged_batch)
    gold_batch = run_dbt_gold(silver_batch)
    supabase_batch = sync_gold_to_supabase(gold_batch)
    metabase_validated_batch = validate_metabase_api(supabase_batch)
    metabase_refreshed_batch = trigger_dashboard_refresh(metabase_validated_batch)
    finalize_watermark(metabase_refreshed_batch)
