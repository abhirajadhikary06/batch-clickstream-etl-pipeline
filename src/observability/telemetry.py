from __future__ import annotations

import base64
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import Status, StatusCode, Span

from src.warehouse.env import load_repo_env


SERVICE = "batch-clickstream-etl-pipeline"
DEFAULT_BASE_URL = "http://localhost:5080"
DEFAULT_ORG = "default"
DEFAULT_STREAM = "pipeline_events"

_LOCK = threading.Lock()
_INITIALIZED = False
_TRACER = None
_METER = None
_ACTIVE_STAGE_OBSERVATIONS: dict[str, "StageObservation"] = {}


@dataclass
class StageObservation:
    span: Span
    start_perf: float
    stage: str
    op_name: str
    job_name: str | None
    run_id: str | None


def _first_env(*names: str) -> str | None:
    for name in names:
        value = __import__("os").getenv(name)
        if value:
            return value.strip()
    return None


def _env_bool(name: str, default: bool = True) -> bool:
    value = __import__("os").getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _base_url() -> str:
    return _first_env("OPENOBSERVE_BASE_URL", "OPENOBSERVE_HOST") or DEFAULT_BASE_URL


def _org() -> str:
    return _first_env("OPENOBSERVE_ORG", "OPENOBSERVE_ORGANIZATION") or DEFAULT_ORG


def _stream() -> str:
    return _first_env("OPENOBSERVE_STREAM", "OPENOBSERVE_LOG_STREAM") or DEFAULT_STREAM


def _trace_endpoint() -> str:
    return _first_env("OPENOBSERVE_OTLP_TRACES_URL") or f"{_base_url()}/api/{_org()}/v1/traces"


def _metrics_endpoint() -> str:
    return _first_env("OPENOBSERVE_OTLP_METRICS_URL") or f"{_base_url()}/api/{_org()}/v1/metrics"


def _log_endpoint() -> str:
    return _first_env("OPENOBSERVE_INGEST_URL") or f"{_base_url()}/api/{_org()}/{_stream()}/_json"


def _auth_header() -> dict[str, str]:
    """Generate authentication header for OpenObserve.
    
    Priority order:
    1. OPENOBSERVE_AUTH_HEADER (custom override)
    2. Basic Auth (username + password) - Primary method for self-hosted
    3. Bearer token (for cloud/API access)
    
    Returns:
        dict: Authorization header
    """
    header_override = _first_env("OPENOBSERVE_AUTH_HEADER")
    if header_override:
        return {"Authorization": header_override}

    # Primary: Basic Auth (username + password) for self-hosted OpenObserve
    username = _first_env("OPENOBSERVE_USERNAME", "ZO_ROOT_USER_EMAIL")
    password = _first_env("OPENOBSERVE_PASSWORD", "ZO_ROOT_USER_PASSWORD")
    if username and password:
        encoded = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {encoded}"}

    # Fallback: Bearer token (for cloud/API access)
    token = _first_env("OPENOBSERVE_TOKEN")
    if token:
        return {"Authorization": f"Bearer {token}"}

    return {}


def _headers() -> dict[str, str]:
    # OTLP exporters set protocol-specific headers/content-type themselves.
    return _auth_header()


def _resource() -> Resource:
    environment = _first_env("APP_ENV", "ENVIRONMENT") or "local"
    return Resource.create(
        {
            SERVICE_NAME: SERVICE,
            "service.namespace": "clickstream",
            "deployment.environment": environment,
        }
    )


def _ensure_initialized() -> None:
    global _INITIALIZED, _TRACER, _METER

    if _INITIALIZED:
        return

    load_repo_env()

    with _LOCK:
        if _INITIALIZED:
            return

        resource = _resource()
        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(
            SimpleSpanProcessor(
                OTLPSpanExporter(
                    endpoint=_trace_endpoint(),
                    headers=_headers(),
                )
            )
        )
        trace.set_tracer_provider(tracer_provider)

        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(
                endpoint=_metrics_endpoint(),
                headers=_headers(),
            ),
            export_interval_millis=int(__import__("os").getenv("OPENOBSERVE_METRIC_EXPORT_INTERVAL_MS", "5000")),
        )
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)

        _TRACER = trace.get_tracer(SERVICE)
        _METER = metrics.get_meter(SERVICE)
        _register_metrics()
        _INITIALIZED = True


def _register_metrics() -> None:
    meter = _METER
    if meter is None:
        return

    global _STAGE_RUNS_TOTAL, _STAGE_DURATION_MS, _FAILURES_TOTAL, _INGESTED_EVENTS_TOTAL, _RAW_ROWS_TOTAL, _DBT_RUNS_TOTAL, _WATERMARK_AGE_SECONDS

    _STAGE_RUNS_TOTAL = meter.create_counter("pipeline_stage_runs_total")
    _STAGE_DURATION_MS = meter.create_histogram("pipeline_stage_duration_ms")
    _FAILURES_TOTAL = meter.create_counter("pipeline_failures_total")
    _INGESTED_EVENTS_TOTAL = meter.create_counter("pipeline_ingested_events_total")
    _RAW_ROWS_TOTAL = meter.create_counter("pipeline_raw_rows_loaded_total")
    _DBT_RUNS_TOTAL = meter.create_counter("pipeline_dbt_runs_total")
    _WATERMARK_AGE_SECONDS = meter.create_histogram("pipeline_watermark_age_seconds")


def _make_key(context: Any, stage: str) -> str:
    return f"{context.run_id}:{context.job_name}:{context.op.name}:{stage}"


def _safe_attr_value(value: Any) -> Any:
    if value is None:
        return "null"
    if isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, dict):
        return str(value)
    if isinstance(value, list):
        return str(value)
    return str(value)


def _common_attributes(context: Any, stage: str) -> dict[str, Any]:
    return {
        "pipeline.stage": stage,
        "dagster.job_name": context.job_name,
        "dagster.op_name": context.op.name,
        "dagster.run_id": context.run_id,
    }


def _record_metric_flush() -> None:
    with _LOCK:
        provider = trace.get_tracer_provider()
        with __import__("contextlib").suppress(Exception):
            provider.force_flush()
        meter_provider = metrics.get_meter_provider()
        with __import__("contextlib").suppress(Exception):
            meter_provider.force_flush()


def emit_stage_telemetry(context: Any, stage: str, status: str, details: dict[str, Any] | None = None) -> None:
    _ensure_initialized()

    key = _make_key(context, stage)
    attributes = _common_attributes(context, stage)
    details = details or {}

    with _LOCK:
        if status == "start":
            span = _TRACER.start_span(f"dagster.{stage}")
            observation = StageObservation(
                span=span,
                start_perf=perf_counter(),
                stage=stage,
                op_name=context.op.name,
                job_name=context.job_name,
                run_id=context.run_id,
            )
            for name, value in attributes.items():
                span.set_attribute(name, _safe_attr_value(value))
            for name, value in details.items():
                span.set_attribute(name, _safe_attr_value(value))
            _ACTIVE_STAGE_OBSERVATIONS[key] = observation
            span.add_event("stage.start", attributes={"status": "start"})
            return

        observation = _ACTIVE_STAGE_OBSERVATIONS.pop(key, None)

    if observation is None:
        span = _TRACER.start_span(f"dagster.{stage}")
        observation = StageObservation(
            span=span,
            start_perf=perf_counter(),
            stage=stage,
            op_name=context.op.name,
            job_name=context.job_name,
            run_id=context.run_id,
        )

    duration_ms = round((perf_counter() - observation.start_perf) * 1000, 2)
    span = observation.span
    for name, value in attributes.items():
        span.set_attribute(name, _safe_attr_value(value))
    for name, value in details.items():
        span.set_attribute(name, _safe_attr_value(value))
    span.set_attribute("pipeline.status", status)
    span.set_attribute("pipeline.duration_ms", duration_ms)
    span.add_event(f"stage.{status}", attributes={"duration_ms": duration_ms})

    if status == "error":
        error_message = _safe_attr_value(details.get("error", "stage failed"))
        span.record_exception(Exception(error_message))
        span.set_status(Status(StatusCode.ERROR, str(error_message)))
        _FAILURES_TOTAL.add(1, attributes={**attributes, "status": status})
    else:
        span.set_status(Status(StatusCode.OK))

    _STAGE_RUNS_TOTAL.add(1, attributes={**attributes, "status": status})
    _STAGE_DURATION_MS.record(duration_ms, attributes={**attributes, "status": status})

    if "ingested" in details and isinstance(details["ingested"], int):
        _INGESTED_EVENTS_TOTAL.add(details["ingested"], attributes=attributes)

    if "raw_rows" in details and isinstance(details["raw_rows"], int):
        _RAW_ROWS_TOTAL.add(details["raw_rows"], attributes=attributes)

    if "model" in details:
        _DBT_RUNS_TOTAL.add(1, attributes={**attributes, "model": _safe_attr_value(details["model"]), "status": status})

    watermark_value = details.get("watermark")
    if isinstance(watermark_value, str) and watermark_value not in {"null", "unchanged"}:
        try:
            watermark_dt = datetime.fromisoformat(watermark_value.replace("Z", "+00:00"))
            if watermark_dt.tzinfo is None:
                watermark_dt = watermark_dt.replace(tzinfo=timezone.utc)
            age_seconds = max(0.0, (datetime.now(timezone.utc) - watermark_dt).total_seconds())
            _WATERMARK_AGE_SECONDS.record(age_seconds, attributes=attributes)
        except ValueError:
            pass

    with _LOCK:
        span.end()

    _record_metric_flush()
