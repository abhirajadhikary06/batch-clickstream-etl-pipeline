from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone
from typing import Any

import requests
from requests import RequestException

from src.warehouse.env import load_repo_env


def _get_ingest_config() -> tuple[str, str, str, int]:
    """Get OpenObserve ingest configuration from environment.
    
    Returns:
        tuple: (endpoint, username, password, timeout_seconds)
    """
    load_repo_env()

    endpoint = os.getenv("OPENOBSERVE_INGEST_URL", "").strip()
    username = os.getenv("OPENOBSERVE_USERNAME", "").strip()
    password = os.getenv("OPENOBSERVE_PASSWORD", "").strip()
    timeout_seconds = int(os.getenv("OPENOBSERVE_TIMEOUT_SECONDS", "10"))

    return endpoint, username, password, timeout_seconds


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_pipeline_event(stage: str, status: str, details: dict[str, Any] | None = None) -> bool:
    """Log a pipeline event to OpenObserve.
    
    Args:
        stage: Pipeline stage name
        status: Event status (e.g., 'success', 'failure')
        details: Optional additional details
        
    Returns:
        bool: True if event was logged successfully, False otherwise
    """
    endpoint, username, password, timeout_seconds = _get_ingest_config()
    if not endpoint:
        return False

    payload = {
        "ts": _utc_now_iso(),
        "stage": stage,
        "status": status,
        "details": details or {},
    }

    headers = {"Content-Type": "application/json"}
    
    # OpenObserve uses Basic Authentication
    if username and password:
        # Create Basic Auth header: base64(username:password)
        credentials = f"{username}:{password}".encode()
        auth_token = base64.b64encode(credentials).decode()
        headers["Authorization"] = f"Basic {auth_token}"

    try:
        response = requests.post(
            endpoint,
            data=json.dumps(payload),
            headers=headers,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        return True
    except (RequestException, ValueError):
        # Observability must be best-effort and never break the data pipeline.
        return False


def log_stage_event(
    stage: str,
    status: str,
    *,
    op_name: str,
    job_name: str | None,
    run_id: str | None,
    details: dict[str, Any] | None = None,
) -> bool:
    payload_details: dict[str, Any] = {
        "op_name": op_name,
    }
    if job_name:
        payload_details["job_name"] = job_name
    if run_id:
        payload_details["run_id"] = run_id
    if details:
        payload_details.update(details)

    return log_pipeline_event(stage=stage, status=status, details=payload_details)
