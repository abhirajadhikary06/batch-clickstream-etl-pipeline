from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import requests

from src.warehouse.env import load_repo_env


def log_pipeline_event(stage: str, status: str, details: dict[str, Any] | None = None) -> None:
    load_repo_env()

    endpoint = os.getenv("OPENOBSERVE_INGEST_URL", "").strip()
    if not endpoint:
        return

    token = os.getenv("OPENOBSERVE_TOKEN", "").strip()
    timeout_seconds = int(os.getenv("OPENOBSERVE_TIMEOUT_SECONDS", "10"))

    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "status": status,
        "details": details or {},
    }

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = requests.post(
        endpoint,
        data=json.dumps(payload),
        headers=headers,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
