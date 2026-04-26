from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

from src.ingestion.normalize_payload import normalize_events
from src.ingestion.schemas import ClickstreamEvent


API_URL = "https://url-fake-clickstream.fastapicloud.dev/events"


def to_iso_z(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_events_since(since_ts: datetime, timeout_sec: int = 30) -> List[ClickstreamEvent]:
    params = {"since": to_iso_z(since_ts)}
    response = requests.get(API_URL, params=params, timeout=timeout_sec)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError("Expected API response to be a JSON array")

    return normalize_events(data)
