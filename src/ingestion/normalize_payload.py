from __future__ import annotations

import ipaddress
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from src.ingestion.schemas import ClickstreamEvent


REQUIRED_KEYS = {
    "event_id",
    "timestamp",
    "url_id",
    "ip_address",
    "user_agent",
    "referrer",
    "country",
}


def _normalize_timestamp(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    ts = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    return ts.isoformat().replace("+00:00", "Z")


def _normalize_event(raw: Dict[str, Any]) -> ClickstreamEvent:
    missing = REQUIRED_KEYS.difference(raw.keys())
    if missing:
        raise ValueError(f"missing required keys: {sorted(missing)}")

    event_id = str(raw["event_id"]).strip()
    UUID(event_id)

    timestamp = _normalize_timestamp(raw["timestamp"])

    url_id = int(raw["url_id"])
    if url_id <= 0:
        raise ValueError("url_id must be > 0")

    ip_address = str(raw["ip_address"]).strip()
    ipaddress.ip_address(ip_address)

    user_agent = str(raw["user_agent"]).strip()
    if not user_agent:
        raise ValueError("user_agent cannot be empty")

    referrer_raw = raw["referrer"]
    referrer: Optional[str]
    if referrer_raw is None:
        referrer = None
    else:
        referrer = str(referrer_raw).strip() or None

    country = str(raw["country"]).strip().upper()
    if len(country) != 2:
        raise ValueError("country must be ISO-2 code")

    return {
        "event_id": event_id,
        "timestamp": timestamp,
        "url_id": url_id,
        "ip_address": ip_address,
        "user_agent": user_agent,
        "referrer": referrer,
        "country": country,
    }


def normalize_events(payload: List[Any]) -> List[ClickstreamEvent]:
    normalized: List[ClickstreamEvent] = []
    for idx, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"event at index {idx} must be an object")
        try:
            normalized.append(_normalize_event(item))
        except Exception as exc:
            raise ValueError(f"invalid event at index {idx}: {exc}") from exc
    return normalized
