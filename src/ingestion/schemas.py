from __future__ import annotations

from typing import Optional, TypedDict


class ClickstreamEvent(TypedDict):
    event_id: str
    timestamp: str
    url_id: int
    ip_address: str
    user_agent: str
    referrer: Optional[str]
    country: str
