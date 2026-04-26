from __future__ import annotations

import os
from dataclasses import dataclass

import requests

from src.warehouse.env import load_repo_env


@dataclass(frozen=True)
class MetabaseSettings:
    base_url: str
    api_key: str | None
    username: str | None
    password: str | None
    enabled: bool
    timeout_seconds: int = 20


def get_metabase_settings() -> MetabaseSettings:
    load_repo_env()

    base_url = os.getenv("METABASE_URL", "http://localhost:3001").rstrip("/")
    api_key = os.getenv("METABASE_API_KEY")
    username = os.getenv("METABASE_USERNAME")
    password = os.getenv("METABASE_PASSWORD")
    enabled = os.getenv("METABASE_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    timeout_seconds = int(os.getenv("METABASE_TIMEOUT_SECONDS", "20"))

    return MetabaseSettings(
        base_url=base_url,
        api_key=api_key,
        username=username,
        password=password,
        enabled=enabled,
        timeout_seconds=timeout_seconds,
    )


def _build_auth_headers(settings: MetabaseSettings) -> dict[str, str]:
    headers: dict[str, str] = {}

    if settings.api_key:
        headers["x-api-key"] = settings.api_key
        return headers

    if settings.username and settings.password:
        response = requests.post(
            f"{settings.base_url}/api/session",
            json={"username": settings.username, "password": settings.password},
            timeout=settings.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        session_id = data.get("id")
        if not session_id:
            raise RuntimeError("Metabase login succeeded but no session id returned")
        headers["X-Metabase-Session"] = str(session_id)

    if not headers:
        raise RuntimeError(
            "Metabase auth is required: set METABASE_API_KEY or METABASE_USERNAME/METABASE_PASSWORD."
        )

    return headers


def validate_metabase_api() -> str:
    settings = get_metabase_settings()
    if not settings.enabled:
        raise RuntimeError("Metabase is mandatory for BI. Set METABASE_ENABLED=true.")

    response = requests.get(f"{settings.base_url}/api/health", timeout=settings.timeout_seconds)
    response.raise_for_status()
    return "ok"


def trigger_dashboard_refresh() -> str:
    settings = get_metabase_settings()
    if not settings.enabled:
        raise RuntimeError("Metabase is mandatory for BI. Set METABASE_ENABLED=true.")

    card_ids_raw = os.getenv("METABASE_CARD_IDS", "").strip()
    if not card_ids_raw:
        raise RuntimeError("METABASE_CARD_IDS is required for dashboard refresh.")

    headers = _build_auth_headers(settings)
    card_ids = [cid.strip() for cid in card_ids_raw.split(",") if cid.strip()]
    refreshed = 0

    for card_id in card_ids:
        # Re-run card query so dashboard tiles pick up latest warehouse state.
        response = requests.post(
            f"{settings.base_url}/api/card/{card_id}/query/json",
            headers=headers,
            json={"parameters": []},
            timeout=settings.timeout_seconds,
        )
        response.raise_for_status()
        refreshed += 1

    return f"refreshed_cards={refreshed}"
