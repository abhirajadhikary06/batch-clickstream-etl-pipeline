from __future__ import annotations

import os
from dataclasses import dataclass

from crate import client

from src.warehouse.env import load_repo_env


@dataclass(frozen=True)
class CrateSettings:
    url: str
    username: str
    password: str
    verify_ssl_cert: bool = True


def _env_bool(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def get_crate_settings() -> CrateSettings:
    load_repo_env()

    password = _first_env("CRATE_PASSWORD", "CRATEDB_PASSWORD")
    if not password:
        raise RuntimeError("CRATE_PASSWORD or CRATEDB_PASSWORD is not set")

    url = _first_env(
        "CRATE_URL",
        "CRATEDB_HOST",
    ) or "https://crate-db-warehouse.eks1.us-west-2.aws.cratedb.net:4200"

    username = _first_env(
        "CRATE_USERNAME",
        "CRATEDB_USERNAME",
    ) or "admin"

    verify_ssl_cert = _env_bool("CRATE_VERIFY_SSL_CERT", _env_bool("CRATEDB_VERIFY_SSL_CERT", True))

    return CrateSettings(
        url=url,
        username=username,
        password=password,
        verify_ssl_cert=verify_ssl_cert,
    )


def connect_to_crate():
    settings = get_crate_settings()
    return client.connect(
        settings.url,
        username=settings.username,
        password=settings.password,
        verify_ssl_cert=settings.verify_ssl_cert,
    )


def smoke_test_crate_connection() -> tuple[str, ...] | None:
    with connect_to_crate() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.cluster")
        return cursor.fetchone()
