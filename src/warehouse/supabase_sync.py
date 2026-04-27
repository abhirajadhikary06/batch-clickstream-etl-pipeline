from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Sequence

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from src.warehouse.crate_connection import connect_to_crate
from src.warehouse.env import load_repo_env


@dataclass(frozen=True)
class SupabaseGoldSyncSettings:
    supabase_db_url: str
    supabase_schema: str = "analytics_gold"
    supabase_table: str = "gold_url_daily_metrics"
    crate_source_schema: str = "analytics_gold"
    crate_source_table: str = "gold_url_daily_metrics"


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()
    return None


def _validated_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def get_supabase_gold_sync_settings() -> SupabaseGoldSyncSettings:
    load_repo_env()

    supabase_db_url = _first_env("SUPABASE_DB_URL", "SUPABASE_POSTGRES_URL")
    if not supabase_db_url:
        raise RuntimeError("SUPABASE_DB_URL or SUPABASE_POSTGRES_URL is not set")

    return SupabaseGoldSyncSettings(
        supabase_db_url=supabase_db_url,
        supabase_schema=_first_env("SUPABASE_GOLD_SCHEMA") or "analytics_gold",
        supabase_table=_first_env("SUPABASE_GOLD_TABLE") or "gold_url_daily_metrics",
        crate_source_schema=_first_env("CRATE_GOLD_SCHEMA") or "analytics_gold",
        crate_source_table=_first_env("CRATE_GOLD_TABLE") or "gold_url_daily_metrics",
    )


def _as_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000.0 if float(value) > 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"Unsupported datetime value: {type(value)!r}")


def _as_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000.0 if float(value) > 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, tz=timezone.utc).date()
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise TypeError(f"Unsupported date value: {type(value)!r}")


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, Decimal)):
        return int(value)
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"Unsupported integer value: {type(value)!r}")


def fetch_gold_metrics_from_crate(settings: SupabaseGoldSyncSettings) -> list[tuple[Any, ...]]:
    source_schema = _validated_identifier(settings.crate_source_schema)
    source_table = _validated_identifier(settings.crate_source_table)
    query = f"""
        SELECT
            event_date,
            url_id,
            country,
            click_count,
            unique_visitors,
            unique_user_agents,
            last_event_ts
        FROM {source_schema}.{source_table}
    """

    with connect_to_crate() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()


def _normalize_rows(rows: Sequence[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    normalized: list[tuple[Any, ...]] = []
    for row in rows:
        normalized.append(
            (
                _as_date(row[0]),
                _as_int(row[1]),
                str(row[2]),
                _as_int(row[3]),
                _as_int(row[4]),
                _as_int(row[5]),
                _as_iso_datetime(row[6]),
            )
        )
    return normalized


def ensure_supabase_gold_table(cursor, settings: SupabaseGoldSyncSettings) -> None:
    cursor.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(settings.supabase_schema))
    )
    cursor.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                event_date DATE NOT NULL,
                url_id BIGINT NOT NULL,
                country TEXT NOT NULL,
                click_count BIGINT NOT NULL,
                unique_visitors BIGINT NOT NULL,
                unique_user_agents BIGINT NOT NULL,
                last_event_ts TIMESTAMPTZ,
                PRIMARY KEY (event_date, url_id, country)
            );
            """
        ).format(
            sql.Identifier(settings.supabase_schema),
            sql.Identifier(settings.supabase_table),
        )
    )


def replace_supabase_gold_metrics(settings: SupabaseGoldSyncSettings, rows: Sequence[tuple[Any, ...]]) -> int:
    normalized_rows = _normalize_rows(rows)

    with psycopg2.connect(settings.supabase_db_url) as conn:
        with conn.cursor() as cursor:
            ensure_supabase_gold_table(cursor, settings)
            cursor.execute(
                sql.SQL("TRUNCATE TABLE {}.{};").format(
                    sql.Identifier(settings.supabase_schema),
                    sql.Identifier(settings.supabase_table),
                )
            )

            if normalized_rows:
                insert_template = "(%s,%s,%s,%s,%s,%s,%s)"
                insert_sql = sql.SQL(
                    """
                    INSERT INTO {}.{} (
                        event_date,
                        url_id,
                        country,
                        click_count,
                        unique_visitors,
                        unique_user_agents,
                        last_event_ts
                    ) VALUES %s;
                    """
                ).format(
                    sql.Identifier(settings.supabase_schema),
                    sql.Identifier(settings.supabase_table),
                )
                execute_values(cursor, insert_sql.as_string(conn), normalized_rows, template=insert_template)

        conn.commit()

    return len(normalized_rows)


def sync_gold_metrics_to_supabase() -> int:
    settings = get_supabase_gold_sync_settings()
    rows = fetch_gold_metrics_from_crate(settings)
    return replace_supabase_gold_metrics(settings, rows)
