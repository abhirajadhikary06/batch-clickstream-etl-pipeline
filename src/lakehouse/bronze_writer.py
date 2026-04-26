from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

from src.ingestion.schemas import ClickstreamEvent


def _parse_event_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def write_bronze_batch(
    events: List[ClickstreamEvent],
    base_path: str = "data/bronze",
    batch_ts: Optional[datetime] = None,
) -> Optional[datetime]:
    if not events:
        return None

    now = (batch_ts or datetime.now(timezone.utc)).astimezone(timezone.utc)
    dt_partition = now.strftime("%Y-%m-%d")
    hour_partition = now.strftime("%H")

    output_dir = Path(base_path) / f"dt={dt_partition}" / f"hour={hour_partition}"
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(events)
    file_name = f"batch_{now.strftime('%Y%m%dT%H%M%S')}.parquet"
    output_path = output_dir / file_name
    df.to_parquet(output_path, index=False)

    max_event_ts = max(_parse_event_ts(event["timestamp"]) for event in events if "timestamp" in event)
    return max_event_ts
