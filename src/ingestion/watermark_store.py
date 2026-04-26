from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class WatermarkStore:
    def __init__(self, path: str = "data/checkpoints/watermark.json") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def get(self) -> Optional[datetime]:
        if not self.path.exists():
            return None

        with self.path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        value = payload.get("last_successful_timestamp")
        if not value:
            return None

        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

    def set(self, ts: datetime) -> None:
        ts_utc = ts.astimezone(timezone.utc)
        payload = {
            "last_successful_timestamp": ts_utc.isoformat().replace("+00:00", "Z")
        }

        with self.path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
