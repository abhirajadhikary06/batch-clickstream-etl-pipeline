from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv


def load_repo_env() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    load_dotenv(repo_root / ".env", override=False)
