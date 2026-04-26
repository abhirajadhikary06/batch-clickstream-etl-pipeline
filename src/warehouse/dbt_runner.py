from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Sequence

from src.warehouse.env import load_repo_env


def _derive_crate_settings(env: dict[str, str]) -> None:
    if not env.get("CRATE_URL"):
        crate_url = env.get("CRATEDB_HOST")
        if crate_url:
            env["CRATE_URL"] = crate_url

    if not env.get("CRATE_USERNAME"):
        crate_user = env.get("CRATEDB_USERNAME")
        if crate_user:
            env["CRATE_USERNAME"] = crate_user

    if not env.get("CRATE_PASSWORD"):
        crate_password = env.get("CRATEDB_PASSWORD")
        if crate_password:
            env["CRATE_PASSWORD"] = crate_password

    if not env.get("CRATE_VERIFY_SSL_CERT"):
        crate_verify_ssl = env.get("CRATEDB_VERIFY_SSL_CERT")
        if crate_verify_ssl:
            env["CRATE_VERIFY_SSL_CERT"] = crate_verify_ssl

    env.setdefault("DBT_SCHEMA", "analytics")


def run_dbt_command(args: Sequence[str], project_dir: str = "dbt", profiles_dir: str = "dbt") -> None:
    load_repo_env()
    repo_root = Path(__file__).resolve().parents[2]
    project_path = (repo_root / project_dir).resolve()
    profiles_path = (repo_root / profiles_dir).resolve()

    dbt_executable = shutil.which("dbt")
    if dbt_executable is not None:
        command = [dbt_executable, *args, "--project-dir", str(project_path), "--profiles-dir", str(profiles_path)]
    else:
        venv_dbt = Path(sys.executable).with_name("dbt.exe")
        if not venv_dbt.exists():
            raise RuntimeError("dbt executable not found. Ensure dbt is installed in the active virtual environment.")
        command = [str(venv_dbt), *args, "--project-dir", str(project_path), "--profiles-dir", str(profiles_path)]

    env = os.environ.copy()
    _derive_crate_settings(env)
    env["DBT_PROFILES_DIR"] = str(profiles_path)
    env["PYTHONPATH"] = str(repo_root) + os.pathsep + env.get("PYTHONPATH", "")
    subprocess.run(command, check=True, env=env, cwd=str(repo_root))
