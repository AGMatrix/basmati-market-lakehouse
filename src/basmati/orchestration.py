"""Helpers for Airflow tasks that call the analytics venv (dbt, DuckDB, GX)."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

ANALYTICS_BIN = Path(os.environ.get("ANALYTICS_BIN", "/opt/analytics/bin"))
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", "/opt/project"))


def run_analytics(args: list[str], cwd: Path | None = None) -> None:
    """Run a command from the analytics venv, streaming output into the task log."""
    env = {**os.environ, "PYTHONPATH": str(PROJECT_ROOT / "src")}
    cmd = [str(ANALYTICS_BIN / args[0]), *args[1:]]
    print("$", " ".join(cmd))
    subprocess.run(cmd, cwd=cwd or PROJECT_ROOT, env=env, check=True)  # noqa: S603


def publish_serving_copy(db_path: Path) -> Path:
    """Copy the dbt-built DuckDB file to the read-only serving file the dashboard opens.

    DuckDB allows one writing process at a time, so the dashboard never opens the file dbt
    writes. The copy goes to a temp name first and is renamed into place (atomic on one
    filesystem), so readers see either the old or the new file, never a partial one.
    """
    serving = db_path.with_name("warehouse_serving.duckdb")
    tmp = serving.with_suffix(".duckdb.tmp")
    shutil.copyfile(db_path, tmp)
    os.replace(tmp, serving)
    return serving
