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
    tmp.unlink(missing_ok=True)  # leftover from an interrupted copy
    # The copy needs the file's full size free; on a full disk it would fail half-way and
    # leave a multi-GB partial file. Check first and fail with a clear message instead.
    need, free = db_path.stat().st_size, shutil.disk_usage(db_path.parent).free
    if free < need * 1.1:
        raise OSError(f"not enough disk space to publish: need {need / 1e9:.1f} GB, "
                      f"{free / 1e9:.1f} GB free")  # fmt: skip
    try:
        shutil.copyfile(db_path, tmp)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    os.replace(tmp, serving)
    return serving
