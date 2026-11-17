"""silver -> dbt (DuckDB) -> quality gate -> serving copy.

Triggered when either silver asset updates (prices OR weather: an Airflow 3 asset expression).

Runs only after silver passed its lake quality gate (the asset is emitted by that gate).
The serving copy that the dashboard reads is replaced only if the warehouse gate passes,
so a failed run leaves the dashboard on the last good build.
"""

from __future__ import annotations

from pathlib import Path

from airflow.sdk import dag, task

from basmati.assets import SILVER_PRICES, SILVER_WEATHER, WAREHOUSE


@dag(
    schedule=(SILVER_PRICES | SILVER_WEATHER),
    catchup=False,
    max_active_runs=1,
    tags=["warehouse", "dbt"],
)
def warehouse_build():
    @task
    def dbt_build() -> None:
        from basmati.orchestration import PROJECT_ROOT, run_analytics

        run_analytics(["dbt", "build", "--fail-fast"], cwd=PROJECT_ROOT / "dbt")

    @task
    def warehouse_gate() -> None:
        from basmati.orchestration import run_analytics

        run_analytics(["python", "-m", "basmati.quality.checks", "warehouse"])

    @task(outlets=[WAREHOUSE])
    def publish() -> str:
        import os

        from basmati.orchestration import publish_serving_copy

        return str(publish_serving_copy(Path(os.environ["DUCKDB_PATH"])))

    dbt_build() >> warehouse_gate() >> publish()


warehouse_build()
