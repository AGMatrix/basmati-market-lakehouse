"""silver -> dbt (DuckDB) -> quality gate -> serving copy.

Triggered when either silver asset updates (prices OR weather: an Airflow 3 asset expression).

Runs only after silver passed its lake quality gate (the asset is emitted by that gate).
The serving copy that the dashboard reads is replaced only if the warehouse gate passes,
so a failed run leaves the dashboard on the last good build. After that, a basmati-only
extract is uploaded to a private S3 bucket for the hosted dashboard (if configured).
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

    @task
    def publish_hosted() -> str:
        """Upload the basmati extract for the hosted dashboard (skipped if S3 isn't set up)."""
        import os

        from airflow.sdk.exceptions import AirflowSkipException

        from basmati.orchestration import PROJECT_ROOT, run_analytics
        from basmati.serving import upload_extract

        if not os.environ.get("SERVING_S3_BUCKET"):
            raise AirflowSkipException("SERVING_S3_BUCKET not set")
        data = PROJECT_ROOT / "data"
        out = data / "basmati_demo.duckdb"
        run_analytics(["python", "-m", "basmati.serving", "export",
                       str(data / "warehouse_serving.duckdb"), str(out)])  # fmt: skip
        return upload_extract(out)

    dbt_build() >> warehouse_gate() >> publish() >> publish_hosted()


warehouse_build()
