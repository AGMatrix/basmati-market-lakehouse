"""Open-Meteo weather for the top basmati districts -> silver.weather_daily (Iceberg).

Every 3 hours. Incremental per district; when the free quota is spent the run saves
what it has and stops, and the next run continues (see basmati.ingest.weather).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.sdk import dag, task

from basmati.assets import SILVER_WEATHER


@dag(
    # Every 3 hours: the first 11-year load spans several Open-Meteo hourly quotas and
    # resumes each run; once complete, each run only refreshes the last week (cheap).
    schedule="0 */3 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["ingest", "weather", "spark"],
)
def weather_daily():
    @task(outlets=[SILVER_WEATHER])
    def refresh() -> dict:
        from basmati.transform.jobs import run_weather

        metrics = run_weather(date(2015, 1, 1), date.today() - timedelta(days=2))
        print(metrics)
        return metrics

    refresh()


weather_daily()
