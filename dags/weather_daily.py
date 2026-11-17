"""Open-Meteo weather for the top basmati districts -> silver.weather_daily (Iceberg).

Daily at 02:00 UTC. Reloads each district's history from 2015 in one call (the archive
API lags real time by a few days, so recent days fill in on later runs).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.sdk import dag, task

from basmati.assets import SILVER_WEATHER


@dag(
    schedule="0 2 * * *",
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
