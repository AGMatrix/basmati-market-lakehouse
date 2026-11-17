"""Raw -> silver.daily_price (Spark, Iceberg). Triggered by the raw_agmarknet asset.

Airflow 3 assets: this DAG has no clock schedule. It runs when ingestion reports new or
revised raw partitions. If several ingest runs land before it starts (e.g. during a
backfill), Airflow folds them into one run and we process the union of their dates.

Manual rebuild: trigger with {"days": ["2024-09-13", ...]} or {"all": true}.
"""

from __future__ import annotations

from datetime import date

from airflow.sdk import Param, dag, task

from basmati.assets import RAW_AGMARKNET, SILVER_PRICES


@dag(
    schedule=[RAW_AGMARKNET],
    catchup=False,
    max_active_runs=1,
    params={
        "days": Param([], type="array", description="Rebuild these dates (YYYY-MM-DD)"),
        "all": Param(False, type="boolean", description="Rebuild every ingested date"),
    },
    tags=["transform", "spark", "iceberg"],
)
def silver_daily_price():
    @task(outlets=[SILVER_PRICES])
    def build(params, triggering_asset_events, outlet_events) -> dict:
        from basmati.transform.jobs import all_ingested_days, run_silver

        if params["all"]:
            days = all_ingested_days()
        elif params["days"]:
            days = [date.fromisoformat(d) for d in params["days"]]
        else:
            days = sorted(
                {
                    date.fromisoformat(d)
                    for event in triggering_asset_events[RAW_AGMARKNET]
                    for d in (event.extra or {}).get("changed_dates", [])
                }
            )
        if not days:
            print("No changed raw partitions; nothing to do.")
            return {"days": 0}
        print(f"Building {len(days)} day(s): {days[0]} .. {days[-1]}")
        metrics = run_silver(days)
        print(metrics)
        outlet_events[SILVER_PRICES].extra = {
            "first_day": metrics.get("first_day"),
            "last_day": metrics.get("last_day"),
        }
        return metrics

    build()


silver_daily_price()
