"""Daily Agmarknet ingestion: all commodities, all states, into raw/agmarknet/dt=YYYY-MM-DD/.

- Scheduled runs pull the day that just ended in India plus a look-back window, because
  Agmarknet revises recent days; only days whose content hash changed are rewritten.
- Backfill: `make backfill FROM=2023-01-01 TO=2023-12-31` (Airflow 3 backfill, one run
  per day, no look-back, one API caller at a time via the `agmarknet_api` pool).
- Emits the `raw_agmarknet` asset with the list of changed dates; the Spark DAG reads it.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.sdk import Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

from basmati.assets import RAW_AGMARKNET


@dag(
    # 21:30 UTC = 03:00 IST: the Indian trading day has ended and most markets have reported.
    schedule=CronTriggerTimetable("30 21 * * *", timezone="UTC"),
    start_date=datetime(2010, 1, 1),  # backfill can reach the history resource's start
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    params={
        "lookback_days": Param(7, type="integer", minimum=0, maximum=60),
        "day": Param(None, type=["null", "string"], format="date", description="Override date"),
    },
    tags=["ingest", "agmarknet"],
)
def agmarknet_daily():
    @task(outlets=[RAW_AGMARKNET], pool="agmarknet_api")
    def ingest(params, dag_run, outlet_events, logical_date=None) -> dict:
        from basmati.config import Settings
        from basmati.ingest.agmarknet import AgmarknetClient, Resource
        from basmati.ingest.pipeline import ingest_day, lookback_days
        from basmati.ingest.raw_store import RawStore, s3_client

        s = Settings.from_env()
        # Airflow 3: manual runs have no logical_date; run_after is always set.
        when = logical_date or dag_run.run_after
        day = date.fromisoformat(params["day"]) if params["day"] else when.date()
        # Backfill runs cover every day themselves; look-back would re-pull each day 8 times.
        lookback = 0 if dag_run.run_type == "backfill" else params["lookback_days"]

        client = AgmarknetClient(
            s.data_gov_in_api_key,
            max_rps=s.agmarknet_max_rps,
            proxy=s.agmarknet_proxy,
            page_size=s.agmarknet_page_size,
        )
        store = RawStore(s3_client(s), s.lake_bucket)
        history = Resource.history(s.agmarknet_history_resource_id)
        current = Resource.current(s.agmarknet_current_resource_id)

        results = [
            ingest_day(client, store, d, history, current) for d in lookback_days(day, lookback)
        ]
        for r in results:
            print(f"{r.day}  rows={r.rows:>6}  source={r.source}  changed={r.changed}")

        changed = [r.day.isoformat() for r in results if r.changed]
        summary = {
            "day": day.isoformat(),
            "days_checked": len(results),
            "changed_dates": changed,
            "rows": sum(r.rows for r in results),
            "api_calls": client.calls,
        }
        # The asset event carries which partitions to (re)process downstream.
        outlet_events[RAW_AGMARKNET].extra = {
            "changed_dates": changed,
            "run_type": str(dag_run.run_type),  # downstream checks freshness only for daily runs
        }
        return summary

    ingest()


agmarknet_daily()
