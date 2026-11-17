"""Entry points the Airflow DAGs call. Each returns a metrics dict (logged + kept in XCom)."""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import date

from basmati.config import Settings
from basmati.ingest.raw_store import RawStore, s3_client


def all_ingested_days() -> list[date]:
    settings = Settings.from_env(require_api=False)
    return RawStore(s3_client(settings), settings.lake_bucket).list_days()


def run_silver(days: list[date]) -> dict:
    """(Re)build silver.daily_price for exactly these arrival dates."""
    from basmati.transform.silver import ensure_table, read_silver, write_days
    from basmati.transform.spark_session import build_spark

    settings = Settings.from_env(require_api=False)
    store = RawStore(s3_client(settings), settings.lake_bucket)

    days_by_source: dict[str, list[date]] = defaultdict(list)
    raw_rows, missing = 0, []
    for d in sorted(set(days)):
        m = store.read_manifest(d)
        if m is None:  # never ingested: nothing to build, and don't wipe the table for it
            missing.append(d.isoformat())
            continue
        days_by_source[m.source].append(d)
        raw_rows += m.row_count
    target_days = [d for ds in days_by_source.values() for d in ds]
    if not target_days:
        return {"days": 0, "raw_rows": 0, "silver_rows": 0, "missing_raw": missing}

    start = time.monotonic()
    spark = build_spark("silver_daily_price")
    try:
        ensure_table(spark)
        silver = read_silver(spark, settings.lake_bucket, dict(days_by_source))
        rows = write_days(silver, target_days)
    finally:
        spark.stop()
    return {
        "days": len(target_days),
        "first_day": min(target_days).isoformat(),
        "last_day": max(target_days).isoformat(),
        "raw_rows": raw_rows,
        "silver_rows": rows,
        "dropped_rows": raw_rows - rows,  # invalid keys + duplicate keys
        "missing_raw": missing,
        "seconds": round(time.monotonic() - start, 1),
        "built_days": [d.isoformat() for d in sorted(target_days)],
    }


def run_weather(start: date, end: date, top_n: int = 40) -> dict:
    """Pick basmati districts, refresh their Open-Meteo history, rebuild silver.weather_daily."""
    from basmati.ingest import weather as ingest_weather
    from basmati.transform.spark_session import build_spark
    from basmati.transform.weather import basmati_districts, read_weather, write_weather

    settings = Settings.from_env(require_api=False)
    spark = build_spark("silver_weather_daily")
    try:
        wanted = basmati_districts(spark, top_n)
        metrics = ingest_weather.ingest(
            s3_client(settings), settings.lake_bucket, wanted, start, end
        )
        metrics["silver_rows"] = write_weather(spark, read_weather(spark, settings.lake_bucket))
    finally:
        spark.stop()
    return metrics
