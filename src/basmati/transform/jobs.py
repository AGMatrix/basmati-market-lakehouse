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
    }
