"""Daily refresh of the hosted dashboard, run by GitHub Actions (no Docker, no Mac).

The full lakehouse (Airflow, MinIO, Iceberg) runs locally. This job keeps the *hosted*
dashboard current every day with the same code, on a smaller slice of the data:

  1. ingest:  the same `ingest_day` (7-day look-back, content hashes, manifests), writing
              the raw zone to the S3 bucket instead of MinIO; only changed days continue
  2. silver:  the same Spark `build_silver` (local mode) on those days
  3. lite lake: keep rice + basmati-candidate rows (LITE_ROWS) in a DuckDB file
              (lite/lite_lake.duckdb) that stands in for the Iceberg catalog
  4. dbt build --target lite (same models, tests, contracts, snapshot) + warehouse gate
  5. retrain the weekly price forecast (basmati.forecast) and backtest it
  6. gate passed -> export the dashboard extract and upload it; save lake + warehouse

Every S3 object is replaced with a single PUT, so readers see the old or the new file.

  python -m basmati.lite             (env: see docs/DAILY_REFRESH.md)
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

LAKE_KEY = "lite/lite_lake.duckdb"
WAREHOUSE_KEY = "lite/warehouse_lite.duckdb"
IST = timezone(timedelta(hours=5, minutes=30))

# Same basmati-signal regex as dbt/models/intermediate/int_basmati_candidates.sql
# (tests/test_lite.py fails if they drift apart), so the lite lake keeps every row the
# basmati models and the unmapped-variety gate look at, plus all rice for the mandi tables.
BASMATI_SIGNAL = (
    r"bas[aeiou]*h?m[aeiou]*t+h?[aeiou]|1121|1509|1718|1401|1847|1885|1886|1692|pusa|sugandh"
    r"|taraori|dehradun|\bd\.?b\.?\b|370|386|csr ?30"
)
LITE_ROWS = f"""
    lower(commodity) = 'rice'
    or regexp_matches(lower(commodity), 'basmati')
    or (regexp_matches(lower(commodity), 'paddy|rice|dhan')
        and regexp_matches(lower(variety), '{BASMATI_SIGNAL}'))
"""


def replace_days(lake: Path, silver, days: list[date]) -> int:
    """Delete `days` from lite silver and insert their new rows (one transaction)."""
    import duckdb

    con = duckdb.connect(str(lake))
    con.register("new_rows", silver)
    con.execute("BEGIN")
    con.execute(
        "DELETE FROM silver.daily_price WHERE arrival_date IN (SELECT unnest(?::DATE[]))",
        [days],
    )
    con.execute(f"INSERT INTO silver.daily_price BY NAME SELECT * FROM new_rows WHERE {LITE_ROWS}")  # noqa: S608
    kept = con.sql(
        "SELECT count(*) FROM silver.daily_price WHERE arrival_date IN (SELECT unnest(?::DATE[]))",
        params=[days],
    ).fetchone()[0]
    con.execute("COMMIT")
    con.close()
    return kept


def build_silver_local(records_by_day: dict[date, tuple[str, list[dict]]]):
    """The production Spark transform, in local mode, on in-memory raw records -> pandas."""
    from pyspark.sql import SparkSession

    from basmati.transform.silver import RAW_SCHEMAS, build_silver

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("basmati-lite")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    parts = []
    for source in ("history", "current"):
        rows = [r for s, recs in records_by_day.values() if s == source for r in recs]
        if rows:
            schema = RAW_SCHEMAS[source]
            data = [
                tuple(
                    str(r.get(f.name)) if r.get(f.name) is not None else None for f in schema.fields
                )
                for r in rows
            ]
            parts.append(build_silver(spark.createDataFrame(data, schema), source).toPandas())
    spark.stop()
    import pandas as pd

    return pd.concat(parts, ignore_index=True) if parts else None


def run(env: dict[str, str] | None = None) -> dict:
    import boto3

    from basmati.ingest.agmarknet import AgmarknetClient, Resource
    from basmati.ingest.pipeline import ingest_day, lookback_days
    from basmati.ingest.raw_store import RawStore
    from basmati.orchestration import PROJECT_ROOT
    from basmati.quality.checks import run_warehouse_gate, write_report
    from basmati.serving import DEFAULT_KEY, export_extract

    env = dict(os.environ if env is None else env)
    bucket = env["SERVING_S3_BUCKET"]
    s3 = boto3.client(
        "s3",
        region_name=env.get("SERVING_AWS_REGION") or "us-east-1",
        aws_access_key_id=env["SERVING_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=env["SERVING_AWS_SECRET_ACCESS_KEY"],
    )
    work = Path(env.get("LITE_WORK_DIR") or tempfile.mkdtemp(prefix="basmati_lite_"))
    work.mkdir(parents=True, exist_ok=True)
    lake, warehouse = work / "lite_lake.duckdb", work / "warehouse_lite.duckdb"
    s3.download_file(bucket, LAKE_KEY, str(lake))
    s3.download_file(bucket, WAREHOUSE_KEY, str(warehouse))

    # 1. ingest: yesterday (India time) + 7-day look-back; only new or revised days go on.
    client = AgmarknetClient(
        env["DATA_GOV_IN_API_KEY"],
        max_rps=float(env.get("AGMARKNET_MAX_RPS") or 1),
        proxy=env.get("AGMARKNET_PROXY") or None,
        page_size=int(env.get("AGMARKNET_PAGE_SIZE") or 5000),
    )
    store = RawStore(s3, bucket)
    history = Resource.history(env["AGMARKNET_HISTORY_RESOURCE_ID"])
    current = Resource.current(env["AGMARKNET_CURRENT_RESOURCE_ID"])
    yesterday = datetime.now(IST).date() - timedelta(days=1)
    lookback = int(env.get("LITE_LOOKBACK_DAYS") or 7)
    results = [
        ingest_day(client, store, d, history, current) for d in lookback_days(yesterday, lookback)
    ]
    for r in results:
        print(f"{r.day}  rows={r.rows:>6}  source={r.source}  changed={r.changed}")
    # LITE_FORCE=1 rebuilds every checked day from the raw zone, e.g. after re-seeding.
    force = env.get("LITE_FORCE") == "1"
    changed = [r.day for r in results if r.changed or (force and r.rows)]
    metrics = {"checked_days": len(results), "changed_days": [d.isoformat() for d in changed]}
    if not changed:
        print("No new or revised days; nothing to publish.")
        return metrics

    # 2-3. silver (same Spark code) -> lite lake
    records = {d: (store.read_manifest(d).source, store.read_day(d)) for d in changed}
    silver = build_silver_local(records)
    metrics["lite_rows_kept"] = replace_days(lake, silver, changed) if silver is not None else 0

    # 4. dbt + warehouse gate, against the lite lake
    dbt_env = {**env, "DBT_TARGET": "lite", "DUCKDB_PATH": str(warehouse), "LITE_LAKE": str(lake)}
    dbt = [
        str(Path(sys.executable).parent / "dbt"),
        "build",
        "--fail-fast",
        "--target",
        "lite",
        "--profiles-dir",
        str(PROJECT_ROOT / "dbt"),
        "--project-dir",
        str(PROJECT_ROOT / "dbt"),
    ]
    subprocess.run(dbt, env=dbt_env, check=True)  # noqa: S603
    os.environ["LITE_LAKE"] = str(lake)  # the gate attaches the lite lake as `lake`
    gate = run_warehouse_gate(str(warehouse))
    print(write_report(gate, "lite warehouse").read_text())
    if not gate.success:
        raise SystemExit("warehouse gate failed: nothing published (see the report above)")

    # 5. retrain the price forecast on the refreshed warehouse (writes ml.* tables)
    from basmati.forecast import run as run_forecast

    metrics["forecast"] = run_forecast(str(warehouse))
    print("forecast", metrics["forecast"])

    # 6. publish: extract for the app, then the state the next run builds on
    extract = work / "basmati_demo.duckdb"
    metrics["extract_rows"] = export_extract(warehouse, extract)
    s3.upload_file(str(extract), bucket, env.get("SERVING_S3_KEY") or DEFAULT_KEY)
    s3.upload_file(str(lake), bucket, LAKE_KEY)
    s3.upload_file(str(warehouse), bucket, WAREHOUSE_KEY)
    print("published", metrics)
    return metrics


if __name__ == "__main__":
    run()
