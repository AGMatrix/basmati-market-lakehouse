"""One-time seed for the daily GitHub refresh: build the lite lake + warehouse from the full
local lakehouse, check them, and (with --upload) put them in the S3 bucket.

  uv run --env-file .env python scripts/seed_lite.py [--upload]

Needs the local stack up (MinIO + Iceberg REST) for reading silver.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import duckdb

from basmati.lite import LAKE_KEY, LITE_ROWS, WAREHOUSE_KEY
from basmati.quality.checks import lake_connection, run_warehouse_gate
from basmati.serving import DEFAULT_KEY, export_extract

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
LAKE, WAREHOUSE, EXTRACT = (
    DATA / n for n in ("lite_lake.duckdb", "warehouse_lite.duckdb", "basmati_demo.duckdb")
)


def build_lake() -> None:
    LAKE.unlink(missing_ok=True)
    con = lake_connection(":memory:")  # Iceberg REST catalog attached as `lake`
    con.execute(f"ATTACH '{LAKE}' AS lite")
    con.execute("CREATE SCHEMA lite.silver")
    sql = "CREATE TABLE lite.silver.daily_price AS SELECT * FROM lake.silver.daily_price "
    con.execute(sql + f"WHERE {LITE_ROWS} ORDER BY arrival_date")
    con.execute("CREATE TABLE lite.silver.weather_daily AS SELECT * FROM lake.silver.weather_daily")
    for t in ("daily_price", "weather_daily"):
        print(t, con.sql(f"SELECT count(*) FROM lite.silver.{t}").fetchone()[0])  # noqa: S608
    con.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--upload", action="store_true")
    args = p.parse_args()
    build_lake()
    WAREHOUSE.unlink(missing_ok=True)
    env = {**os.environ, "DUCKDB_PATH": str(WAREHOUSE), "LITE_LAKE": str(LAKE)}
    dbt = [str(Path(sys.executable).parent / "dbt"), "build", "--target", "lite"]
    subprocess.run(dbt, cwd=ROOT / "dbt", env=env, check=True)  # noqa: S603
    os.environ["LITE_LAKE"] = str(LAKE)
    gate = run_warehouse_gate(str(WAREHOUSE))
    print("warehouse gate:", "PASSED" if gate.success else "FAILED")
    if not gate.success:
        sys.exit(1)
    print(export_extract(WAREHOUSE, EXTRACT))
    for f in (LAKE, WAREHOUSE, EXTRACT):
        with duckdb.connect(str(f), read_only=True):
            pass
        print(f"{f.name}: {f.stat().st_size / 1e6:.1f} MB")
    if args.upload:
        import boto3

        s3 = boto3.client(
            "s3",
            region_name=os.environ.get("SERVING_AWS_REGION") or "us-east-1",
            aws_access_key_id=os.environ["SERVING_AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["SERVING_AWS_SECRET_ACCESS_KEY"],
        )
        bucket = os.environ["SERVING_S3_BUCKET"]
        for f, key in ((LAKE, LAKE_KEY), (WAREHOUSE, WAREHOUSE_KEY), (EXTRACT, DEFAULT_KEY)):
            s3.upload_file(str(f), bucket, key)
            print("uploaded", f"s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
