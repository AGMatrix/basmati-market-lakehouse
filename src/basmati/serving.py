"""Publish a small serving extract of the warehouse for the hosted dashboard.

The full serving warehouse is ~4.6 GB (55M price rows), but the dashboard reads only
basmati rows (~0.25%). `export_extract` writes those rows plus the dimensions and the
policy mart into a few-MB DuckDB file. `upload_extract` puts it in a private S3 bucket,
which the hosted app (Streamlit Community Cloud) reads with a read-only key, so the
hosted dashboard refreshes after every warehouse build without data going into git.

  python -m basmati.serving export SRC OUT   (needs duckdb: analytics venv or uv env)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

DEFAULT_KEY = "serving/basmati_demo.duckdb"
COPIED_TABLES = ("dim_variety", "dim_policy_event", "mart_basmati_price_vs_policy")


def export_extract(src: Path, out: Path) -> dict[str, int]:
    """Basmati-only copy of the tables the dashboard reads. Returns row counts per table."""
    import duckdb

    tmp = out.with_suffix(".tmp")
    tmp.unlink(missing_ok=True)
    con = duckdb.connect(str(tmp))
    con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")
    con.execute("CREATE SCHEMA marts")
    con.execute("""
        CREATE TABLE marts.fct_daily_price AS
        SELECT f.* FROM src.marts.fct_daily_price f
        JOIN src.marts.dim_variety v USING (variety_id) WHERE v.is_basmati
        ORDER BY f.arrival_date
    """)
    con.execute("""
        CREATE TABLE marts.dim_market AS SELECT * FROM src.marts.dim_market
        WHERE market_version_id IN (SELECT DISTINCT market_version_id FROM marts.fct_daily_price)
    """)
    for t in COPIED_TABLES:
        con.execute(f"CREATE TABLE marts.{t} AS SELECT * FROM src.marts.{t}")  # noqa: S608
    con.execute("DETACH src")
    counts = {
        t: con.sql(f"SELECT count(*) FROM marts.{t}").fetchone()[0]  # noqa: S608
        for (t,) in con.sql("SELECT table_name FROM information_schema.tables").fetchall()
    }
    con.close()
    os.replace(tmp, out)  # readers never see a half-written file
    return counts


def upload_extract(path: Path, env: dict[str, str] | None = None, s3=None) -> str | None:
    """PUT the extract to s3://$SERVING_S3_BUCKET/$SERVING_S3_KEY. None when not configured.

    Uses its own credentials (SERVING_AWS_*), a key allowed only s3:PutObject on that one
    object, separate from the MinIO keys. A single PUT is atomic: readers get the old or the
    new file.
    """
    env = os.environ if env is None else env
    bucket = env.get("SERVING_S3_BUCKET", "")
    if not bucket:
        return None
    key = env.get("SERVING_S3_KEY") or DEFAULT_KEY
    if s3 is None:
        import boto3

        s3 = boto3.client(
            "s3",
            region_name=env.get("SERVING_AWS_REGION") or "us-east-1",
            aws_access_key_id=env["SERVING_AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=env["SERVING_AWS_SECRET_ACCESS_KEY"],
        )
    s3.upload_file(str(path), bucket, key)
    return f"s3://{bucket}/{key}"


if __name__ == "__main__":
    if len(sys.argv) != 4 or sys.argv[1] != "export":
        sys.exit("usage: python -m basmati.serving export SRC OUT")
    src, out = Path(sys.argv[2]), Path(sys.argv[3])
    for table, n in export_extract(src, out).items():
        print(table, n)
    print(f"{out}: {out.stat().st_size / 1e6:.1f} MB")
