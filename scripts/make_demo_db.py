"""Build a small read-only copy of the dashboard's tables for the hosted demo.

The full serving warehouse is ~4.6 GB (55M price rows). The dashboard only reads basmati
rows, so this keeps those plus the dimensions and the policy mart (a few MB), and the hosted
app (Streamlit Community Cloud) can run without Docker, MinIO or the lake.

Run: make demo-db   (after a successful warehouse_build has published the serving copy)
"""

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data" / "warehouse_serving.duckdb"
OUT = ROOT / "dashboard" / "demo" / "basmati_demo.duckdb"


def main() -> None:
    OUT.unlink(missing_ok=True)
    con = duckdb.connect(str(OUT))
    con.execute(f"ATTACH '{SRC}' AS src (READ_ONLY)")
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
    for t in ("dim_variety", "dim_policy_event", "mart_basmati_price_vs_policy"):
        con.execute(f"CREATE TABLE marts.{t} AS SELECT * FROM src.marts.{t}")  # noqa: S608 (fixed names)
    con.execute("DETACH src")
    for (t,) in con.sql("SELECT table_name FROM information_schema.tables ORDER BY 1").fetchall():
        print(t, con.sql(f"SELECT count(*) FROM marts.{t}").fetchone()[0])  # noqa: S608
    con.close()
    print(f"{OUT.relative_to(ROOT)}: {OUT.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
