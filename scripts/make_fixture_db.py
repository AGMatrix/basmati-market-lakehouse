# ruff: noqa: E501  (one fixture row per line reads better than wrapped tuples)
"""Build dbt/fixtures/lake_fixture.duckdb: a tiny stand-in for the Iceberg lake, for CI.

The `ci` dbt profile attaches this file as `lake`, so `lake.silver.daily_price` resolves
exactly as it does against the real REST catalog. Rows are hand-written around the
Sep 2024 MEP removal, plus a few cases the models must handle (repeat-free keys, a
non-basmati paddy, a unit-mixup price below the plausibility floor).
"""

from pathlib import Path

import duckdb

OUT = Path(__file__).resolve().parents[1] / "dbt" / "fixtures" / "lake_fixture.duckdb"

ROWS = [
    # date, state, district, market, commodity, code, variety, grade, min, max, modal
    ("2024-08-20", "Haryana", "Karnal", "Karnal", "Paddy(Basmati)", "414", "1121", "FAQ", 3900, 4300, 4100),
    ("2024-09-10", "Haryana", "Karnal", "Karnal", "Paddy(Basmati)", "414", "1121", "FAQ", 4000, 4400, 4200),
    ("2024-09-20", "Haryana", "Karnal", "Karnal", "Paddy(Basmati)", "414", "1121", "FAQ", 4300, 4700, 4500),
    ("2024-09-10", "Punjab", "Amritsar", "Amritsar", "Paddy(Basmati)", "414", "Basmati 1509", "FAQ", 3000, 3300, 3150),
    ("2024-09-25", "Punjab", "Amritsar", "Amritsar", "Paddy(Basmati)", "414", "Basmati 1509", "FAQ", 3200, 3500, 3350),
    ("2024-09-10", "Punjab", "Amritsar", "Amritsar", "Paddy(Common)", "2", "Sarvati", "FAQ", 2200, 2500, 2350),
    ("2024-09-10", "Delhi", "Delhi", "Ghazipur", "Rose(Local)", "1", "Other", "FAQ", 2, 3, 2.5),
]  # fmt: skip

if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.unlink(missing_ok=True)
    con = duckdb.connect(str(OUT))
    con.sql("create schema silver")
    con.sql(
        """
        create table silver.daily_price (
            arrival_date date, state varchar, district varchar, market varchar,
            commodity varchar, commodity_code varchar, variety varchar, grade varchar,
            min_price double, max_price double, modal_price double, price_unit varchar,
            source varchar, ingested_at timestamptz, report_count integer
        )
        """
    )
    con.executemany(
        "insert into silver.daily_price values "
        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Rs/quintal', 'history', now(), 1)",
        ROWS,
    )
    con.close()
    print(f"wrote {OUT} ({len(ROWS)} rows)")
