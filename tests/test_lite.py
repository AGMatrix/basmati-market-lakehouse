from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from basmati.lite import BASMATI_SIGNAL, LITE_ROWS, replace_days

ROOT = Path(__file__).resolve().parents[1]


def test_basmati_signal_matches_the_dbt_candidate_model():
    sql = (ROOT / "dbt/models/intermediate/int_basmati_candidates.sql").read_text()
    assert f"'{BASMATI_SIGNAL}'" in sql.replace("\n", "").replace(" ", "") or (
        BASMATI_SIGNAL in sql.replace("\n            ", "")
    )


def _lake(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA silver")
    con.execute(
        "CREATE TABLE silver.daily_price (arrival_date DATE, commodity VARCHAR, "
        "variety VARCHAR, modal_price DOUBLE)"
    )
    con.execute(
        "INSERT INTO silver.daily_price VALUES "
        "('2026-11-15', 'Rice', 'Common', 1.0), ('2026-11-16', 'Rice', 'Common', 2.0)"
    )
    con.close()


def test_replace_days_rewrites_only_those_days_and_keeps_rice_and_basmati(tmp_path):
    lake = tmp_path / "lake.duckdb"
    _lake(lake)
    new = pd.DataFrame(
        {
            "arrival_date": [date(2026, 11, 16)] * 4,
            "commodity": ["Rice", "Paddy(Common)", "Paddy(Common)", "Onion"],
            "variety": ["Sona", "1121", "Hybrid", "Red"],
            "modal_price": [3.0, 4.0, 5.0, 6.0],
        }
    )
    kept = replace_days(lake, new, [date(2026, 11, 16)])
    rows = (
        duckdb.connect(str(lake))
        .sql("select arrival_date::varchar, variety from silver.daily_price order by all")
        .fetchall()
    )
    assert kept == 2  # rice + basmati-signal paddy; common paddy and onion dropped
    assert rows == [("2026-11-15", "Common"), ("2026-11-16", "1121"), ("2026-11-16", "Sona")]


def test_lite_rows_filter_is_valid_sql():
    duckdb.sql(f"select * from (select 'Rice' as commodity, 'x' as variety) where {LITE_ROWS}")  # noqa: S608
