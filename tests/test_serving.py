from pathlib import Path

import duckdb

from basmati.serving import DEFAULT_KEY, export_extract, upload_extract


def _warehouse(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA marts")
    con.execute(
        "CREATE TABLE marts.dim_variety AS SELECT * FROM (VALUES "
        "('v1', true), ('v2', false)) t(variety_id, is_basmati)"
    )
    con.execute(
        "CREATE TABLE marts.fct_daily_price AS SELECT * FROM (VALUES "
        "(DATE '2026-11-16', 'm1', 'c1', 'v1', 3000.0),"
        " (DATE '2026-11-16', 'm2', 'c2', NULL, 900.0),"
        " (DATE '2026-11-16', 'm3', 'c3', NULL, 3200.0))"
        " t(arrival_date, market_version_id, commodity_id, variety_id, modal_price)"
    )
    con.execute(
        "CREATE TABLE marts.dim_market AS SELECT * FROM (VALUES ('m1'), ('m2'), ('m3')) "
        "t(market_version_id)"
    )
    con.execute(
        "CREATE TABLE marts.dim_commodity AS SELECT * FROM (VALUES "
        "('c1', 'Paddy(Basmati)'), ('c2', 'Onion'), ('c3', 'Rice')) t(commodity_id, commodity)"
    )
    con.execute("CREATE TABLE marts.dim_policy_event AS SELECT 'e1' AS event_id")
    con.execute("CREATE TABLE marts.mart_basmati_price_vs_policy AS SELECT 'e1' AS event_id")
    con.close()


def test_export_keeps_basmati_and_rice_rows_and_their_markets(tmp_path):
    src, out = tmp_path / "w.duckdb", tmp_path / "x.duckdb"
    _warehouse(src)
    counts = export_extract(src, out)
    assert counts["fct_daily_price"] == 2  # basmati paddy + non-basmati rice, not onion
    assert counts["dim_market"] == 2  # only markets that have those rows
    assert not out.with_suffix(".tmp").exists()  # written to a temp name, then renamed


class FakeS3:
    def __init__(self):
        self.calls = []

    def upload_file(self, path, bucket, key):
        self.calls.append((path, bucket, key))


def test_upload_is_skipped_without_a_bucket(tmp_path):
    assert upload_extract(tmp_path / "x.duckdb", env={}, s3=FakeS3()) is None


def test_upload_puts_the_file_at_the_configured_key(tmp_path):
    s3 = FakeS3()
    uri = upload_extract(tmp_path / "x.duckdb", env={"SERVING_S3_BUCKET": "b"}, s3=s3)
    assert uri == f"s3://b/{DEFAULT_KEY}"
    assert s3.calls == [(str(tmp_path / "x.duckdb"), "b", DEFAULT_KEY)]
