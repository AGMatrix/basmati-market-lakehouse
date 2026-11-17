from basmati.orchestration import publish_serving_copy


def test_publish_replaces_serving_copy_atomically(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    db.write_bytes(b"v1")
    serving = publish_serving_copy(db)
    assert serving.name == "warehouse_serving.duckdb" and serving.read_bytes() == b"v1"
    db.write_bytes(b"v2")
    publish_serving_copy(db)
    assert serving.read_bytes() == b"v2"
    assert not list(tmp_path.glob("*.tmp"))  # temp file renamed away, not left behind
