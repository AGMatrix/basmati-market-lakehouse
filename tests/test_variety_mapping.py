import csv
import importlib.util
import json
from pathlib import Path

import pytest

pytest.importorskip("anthropic")
spec = importlib.util.spec_from_file_location(
    "variety_mapping", Path(__file__).parents[1] / "scripts" / "variety_mapping.py"
)
vm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vm)


def row(raw, approved, final_group, llm_group="1121", is_basmati="True"):
    return {
        "approved": approved, "commodity": "Paddy(Basmati)", "raw_variety": raw,
        "n_rows": 5, "median_price": 4500,
        "final_is_basmati": is_basmati, "final_variety_group": final_group,
        "final_form": "paddy", "final_processing": "not_applicable",
        "llm_is_basmati": "True", "llm_variety_group": llm_group,
        "llm_form": "paddy", "llm_processing": "not_applicable",
        "confidence": 0.9, "reason": "test",
    }  # fmt: skip


@pytest.fixture
def paths(tmp_path, monkeypatch):
    seed = tmp_path / "seed.csv"
    seed.write_text(",".join(vm.SEED_COLUMNS) + "\n")
    monkeypatch.setattr(vm, "SEED", seed)
    monkeypatch.setattr(vm, "SUGGESTIONS", tmp_path / "suggestions.csv")
    monkeypatch.setattr(vm, "LOG", tmp_path / "log.jsonl")
    monkeypatch.setattr(vm, "ROOT", tmp_path)
    return tmp_path


def write(path, rows):
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def test_merge_appends_only_approved_and_logs_acceptance(paths):
    write(vm.SUGGESTIONS, [
        row("1121", "y", "1121"),                      # accepted unchanged
        row("Pusa 1509", "Y", "1509"),                 # edited: llm said 1121
        row("Sarbati", "n", "Not Basmati", is_basmati="False"),  # rejected
    ])  # fmt: skip
    vm.cmd_merge(None)
    seed = list(csv.DictReader(vm.SEED.open()))
    # Same line ending as the header: DuckDB's CSV sniffer rejects a file mixing \n and \r\n.
    assert b"\r" not in vm.SEED.read_bytes()
    assert [(r["raw_variety"], r["mapped_by"]) for r in seed] == [
        ("1121", "llm_accepted"),
        ("Pusa 1509", "llm_edited"),
    ]
    log = json.loads(vm.LOG.read_text().splitlines()[-1])
    assert (log["accepted_unchanged"], log["edited"], log["rejected"]) == (1, 1, 1)
    assert log["share_accepted_unchanged"] == pytest.approx(1 / 3, abs=1e-3)


def test_merge_refuses_until_every_row_has_a_decision(paths):
    write(vm.SUGGESTIONS, [row("1121", "y", "1121"), row("1509", "", "1509")])
    with pytest.raises(SystemExit, match="1 rows have no decision"):
        vm.cmd_merge(None)
    assert len(vm.SEED.read_text().splitlines()) == 1  # header only: nothing merged
