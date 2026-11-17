"""LLM-assisted basmati variety mapping, with a human in the loop. Never runs in the pipeline.

    make map-suggest   # unmapped names -> review/variety_suggestions.csv
    (open the CSV, set `approved` to y or n on each row, edit the final_* columns if needed)
    make map-merge     # approved rows -> dbt/seeds/basmati_variety_map.csv

`suggest` asks Claude for structured suggestions (schema-validated, retried) for every
unmapped (commodity, variety) pair the warehouse flags. `merge` appends only rows a person
marked approved, records whether each was accepted unchanged or edited, and logs the share
accepted unchanged to review/mapping_log.jsonl. Nothing reaches the seed automatically.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import anthropic
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

SUGGESTIONS = ROOT / "review" / "variety_suggestions.csv"
LOG = ROOT / "review" / "mapping_log.jsonl"
SEED = ROOT / "dbt" / "seeds" / "basmati_variety_map.csv"
SEED_COLUMNS = ["commodity", "raw_variety", "is_basmati", "variety_group", "form",
                "processing", "mapped_by", "reviewed_on"]  # fmt: skip
FIELDS = ["is_basmati", "variety_group", "form", "processing"]

MODEL = "claude-opus-5-5"
BATCH_SIZE = 25
MAX_ATTEMPTS = 3

VarietyGroup = Literal["1121", "1509", "1718", "1401", "1847", "1885", "1886", "1692",
                       "Traditional", "Other Basmati", "Not Basmati"]  # fmt: skip


class Suggestion(BaseModel):
    commodity: str = Field(description="Copied exactly from the input")
    raw_variety: str = Field(description="Copied exactly from the input")
    is_basmati: bool
    variety_group: VarietyGroup
    form: Literal["paddy", "rice"]
    processing: Literal["raw", "steam", "sella", "not_applicable", "unknown"]
    confidence: float = Field(ge=0, le=1)
    reason: str = Field(description="One sentence: the evidence for this mapping")


class SuggestionBatch(BaseModel):
    suggestions: list[Suggestion]


SYSTEM = """You map raw variety names from India's Agmarknet mandi price data to a clean \
basmati taxonomy for a price analysis. Each input is a (commodity, variety) pair as \
reported by a market, with its row count and median modal price in Rs per quintal.

Rules:
- variety_group: the Pusa Basmati number when the name identifies one (1121, 1509, 1718, \
1401 = Pusa Basmati 6, 1847, 1885, 1886, 1692). "Traditional" for traditional basmati \
(Taraori, Basmati 370/386, CSR 30, Dehradun). "Other Basmati" when it is basmati but the \
variety cannot be identified. "Not Basmati" for non-basmati rice/paddy (Sona Masuri, IR-64, \
Sarbati/Sharbati, PR varieties, generic Common/Fine/Coarse, and "Non Basmati" names).
- is_basmati is false exactly when variety_group is "Not Basmati".
- form: "paddy" if the commodity or name is paddy/dhan, otherwise "rice".
- processing: for rice, "steam" / "sella" (parboiled; also spelled sela, golden sella) / \
"raw" when the name says so, else "unknown". For paddy always "not_applicable".
- Use the median price as a sanity check: basmati paddy and rice trade well above common \
paddy and rice. Lower confidence when name and price disagree or the name is ambiguous.
- Return exactly one suggestion per input pair, with commodity and raw_variety copied \
exactly as given. Do not add or drop pairs."""


def fetch_unmapped() -> list[dict]:
    from basmati.quality.checks import lake_connection

    con = lake_connection(str(ROOT / "data" / "warehouse.duckdb"), read_only=True)
    rows = con.sql(
        """
        select commodity, raw_variety, n_rows, round(median_modal_price) as median_price
        from intermediate.int_basmati_candidates
        where not is_mapped
        order by n_rows desc
        """
    ).fetchall()
    return [dict(zip(["commodity", "raw_variety", "n_rows", "median_price"], r, strict=True))
            for r in rows]  # fmt: skip


def suggest_batch(client: anthropic.Anthropic, batch: list[dict]) -> list[Suggestion]:
    """One API call per batch; retried if the reply is invalid or doesn't cover the inputs."""
    wanted = {(b["commodity"], b["raw_variety"]) for b in batch}
    prompt = "Map these pairs:\n" + json.dumps(batch, ensure_ascii=False, indent=1)
    problem = ""
    for _attempt in range(MAX_ATTEMPTS):
        content = (
            prompt if not problem else f"{prompt}\n\nYour previous reply was rejected: {problem}"
        )
        response = client.beta.messages.parse(
            model=MODEL,
            max_tokens=16000,
            output_config={"effort": "medium"},
            # Server-side refusal fallback: if the model declines, the API retries on a
            # fallback model inside the same call.
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            system=SYSTEM,
            messages=[{"role": "user", "content": content}],
            output_format=SuggestionBatch,
        )
        if response.stop_reason == "refusal":
            problem = "the request was declined"
            continue
        if response.stop_reason == "max_tokens" or response.parsed_output is None:
            problem = "the reply was cut off or did not match the schema"
            continue
        got = response.parsed_output.suggestions
        keys = [(s.commodity, s.raw_variety) for s in got]
        if set(keys) != wanted or len(keys) != len(wanted):
            missing, extra = wanted - set(keys), set(keys) - wanted
            problem = (
                "pairs must match the input exactly; "
                f"missing={sorted(missing)[:5]} extra={sorted(extra)[:5]}"
            )
            continue
        bad = [s for s in got if s.is_basmati == (s.variety_group == "Not Basmati")]
        if bad:
            names = [s.raw_variety for s in bad][:5]
            problem = f"is_basmati must be false exactly for 'Not Basmati': {names}"
            continue
        return got
    raise RuntimeError(f"batch failed after {MAX_ATTEMPTS} attempts: {problem}")


def cmd_suggest(_args) -> None:
    unmapped = fetch_unmapped()
    if not unmapped:
        print("No unmapped basmati candidates. Nothing to do.")
        return
    client = anthropic.Anthropic()
    by_key = {(u["commodity"], u["raw_variety"]): u for u in unmapped}
    rows = []
    for i in range(0, len(unmapped), BATCH_SIZE):
        batch = unmapped[i : i + BATCH_SIZE]
        print(f"batch {i // BATCH_SIZE + 1}: {len(batch)} names")
        for s in suggest_batch(client, batch):
            u = by_key[(s.commodity, s.raw_variety)]
            llm = {f"llm_{f}": getattr(s, f) for f in FIELDS}
            rows.append({
                "approved": "",
                "commodity": s.commodity, "raw_variety": s.raw_variety,
                "n_rows": u["n_rows"], "median_price": u["median_price"],
                **{f"final_{f}": getattr(s, f) for f in FIELDS},
                **llm, "confidence": s.confidence, "reason": s.reason,
            })  # fmt: skip
    SUGGESTIONS.parent.mkdir(exist_ok=True)
    with SUGGESTIONS.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(sorted(rows, key=lambda r: -r["n_rows"]))
    print(f"\n{len(rows)} suggestions -> {SUGGESTIONS.relative_to(ROOT)}")
    print(
        "Review each row: set approved=y (or n), edit final_* columns if the suggestion is wrong."
    )


def _norm(v) -> str:
    return str(v).strip().lower()


def cmd_merge(_args) -> None:
    with SUGGESTIONS.open() as fh:
        rows = list(csv.DictReader(fh))
    pending = [r for r in rows if _norm(r["approved"]) not in ("y", "yes", "n", "no")]
    if pending:
        sys.exit(
            f"{len(pending)} rows have no decision yet (approved must be y or n). Nothing merged."
        )
    with SEED.open() as fh:
        existing = {(r["commodity"], r["raw_variety"]) for r in csv.DictReader(fh)}
    approved = [r for r in rows if _norm(r["approved"]) in ("y", "yes")]
    today = date.today().isoformat()
    new, unchanged, edited = [], 0, 0
    for r in approved:
        if (r["commodity"], r["raw_variety"]) in existing:
            continue
        same = all(_norm(r[f"final_{f}"]) == _norm(r[f"llm_{f}"]) for f in FIELDS)
        unchanged += same
        edited += not same
        new.append({
            "commodity": r["commodity"], "raw_variety": r["raw_variety"],
            "is_basmati": _norm(r["final_is_basmati"]) == "true",
            "variety_group": r["final_variety_group"].strip(), "form": r["final_form"].strip(),
            "processing": r["final_processing"].strip(),
            "mapped_by": "llm_accepted" if same else "llm_edited", "reviewed_on": today,
        })  # fmt: skip
    with SEED.open("a", newline="") as fh:
        csv.DictWriter(fh, fieldnames=SEED_COLUMNS).writerows(new)
    stats = {
        "at": datetime.now(UTC).isoformat(timespec="seconds"),
        "model": MODEL,
        "suggested": len(rows),
        "approved": len(approved),
        "rejected": len(rows) - len(approved),
        "accepted_unchanged": unchanged,
        "edited": edited,
        "share_accepted_unchanged": round(unchanged / len(rows), 3) if rows else None,
        "merged": len(new),
    }
    with LOG.open("a") as fh:
        fh.write(json.dumps(stats) + "\n")
    print(json.dumps(stats, indent=2))
    print(f"Appended {len(new)} rows to {SEED.relative_to(ROOT)}. Next: make rebuild-warehouse")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("suggest").set_defaults(fn=cmd_suggest)
    sub.add_parser("merge").set_defaults(fn=cmd_merge)
    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
