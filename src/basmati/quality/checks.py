"""Data-quality gates with GX Core (Great Expectations 1.x).

Two gates, split by layer (docs/DESIGN_DECISIONS.md #14):
- `lake`:      on silver.daily_price for the days just (re)built, BEFORE dbt runs.
               Price bounds, min <= modal <= max, nulls, row-count drift, freshness.
- `warehouse`: on dbt's output, AFTER dbt builds and BEFORE the serving copy is published.
               Unmapped basmati variety names (listed in the report) and basmati price bounds.

A failed expectation fails the Airflow task. Every run writes GX Data Docs (HTML) and a
short Markdown report to reports/quality/ that names the failing checks and sample rows.

Run: python -m basmati.quality.checks lake --days 2026-11-14,2026-11-15 [--freshness]
     python -m basmati.quality.checks warehouse
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[3]))
REPORT_DIR = ROOT / "reports" / "quality"
GX_DIR = ROOT / "gx"

# Thresholds, measured on real data (2026-11-17, see docs/DESIGN_DECISIONS.md #14):
# ~0.1-0.25% of rows carry a different unit in the Rs/quintal column (Delhi flowers per
# bunch, Tripura fish/potato per kg...; no unit column to convert by). They stay in silver,
# are excluded by dbt staging, and this check fails only if their share jumps above 1%.
PRICE_MIN, PRICE_MAX = 10.0, 5_000_000.0  # Rs/quintal, any commodity (saffron is ~1e6)
PRICE_BOUNDS_MOSTLY = 0.99
BASMATI_MIN, BASMATI_MAX = 1_000.0, 25_000.0  # Rs/quintal, paddy and rice
PRICE_TOLERANCE = 0.01  # the source rounds modal to 2 decimals but not min/max
DRIFT_LOW, DRIFT_HIGH = 0.3, 3.0  # day's rows / trailing-7-day average (Sundays ~0.45)
FRESHNESS_DAYS = 3


# --- data access ----------------------------------------------------------------------------


def lake_connection(
    db_path: str = ":memory:", read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    """DuckDB with the Iceberg REST catalog attached as `lake` (same settings as dbt)."""
    con = duckdb.connect(db_path, read_only=read_only)
    con.sql("INSTALL iceberg; INSTALL httpfs; LOAD iceberg; LOAD httpfs;")
    con.execute(
        "CREATE OR REPLACE SECRET lake_s3 (TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?, "
        "URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')",
        [
            os.environ["MINIO_ROOT_USER"],
            os.environ["MINIO_ROOT_PASSWORD"],
            os.environ.get("DUCKDB_S3_ENDPOINT", "localhost:9000"),
        ],
    )
    endpoint = os.environ.get("DUCKDB_ICEBERG_ENDPOINT", "http://localhost:8181")
    con.sql(
        f"ATTACH IF NOT EXISTS 'warehouse' AS lake "
        f"(TYPE iceberg, ENDPOINT '{endpoint}', AUTHORIZATION_TYPE 'none')"
    )
    return con


def lake_frames(con, days: list[date]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rows for `days`, and per-day row counts with the ratio to the trailing 7 days."""
    day_list = ", ".join(f"DATE '{d.isoformat()}'" for d in days)
    rows = con.sql(
        f"""
        select arrival_date, state, market, commodity, variety, min_price, max_price,
               modal_price, modal_price + {PRICE_TOLERANCE} as modal_hi,
               max_price + {PRICE_TOLERANCE} as max_hi
        from lake.silver.daily_price
        where arrival_date in ({day_list})
        """  # noqa: S608 (dates are formatted from date objects)
    ).df()
    lo = min(days) - timedelta(days=7)
    counts = con.sql(
        f"""
        with c as (
            select arrival_date, count(*) as n_rows from lake.silver.daily_price
            where arrival_date between DATE '{lo.isoformat()}' and DATE '{max(days).isoformat()}'
            group by 1
        )
        select arrival_date, n_rows,
               avg(n_rows) over (order by arrival_date
                                 range between interval 7 day preceding
                                           and interval 1 day preceding) as trailing_avg
        from c
        """  # noqa: S608
    ).df()
    counts = counts[counts["arrival_date"].dt.date.isin(days)].copy()
    counts["ratio_to_trailing_7d"] = counts["n_rows"] / counts["trailing_avg"]
    return rows, counts


# --- GX plumbing ----------------------------------------------------------------------------


@dataclass
class GateResult:
    gate: str
    success: bool
    checks: list[dict] = field(default_factory=list)
    docs_url: str | None = None


def _context():
    GX_DIR.mkdir(parents=True, exist_ok=True)
    return gx.get_context(mode="file", project_root_dir=str(GX_DIR.parent))


def _validate(context, name: str, df: pd.DataFrame, expectations: list) -> list[dict]:
    source = context.data_sources.add_or_update_pandas(name=f"{name}_source")
    asset = source.add_dataframe_asset(name=name)
    batch_def = asset.add_batch_definition_whole_dataframe(f"{name}_batch")
    suite = context.suites.add_or_update(gx.ExpectationSuite(name=f"{name}_suite"))
    for e in suite.expectations[:]:
        suite.delete_expectation(e)
    for e in expectations:
        suite.add_expectation(e)
    vd = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=f"{name}_validation", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name=f"{name}_checkpoint",
            validation_definitions=[vd],
            actions=[gx.checkpoint.UpdateDataDocsAction(name="update_docs")],
            result_format={"result_format": "SUMMARY", "partial_unexpected_count": 10},
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})
    out = []
    for vr in result.run_results.values():
        for r in vr.results:
            cfg = r.expectation_config
            res = r.result or {}
            out.append(
                {
                    "dataset": name,
                    "check": cfg.type,
                    "column": cfg.kwargs.get("column") or cfg.kwargs.get("column_A"),
                    "success": bool(r.success),
                    "observed": res.get("observed_value"),
                    "unexpected_count": res.get("unexpected_count"),
                    "unexpected_percent": res.get("unexpected_percent"),
                    "sample": res.get("partial_unexpected_list", [])[:10],
                }
            )
    return out


# --- gates ----------------------------------------------------------------------------------


def run_lake_gate(days: list[date], freshness: bool = False) -> GateResult:
    con = lake_connection()
    rows, counts = lake_frames(con, days)
    context = _context()
    checks = _validate(
        context,
        "silver_rows",
        rows,
        [
            gxe.ExpectTableRowCountToBeBetween(min_value=1),
            *[
                gxe.ExpectColumnValuesToNotBeNull(column=c)
                for c in ("arrival_date", "state", "market", "commodity", "modal_price")
            ],
            gxe.ExpectColumnValuesToBeBetween(
                column="modal_price",
                min_value=PRICE_MIN,
                max_value=PRICE_MAX,
                mostly=PRICE_BOUNDS_MOSTLY,
            ),
            # min <= modal and modal <= max, with a 1-paisa tolerance for source rounding
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A="modal_hi",
                column_B="min_price",
                or_equal=True,
                ignore_row_if="either_value_is_missing",
                mostly=0.999,
            ),  # fmt: skip
            gxe.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A="max_hi",
                column_B="modal_price",
                or_equal=True,
                ignore_row_if="either_value_is_missing",
                mostly=0.999,
            ),  # fmt: skip
        ],
    )
    drift = counts.dropna(subset=["trailing_avg"])
    if not drift.empty:
        checks += _validate(
            context,
            "silver_daily_counts",
            drift,
            [
                gxe.ExpectColumnValuesToBeBetween(
                    column="ratio_to_trailing_7d", min_value=DRIFT_LOW, max_value=DRIFT_HIGH
                )
            ],
        )
    if freshness:
        latest = con.sql("select max(arrival_date) as d from lake.silver.daily_price").df()
        checks += _validate(
            context,
            "silver_freshness",
            latest,
            [
                gxe.ExpectColumnMaxToBeBetween(
                    column="d",
                    min_value=pd.Timestamp(date.today() - timedelta(days=FRESHNESS_DAYS)),
                )
            ],
        )
    return GateResult("lake", all(c["success"] for c in checks), checks, _docs(context))


def run_warehouse_gate(db_path: str) -> GateResult:
    # Intermediate models are views over the Iceberg catalog, so attach it too.
    con = lake_connection(db_path, read_only=True)
    unmapped = con.sql(
        """
        select commodity, raw_variety, n_rows, first_seen, last_seen, median_modal_price
        from intermediate.int_basmati_candidates
        where not is_mapped
        order by n_rows desc
        """
    ).df()
    unmapped["name"] = unmapped["commodity"] + " | " + unmapped["raw_variety"]
    basmati = con.sql(
        "select modal_price, variety_group, form from intermediate.int_basmati_prices"
    ).df()
    context = _context()
    checks = _validate(
        context, "unmapped_basmati_varieties", unmapped, [gxe.ExpectTableRowCountToEqual(value=0)]
    )
    checks[-1]["sample"] = unmapped["name"].head(50).tolist()
    if not basmati.empty:
        checks += _validate(
            context,
            "basmati_prices",
            basmati,
            [
                gxe.ExpectColumnValuesToBeBetween(
                    column="modal_price", min_value=BASMATI_MIN, max_value=BASMATI_MAX, mostly=0.99
                )
            ],
        )
    return GateResult("warehouse", all(c["success"] for c in checks), checks, _docs(context))


def _docs(context) -> str | None:
    try:
        sites = context.get_docs_sites_urls()
        return sites[0]["site_url"] if sites else None
    except Exception:  # noqa: BLE001 (docs are a convenience; never fail the gate over them)
        return None


# --- report ---------------------------------------------------------------------------------


def write_report(result: GateResult, label: str) -> Path:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    status = "PASSED" if result.success else "FAILED"
    lines = [
        f"# Quality gate `{result.gate}`: {status}",
        "",
        f"- Run: {stamp} ({label})",
        f"- Checks: {len(result.checks)}, failed: {sum(not c['success'] for c in result.checks)}",
        f"- GX Data Docs: {result.docs_url or 'n/a'}",
        "",
        "| Result | Dataset | Check | Column | Observed / unexpected |",
        "|---|---|---|---|---|",
    ]
    for c in result.checks:
        observed = c["observed"] if c["observed"] is not None else ""
        if c["unexpected_count"]:
            observed = f"{c['unexpected_count']} rows ({c['unexpected_percent']:.3f}%)"
        mark = "pass" if c["success"] else "**FAIL**"
        lines.append(
            f"| {mark} | {c['dataset']} | {c['check']} | {c['column'] or ''} | {observed} |"
        )
    failed = [c for c in result.checks if not c["success"] and c["sample"]]
    for c in failed:
        lines += ["", f"## {c['dataset']}: {c['check']} - examples", ""]
        lines += [f"- `{s}`" for s in c["sample"]]
    text = "\n".join(lines) + "\n"
    path = REPORT_DIR / f"{result.gate}_{stamp}.md"
    path.write_text(text)
    (REPORT_DIR / f"{result.gate}_latest.md").write_text(text)
    (REPORT_DIR / f"{result.gate}_latest.json").write_text(
        json.dumps(
            {"gate": result.gate, "success": result.success, "checks": result.checks},
            default=str,
            indent=2,
        )  # fmt: skip
    )
    return path


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("gate", choices=["lake", "warehouse"])
    p.add_argument("--days", default="", help="comma-separated YYYY-MM-DD (lake gate)")
    p.add_argument("--freshness", action="store_true", help="also check data is recent")
    p.add_argument(
        "--db", default=os.environ.get("DUCKDB_PATH", str(ROOT / "data/warehouse.duckdb"))
    )
    a = p.parse_args(argv)
    if a.gate == "lake":
        days = [date.fromisoformat(d) for d in a.days.split(",") if d]
        if not days:
            print("no days given; nothing to check")
            return 0
        result = run_lake_gate(days, a.freshness)
        label = f"{len(days)} day(s) {min(days)}..{max(days)}"
    else:
        result = run_warehouse_gate(a.db)
        label = a.db
    path = write_report(result, label)
    print(path.read_text())
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
