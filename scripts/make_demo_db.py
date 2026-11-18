"""Rebuild the basmati-only snapshot committed for the dashboard's offline fallback.

The hosted dashboard normally reads the copy that warehouse_build uploads to S3
(see basmati.serving); this committed file is used when S3 is not configured.

Run: make demo-db   (after a successful warehouse_build has published the serving copy)
"""

from pathlib import Path

from basmati.serving import export_extract

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data" / "warehouse_serving.duckdb"
OUT = ROOT / "dashboard" / "demo" / "basmati_demo.duckdb"

if __name__ == "__main__":
    for table, n in export_extract(SRC, OUT).items():
        print(table, n)
    print(f"{OUT.relative_to(ROOT)}: {OUT.stat().st_size / 1e6:.1f} MB")
