"""Step 0 probe: confirm the data.gov.in mandi resources with YOUR key.

Usage:  DATA_GOV_IN_API_KEY=... uv run python scripts/probe_sources.py <resource-id> [<resource-id> ...]

For each resource it prints: total record count, field names, one sample row,
and whether a date filter (filters[arrival_date]) returns historical rows.
Nothing is written to disk.
"""

import json
import os
import sys
import urllib.parse
import urllib.request

BASE = "https://api.data.gov.in/resource/"


def get(resource_id: str, **params: str) -> dict:
    query = {"api-key": os.environ["DATA_GOV_IN_API_KEY"], "format": "json", "limit": "5", **params}
    url = BASE + resource_id + "?" + urllib.parse.urlencode(query)
    with urllib.request.urlopen(url, timeout=60) as resp:  # noqa: S310 (fixed https host)
        return json.load(resp)


def probe(resource_id: str) -> None:
    print(f"\n=== {resource_id}")
    body = get(resource_id)
    print("title:", body.get("title"))
    print("total records:", body.get("total"))
    print("fields:", [f.get("id") for f in body.get("field", [])])
    records = body.get("records", [])
    print("sample:", json.dumps(records[:1], indent=2))
    for day in ("01/09/2023", "2023-09-01"):
        hist = get(resource_id, **{"filters[arrival_date]": day})
        print(f"history filter arrival_date={day}: total={hist.get('total')}")


if __name__ == "__main__":
    for rid in sys.argv[1:]:
        probe(rid)
