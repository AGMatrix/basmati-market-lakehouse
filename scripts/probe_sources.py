"""Step 0 probe: confirm the data.gov.in mandi resources with YOUR key.

Usage (reads DATA_GOV_IN_API_KEY from the environment; the key is never printed):
    set -a; source .env; set +a
    python3 scripts/probe_sources.py <resource-id> [<resource-id> ...]

For each resource it prints: HTTP reachability, title, total record count,
field ids, one sample row, and whether a date filter returns historical rows.
Nothing is written to disk. Standard library only.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://api.data.gov.in/resource/"
# api.data.gov.in never answers the default "Python-urllib" User-Agent; any custom one works.
HEADERS = {"User-Agent": "basmati-lakehouse-probe/0.1"}
HISTORY_DAYS = ("01/09/2023", "2023-09-01", "01-09-2023")  # formats differ between resources


def get(resource_id: str, **params: str) -> tuple[int | None, dict | str]:
    query = {"api-key": os.environ["DATA_GOV_IN_API_KEY"], "format": "json", "limit": "3", **params}
    url = BASE + resource_id + "?" + urllib.parse.urlencode(query)
    start = time.monotonic()
    try:
        req = urllib.request.Request(url, headers=HEADERS)  # noqa: S310 (fixed https host)
        with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
            body = json.load(resp)
            print(f"  HTTP {resp.status} in {time.monotonic() - start:.1f}s")
            return resp.status, body
    except urllib.error.HTTPError as e:
        return e.code, e.read()[:300].decode(errors="replace")
    except (urllib.error.URLError, TimeoutError) as e:
        return None, f"network error: {e}"


def probe(resource_id: str) -> None:
    print(f"\n=== {resource_id}")
    status, body = get(resource_id)
    if not isinstance(body, dict):
        print(f"  FAILED status={status}: {body}")
        return
    fields = [f.get("id") for f in body.get("field", [])]
    print("  title:", body.get("title"))
    print("  total records:", body.get("total"))
    print("  updated:", body.get("updated_date"))
    print("  fields:", fields)
    print("  sample:", json.dumps(body.get("records", [])[:1], indent=2, ensure_ascii=False))

    date_fields = [f for f in fields if f and "date" in f.lower()]
    for field in date_fields:
        for day in HISTORY_DAYS:
            _, hist = get(resource_id, **{f"filters[{field}]": day})
            total = hist.get("total") if isinstance(hist, dict) else hist
            print(f"  history filter {field}={day}: total={total}")


if __name__ == "__main__":
    if "DATA_GOV_IN_API_KEY" not in os.environ or len(sys.argv) < 2:
        sys.exit(__doc__)
    for rid in sys.argv[1:]:
        probe(rid)
