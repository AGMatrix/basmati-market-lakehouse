"""Open-Meteo daily weather for basmati districts -> raw/weather/ in the lake.

- Districts are geocoded once with Open-Meteo's geocoder and cached in
  raw/weather/_districts.json. The point is the geocoder's match for the district name
  (usually the district headquarters town), not a true district centroid.
- History comes from the archive API (ERA5-based reanalysis, a few days behind real time),
  incremental per district (missing dates + a 7-day refresh), one object per district
  (raw/weather/district=<slug>/daily.json.gz), rewritten whole on each change: idempotent.

Open-Meteo is free for non-commercial use, needs no key, and is not geo-blocked.
"""

from __future__ import annotations

import gzip
import json
import logging
import re
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import date, timedelta

import requests

log = logging.getLogger(__name__)

GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_VARS = ["precipitation_sum", "temperature_2m_max", "temperature_2m_min"]
PREFIX = "raw/weather"
REFRESH_DAYS = 7  # the archive fills recent days in late, so re-pull the last week
MAX_ATTEMPTS = 6
USER_AGENT = "basmati-lakehouse/0.1"


@dataclass(frozen=True)
class District:
    state: str
    district: str
    latitude: float | None = None
    longitude: float | None = None
    matched_name: str | None = None
    status: str = "unresolved"  # resolved | unresolved

    @property
    def slug(self) -> str:
        return slugify(f"{self.state}-{self.district}")


def slugify(text: str) -> str:
    t = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", t.lower()).strip("-")


def _candidates(name: str) -> list[str]:
    """'Jalaun (orai)' -> ['Jalaun (orai)', 'Jalaun', 'orai']: try the plain and bracketed parts."""
    names = [name]
    m = re.match(r"^(.*?)\s*\((.*?)\)\s*$", name)
    if m:
        names += [m.group(1), m.group(2)]
    return [n for n in dict.fromkeys(x.strip() for x in names) if n]


def geocode(session: requests.Session, state: str, district: str) -> District:
    """First geocoder hit in India whose admin1 (state) matches; else unresolved."""
    for name in _candidates(district):
        r = session.get(
            GEOCODE_URL,
            params={"name": name, "count": 20, "country_code": "IN", "format": "json"},
            timeout=30,
        )
        r.raise_for_status()
        for hit in r.json().get("results", []) or []:
            if slugify(hit.get("admin1", "")) == slugify(state):
                return District(state, district, hit["latitude"], hit["longitude"],
                                hit.get("name"), "resolved")  # fmt: skip
    return District(state, district)


def fetch_daily(
    session: requests.Session,
    d: District,
    start: date,
    end: date,
    sleep=time.sleep,
) -> list[dict]:
    """One archive call; on 429 waits (Retry-After, else exponential) and retries.

    Open-Meteo's free tier weights a call by its date range and variables, so a multi-year
    history counts as many calls: 429s are expected on a first load and are not an error.
    """
    params = {
        "latitude": d.latitude,
        "longitude": d.longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(DAILY_VARS),
        "timezone": "Asia/Kolkata",
    }
    for attempt in range(MAX_ATTEMPTS):
        r = session.get(ARCHIVE_URL, params=params, timeout=120)
        if r.status_code != 429:
            break
        wait = float(r.headers.get("Retry-After") or 60 * 2**attempt)
        log.warning("open-meteo 429 for %s, waiting %.0fs", d.slug, wait)
        sleep(min(wait, 900))
    r.raise_for_status()
    daily = r.json()["daily"]
    return [
        {
            "date": day,
            "precipitation_mm": daily["precipitation_sum"][i],
            "temperature_max_c": daily["temperature_2m_max"][i],
            "temperature_min_c": daily["temperature_2m_min"][i],
        }
        for i, day in enumerate(daily["time"])
    ]


class WeatherStore:
    def __init__(self, s3, bucket: str):
        self._s3 = s3
        self._bucket = bucket

    def load_districts(self) -> dict[str, District]:
        try:
            body = self._s3.get_object(Bucket=self._bucket, Key=f"{PREFIX}/_districts.json")
        except self._s3.exceptions.NoSuchKey:
            return {}
        return {(x := District(**row)).slug: x for row in json.loads(body["Body"].read())}

    def save_districts(self, districts: dict[str, District]) -> None:
        rows = [asdict(d) for d in sorted(districts.values(), key=lambda d: d.slug)]
        self._s3.put_object(
            Bucket=self._bucket,
            Key=f"{PREFIX}/_districts.json",
            Body=json.dumps(rows, indent=1).encode(),
        )

    def read_district(self, d: District) -> list[dict]:
        try:
            body = self._s3.get_object(
                Bucket=self._bucket, Key=f"{PREFIX}/district={d.slug}/daily.json.gz"
            )
        except self._s3.exceptions.NoSuchKey:
            return []
        return json.loads(gzip.decompress(body["Body"].read()))["daily"]

    def write_district(self, d: District, rows: list[dict]) -> None:
        payload = {"district": asdict(d), "daily": rows}
        self._s3.put_object(
            Bucket=self._bucket,
            Key=f"{PREFIX}/district={d.slug}/daily.json.gz",
            Body=gzip.compress(json.dumps(payload).encode(), mtime=0),
        )


def ingest(
    s3, bucket: str, wanted: list[tuple[str, str]], start: date, end: date, pause: float = 5.0
) -> dict:
    """Geocode new districts, then refresh every resolved one. Returns run metrics."""
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    store = WeatherStore(s3, bucket)
    districts = store.load_districts()
    for state, name in wanted:
        d = District(state, name)
        if d.slug not in districts:
            districts[d.slug] = geocode(session, state, name)
            time.sleep(pause)
    store.save_districts(districts)

    resolved = [d for d in districts.values() if d.status == "resolved"]
    rows = calls = 0
    for d in resolved:
        # Incremental + resumable: fetch only what is missing (plus a refresh window) and
        # save each district as soon as it is done, so a retried run continues, not restarts.
        have = {r["date"]: r for r in store.read_district(d)}
        last = max(have) if have else None
        since = start if last is None else date.fromisoformat(last) - timedelta(REFRESH_DAYS)
        since = max(since, start)
        if since <= end:
            for r in fetch_daily(session, d, since, end):
                have[r["date"]] = r
            calls += 1
            store.write_district(d, [have[k] for k in sorted(have)])
            time.sleep(pause)
        rows += len(have)
    unresolved = [f"{d.state} / {d.district}" for d in districts.values() if d.status != "resolved"]
    if unresolved:
        log.warning("not geocoded (no match in the state): %s", unresolved)
    return {"districts": len(resolved), "rows": rows, "api_calls": calls, "unresolved": unresolved}
