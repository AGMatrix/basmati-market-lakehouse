import gzip
import json
from datetime import date

import boto3
import pytest
import requests

from basmati.ingest.agmarknet import (
    AgmarknetClient,
    AgmarknetError,
    RateLimiter,
    Resource,
    content_hash,
)
from basmati.ingest.pipeline import ingest_day, lookback_days
from basmati.ingest.raw_store import Manifest, RawStore

DAY = date(2023, 9, 1)
HIST = Resource.history("hist-id")
CUR = Resource.current("cur-id")


class FakeResponse:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body

    def json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    """Replays queued responses; records every call's params."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}
        self.proxies = {}

    def get(self, url, params, timeout):
        self.calls.append((url, dict(params)))
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def rec(day="01/09/2023", field="Arrival_Date", **kw):
    return {field: day, "Commodity": "Paddy(Basmati)", "Modal_Price": "3500", **kw}


def client(responses, **kw):
    session = FakeSession(responses)
    c = AgmarknetClient("k", session=session, sleep=lambda s: None, page_size=2, **kw)
    return c, session


# --- client -------------------------------------------------------------------------------


def test_pages_until_total_and_sends_date_filter():
    c, s = client(
        [
            FakeResponse(200, {"total": 3, "records": [rec(Market="a"), rec(Market="b")]}),
            FakeResponse(200, {"total": 3, "records": [rec(Market="c")]}),
        ]
    )
    rows = c.fetch_day(HIST, DAY)
    assert [r["Market"] for r in rows] == ["a", "b", "c"]
    assert [p["offset"] for _, p in s.calls] == [0, 2]
    assert s.calls[0][1]["filters[Arrival_Date]"] == "01/09/2023"
    assert s.headers["User-Agent"].startswith("basmati")  # the API ignores the default UA


def test_retries_transient_errors_then_succeeds():
    ok = FakeResponse(200, {"total": 1, "records": [rec()]})
    c, s = client(
        [
            FakeResponse(503, {}),
            requests.ConnectionError("reset"),
            FakeResponse(200, ValueError("truncated json")),
            ok,
        ]
    )
    assert len(c.fetch_day(HIST, DAY)) == 1
    assert c.calls == 4


def test_gives_up_after_max_attempts():
    c, _ = client([FakeResponse(429, {})] * 3, max_attempts=3)
    with pytest.raises(AgmarknetError, match="after 3 attempts: HTTP 429"):
        c.fetch_day(HIST, DAY)


def test_client_error_is_not_retried():
    c, _ = client([FakeResponse(403, {})])
    with pytest.raises(requests.HTTPError):
        c.fetch_day(HIST, DAY)
    assert c.calls == 1


def test_rejects_rows_for_another_date():
    c, _ = client([FakeResponse(200, {"total": 1, "records": [rec(day="02/09/2023")]})])
    with pytest.raises(AgmarknetError, match="02/09/2023"):
        c.fetch_day(HIST, DAY)


def test_proxy_is_applied_to_session():
    c, s = client([], proxy="socks5h://host:1080")
    assert s.proxies == {"http": "socks5h://host:1080", "https": "socks5h://host:1080"}


def test_rate_limiter_spaces_calls():
    t = [0.0]
    slept = []

    def sleep(d):
        slept.append(d)
        t[0] += d

    rl = RateLimiter(2.0, clock=lambda: t[0], sleep=sleep)
    rl.wait()
    rl.wait()
    t[0] += 0.2
    rl.wait()
    assert slept == pytest.approx([0.5, 0.3])


def test_content_hash_ignores_order():
    a, b = rec(Market="a"), rec(Market="b")
    assert content_hash([a, b]) == content_hash([b, a])
    assert content_hash([a]) != content_hash([a, b])


# --- raw store + pipeline (in-memory S3) --------------------------------------------------


class MemoryS3:
    """The three S3 calls RawStore uses, backed by a dict."""

    def __init__(self):
        self.objects = {}
        self.puts = 0
        self.exceptions = boto3.client("s3", region_name="us-east-1").exceptions

    def put_object(self, Bucket, Key, Body, **_):  # noqa: N803 (boto3 names)
        self.objects[Key] = Body
        self.puts += 1

    def get_object(self, Bucket, Key):  # noqa: N803
        if Key not in self.objects:
            raise self.exceptions.NoSuchKey({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        class Body:
            def __init__(self, b):
                self.b = b

            def read(self):
                return self.b

        return {"Body": Body(self.objects[Key])}


def test_ingest_writes_then_skips_unchanged_then_detects_revision():
    s3 = MemoryS3()
    store = RawStore(s3, "lake")
    page = {"total": 1, "records": [rec()]}

    c, _ = client([FakeResponse(200, page)])
    first = ingest_day(c, store, DAY, HIST, CUR)
    assert first.changed and first.rows == 1 and first.source == "history"
    assert store.read_day(DAY) == [rec()]
    keys = sorted(s3.objects)
    assert keys == [
        "raw/agmarknet/dt=2023-09-01/_manifest.json",
        "raw/agmarknet/dt=2023-09-01/records.json.gz",
    ]

    c, _ = client([FakeResponse(200, page)])
    puts = s3.puts
    again = ingest_day(c, store, DAY, HIST, CUR)
    assert not again.changed and s3.puts == puts  # idempotent: nothing rewritten

    revised = {"total": 2, "records": [rec(), rec(Market="late")]}
    c, _ = client([FakeResponse(200, revised)])
    third = ingest_day(c, store, DAY, HIST, CUR)
    assert third.changed and third.rows == 2
    assert store.read_manifest(DAY).row_count == 2


def test_falls_back_to_current_resource_and_records_empty_days():
    store = RawStore(MemoryS3(), "lake")
    today_row = rec(field="arrival_date")
    c, s = client(
        [
            FakeResponse(200, {"total": 0, "records": []}),
            FakeResponse(200, {"total": 1, "records": [today_row]}),
        ]
    )
    r = ingest_day(c, store, DAY, HIST, CUR)
    assert r.source == "current" and r.rows == 1
    assert s.calls[1][0].endswith("cur-id")

    c, _ = client([FakeResponse(200, {"total": 0, "records": []})] * 2)
    empty = ingest_day(c, store, date(2023, 9, 3), HIST, CUR)
    assert empty.rows == 0 and empty.source is None
    assert store.read_manifest(date(2023, 9, 3)).source == "none"


def test_records_are_gzipped_deterministically():
    s3 = MemoryS3()
    store = RawStore(s3, "lake")
    m = Manifest.new(DAY, source="history", resource_id="h", row_count=1,
                     content_hash="x", api_calls=1, fetch_seconds=0.1)  # fmt: skip
    store.write_day(DAY, [rec()], m)
    first = s3.objects["raw/agmarknet/dt=2023-09-01/records.json.gz"]
    store.write_day(DAY, [rec()], m)
    assert s3.objects["raw/agmarknet/dt=2023-09-01/records.json.gz"] == first
    assert json.loads(gzip.decompress(first)) == [rec()]


def test_lookback_days_oldest_first():
    assert lookback_days(DAY, 2) == [date(2023, 8, 30), date(2023, 8, 31), DAY]
    assert lookback_days(DAY, 0) == [DAY]
