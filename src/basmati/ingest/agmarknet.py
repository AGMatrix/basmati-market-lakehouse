"""Agmarknet (data.gov.in) client: fetch one arrival date, all commodities, all states.

Two data.gov.in resources serve the same Agmarknet data:
- *current*: today's prices only, lower-case field names, integer prices.
- *history*: variety-wise prices back to ~2010, Title-case field names, string prices.
Both are filtered by arrival date and paged with offset/limit. The raw zone keeps each
response's records exactly as the API sent them; normalising the two shapes is Spark's job.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://api.data.gov.in/resource/"
# api.data.gov.in never answers the default python User-Agent; any custom one works.
USER_AGENT = "basmati-lakehouse/0.1"


@dataclass(frozen=True)
class Resource:
    """One data.gov.in resource and the spelling of its arrival-date field."""

    kind: str  # "current" | "history"
    resource_id: str
    date_field: str

    @classmethod
    def current(cls, resource_id: str) -> Resource:
        return cls("current", resource_id, "arrival_date")

    @classmethod
    def history(cls, resource_id: str) -> Resource:
        return cls("history", resource_id, "Arrival_Date")


class AgmarknetError(RuntimeError):
    """The API failed after all retries, or answered with something unusable."""


class RateLimiter:
    """At most `max_rps` calls per second from this process (min interval between calls)."""

    def __init__(
        self,
        max_rps: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._interval = 1.0 / max_rps
        self._clock = clock
        self._sleep = sleep
        self._last: float | None = None

    def wait(self) -> None:
        now = self._clock()
        if self._last is not None:
            delay = self._last + self._interval - now
            if delay > 0:
                self._sleep(delay)
                now += delay
        self._last = now


def ddmmyyyy(d: date) -> str:
    return d.strftime("%d/%m/%Y")


class AgmarknetClient:
    # 429 = throttled, 5xx = server trouble: worth retrying. 4xx otherwise = our bug.
    RETRY_STATUS = frozenset({429, 500, 502, 503, 504})

    def __init__(
        self,
        api_key: str,
        *,
        max_rps: float = 1.0,
        proxy: str | None = None,
        page_size: int = 1000,
        max_attempts: int = 6,
        backoff_base: float = 2.0,
        timeout: float = 60.0,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._api_key = api_key
        self._page_size = page_size
        self._max_attempts = max_attempts
        self._backoff_base = backoff_base
        self._timeout = timeout
        self._sleep = sleep
        self._limiter = RateLimiter(max_rps, sleep=sleep)
        self._session = session or requests.Session()
        self._session.headers["User-Agent"] = USER_AGENT
        if proxy:
            self._session.proxies = {"http": proxy, "https": proxy}
        self.calls = 0  # API calls made, for run metrics

    def _get(self, resource: Resource, params: dict[str, str | int]) -> dict:
        query = {"api-key": self._api_key, "format": "json", **params}
        url = BASE_URL + resource.resource_id
        for attempt in range(1, self._max_attempts + 1):
            self._limiter.wait()
            self.calls += 1
            try:
                resp = self._session.get(url, params=query, timeout=self._timeout)
                if resp.status_code not in self.RETRY_STATUS:
                    resp.raise_for_status()
                    body = resp.json()
                    if not isinstance(body, dict) or "records" not in body:
                        raise AgmarknetError(f"unexpected response shape: {str(body)[:200]}")
                    return body
                problem = f"HTTP {resp.status_code}"
            except (requests.ConnectionError, requests.Timeout, ValueError) as e:
                # ValueError covers a truncated / non-JSON body (gateway error pages).
                problem = type(e).__name__
            if attempt == self._max_attempts:
                break
            # Exponential backoff with full jitter: spreads retries, avoids hammering.
            delay = random.uniform(0, self._backoff_base * 2 ** (attempt - 1))  # noqa: S311
            log.warning("agmarknet %s (attempt %d), retrying in %.1fs", problem, attempt, delay)
            self._sleep(delay)
        raise AgmarknetError(f"{resource.kind} resource failed after {attempt} attempts: {problem}")

    def iter_pages(self, resource: Resource, day: date) -> Iterator[list[dict]]:
        """Yield record pages for one arrival date until the reported total is reached."""
        offset, total = 0, None
        while total is None or offset < total:
            body = self._get(
                resource,
                {
                    f"filters[{resource.date_field}]": ddmmyyyy(day),
                    "offset": offset,
                    "limit": self._page_size,
                },
            )
            records = body["records"]
            total = int(body.get("total") or 0)
            if not records:
                if offset < total:
                    log.warning("empty page at offset %d of %d for %s", offset, total, day)
                break
            yield records
            offset += len(records)

    def fetch_day(self, resource: Resource, day: date) -> list[dict]:
        records = [r for page in self.iter_pages(resource, day) for r in page]
        expected = ddmmyyyy(day)
        wrong = {r.get(resource.date_field) for r in records} - {expected}
        if wrong:
            # The API parses the filter leniently; refuse to file rows under the wrong day.
            raise AgmarknetError(f"asked for {expected}, got arrival dates {sorted(wrong)[:5]}")
        return records


def content_hash(records: list[dict]) -> str:
    """Order-independent hash of a day's records: same data in any page order -> same hash."""
    lines = sorted(json.dumps(r, sort_keys=True, ensure_ascii=False) for r in records)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()
