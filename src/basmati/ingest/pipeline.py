"""Ingest one arrival date into the raw zone and report whether its content changed."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta

from basmati.ingest.agmarknet import AgmarknetClient, Resource, content_hash
from basmati.ingest.raw_store import Manifest, RawStore

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DayResult:
    day: date
    rows: int
    source: str | None
    changed: bool  # new partition, or the source revised a day we already had


def ingest_day(
    client: AgmarknetClient,
    store: RawStore,
    day: date,
    history: Resource,
    current: Resource,
) -> DayResult:
    """Fetch `day` (history resource first, then today's resource) and store it if new or revised.

    A day with no rows (Sunday, holiday, not yet published) is still recorded with an empty
    partition, so "checked, nothing there" is distinguishable from "never fetched".
    """
    start, calls0 = time.monotonic(), client.calls
    records, used = [], history
    for resource in (history, current):
        used = resource
        records = client.fetch_day(resource, day)
        if records:
            break

    digest = content_hash(records)
    previous = store.read_manifest(day)
    if previous is not None and previous.content_hash == digest:
        log.info("%s unchanged (%d rows), not rewritten", day, len(records))
        return DayResult(day, len(records), used.kind if records else None, changed=False)

    if previous is not None:
        log.info("%s REVISED: %d -> %d rows", day, previous.row_count, len(records))
    store.write_day(
        day,
        records,
        Manifest.new(
            day,
            source=used.kind if records else "none",
            resource_id=used.resource_id,
            row_count=len(records),
            content_hash=digest,
            api_calls=client.calls - calls0,
            fetch_seconds=round(time.monotonic() - start, 2),
        ),
    )
    return DayResult(day, len(records), used.kind if records else None, changed=True)


def lookback_days(day: date, lookback: int) -> list[date]:
    """`day` plus the `lookback` days before it, oldest first (re-pulls catch late revisions)."""
    return [day - timedelta(days=n) for n in range(lookback, -1, -1)]
