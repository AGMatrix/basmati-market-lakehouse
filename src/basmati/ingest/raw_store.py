"""Raw zone in object storage: one immutable snapshot of the source per arrival date.

Layout (bucket = LAKE_BUCKET):
    raw/agmarknet/dt=YYYY-MM-DD/records.json.gz   the API records, as sent
    raw/agmarknet/dt=YYYY-MM-DD/_manifest.json    row count, content hash, source, timing

Idempotency: each object is written with a single PUT, which S3 (and MinIO) apply
atomically, so a re-run replaces a day completely and never leaves duplicates. The
manifest is written last and is the commit marker: a partition without one is incomplete.
"""

from __future__ import annotations

import gzip
import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime

PREFIX = "raw/agmarknet"


def s3_client(settings):
    """boto3 S3 client for the lake (MinIO locally)."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )


def partition_prefix(day: date) -> str:
    return f"{PREFIX}/dt={day.isoformat()}"


@dataclass(frozen=True)
class Manifest:
    arrival_date: str
    source: str  # "current" | "history"
    resource_id: str
    row_count: int
    content_hash: str
    api_calls: int
    fetch_seconds: float
    ingested_at: str

    @classmethod
    def new(cls, day: date, **kw) -> Manifest:
        return cls(
            arrival_date=day.isoformat(),
            ingested_at=datetime.now(UTC).isoformat(timespec="seconds"),
            **kw,
        )


class RawStore:
    def __init__(self, s3, bucket: str):
        self._s3 = s3
        self._bucket = bucket

    def read_manifest(self, day: date) -> Manifest | None:
        try:
            obj = self._s3.get_object(
                Bucket=self._bucket, Key=f"{partition_prefix(day)}/_manifest.json"
            )
        except self._s3.exceptions.NoSuchKey:
            return None
        return Manifest(**json.loads(obj["Body"].read()))

    def write_day(self, day: date, records: list[dict], manifest: Manifest) -> None:
        prefix = partition_prefix(day)
        body = gzip.compress(json.dumps(records, ensure_ascii=False).encode(), mtime=0)
        self._s3.put_object(
            Bucket=self._bucket,
            Key=f"{prefix}/records.json.gz",
            Body=body,
            ContentType="application/json",
            ContentEncoding="gzip",
        )
        self._s3.put_object(
            Bucket=self._bucket,
            Key=f"{prefix}/_manifest.json",
            Body=json.dumps(asdict(manifest), indent=2).encode(),
            ContentType="application/json",
        )

    def read_day(self, day: date) -> list[dict]:
        obj = self._s3.get_object(
            Bucket=self._bucket, Key=f"{partition_prefix(day)}/records.json.gz"
        )
        return json.loads(gzip.decompress(obj["Body"].read()))

    def list_days(self) -> list[date]:
        """Every arrival date that has a committed partition (manifest present)."""
        days = []
        for page in self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket, Prefix=f"{PREFIX}/dt="
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/_manifest.json"):
                    days.append(date.fromisoformat(key.split("dt=")[1].split("/")[0]))
        return sorted(days)
