"""Settings from environment variables (.env locally, docker-compose in containers)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

_API_VARS = (
    "DATA_GOV_IN_API_KEY",
    "AGMARKNET_CURRENT_RESOURCE_ID",
    "AGMARKNET_HISTORY_RESOURCE_ID",
)
_S3_VARS = (
    "S3_ENDPOINT",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
)


class ConfigError(RuntimeError):
    """A required setting is missing or invalid."""


@dataclass(frozen=True)
class Settings:
    # repr=False keeps secrets out of logs and tracebacks.
    data_gov_in_api_key: str = field(repr=False)
    agmarknet_current_resource_id: str
    agmarknet_history_resource_id: str
    s3_endpoint: str
    s3_access_key: str = field(repr=False)
    s3_secret_key: str = field(repr=False)
    lake_bucket: str = "lake"
    agmarknet_max_rps: float = 1.0
    agmarknet_proxy: str | None = None

    @classmethod
    def from_env(
        cls, env: Mapping[str, str] | None = None, *, require_api: bool = True
    ) -> Settings:
        """require_api=False for jobs that only touch the lake (Spark, quality checks)."""
        env = os.environ if env is None else env
        required = (_API_VARS if require_api else ()) + _S3_VARS
        missing = [name for name in required if not env.get(name, "").strip()]
        if missing:
            raise ConfigError(f"missing environment variables: {', '.join(missing)}")
        try:
            max_rps = float(env.get("AGMARKNET_MAX_RPS") or 1.0)
        except ValueError as e:
            raise ConfigError(f"AGMARKNET_MAX_RPS must be a number: {e}") from e
        if max_rps <= 0:
            raise ConfigError("AGMARKNET_MAX_RPS must be > 0")
        return cls(
            data_gov_in_api_key=env.get("DATA_GOV_IN_API_KEY", "").strip(),
            agmarknet_current_resource_id=env.get("AGMARKNET_CURRENT_RESOURCE_ID", "").strip(),
            agmarknet_history_resource_id=env.get("AGMARKNET_HISTORY_RESOURCE_ID", "").strip(),
            s3_endpoint=env["S3_ENDPOINT"].strip(),
            s3_access_key=env["S3_ACCESS_KEY"].strip(),
            s3_secret_key=env["S3_SECRET_KEY"].strip(),
            lake_bucket=(env.get("LAKE_BUCKET") or "lake").strip(),
            agmarknet_max_rps=max_rps,
            agmarknet_proxy=(env.get("AGMARKNET_PROXY") or "").strip() or None,
        )
