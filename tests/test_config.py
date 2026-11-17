import pytest

from basmati.config import ConfigError, Settings

BASE = {
    "DATA_GOV_IN_API_KEY": "secret-key",
    "AGMARKNET_CURRENT_RESOURCE_ID": "current-id",
    "AGMARKNET_HISTORY_RESOURCE_ID": "history-id",
    "S3_ENDPOINT": "http://minio:9000",
    "S3_ACCESS_KEY": "access",
    "S3_SECRET_KEY": "s3-secret",
}


def test_defaults_and_empty_proxy():
    s = Settings.from_env({**BASE, "AGMARKNET_PROXY": "  "})
    assert s.agmarknet_proxy is None
    assert s.agmarknet_max_rps == 1.0
    assert s.lake_bucket == "lake"


def test_missing_vars_are_all_listed():
    env = {k: v for k, v in BASE.items() if k not in ("S3_ENDPOINT", "DATA_GOV_IN_API_KEY")}
    with pytest.raises(ConfigError, match="DATA_GOV_IN_API_KEY, S3_ENDPOINT"):
        Settings.from_env(env)


@pytest.mark.parametrize("bad", ["abc", "0", "-1"])
def test_invalid_rate_limit(bad):
    with pytest.raises(ConfigError, match="AGMARKNET_MAX_RPS"):
        Settings.from_env({**BASE, "AGMARKNET_MAX_RPS": bad})


def test_repr_hides_secrets():
    text = repr(Settings.from_env(BASE))
    assert "secret-key" not in text
    assert "s3-secret" not in text
    assert "current-id" in text


def test_lake_only_jobs_do_not_need_api_key():
    env = {k: v for k, v in BASE.items() if not k.startswith(("DATA_GOV", "AGMARKNET"))}
    s = Settings.from_env(env, require_api=False)
    assert s.data_gov_in_api_key == ""
    with pytest.raises(ConfigError, match="DATA_GOV_IN_API_KEY"):
        Settings.from_env(env)


@pytest.mark.parametrize("bad", ["x", "0", "20000"])
def test_invalid_page_size(bad):
    with pytest.raises(ConfigError, match="AGMARKNET_PAGE_SIZE"):
        Settings.from_env({**BASE, "AGMARKNET_PAGE_SIZE": bad})
