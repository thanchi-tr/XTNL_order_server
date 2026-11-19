"""API settings: the secret is required and strong, never shown; the cache uses its own Redis database."""
from __future__ import annotations

import base64
from pathlib import Path

import pytest

from src.api.settings import (ApiConfigError, ApiSettings, cert_path_from_env, forbidden_env_names,
                              redis_url_with_db, secret_strength)

from tests.api.conftest import SECRET_TEXT

DSN = "postgresql://xtnl_api:pw_not_real@127.0.0.1:55433/xtnl"
BASE = {"XTNL_API_SECRET": SECRET_TEXT, "XTNL_API_DSN": DSN, "XTNL_API_REDIS_URL": "redis://:redis_pw@127.0.0.1:56381/0"}


def test_defaults(tmp_path: Path) -> None:
    s = ApiSettings.from_env(BASE, root=tmp_path)
    assert (s.port, s.bind, s.redis_db, s.rate_per_min) == (47443, "0.0.0.0", 2, 30)
    assert s.cert_path == tmp_path / "var/api-tls/server.crt" and s.key_path == tmp_path / "var/api-tls/server.key"
    assert s.redis_url == "redis://:redis_pw@127.0.0.1:56381/2" and s.dsn == DSN
    assert s.secret == SECRET_TEXT.encode()


def test_secret_never_appears_in_repr_or_describe(tmp_path: Path) -> None:
    s = ApiSettings.from_env(BASE, root=tmp_path)
    for text in (repr(s), str(s), s.describe()):
        assert SECRET_TEXT not in text and "pw_not_real" not in text and "redis_pw" not in text


@pytest.mark.parametrize("value", ["", "   ", "short", "ab" * 31, base64.b64encode(b"x" * 31).decode(),
                                   "not hex and not base64!"])
def test_missing_or_weak_secret_is_refused(value: str) -> None:
    with pytest.raises(ApiConfigError) as exc:
        ApiSettings.from_env({**BASE, "XTNL_API_SECRET": value})
    assert "XTNL_API_SECRET" in str(exc.value)
    if value.strip():
        assert value.strip() not in str(exc.value)


@pytest.mark.parametrize("value,strength", [("ab" * 32, 32), (base64.b64encode(b"k" * 32).decode(), 32),
                                            (base64.urlsafe_b64encode(b"\xfb" * 48).decode().rstrip("="), 48)])
def test_hex_and_base64_secrets_are_accepted(value: str, strength: int) -> None:
    assert secret_strength(value) == strength
    assert ApiSettings.from_env({**BASE, "XTNL_API_SECRET": value}).secret == value.encode()


def test_cache_database_is_never_0_and_is_applied_to_the_apis_own_redis() -> None:
    with pytest.raises(ApiConfigError):
        ApiSettings.from_env({**BASE, "XTNL_API_REDIS_DB": "0"})
    assert ApiSettings.from_env({**BASE, "XTNL_API_REDIS_DB": "5"}).redis_url.endswith(":56381/5")


@pytest.mark.parametrize("name", ["STONE_X_USERNAME", "STONE_X_PASS", "STONE_X_APP_KEY", "STONE_X_BASE_URL",
                                  "STONE_X_STREAM_URL", "CTRADER_CLIENT_SECRET", "CTRADER_ACCESS_TOKEN",
                                  "ORDER_SERVER_DSN", "REDIS_URL", "stone_x_pass",
                                  # plan S36 / design C2: market-data and ARM settings and secrets, the alert
                                  # channels, the dead-man token and the broker environment
                                  "XTNL_MD_REDIS_URL", "XTNL_MD_REDIS_READER_URL", "XTNL_MD_HMAC_KEY",
                                  "XTNL_MD_ENABLED", "XTNL_ARM_CONFIG_DSN", "XTNL_ARM_SMTP_PASSWORD",
                                  "XTNL_ARM_PUSH_TOKEN", "XTNL_SMTP_PASSWORD", "XTNL_SES_SECRET_ACCESS_KEY",
                                  "XTNL_PUSH_TOKEN", "XTNL_DEADMAN_TOKEN", "BROKER_ENV"])
def test_engine_credentials_in_the_environment_refuse_the_start(name: str) -> None:
    """Regression (least privilege): the API child used to inherit the engine's StoneX credentials, its superuser
    DSN and its Redis. Their presence is now a configuration error (exit 78); the message names, never shows."""
    value = "very-secret-value-123"
    with pytest.raises(ApiConfigError) as exc:
        ApiSettings.from_env({**BASE, name: value})
    assert name in str(exc.value) and value not in str(exc.value)
    assert forbidden_env_names({**BASE, name: value}) == [name]
    assert forbidden_env_names({**BASE, name: ""}) == []                       # an empty variable holds nothing


def test_api_dsn_is_required_and_the_api_redis_defaults_to_its_own_instance() -> None:
    env = dict(BASE)
    del env["XTNL_API_DSN"]
    with pytest.raises(ApiConfigError) as exc:
        ApiSettings.from_env(env)
    assert "XTNL_API_DSN" in str(exc.value)
    env = dict(BASE)
    del env["XTNL_API_REDIS_URL"]
    assert ApiSettings.from_env(env).redis_url == "redis://127.0.0.1:56381/2"     # service redis-api, never 56380


@pytest.mark.parametrize("name,value", [("XTNL_API_DSN", "mysql://u@h/db"), ("XTNL_API_REDIS_URL", "http://h:1")])
def test_api_storage_urls_are_checked(name: str, value: str) -> None:
    with pytest.raises(ApiConfigError):
        ApiSettings.from_env({**BASE, name: value})


@pytest.mark.parametrize("name,value", [("XTNL_API_PORT", "0"), ("XTNL_API_PORT", "x"), ("XTNL_API_BIND", "host"),
                                        ("XTNL_API_REDIS_DB", "16"), ("XTNL_API_RATE_PER_MIN", "0")])
def test_bad_settings_are_refused(name: str, value: str) -> None:
    with pytest.raises(ApiConfigError):
        ApiSettings.from_env({**BASE, name: value})


def test_storage_is_required_unless_not_needed() -> None:
    with pytest.raises(ApiConfigError):
        ApiSettings.from_env({"XTNL_API_SECRET": SECRET_TEXT})
    s = ApiSettings.from_env({"XTNL_API_SECRET": SECRET_TEXT, "XTNL_API_PORT": "50000"}, need_storage=False)
    assert s.port == 50000 and s.dsn == "" and s.redis_url == ""


def test_redis_url_with_db() -> None:
    assert redis_url_with_db("redis://127.0.0.1:6379", 2) == "redis://127.0.0.1:6379/2"
    assert redis_url_with_db("rediss://u:p@h:6380/0?ssl_cert_reqs=required", 2) == \
        "rediss://u:p@h:6380/2?ssl_cert_reqs=required"
    assert redis_url_with_db("unix:///run/redis.sock?db=0", 2) == "unix:///run/redis.sock?db=2"


def test_cert_path(tmp_path: Path) -> None:
    assert cert_path_from_env({}, root=tmp_path) == tmp_path / "var/api-tls/server.crt"
    absolute = tmp_path / "x.crt"
    assert cert_path_from_env({"XTNL_API_TLS_CERT": str(absolute)}, root=Path("/elsewhere")) == absolute
