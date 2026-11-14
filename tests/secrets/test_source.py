"""Secrets source selection, the Secrets Manager mapping, caching, rotation and refresh (4.7). No network."""
from __future__ import annotations

import time

import pytest

from src.secrets.source import (
    AwsSecretsManager,
    EnvSecrets,
    SecretsError,
    parse_secret_string,
    secrets_source_from_env,
)

from tests.secrets.fakes import CountingCredentials, FakeAws

STONEX_SECRET = "stonex-SeCrEt-rotated"


def manager(fake: FakeAws, *, refresh_s: float = 300.0, clock: list[float] | None = None,
            creds: CountingCredentials | None = None) -> AwsSecretsManager:
    t = clock if clock is not None else [0.0]
    return AwsSecretsManager("xtnl/engine", "eu-west-2", credentials=creds or CountingCredentials(),
                             http=fake.client(), refresh_s=refresh_s, monotonic=lambda: t[0])


def test_selection_from_the_environment() -> None:
    assert isinstance(secrets_source_from_env({}), EnvSecrets)
    assert isinstance(secrets_source_from_env({"XTNL_SECRETS_SOURCE": "env"}), EnvSecrets)
    with pytest.raises(SecretsError, match="XTNL_SECRETS_SOURCE must be one of env, aws-sm"):
        secrets_source_from_env({"XTNL_SECRETS_SOURCE": "vault"})
    with pytest.raises(SecretsError, match="XTNL_SECRETS_ID, XTNL_SECRETS_REGION"):
        secrets_source_from_env({"XTNL_SECRETS_SOURCE": "aws-sm"})
    src = secrets_source_from_env({"XTNL_SECRETS_SOURCE": "aws-sm", "XTNL_SECRETS_ID": "xtnl/engine",
                                   "XTNL_SECRETS_REGION": "eu-west-2"}, credentials=CountingCredentials())
    assert isinstance(src, AwsSecretsManager) and src.kind == "aws-sm"
    src.close()


def test_env_source_is_the_environment_unchanged() -> None:
    env = {"STONE_X_PASS": "p"}
    src = EnvSecrets()
    assert src.resolve(env) == env and src.resolve_cached(env) == env and src.version_id is None


def test_secret_is_laid_over_the_environment_but_never_the_gate() -> None:
    fake = FakeAws()
    fake.secret = {"STONE_X_PASS": STONEX_SECRET, "XTNL_SMTP_PORT": 2525, "BROKER_ENV": "live",
                   "XTNL_USE_MOCK_BROKER": "1", "XTNL_SECRETS_ID": "other"}
    src = manager(fake)
    out = src.resolve({"STONE_X_PASS": "CHANGE_ME", "BROKER_ENV": "sandbox", "STONE_X_USERNAME": "u"})
    assert out["STONE_X_PASS"] == STONEX_SECRET and out["XTNL_SMTP_PORT"] == "2525"
    assert out["BROKER_ENV"] == "sandbox" and "XTNL_USE_MOCK_BROKER" not in out and "XTNL_SECRETS_ID" not in out
    assert out["STONE_X_USERNAME"] == "u"
    assert src.version_id == "v1"
    (body,) = fake.bodies("GetSecretValue")
    assert body == {"SecretId": "xtnl/engine"}
    assert fake.requests[0].url == "https://secretsmanager.eu-west-2.amazonaws.com/"
    assert STONEX_SECRET not in repr(src)


def test_cached_until_the_refresh_interval_then_rotation_is_picked_up() -> None:
    fake = FakeAws()
    fake.secret = {"STONE_X_PASS": "one"}
    clock = [0.0]
    src = manager(fake, refresh_s=300.0, clock=clock)
    with pytest.raises(SecretsError, match="not been read"):
        src.resolve_cached({})                 # never does I/O
    assert src.resolve({})["STONE_X_PASS"] == "one"
    fake.secret, fake.version = {"STONE_X_PASS": "two"}, "v2"
    clock[0] = 299.0
    assert src.resolve({})["STONE_X_PASS"] == "one" and src.reads == 1
    clock[0] = 300.0
    assert src.resolve({})["STONE_X_PASS"] == "two" and src.reads == 2 and src.version_id == "v2"
    assert src.resolve_cached({})["STONE_X_PASS"] == "two"


def test_invalidate_wakes_the_refresher_thread() -> None:
    fake = FakeAws()
    fake.secret = {"STONE_X_PASS": "one"}
    src = manager(fake, refresh_s=3600.0)
    src.resolve({})
    src.start()
    try:
        fake.secret, fake.version = {"STONE_X_PASS": "two"}, "v2"
        src.invalidate()
        deadline = time.monotonic() + 5.0
        while src.version_id != "v2" and time.monotonic() < deadline:
            time.sleep(0.01)
        assert src.resolve_cached({})["STONE_X_PASS"] == "two"
    finally:
        src.close()


def test_auth_error_refreshes_the_aws_credentials_and_retries() -> None:
    fake = FakeAws()
    fake.secret = {"STONE_X_PASS": "one"}
    fake.fail_next.append((400, "UnrecognizedClientException"))
    creds = CountingCredentials()
    src = manager(fake, creds=creds)
    assert src.resolve({})["STONE_X_PASS"] == "one"
    assert creds.refreshes == 1 and len(fake.requests) == 2


def test_read_errors_name_no_values() -> None:
    fake = FakeAws()
    fake.fail_next.append((400, "AccessDeniedException"))
    with pytest.raises(SecretsError, match="AccessDeniedException"):
        manager(fake).resolve({})
    fake.secret = '{"STONE_X_PASS": "' + STONEX_SECRET + '"'          # truncated JSON
    with pytest.raises(SecretsError) as info:
        manager(fake).resolve({})
    assert STONEX_SECRET not in str(info.value)


def test_parse_secret_string_rejects_non_variables() -> None:
    assert parse_secret_string('{"A_B": "x", "FLAG_ON": true}') == {"A_B": "x", "FLAG_ON": "1"}
    with pytest.raises(SecretsError, match="JSON object"):
        parse_secret_string('["x"]')
    with pytest.raises(SecretsError, match="not an environment variable name"):
        parse_secret_string('{"lower": "x"}')
    with pytest.raises(SecretsError, match="STONE_X_PASS must hold a string") as info:
        parse_secret_string('{"STONE_X_PASS": {"nested": "' + STONEX_SECRET + '"}}')
    assert STONEX_SECRET not in str(info.value)


def test_secrets_manager_config_errors() -> None:
    with pytest.raises(SecretsError, match="XTNL_SECRETS_ID"):
        AwsSecretsManager("", "eu-west-2", credentials=CountingCredentials())
    with pytest.raises(SecretsError, match="XTNL_SECRETS_REGION"):
        AwsSecretsManager("x", "not a region", credentials=CountingCredentials())
