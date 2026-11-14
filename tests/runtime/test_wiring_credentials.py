"""Credentials hardening in the wiring (IMPLEMENTATION_PLAN 4.7): the secrets source feeds every setting,
the KMS token store turns Pepperstone token refresh on, Patroni REST credentials and etcd client TLS.
No network: the secret is served by a fake (or httpx.MockTransport), KMS by a fake Cipher."""
from __future__ import annotations

import ssl
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import httpx
import pytest

from src.adapters.pepperstone.adapter import PepperstoneAdapter
from src.adapters.stonex.adapter import StoneXAdapter
from src.db.token_store import KmsTokenStore
from src.runtime import etcd as etcd_mod
from src.runtime.etcd import EtcdTls, HttpEtcdClient, tls_context
from src.runtime.patroni import patroni_auth_from_env
from src.runtime.wiring import (
    TOKEN_REFRESH_MIN_LEASE_S,
    EngineHooks,
    LeaseGuardedTokenStore,
    WiringError,
    adapter_factory,
    build_engine,
    load_settings,
    make_adapter,
)
from src.secrets.source import AwsSecretsManager, SecretsError, SourceKind, overlay

from tests.runtime.test_wiring import BASE, CTRADER, EMAIL, MOCK, SECRETS, STONEX, cfg
from tests.secrets.fakes import CountingCredentials, FakeAws

KMS = {"XTNL_TOKEN_KMS_KEY_ID": "alias/xtnl-ctrader-tokens", "XTNL_KMS_REGION": "eu-west-2"}
PATRONI_SECRET = "patroni-SeCrEt-jkl"
SM_STONEX = "sm-stonex-SeCrEt-1"
SM_SMTP = "sm-smtp-SeCrEt-2"


class FakeSource:
    """SecretsSource with a scripted secret; counts reads, never does I/O in resolve_cached."""

    kind: SourceKind = "aws-sm"

    def __init__(self, values: dict[str, str], version: str = "v1") -> None:
        self.values = values
        self.version_id: str | None = version
        self.started = self.closed = False
        self.reads = 0

    def resolve(self, base: Mapping[str, str]) -> dict[str, str]:
        self.reads += 1
        return overlay(base, self.values)[0]

    def resolve_cached(self, base: Mapping[str, str]) -> dict[str, str]:
        return overlay(base, self.values)[0]

    def invalidate(self) -> None:
        return None

    def start(self) -> None:
        self.started = True

    def close(self) -> None:
        self.closed = True


class NullCipher:
    key_id = "alias/xtnl-ctrader-tokens"

    async def encrypt(self, plaintext: bytes, context: Mapping[str, str]) -> bytes:
        raise AssertionError("not used")

    async def decrypt(self, blob: bytes, context: Mapping[str, str], *, key_id: str | None = None) -> bytes:
        raise AssertionError("not used")


# --- KMS token store -> token refresh ---------------------------------------------------------------------
async def test_pepperstone_refresh_is_enabled_only_with_a_token_store() -> None:
    env = {**CTRADER, **KMS}
    s = load_settings(cfg(env), env)
    assert s.token_kms is not None and s.token_kms.key_id == KMS["XTNL_TOKEN_KMS_KEY_ID"]
    assert not [w for w in s.warnings if "token store" in w]
    plain = load_settings(cfg(CTRADER), CTRADER)
    assert plain.token_kms is None and [w for w in plain.warnings if "token store" in w]
    store: Any = object()
    with_store = make_adapter(s.adapters[0], lambda: None, token_store=store)
    without = make_adapter(s.adapters[0], lambda: None)
    assert isinstance(with_store, PepperstoneAdapter) and isinstance(without, PepperstoneAdapter)
    assert with_store._allow_refresh is True and with_store._tokens._store is store
    assert without._allow_refresh is False and without._tokens._store is None
    built = adapter_factory(s.adapters, lambda: True, token_stores={"pepperstone": store})()
    assert built[0]._tokens._store is store  # type: ignore[attr-defined]
    for a in (with_store, without, *built):
        await a.close()


def test_half_configured_kms_is_refused() -> None:
    env = {**CTRADER, "XTNL_TOKEN_KMS_KEY_ID": "k"}
    with pytest.raises(WiringError, match="XTNL_TOKEN_KMS_KEY_ID and XTNL_KMS_REGION must be set together"):
        load_settings(cfg(env), env)


async def test_build_engine_wires_the_kms_token_store(tmp_path: Path) -> None:
    env = {**CTRADER, **KMS}
    engine = build_engine(cfg(env, tmp_path), environ=env, hooks=EngineHooks(token_cipher=NullCipher()))
    try:
        (adapter,) = engine.context.adapters()
        assert isinstance(adapter, PepperstoneAdapter) and adapter._allow_refresh is True
        guarded = adapter._tokens._store
        assert isinstance(guarded, LeaseGuardedTokenStore) and guarded.min_lease_s == TOKEN_REFRESH_MIN_LEASE_S == 8.0
        store = guarded._inner
        assert isinstance(store, KmsTokenStore) and store._source is engine.control
        assert store._epoch() is engine.keeper.epoch is None        # no lease: begin_refresh refuses
        assert engine.context.credential_generation(adapter) == "token:0"
        await adapter.close()
    finally:
        await engine.aclose()


# --- secrets source -----------------------------------------------------------------------------------------
async def test_settings_read_through_the_secret_and_rotate_per_term(tmp_path: Path) -> None:
    env = {**STONEX, **EMAIL, "STONE_X_PASS": "CHANGE_ME", "XTNL_SMTP_PASSWORD": "CHANGE_ME"}
    source = FakeSource({"STONE_X_PASS": SM_STONEX, "XTNL_SMTP_PASSWORD": SM_SMTP, "BROKER_ENV": "live_readonly",
                         "XTNL_PATRONI_USER": "patroni_api", "XTNL_PATRONI_PASSWORD": PATRONI_SECRET})
    engine = build_engine(cfg(env, tmp_path), environ=env, hooks=EngineHooks(secrets=source))
    try:
        s = engine.settings
        assert s.secrets_source == "aws-sm" and s.secrets_version == "v1" and source.started
        assert s.adapters[0].stonex is not None and s.adapters[0].stonex.password == SM_STONEX
        assert s.adapters[0].stonex.env == "sandbox"               # BROKER_ENV never comes from the secret
        email: Any = s.notifications.email
        assert email.password == SM_SMTP
        assert engine.context.patroni.has_auth
        text = repr(s) + s.describe() + repr(engine.context.patroni)
        assert SM_STONEX not in text and SM_SMTP not in text and PATRONI_SECRET not in text
        (first,) = engine.context.adapters()
        assert isinstance(first, StoneXAdapter)
        assert engine.context.credential_generation(first) == "sm:v1"
        # Rotation: the next leader term builds from the latest cached secret, as a new generation.
        source.values = {**source.values, "STONE_X_PASS": "rotated"}
        source.version_id = "v2"
        (second,) = engine.context.adapters()
        assert second._settings.password == "rotated"                # type: ignore[attr-defined]
        assert engine.context.credential_generation(second) == "sm:v2"
        assert engine.context.credential_generation(first) == "sm:v1"
        for a in (first, second):
            await a.close()
    finally:
        await engine.aclose()
    assert source.closed


async def test_bad_rotated_secret_keeps_the_last_good_settings(tmp_path: Path) -> None:
    env = {**STONEX}
    source = FakeSource({"STONE_X_PASS": SM_STONEX})
    engine = build_engine(cfg(env, tmp_path), environ={k: v for k, v in env.items() if k != "STONE_X_PASS"},
                          hooks=EngineHooks(secrets=source))
    try:
        source.values = {}                                          # STONE_X_PASS gone from the secret
        (adapter,) = engine.context.adapters()
        assert adapter._settings.password == SM_STONEX              # type: ignore[attr-defined]
        await adapter.close()
    finally:
        await engine.aclose()


def test_load_settings_with_a_real_secrets_manager_client() -> None:
    fake = FakeAws()
    fake.secret = {"STONE_X_PASS": SM_STONEX, "XTNL_SMTP_PASSWORD": SM_SMTP}
    source = AwsSecretsManager("xtnl/engine", "eu-west-2", credentials=CountingCredentials(), http=fake.client())
    env = {**STONEX, **EMAIL, "STONE_X_PASS": "CHANGE_ME"}
    s = load_settings(cfg(env), env, secrets=source)
    assert s.adapters[0].stonex is not None and s.adapters[0].stonex.password == SM_STONEX
    email: Any = s.notifications.email
    assert email.password == SM_SMTP and s.secrets_version == "v1"
    assert fake.requests[0].headers["x-amz-target"] == "secretsmanager.GetSecretValue"


def test_unreadable_secret_is_a_wiring_error_naming_no_value() -> None:
    fake = FakeAws()
    fake.fail_next.append((400, "AccessDeniedException"))
    source = AwsSecretsManager("xtnl/engine", "eu-west-2", credentials=CountingCredentials(), http=fake.client())
    with pytest.raises(WiringError, match="secrets \\(aws-sm\\): cannot read the secret.*AccessDeniedException"):
        load_settings(cfg(MOCK), MOCK, secrets=source)


def test_secrets_source_selection_errors_are_wiring_errors(tmp_path: Path) -> None:
    env = {**MOCK, "XTNL_SECRETS_SOURCE": "aws-sm"}
    with pytest.raises(WiringError, match="XTNL_SECRETS_ID, XTNL_SECRETS_REGION"):
        load_settings(cfg(env), env)
    with pytest.raises(WiringError, match="XTNL_SECRETS_ID"):
        build_engine(cfg(env, tmp_path), environ=env)
    assert issubclass(SecretsError, RuntimeError)


def test_env_source_is_the_default_and_unchanged() -> None:
    env = {**STONEX, **CTRADER, **EMAIL}
    s = load_settings(cfg(env), env)
    assert s.secrets_source == "env" and s.secrets_version is None
    assert not [x for x in SECRETS if x in repr(s) + s.describe()]


# --- Patroni REST credentials -------------------------------------------------------------------------------
async def test_patroni_credentials(tmp_path: Path) -> None:
    assert patroni_auth_from_env({}) is None
    assert patroni_auth_from_env({"XTNL_PATRONI_USER": "u", "XTNL_PATRONI_PASSWORD": "p"}) == ("u", "p")
    with pytest.raises(ValueError, match="must be set together"):
        patroni_auth_from_env({"XTNL_PATRONI_USER": "u"})
    env = {**MOCK, "XTNL_PATRONI_PASSWORD": PATRONI_SECRET}
    with pytest.raises(WiringError, match="XTNL_PATRONI_USER and XTNL_PATRONI_PASSWORD") as info:
        load_settings(cfg(env), env)
    assert PATRONI_SECRET not in str(info.value)
    assert [w for w in load_settings(cfg(MOCK), MOCK).warnings if "XTNL_PATRONI_USER" in w]
    env = {**MOCK, "XTNL_PATRONI_USER": "patroni_api", "XTNL_PATRONI_PASSWORD": PATRONI_SECRET}
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(503)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    engine = build_engine(cfg(env, tmp_path), environ=env, hooks=EngineHooks(patroni_client=client))
    try:
        patroni = engine.context.patroni.with_incident_guard(lambda: True)
        resp = await patroni._post("https://10.0.1.10:8008/switchover", {})
        assert resp.status_code == 503
        assert seen[0].headers["authorization"].startswith("Basic ")
        assert PATRONI_SECRET not in repr(engine.context.patroni)
    finally:
        await engine.aclose()
        await client.aclose()


# --- etcd client TLS -----------------------------------------------------------------------------------------
def test_etcd_tls_from_env() -> None:
    assert EtcdTls.from_env({}) == EtcdTls() and EtcdTls().verify is True and EtcdTls().cert is None
    tls = EtcdTls.from_env({"XTNL_ETCD_CA": "/ca.crt", "XTNL_ETCD_CERT": "/c.crt", "XTNL_ETCD_KEY": "/c.key"})
    assert tls.verify == "/ca.crt" and tls.cert == ("/c.crt", "/c.key")
    with pytest.raises(ValueError, match="XTNL_ETCD_CERT and XTNL_ETCD_KEY must be set together"):
        EtcdTls.from_env({"XTNL_ETCD_CERT": "/c.crt"})


class RecordingContext:
    def __init__(self, cafile: str | None) -> None:
        self.cafile = cafile
        self.chain: tuple[str, str] | None = None

    def load_cert_chain(self, certfile: str, keyfile: str) -> None:
        self.chain = (certfile, keyfile)


def test_etcd_client_presents_the_client_certificate(monkeypatch: pytest.MonkeyPatch) -> None:
    made: list[RecordingContext] = []

    def fake_ctx(*, cafile: str | None = None) -> RecordingContext:
        made.append(RecordingContext(cafile))
        return made[-1]

    monkeypatch.setattr(etcd_mod.ssl, "create_default_context", fake_ctx)
    captured: dict[str, Any] = {}
    real_client = httpx.Client

    def spy_client(**kwargs: Any) -> httpx.Client:
        captured.update(kwargs)
        return real_client(transport=httpx.MockTransport(lambda r: httpx.Response(200)))

    monkeypatch.setattr(etcd_mod.httpx, "Client", spy_client)
    client = HttpEtcdClient(["https://10.0.0.1:2379"], verify="/etc/xtnl/pki/etcd-ca.crt",
                            cert=("/etc/xtnl/pki/etcd-client.crt", "/etc/xtnl/pki/etcd-client.key"))
    try:
        ctx = made[0]                      # (httpx itself may build more with the patched factory)
        assert ctx.cafile == "/etc/xtnl/pki/etcd-ca.crt"
        assert ctx.chain == ("/etc/xtnl/pki/etcd-client.crt", "/etc/xtnl/pki/etcd-client.key")
        assert captured["verify"] is ctx and client.client_cert
    finally:
        client.close()


def test_etcd_tls_context_variants(tmp_path: Path) -> None:
    assert tls_context(True) is True and tls_context(False) is False
    ctx = ssl.create_default_context()
    assert tls_context(ctx) is ctx
    with pytest.raises(ValueError, match="CA bundle .* cannot be loaded"):
        tls_context(str(tmp_path / "missing-ca.crt"))
    with pytest.raises(ValueError, match="client certificate .* cannot be loaded"):
        tls_context(True, (str(tmp_path / "missing.crt"), str(tmp_path / "missing.key")))
    with pytest.raises(ValueError, match="verify=False"):
        tls_context(False, ("/c.crt", "/c.key"))


async def test_build_engine_refuses_unloadable_etcd_tls(tmp_path: Path) -> None:
    env = {**MOCK, "XTNL_ETCD_CA": str(tmp_path / "no-ca.crt")}
    with pytest.raises(WiringError, match="etcd CA bundle"):
        build_engine(cfg(env, tmp_path), environ=env)
    env = {**MOCK, "XTNL_ETCD_KEY": "/k"}
    with pytest.raises(WiringError, match="XTNL_ETCD_CERT and XTNL_ETCD_KEY"):
        build_engine(cfg(env, tmp_path), environ=env)
    assert "XTNL_ETCD_CERT" in str([w for w in load_settings(
        cfg({**MOCK, "XTNL_ETCD_CERT": "/c", "XTNL_ETCD_KEY": "/k"}),
        {**MOCK, "XTNL_ETCD_CERT": "/c", "XTNL_ETCD_KEY": "/k"}).warnings])   # http:// endpoints in BASE
    assert BASE["XTNL_ETCD_ENDPOINTS"].startswith("http://")
