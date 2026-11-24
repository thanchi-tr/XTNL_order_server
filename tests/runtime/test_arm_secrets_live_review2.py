"""Live review round 2 (docs/design/live-review-2026-11-25.md): the optional services follow the secrets source.

operations.md §13: on AWS the `XTNL_MD_*` secrets, `XTNL_ARM_CONFIG_DSN` and the `XTNL_ARM_*` channel credentials come
from the secrets source, with rotation without a restart; oversee-rules.md §10: channel credentials are refreshed like
other secrets and re-read on any auth error; plan S29: the ARM channel credentials follow a `ChannelSource`.
Before: `build_engine` handed the raw process environment to the optional services, so on AWS the ARM saw its channel
credentials as missing ('ARM not started') and a rotated ARM credential needed a restart. Synthetic values only; no
assertion message carries a value.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.notifications.factory import ARM_CHANNEL_NAMES, ChannelSet
from src.notifications.rotation import RotatingChannels
from src.runtime.optional import OptionalBootstrapRunner, OptionalDeps
from src.runtime.optional_plan import default_arm_channels, default_config_source, production_plan
from src.runtime.optional_settings import MD_ENABLED_VAR, OptionalFlags, arm_channel_problems, resolved_environ
from src.runtime.wiring import EngineHooks, build_engine
from src.secrets.source import EnvSecrets
from tests.runtime.fakes import FakeClock as EtcdClock
from tests.runtime.fakes import FakeEtcd
from tests.runtime.test_optional_bootstrap import ARM_CHANNELS, MD_ENV
from tests.runtime.test_optional_plan import (  # noqa: F401  (fixtures)
    HangPools,
    Pages,
    deps,
    make_session,
    md_redis,
    redis,
    running,
    runtime_of,
    until,
)
from tests.runtime.test_wiring import MOCK, cfg
from tests.runtime.test_wiring_credentials import FakeSource

pytestmark = pytest.mark.usefixtures("stop_at_entry_on")

SECRET_KEYS = ("XTNL_ARM_PUSH_TOKEN", "XTNL_ARM_SMTP_USERNAME", "XTNL_ARM_SMTP_PASSWORD")
#: The non-secret ARM channel settings in the environment; the credentials only in the secret (AWS).
ARM_PLAIN = {k: v for k, v in ARM_CHANNELS.items() if k not in SECRET_KEYS}
SECRET = {"XTNL_ARM_PUSH_TOKEN": "sm-push-1", "XTNL_ARM_SMTP_USERNAME": "arm-user",
          "XTNL_ARM_SMTP_PASSWORD": "sm-smtp-1"}


class CountingSource(FakeSource):
    def __init__(self, values: dict[str, str]) -> None:
        super().__init__(values)
        self.invalidations = 0

    def invalidate(self) -> None:
        self.invalidations += 1


def _session_stub(closers: list[str]) -> Any:
    def register(name: str, *_fns: Any) -> bool:
        closers.append(name)
        return True
    return SimpleNamespace(http_client=None, register_closer=register, email_footer=lambda: ("node-a", "c:7"),
                           ctx=SimpleNamespace(http_client=lambda: SimpleNamespace(aclose=None), smtp_factory=None))


# ----------------------------------------------------------------------------------------- unit: the factories


def test_arm_channels_rotate_with_the_secret_and_an_auth_error_invalidates() -> None:
    source = CountingSource(dict(SECRET))
    built = default_arm_channels(ARM_PLAIN, source)(_session_stub([]))
    assert isinstance(built, RotatingChannels)
    assert {c.name for c in built.current.channels} == set(ARM_CHANNEL_NAMES)
    assert built.poll() is None                                   # unchanged secret: nothing rebuilt
    source.values = {**SECRET, "XTNL_ARM_SMTP_PASSWORD": "sm-smtp-2"}   # rotated in Secrets Manager
    fresh = built.poll()
    assert fresh is not None and built.rebuilds == 1
    assert {c.name for c in fresh.channels} == set(ARM_CHANNEL_NAMES)
    built.invalidate()                                            # the relay's auth-error path
    assert source.invalidations == 1


def test_env_source_keeps_the_fixed_channel_set() -> None:
    env = {**ARM_PLAIN, **SECRET}
    for secrets in (None, EnvSecrets()):
        built = default_arm_channels(env, secrets)(_session_stub([]))
        assert isinstance(built, ChannelSet) and {c.name for c in built.channels} == set(ARM_CHANNEL_NAMES)


def test_channel_problems_read_the_secret_laid_over_the_environment() -> None:
    source = CountingSource(dict(SECRET))
    assert any("XTNL_ARM_PUSH_TOKEN" in p for p in arm_channel_problems(ARM_PLAIN))     # the raw environment
    assert arm_channel_problems(resolved_environ(source, ARM_PLAIN)) == []
    d = OptionalDeps(environ=ARM_PLAIN, repo_root=Path("."), spool_dir=Path("."), secrets=source)
    assert arm_channel_problems(d.resolved_environ()) == []
    plain = OptionalDeps(environ=ARM_PLAIN, repo_root=Path("."), spool_dir=Path("."))
    assert plain.resolved_environ() is ARM_PLAIN                  # no source: the environment as is


def test_config_dsn_is_read_through_the_secrets_source() -> None:
    from src.armconfig.client import DSN_VAR

    session: Any = SimpleNamespace()
    assert default_config_source({})(session) is None
    got = default_config_source({}, CountingSource({DSN_VAR: "postgresql://u:pw@db.example.invalid/x"}))(session)
    assert isinstance(got, list) and got                          # the DSN is seen (its CA file is missing)
    assert not any("pw@" in p or "example.invalid" in p for p in got)


# ------------------------------------------------------------------------------ the ARM graph on an AWS source


async def test_aws_source_starts_the_arm_with_rotating_channels(tmp_path: Path, redis: Any, md_redis: Any) -> None:
    source = CountingSource({**SECRET, "XTNL_MD_HMAC_KEY": MD_ENV["XTNL_MD_HMAC_KEY"]})
    env = {**{k: v for k, v in MD_ENV.items() if k != "XTNL_MD_HMAC_KEY"}, **ARM_PLAIN}
    pages = Pages()
    s = make_session(tmp_path, redis, flags=OptionalFlags(md_enabled=True, arm_enabled=True), pages=pages)
    runner = OptionalBootstrapRunner(s, deps(tmp_path, md_redis, env, arm_pool=HangPools(), secrets=source,
                                             plan=production_plan(env, secrets=source)))
    async with running(runner):
        await until(lambda: runner.arm_started)
        assert "ARM not started" not in pages.titles()
        graph = runtime_of(runner).graph
        assert graph is not None
        relay = graph.relay
        rc = relay._source  # noqa: SLF001
        assert isinstance(rc, RotatingChannels)
        assert set(relay.status()) == set(ARM_CHANNEL_NAMES)
        source.values = {**source.values, "XTNL_ARM_SMTP_PASSWORD": "sm-smtp-2"}   # rotated, no restart
        relay._poll_source(10**13)  # noqa: SLF001
        assert rc.rebuilds == 1
        relay._auth_failed(10**13)  # noqa: SLF001   a rejected credential re-reads the secret at once
        assert source.invalidations == 1


async def test_build_engine_hands_the_secrets_source_to_the_optional_services(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.runtime.optional as optional_mod

    seen: list[OptionalDeps] = []
    real_make = optional_mod.make_optional_bootstrap

    def spy_make(d: OptionalDeps) -> Any:
        seen.append(d)
        return real_make(d)

    monkeypatch.setattr(optional_mod, "make_optional_bootstrap", spy_make)
    on = {**MOCK, MD_ENABLED_VAR: "1"}
    source = CountingSource({})
    engine = build_engine(cfg(on, tmp_path), environ=on,
                          hooks=EngineHooks(etcd=FakeEtcd(EtcdClock()), secrets=source, require_keeper_thread=False))
    try:
        assert len(seen) == 1 and seen[0].secrets is source
    finally:
        await engine.aclose()
