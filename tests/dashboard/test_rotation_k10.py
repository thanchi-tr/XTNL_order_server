"""K10 (operations.md §13 "rotation without restart"): the dashboard's heartbeat listener token (the engine's
XTNL_HEARTBEAT_COPY_TOKEN copy) rotates through heartbeat.json with a next token and an overlap window, its TLS
files re-load in place, and its PostgreSQL / Redis credentials follow the secrets source without a restart."""
from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Any

import pytest

from src.dashboard import heartbeat as H
from src.dashboard import rotation as R
from src.dashboard.server import DashboardSettings
from src.dashboard.views import DashboardViews
from tests.certutil import make_cert
from tests.db.conftest import FakeConn, FakeSource

OLD = "old-heartbeat-token-0123456789"
NEW = "new-heartbeat-token-0123456789"


class Clock:
    def __init__(self) -> None:
        self.ms = 1_000_000

    def __call__(self) -> int:
        return self.ms


def _write(path: Path, **doc: Any) -> None:
    path.write_text(json.dumps({"listen": "127.0.0.1:8444", **doc}), encoding="utf-8")


def _auth(token: str) -> str:
    return f"Bearer {token}"


async def test_heartbeat_token_rotates_through_the_file_with_next_and_overlap(tmp_path: Path) -> None:
    f = tmp_path / "heartbeat.json"
    _write(f, token=OLD)
    settings = H.HeartbeatListenSettings.from_file(f)
    assert settings is not None
    clock = Clock()
    listener = H.HeartbeatListener(settings, H.HeartbeatBoard(), settings_file=f, clock_ms=clock)
    assert listener.authorized(_auth(OLD)) and not listener.authorized(_auth(NEW))
    _write(f, token=OLD, token_next=NEW)                     # step 1: the next token beside the current one
    await listener.reload_once()
    assert listener.authorized(_auth(OLD)) and listener.authorized(_auth(NEW))
    _write(f, token=NEW)                                     # step 3: promoted; the old one in its overlap window
    await listener.reload_once()
    assert listener.authorized(_auth(NEW)) and listener.authorized(_auth(OLD))
    clock.ms += settings.token_overlap_s * 1000 + 1
    await listener.reload_once()
    assert listener.authorized(_auth(NEW)) and not listener.authorized(_auth(OLD))


async def test_token_overlap_follows_the_file_and_restart_only_keys_warn(
        tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """K10 review: a changed token_overlap_s applies from the next re-read (a token replaced after it gets the new
    window); a changed restart-only key (listen, path, max_age_ms, tls paths) is warned about once, by key name."""
    f = tmp_path / "heartbeat.json"
    _write(f, token=OLD)
    settings = H.HeartbeatListenSettings.from_file(f)
    assert settings is not None and settings.token_overlap_s == 1800
    clock = Clock()
    listener = H.HeartbeatListener(settings, H.HeartbeatBoard(), settings_file=f, clock_ms=clock)
    _write(f, token=OLD, token_overlap_s=600)
    await listener.reload_once()
    assert listener.tokens.overlap_ms == 600_000 and listener.restart_pending == []
    _write(f, token=NEW, token_overlap_s=600)                # replaced: the old token gets the new 600 s window
    await listener.reload_once()
    assert listener.authorized(_auth(OLD))
    clock.ms += 600_000 + 1
    await listener.reload_once()
    assert not listener.authorized(_auth(OLD)) and listener.authorized(_auth(NEW))
    caplog.set_level("WARNING", logger=H.__name__)
    f.write_text(json.dumps({"listen": "127.0.0.1:9444", "token": NEW, "token_overlap_s": 600, "max_age_ms": 120_000,
                             "path": "/other"}), encoding="utf-8")
    await listener.reload_once()
    await listener.reload_once()
    assert listener.restart_pending == ["listen", "max_age_ms", "path"]
    warned = [r.getMessage() for r in caplog.records if "restart" in r.getMessage()]
    assert len(warned) == 1 and "listen, max_age_ms, path" in warned[0] and NEW not in warned[0]
    _write(f, token=NEW, token_overlap_s=600)                # back to the running values: nothing pending
    await listener.reload_once()
    assert listener.restart_pending == []


async def test_an_invalid_or_missing_file_keeps_the_last_valid_tokens(tmp_path: Path) -> None:
    f = tmp_path / "heartbeat.json"
    _write(f, token=OLD)
    settings = H.HeartbeatListenSettings.from_file(f)
    assert settings is not None
    listener = H.HeartbeatListener(settings, H.HeartbeatBoard(), settings_file=f, clock_ms=Clock())
    _write(f, token="short")
    await listener.reload_once()
    assert listener.authorized(_auth(OLD)) and listener.tokens.issue is not None
    f.unlink()
    await listener.reload_once()
    assert listener.authorized(_auth(OLD)) and listener.tokens.issue is not None
    _write(f, token=OLD)
    await listener.reload_once()
    assert listener.tokens.issue is None


def test_settings_validate_the_new_keys() -> None:
    with pytest.raises(H.HeartbeatConfigError, match="token_next"):
        H.HeartbeatListenSettings.parse({"listen": "127.0.0.1:8444", "token": OLD, "token_next": "short"})
    with pytest.raises(H.HeartbeatConfigError, match="token_overlap_s"):
        H.HeartbeatListenSettings.parse({"listen": "127.0.0.1:8444", "token": OLD, "token_overlap_s": 5})
    s = H.HeartbeatListenSettings.parse({"listen": "127.0.0.1:8444", "token": OLD, "token_next": OLD})
    assert s.token_next is None                               # the same token twice is no next token


async def test_unknown_token_asks_for_an_early_reread(tmp_path: Path) -> None:
    f = tmp_path / "heartbeat.json"
    _write(f, token=OLD)
    settings = H.HeartbeatListenSettings.from_file(f)
    assert settings is not None
    listener = H.HeartbeatListener(settings, H.HeartbeatBoard(), settings_file=f, clock_ms=Clock())
    stop = asyncio.Event()
    task = asyncio.create_task(listener.watch(stop, every_s=3600))
    await asyncio.sleep(0.05)
    _write(f, token=NEW)
    assert not listener.authorized(_auth(NEW))               # unknown: asks the watcher to re-read now
    for _ in range(100):
        await asyncio.sleep(0.02)
        if listener.authorized(_auth(NEW)):
            break
    stop.set()
    await asyncio.wait_for(task, 2)
    assert listener.authorized(_auth(NEW))


async def test_heartbeat_tls_files_reload_in_place(tmp_path: Path) -> None:
    crt, key = make_cert(tmp_path, "hb", days=5)
    f = tmp_path / "heartbeat.json"
    _write(f, token=OLD, tls_cert=str(crt), tls_key=str(key))
    settings = H.HeartbeatListenSettings.from_file(f)
    assert settings is not None
    listener = H.HeartbeatListener(settings, H.HeartbeatBoard(), settings_file=f, clock_ms=Clock())
    ctx = listener.ssl_context()
    assert ctx is not None
    new_crt, new_key = make_cert(tmp_path, "hb2", days=400)
    crt.write_bytes(new_crt.read_bytes())
    key.write_bytes(new_key.read_bytes())
    await listener.reload_once()
    assert listener.ssl_context() is ctx and listener._tls is not None and listener._tls.reloads == 1


class FakePool:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.closed = False
        self.conn = FakeConn()

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        return FakeSource(self.conn).acquire()

    async def close(self) -> None:
        self.closed = True


class FakeRedis:
    def __init__(self, url: str) -> None:
        self.url = url
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


async def test_credentials_follow_the_secrets_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(R, "CLOSE_GRACE_S", 0.0)
    pools: list[FakePool] = []

    def make(dsn: str) -> FakePool:
        pools.append(FakePool(dsn))
        return pools[-1]
    env = {"XTNL_DASHBOARD_DSN": "postgresql://xtnl_dashboard:one@h/xtnl",
           "XTNL_DASHBOARD_REDIS_URLS": "node-a=rediss://u:one@a:6380/0"}
    settings = DashboardSettings.from_env(env)
    source = R.RotatingPgSource(settings.dsn, make)
    views = DashboardViews(source, {})

    def clients(urls: Mapping[str, str]) -> dict[str, Any]:
        return {k: FakeRedis(v) for k, v in urls.items()}
    first = clients(settings.redis_urls)
    views.replace_redis(first)
    secret = dict(env)
    watcher = R.CredentialWatcher(lambda: secret, settings, source, views, clients)
    await watcher.check_once()
    assert len(pools) == 1 and source.swaps == 0                   # unchanged: nothing rebuilt
    secret["XTNL_DASHBOARD_DSN"] = "postgresql://xtnl_dashboard:two@h/xtnl"
    secret["XTNL_DASHBOARD_REDIS_URLS"] = "node-a=rediss://u:two@a:6380/0"
    await watcher.check_once()
    await asyncio.sleep(0.01)
    assert [p.dsn for p in pools] == [env["XTNL_DASHBOARD_DSN"], secret["XTNL_DASHBOARD_DSN"]]
    assert pools[0].closed and not pools[1].closed and source.swaps == 1
    async with source.acquire() as conn:
        assert conn is pools[1].conn                               # new requests use the new pool
    assert all(c.closed for c in first.values())
    assert [c.url for c in views._redis.values()] == ["rediss://u:two@a:6380/0"]
    secret["XTNL_DASHBOARD_DSN"] = ""                              # a broken secret keeps the current credentials
    await watcher.check_once()
    assert watcher.issue is not None and len(pools) == 2
    secret["XTNL_DASHBOARD_DSN"] = "postgresql://xtnl_dashboard:two@h/xtnl"
    await watcher.check_once()
    assert watcher.issue is None and len(pools) == 2
    await source.close()
    assert pools[1].closed


def test_env_source_has_no_credential_watcher() -> None:
    from src.dashboard.__main__ import credential_loader
    from src.secrets.source import EnvSecrets
    assert credential_loader(EnvSecrets(), {}) is None

