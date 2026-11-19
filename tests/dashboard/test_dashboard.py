"""Melbourne read-only dashboard (operations.md §9; IMPLEMENTATION_PLAN 5.6). No network beyond a loopback socket
the test opens itself: FakeConn for PostgreSQL, fakeredis for Redis."""
from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import fakeredis
import pytest

from src.cache import keys as K
from src.dashboard import heartbeat as H
from src.dashboard import server as S
from src.dashboard import views as V
from src.observability.http import Request, bound_port, start_server
from tests.db.conftest import FakeConn, FakeSource

NOW = 1_800_000_000_000


def beats(epoch: str | None = "42", *, at: int = NOW - 5_000, node: str = "node-a") -> H.HeartbeatBoard:
    """A heartbeat board holding one leader heartbeat received at `at`."""
    clock = [at]
    board = H.HeartbeatBoard(clock_ms=lambda: clock[0])
    assert board.offer(node, epoch, at - 100)
    return board


def scripted(term_epoch: int = 42, engine_epoch: int = 42) -> FakeConn:
    conn = FakeConn()
    conn.returns(V.SQL_EPOCH, ("cid", engine_epoch, "node-a", NOW - 60_000))
    conn.returns_all(V.SQL_TERMS, [(term_epoch, "node-a", NOW - 70_000, NOW - 50_000, None, None),
                                   (41, "node-b", NOW - 900_000, NOW - 890_000, NOW - 80_000, "lost")])
    conn.returns(V.SQL_CLUSTER, (False, None, None))
    return conn


def test_every_statement_is_a_bounded_select() -> None:
    for sql in V.ALL_SQL:
        assert sql.lstrip().upper().startswith("SELECT"), sql
        assert re.search(r"\bLIMIT \d+", sql) or "WHERE id = 1" in sql, sql
        for word in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER", "DROP", "CREATE", "GRANT", "SET"):
            assert not re.search(rf"{word}", sql.upper()), (word, sql)


def test_every_dashboard_session_is_read_only() -> None:
    assert S.READ_ONLY_SETTINGS["default_transaction_read_only"] == "on"
    assert S.READ_ONLY_SETTINGS["application_name"] == "xtnl-dashboard"


async def test_live_values_only_from_the_redis_with_the_leaders_epoch_and_only_when_fresh() -> None:
    fresh_r, old_r = fakeredis.FakeAsyncRedis(), fakeredis.FakeAsyncRedis()
    try:
        await old_r.set(K.BOOT_ID, "run-x:41")                 # an ex-leader's Redis
        await old_r.hset(K.price_key("pepperstone", "EURUSD"), mapping={"bid": "9", "ask": "9", "ts": NOW})
        await fresh_r.set(K.BOOT_ID, "run-y:42")
        await fresh_r.hset(K.price_key("pepperstone", "EURUSD"), mapping={"bid": "1.1", "ask": "1.2", "ts": NOW - 1_000})
        await fresh_r.hset(K.price_key("pepperstone", "XAUUSD"), mapping={"bid": "2000", "ask": "2001",
                                                                          "ts": NOW - 120_000})
        await fresh_r.hset(K.account_live_key("ACC"), mapping={"adapter_id": "pepperstone", "currency": "USD",
                                                               "equity": "1000", "ts": NOW - 500})
        conn = scripted()
        conn.returns_all(V.SQL_OPEN_SYMBOLS, [("pepperstone", "EURUSD"), ("pepperstone", "XAUUSD")])
        views = V.DashboardViews(FakeSource(conn), {"node-b": old_r, "node-a": fresh_r}, fresh_ms=30_000,
                                 heartbeats=beats("42"))
        term = await views.leader()
        assert term.live_epoch == 42 and term.node == "node-a"
        live = await views.live(term, NOW, ["ACC"])
        assert live["source"] == "node-a" and not live["stale"]
        eur, xau = live["prices"]
        assert eur["bid"] == "1.1" and eur["age_ms"] == 1_000 and eur["fresh"]
        assert xau["fresh"] is False and "bid" not in xau     # stale: age shown, value hidden
        assert live["accounts"][0]["equity"] == "1000"

        conn2 = scripted(term_epoch=43, engine_epoch=43)        # a new leader, its Redis not rebuilt yet
        views2 = V.DashboardViews(FakeSource(conn2), {"node-b": old_r, "node-a": fresh_r}, heartbeats=beats("43"))
        live2 = await views2.live(await views2.leader(), NOW)
        assert live2["stale"] and live2["prices"] == [] and "boot epoch 42" in live2["reason"]

        conn3 = scripted(term_epoch=42, engine_epoch=43)        # engine_epoch moved on: no current term
        views3 = V.DashboardViews(FakeSource(conn3), {"node-a": fresh_r}, heartbeats=beats("42"))
        live3 = await views3.live(await views3.leader(), NOW)
        assert live3["stale"] and live3["epoch"] is None
    finally:
        await fresh_r.aclose()
        await old_r.aclose()


async def test_views_map_rows_and_decimals() -> None:
    from decimal import Decimal
    conn = FakeConn()
    conn.returns_all(V.SQL_ACCOUNTS, [("pepperstone", "ACC", "USD", Decimal("1000.50"), None, 7, NOW)])
    conn.returns_all(V.SQL_DLQ, [("QUEUED", 3)])
    conn.returns(V.SQL_HALT, (5, "SOFT", "SOFT", False, "r1", "why", "system", NOW, False))
    views = V.DashboardViews(FakeSource(conn))
    assert (await views.accounts())[0]["balance"] == "1000.50"
    assert await views.dlq() == {"QUEUED": 3}
    halt = await views.halt()
    assert halt is not None and halt["effective_level"] == "SOFT" and halt["halt_event_id"] == 5


def summary_conn() -> FakeConn:
    conn = scripted()
    conn.returns_all(V.SQL_ACCOUNTS, [("pepperstone", "ACC", "USD", "1", None, 7, NOW)])
    conn.returns_all(V.SQL_POSITIONS, [("pepperstone", "P<script>", "ACC", "EURUSD", "BUY", "1", "1.1", None, None,
                                        "OPEN", "0", "0", 9, NOW)])
    conn.returns(V.SQL_HALT, (5, "NONE", "NONE", False, None, "clear", "system", NOW, False))
    return conn


async def test_page_and_json_endpoints_and_no_write_method() -> None:
    app = S.DashboardApp(V.DashboardViews(FakeSource(summary_conn())), clock_ms=lambda: NOW)
    page = await app.handle(Request("GET", "/"))
    assert page.status == 200 and page.content_type.startswith("text/html")
    body = page.body.decode()
    assert "P&lt;script&gt;" in body and "<script>" not in body              # every value escaped
    assert "Live values stale" in body                                       # no Redis configured
    app2 = S.DashboardApp(V.DashboardViews(FakeSource(summary_conn())), clock_ms=lambda: NOW)
    api = await app2.handle(Request("GET", "/api/summary"))
    data = json.loads(api.body)
    assert data["leader"]["term_epoch"] == 42 and data["halt"]["effective_level"] == "NONE"
    assert (await app2.handle(Request("GET", "/api/nope"))).status == 404
    assert (await app2.handle(Request("GET", "/healthz"))).status == 200

    server = await start_server(app2.handle, "127.0.0.1", 0)
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", bound_port(server))
        writer.write(b"POST /api/summary HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
        await writer.drain()
        resp = await reader.read()
        writer.close()
        assert resp.startswith(b"HTTP/1.1 405")
    finally:
        server.close()
        await server.wait_closed()


async def test_a_database_outage_answers_503_not_a_crash() -> None:
    class Down:
        def acquire(self) -> Any:
            raise ConnectionError("pg down")
    app = S.DashboardApp(V.DashboardViews(Down()))  # type: ignore[arg-type]
    assert (await app.handle(Request("GET", "/api/orders"))).status == 503


def test_settings_from_env() -> None:
    with pytest.raises(S.DashboardConfigError, match="XTNL_DASHBOARD_DSN"):
        S.DashboardSettings.from_env({})
    s = S.DashboardSettings.from_env({"XTNL_DASHBOARD_DSN": "postgresql://d:pw@h/x",
                                      "XTNL_DASHBOARD_REDIS_URLS": "node-a=redis://a:6379/0, redis://b:6379/0"})
    assert dict(s.redis_urls) == {"node-a": "redis://a:6379/0", "redis2": "redis://b:6379/0"}
    assert s.addr == "127.0.0.1:8088" and s.fresh_ms == 30_000
    assert "pw" not in repr(s)
    with pytest.raises(S.DashboardConfigError):
        S.DashboardSettings.from_env({"XTNL_DASHBOARD_DSN": "x", "XTNL_DASHBOARD_FRESH_MS": "abc"})


def test_main_check_config_exits_78_without_a_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.dashboard.__main__ as entry
    from src.dashboard.__main__ import main
    monkeypatch.setattr(entry, "configure_logging", lambda *a, **k: "text")   # leave the test's root logger alone
    monkeypatch.delenv("XTNL_DASHBOARD_DSN", raising=False)
    assert main(["--check-config"]) == 78
    monkeypatch.setenv("XTNL_DASHBOARD_DSN", "postgresql://d:pw@h/x")
    assert main(["--check-config"]) == 0


class _RoleConn:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self.row = row
        self.sql: list[str] = []

    async def fetchrow(self, sql: str, *args: object) -> Any:
        self.sql.append(sql)
        return self.row


async def test_a_writable_or_superuser_role_is_refused_at_connect() -> None:
    """Before the fix: any DSN was accepted; a dashboard pointed at the engine's (table-owning) role or a
    superuser ran with only the session's read-only default between it and a write."""
    ok = _RoleConn(("xtnl_dashboard", False, "on", 0))
    await S.verify_read_only_connection(ok)
    assert ok.sql == [S.SQL_ROLE_CHECK]
    for row, why in ((("xtnl_engine", False, "on", 31), "can write 31 table"),
                     (("postgres", True, "on", 0), "superuser"),
                     (("xtnl_dashboard", False, "off", 0), "default_transaction_read_only")):
        with pytest.raises(S.DashboardRoleError, match=why):
            await S.verify_read_only_connection(_RoleConn(row))
    with pytest.raises(S.DashboardRoleError):
        await S.verify_read_only_connection(_RoleConn(None))
    sql = S.SQL_ROLE_CHECK.upper()
    assert sql.startswith("SELECT")
    for word in ("INSERT INTO", "UPDATE ", "DELETE FROM", "TRUNCATE ", "ALTER ", "DROP ", "CREATE ", "GRANT "):
        assert word not in sql, word


async def test_the_pool_runs_the_role_check_on_every_new_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    class FakeRaw:
        async def fetchrow(self, sql: str, *args: object) -> tuple[bool, int]:
            seen["schema_read"] = sql             # S38f: the 0015 schema detection at pool creation
            return (True, 6)

    class FakePool:
        @asynccontextmanager
        async def acquire(self) -> AsyncIterator[FakeRaw]:
            yield FakeRaw()

    class FakeAsyncpg:
        @staticmethod
        async def create_pool(dsn: str, **kw: Any) -> object:
            seen.update(kw)
            return FakePool()

    real_import = S.importlib.import_module
    monkeypatch.setattr(S.importlib, "import_module",
                        lambda name: FakeAsyncpg if name == "asyncpg" else real_import(name))
    await S.create_read_only_source("postgresql://u@h/db")
    assert seen["init"] is S.verify_read_only_connection
    assert "pg_attribute" in seen["schema_read"]
    assert seen["server_settings"]["default_transaction_read_only"] == "on"
