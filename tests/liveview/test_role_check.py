"""Database access and the role checks (docs/design/live-view.md s4.2, s6 "role check"): only the view-only
`xtnl_liveview` passes; a dashboard / API / engine / superuser / pg_read_all_data role is refused (exit 78, naming
XTNL_LIVEVIEW_DSN, never a value); a login refusal (28000, 28P01, 3D000) is a configuration error at start and on a
later reconnect, while a socket failure, 57P03 or 53300 reports `db: down` and retries."""
from __future__ import annotations

import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from src.db.readonly_role import (
    SQL_PREDEFINED_MEMBERSHIP,
    SQL_READABLE_TABLES,
    SQL_ROLE_CHECK,
    SQL_VIEW_SELECT,
    ReadOnlyRoleError,
    read_only_problems,
    verify_read_only_connection,
    verify_view_only_role,
)
from src.liveview import sql as Q
from src.liveview.probe import EngineProbe
from src.liveview.settings import DSN_VAR, LiveViewSettings

ROOT = Path(__file__).resolve().parents[2]
SECRET = "Sup3rS3cretPw"
DSN = f"postgresql://xtnl_liveview:{SECRET}@127.0.0.1:55433/xtnl"
VIEWS = sorted(Q.ALL_VIEWS)


class RoleConn:
    """Answers the role-check statements from a description of the role."""

    def __init__(self, *, user: str = "xtnl_liveview", superuser: bool = False, writable: int = 0,
                 read_only: str = "on", read_all: bool = False, write_all: bool = False, tables: int = 0,
                 missing: frozenset[str] = frozenset()) -> None:
        self.user, self.superuser, self.writable, self.read_only = user, superuser, writable, read_only
        self.read_all, self.write_all, self.tables, self.missing = read_all, write_all, tables, missing
        self.calls: list[str] = []

    async def fetchrow(self, sql: str, *args: Any) -> tuple[Any, ...] | None:
        self.calls.append(sql)
        if sql == SQL_ROLE_CHECK:
            return (self.user, self.superuser, self.read_only, self.writable)
        if sql == SQL_PREDEFINED_MEMBERSHIP:
            return (self.user, self.read_all, self.write_all)
        raise AssertionError(sql)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self.calls.append(sql)
        if sql == SQL_READABLE_TABLES:
            return self.tables
        if sql == SQL_VIEW_SELECT:
            return args[0] not in self.missing
        raise AssertionError(sql)


async def check(conn: RoleConn) -> None:
    await verify_read_only_connection(conn, dsn_var=DSN_VAR, role_hint="xtnl_liveview")
    await verify_view_only_role(conn, views=VIEWS, role_name="xtnl_liveview", dsn_var=DSN_VAR)


async def test_the_liveview_role_passes() -> None:
    conn = RoleConn()
    await check(conn)
    assert conn.calls.count(SQL_VIEW_SELECT) == len(VIEWS) == 18


@pytest.mark.parametrize(("role", "words"), [
    (dict(user="xtnl_dashboard", tables=12), ["is not xtnl_liveview", "can read 12 table(s)"]),
    (dict(user="xtnl_api", tables=3, missing=frozenset(VIEWS)), ["is not xtnl_liveview", "cannot read 18 view(s)"]),
    (dict(user="xtnl", writable=40, read_only="off"), ["can write 40 table(s)", "default_transaction_read_only"]),
    (dict(user="postgres", superuser=True), ["superuser"]),
    (dict(read_all=True), ["pg_read_all_data"]),
    (dict(write_all=True), ["pg_read_all_data or pg_write_all_data"]),
    (dict(tables=1), ["can read 1 table(s)"]),
    (dict(missing=frozenset({"lv_alerts"})), ["cannot read 1 view(s)"]),
    (dict(read_only="off"), ["default_transaction_read_only"]),
])
async def test_other_roles_are_refused_naming_the_variable(role: dict[str, Any], words: list[str]) -> None:
    with pytest.raises(ReadOnlyRoleError) as ei:
        await check(RoleConn(**role))
    msg = str(ei.value)
    assert DSN_VAR in msg
    for w in words:
        assert w in msg, (w, msg)
    assert SECRET not in msg


async def test_a_missing_role_row_is_refused() -> None:
    assert read_only_problems(None) == ["role not found"]

    class NoRow(RoleConn):
        async def fetchrow(self, sql: str, *args: Any) -> tuple[Any, ...] | None:
            return None

    with pytest.raises(ReadOnlyRoleError, match=DSN_VAR):
        await verify_view_only_role(NoRow(), views=VIEWS, role_name="xtnl_liveview", dsn_var=DSN_VAR)


def test_the_role_check_counts_view_write_grants_too() -> None:
    assert "relkind IN ('r','p','v','m')" in SQL_ROLE_CHECK
    assert "relkind IN ('r','p')" in SQL_READABLE_TABLES
    assert "has_any_column_privilege" in SQL_READABLE_TABLES


def test_readonly_role_imports_only_the_standard_library() -> None:
    import ast
    tree = ast.parse((ROOT / "src" / "db" / "readonly_role.py").read_text(encoding="utf-8"))
    mods = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)} | {
        a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert all(m in ("__future__", "collections.abc", "typing") or (m or "").startswith("src.db.pg") for m in mods)


# ---- the pool ----------------------------------------------------------------------------------------------------------
async def test_create_pool_settings_and_init_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.liveview.__main__ as M
    captured: dict[str, Any] = {}

    class FakeAsyncpg:
        @staticmethod
        async def create_pool(dsn: str, **kw: Any) -> str:
            captured.update(kw, dsn=dsn)
            return "pool"

    monkeypatch.setattr(M.importlib, "import_module", lambda name: FakeAsyncpg if name == "asyncpg" else None)
    seen: list[str] = []

    async def fake_init_connection(conn: Any) -> None:
        seen.append("init_connection")

    monkeypatch.setattr(M, "init_connection", fake_init_connection)
    assert await M.create_pool(DSN) == "pool"
    assert (captured["min_size"], captured["max_size"]) == (1, 2)
    assert captured["server_settings"] == {"application_name": "xtnl-liveview",
                                           "default_transaction_read_only": "on", "statement_timeout": "5000"}
    init = captured["init"]
    await init(RoleConn())
    assert seen == ["init_connection"]
    for bad in (RoleConn(user="xtnl_dashboard"), RoleConn(superuser=True), RoleConn(read_all=True)):
        with pytest.raises(ReadOnlyRoleError):
            await init(bad)


# ---- start-up and reconnect ------------------------------------------------------------------------------------------
class PgError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(f"FATAL: password authentication failed for {DSN}")
        self.sqlstate = code


class FailingPool:
    def __init__(self, exc: BaseException) -> None:
        self.exc = exc
        self.closed = False

    @asynccontextmanager
    async def _acq(self) -> AsyncIterator[Any]:
        raise self.exc
        yield None  # pragma: no cover

    def acquire(self) -> Any:
        return self._acq()

    async def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.closed = True


def make_lv(pool_factory: Any) -> Any:
    import src.liveview.__main__ as M
    settings = LiveViewSettings(dsn=DSN, host="127.0.0.1", port=8090, metrics=None)
    return M.LiveView(settings, pool_factory=pool_factory, probe=EngineProbe(None))


@pytest.mark.parametrize("code", ["28000", "28P01", "3D000"])
async def test_a_login_refusal_at_start_is_a_configuration_error(code: str) -> None:
    import src.liveview.__main__ as M

    async def factory(_dsn: str) -> Any:
        raise PgError(code)

    lv = make_lv(factory)
    with pytest.raises(M.ConfigFailure) as ei:
        await lv.ensure_pool()
    msg = str(ei.value)
    assert DSN_VAR in msg and code in msg
    assert SECRET not in msg and "password authentication" not in msg


@pytest.mark.parametrize("code", ["28000", "28P01", "3D000"])
async def test_a_login_refusal_on_a_later_reconnect_is_a_configuration_error(code: str) -> None:
    import src.liveview.__main__ as M

    async def factory(_dsn: str) -> Any:
        return FailingPool(PgError(code))

    lv = make_lv(factory)
    with pytest.raises(M.ConfigFailure) as ei:
        await lv.poll_once()
    assert DSN_VAR in str(ei.value) and SECRET not in str(ei.value)


async def test_a_role_refusal_at_start_is_a_configuration_error() -> None:
    import src.liveview.__main__ as M

    async def factory(_dsn: str) -> Any:
        raise ReadOnlyRoleError(f"refusing PostgreSQL role 'xtnl_dashboard': is not xtnl_liveview (set {DSN_VAR} ...)")

    with pytest.raises(M.ConfigFailure, match=DSN_VAR):
        await make_lv(factory).ensure_pool()


@pytest.mark.parametrize("exc", [ConnectionRefusedError(), OSError("no route"), TimeoutError(), PgError("57P03"),
                                 PgError("53300")])
async def test_a_temporary_failure_reports_db_down_and_retries(exc: BaseException) -> None:
    calls = 0

    async def factory(_dsn: str) -> Any:
        nonlocal calls
        calls += 1
        raise exc

    lv = make_lv(factory)
    assert await lv.ensure_pool() is False
    assert lv.db_ok is False and lv.db_down_since is not None
    await lv.poll_once()
    assert calls == 2 and lv.hub.poll["db"] == "down" and lv.hub.poll["engine"] == "off"


@pytest.mark.parametrize("code", ["57P03", "53300", "08006"])
async def test_a_temporary_failure_after_connecting_closes_the_pool_and_reports_down(code: str) -> None:
    pools: list[FailingPool] = []

    async def factory(_dsn: str) -> Any:
        pools.append(FailingPool(PgError(code)))
        return pools[-1]

    lv = make_lv(factory)
    await lv.poll_once()
    assert lv.db_ok is False and lv.pool is None and pools[0].closed
    assert lv.hub.poll["db"] == "down"


def test_login_refusal_classification() -> None:
    import src.liveview.__main__ as M
    for code in ("28000", "28P01", "3D000"):
        assert M.is_login_refusal(PgError(code))
    for code in ("57P03", "53300", "08006", "42501"):
        assert not M.is_login_refusal(PgError(code))
    assert not M.is_login_refusal(OSError())


def test_importing_the_service_loads_no_engine_module() -> None:
    code = ("import sys\nimport src.liveview.__main__\n"
            "bad = sorted(m for m in sys.modules if m.startswith(('src.cache', 'redis', 'src.dashboard', "
            "'src.oversee', 'src.adapters', 'src.runtime', 'src.notifications', 'src.ops')))\n"
            "print(','.join(bad))\n")
    r = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    assert r.stdout.strip() == ""
