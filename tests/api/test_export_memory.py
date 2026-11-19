"""The in-memory export variant (src.reports.export.plan_pinescript / plan_texts) the API compiles with: the same
read-only transaction and the same scripts as the file export, nothing written; and the real compiler's error
codes."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.api.pinescript import SQL_ROLE_CHECK, CompileError, pg_compiler, role_problems
from src.reports.export import plan_from_read, plan_pinescript, plan_texts
from src.reports.lifecycle import SQL_LOG_VERSIONS, VERSION_COLUMNS, LogRead, StateVersion, Window, version_from_row

from tests.reports.builders import H, NOW, T0, lint, order, position

WINDOW = Window(NOW - 7 * 24 * H, NOW)


def rows() -> list[tuple[object, ...]]:
    dtos = [order("E1", T0, "WORKING", "1.0811"), order("E1", T0 + H, "FILLED", "1.0811"),
            position("E1", T0 + H, "OPEN", "1.0812"), order("A9", T0 + 2 * H, "WORKING", "0.6611", side="SELL",
                                                             symbol="AUD/USD")]
    return [as_row(v) for v in dtos]


def as_row(v: StateVersion) -> tuple[object, ...]:
    """What SQL_LOG_VERSIONS returns (text columns) for this version."""
    def text(x: object) -> str | None:
        if x is None:
            return None
        if isinstance(x, bool):
            return "true" if x else "false"
        return str(x)
    return tuple(text(getattr(v, c)) for c in VERSION_COLUMNS)


API_ROLE = ("xtnl_api", False, "on", 0)        # SQL_ROLE_CHECK of the migration 0014 role


class FakeConn:
    def __init__(self, log_rows: list[tuple[object, ...]], fail: BaseException | None = None,
                 role: tuple[object, ...] | None = API_ROLE) -> None:
        self.log_rows = log_rows
        self.fail = fail
        self.role = role
        self.statements: list[str] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.statements.append(sql)
        return "OK"

    async def fetchrow(self, sql: str, *args: object) -> Any:
        assert sql == SQL_ROLE_CHECK
        self.statements.append(sql)
        return self.role

    async def fetch(self, sql: str, *args: object) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if self.fail is not None:
            raise self.fail
        return list(self.log_rows) if sql == SQL_LOG_VERSIONS else []


async def test_plan_pinescript_reads_read_only_and_renders_in_memory(tmp_path: Path,
                                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    conn = FakeConn(rows())
    plan = await plan_pinescript(conn, WINDOW, now_ms=NOW)
    texts = plan_texts(plan)
    assert [s for s, _ in texts] == ["AUDUSD", "EURUSD"]
    for _, text in texts:
        assert text.startswith("//@version=6") and lint(text) == []
    assert conn.statements[:3] == ["BEGIN ISOLATION LEVEL REPEATABLE READ", "SET TRANSACTION READ ONLY",
                                   "SET LOCAL statement_timeout = '60s'"]
    assert conn.statements[-1] == "COMMIT"
    for sql in conn.statements:
        assert not any(w in sql.upper().split() for w in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER"))
    assert list(tmp_path.iterdir()) == []                                  # nothing written
    versions = tuple(v for v in (version_from_row(r) for r in rows()) if v is not None)
    same = plan_from_read(LogRead(WINDOW, versions, len(versions), 0, 0, False), now_ms=NOW)
    assert plan_texts(same) == texts                                        # identical to the file export's plan


async def test_pg_compiler_returns_texts_and_closes() -> None:
    closed: list[bool] = []

    async def connect(dsn: str) -> Any:
        async def close() -> None:
            closed.append(True)
        return FakeConn(rows()), close

    out = await pg_compiler("postgresql://x@h/db", connect)(WINDOW, NOW)
    assert [s for s, _ in out] == ["AUDUSD", "EURUSD"] and closed == [True]


async def test_pg_compiler_error_codes() -> None:
    async def unreachable(dsn: str) -> Any:
        raise OSError("connection refused to postgresql://u:hunter2@h/db")

    with pytest.raises(CompileError) as exc:
        await pg_compiler("postgresql://x@h/db", unreachable)(WINDOW, NOW)
    assert exc.value.code == "db_unavailable" and "hunter2" not in str(exc.value)

    class Lost(Exception):
        sqlstate = "08006"

    class Bad(Exception):
        sqlstate = "42P01"

    for err, code in ((Lost(), "db_unavailable"), (Bad(), "db_query_failed")):
        async def failing(dsn: str, err: BaseException = err) -> Any:
            async def close() -> None:
                pass
            return FakeConn([], fail=err), close

        with pytest.raises(CompileError) as exc:
            await pg_compiler("postgresql://x@h/db", failing)(WINDOW, NOW)
        assert exc.value.code == code


@pytest.mark.parametrize("role,problem", [(("xtnl", True, "off", 0), "is a superuser"),
                                          (("xtnl", True, "on", 0), "is a superuser"),
                                          (("engine", False, "on", 7), "can write tables"),
                                          (("weak", False, "off", 0), "session is not default_transaction_read_only"),
                                          (None, "role not found in pg_roles")])
async def test_pg_compiler_refuses_a_role_that_could_write(role: tuple[object, ...] | None, problem: str,
                                                          caplog: pytest.LogCaptureFixture) -> None:
    """Regression (least privilege): the API used to compile over the engine's own superuser DSN, protected only by
    a session default the client can override. Any role that could write is now refused before a read."""
    assert problem in role_problems(role)
    closed: list[bool] = []
    conn = FakeConn(rows(), role=role)

    async def connect(dsn: str) -> Any:
        async def close() -> None:
            closed.append(True)
        return conn, close

    with pytest.raises(CompileError) as exc:
        await pg_compiler("postgresql://xtnl:hunter2@h/db", connect)(WINDOW, NOW)
    assert exc.value.code == "db_role_refused" and closed == [True]
    assert conn.statements == [SQL_ROLE_CHECK]                              # nothing else was read
    assert "hunter2" not in caplog.text


async def test_pg_compiler_accepts_the_api_role_and_checks_it_first() -> None:
    conn = FakeConn(rows())

    async def connect(dsn: str) -> Any:
        async def close() -> None:
            pass
        return conn, close

    await pg_compiler("postgresql://xtnl_api@h/db", connect)(WINDOW, NOW)
    assert conn.statements[0] == SQL_ROLE_CHECK and role_problems(API_ROLE) == []
