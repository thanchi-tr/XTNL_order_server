"""asyncpg adapter: pool settings, codecs (exact decimals in jsonb), Record -> tuple, error classes."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from src.db import pg as P


class RawConn:
    def __init__(self) -> None:
        self.codecs: list[tuple[str, dict[str, Any]]] = []

    async def set_type_codec(self, typename: str, **kw: Any) -> None:
        self.codecs.append((typename, kw))

    async def execute(self, sql: str, *args: object) -> str:
        return "INSERT 0 2"

    async def fetchrow(self, sql: str, *args: object) -> Any:
        return None if args else {"a": 1}.values()

    async def fetch(self, sql: str, *args: object) -> list[Any]:
        return [(1, 2), (3, 4)]


async def test_init_connection_installs_json_codecs() -> None:
    raw = RawConn()
    await P.init_connection(raw)
    assert [t for t, _ in raw.codecs] == ["jsonb", "json"]
    assert all(kw["format"] == "text" and kw["schema"] == "pg_catalog" for _, kw in raw.codecs)


def test_json_codec_keeps_decimals_exact() -> None:
    text = P.json_dumps({"b": Decimal("0.1000000000000000055511151231257827"), "a": [1, (2, 3)]})
    assert text == '{"a":[1,[2,3]],"b":"0.1000000000000000055511151231257827"}'
    assert P.json_loads('{"x": 1.10}')["x"] == Decimal("1.10")
    assert P.json_dumps('{"pre":"encoded"}') == '{"pre":"encoded"}'


def test_pool_kwargs() -> None:
    kw = P.pool_kwargs(application_name="xtnl:7:node-a", statement_timeout_ms=5_000)
    assert kw["init"] is P.init_connection and kw["timeout"] == 3.0
    assert kw["server_settings"] == {"application_name": "xtnl:7:node-a", "statement_timeout": "5000"}


async def test_asyncpg_connection_adapter() -> None:
    conn = P.AsyncpgConnection(RawConn())
    assert await conn.execute("X") == "INSERT 0 2"
    assert await conn.fetchrow("X") == (1,)
    assert await conn.fetchrow("X", 1) is None
    assert await conn.fetch("X") == [(1, 2), (3, 4)]


def test_rowcount_and_errors() -> None:
    assert P.rowcount("INSERT 0 3") == 3 and P.rowcount("UPDATE 1") == 1 and P.rowcount("BEGIN") == 0

    class PgDown(Exception):
        sqlstate = "57P01"

    class Syntax(Exception):
        sqlstate = "42601"

    assert P.is_connection_error(PgDown()) and P.is_connection_error(ConnectionResetError())
    assert not P.is_connection_error(Syntax()) and P.sqlstate(Syntax()) == "42601"
    assert P.text_array([1, None, "x"]) == ["1", None, "x"]
