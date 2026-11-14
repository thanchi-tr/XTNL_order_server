"""Local state, adapter auth state, cTrader token CAS, row mapping, sweep-writer SQL shape."""
from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from types import MappingProxyType

import pytest

from src.db import auth_state as A
from src.db import local_state as L
from src.db import rows as R
from src.db import sweep_writer as W
from src.db import tokens as T
from src.db.epoch import EngineDb, epoch_transaction
from src.db.fence import FENCE, EpochMismatch
from src.oversee.notify import OutboxRow, episode_key
from src.oversee.snapshot import SodMark, TimedMid
from src.oversee.types import BreachState, Cap, Level
from src.ports import AdapterAuthStore, LocalStateReader

from tests.db.conftest import EPOCH, FakeConn, FakeSource
from tests.db.test_snapshot_reader import order_row, position_row


def placeholders(sql: str) -> set[int]:
    return {int(n) for n in re.findall(r"\$(\d+)", sql)}


async def test_local_state_reader(conn: FakeConn) -> None:
    conn.returns_all(L.SQL_ADAPTER_OPEN_ORDERS, [order_row()])
    conn.returns_all(L.SQL_ADAPTER_OPEN_POSITIONS, [position_row()])
    reader: LocalStateReader = L.PgLocalStateReader(EngineDb(FakeSource(conn), lambda: EPOCH))
    orders, positions = await reader.open_entities("stonex")
    assert [o.broker_order_id for o in orders] == ["B1"] and [p.position_id for p in positions] == ["P1"]
    assert orders[0].checksum   # rebuilt DTOs re-seal their checksum
    assert conn.args_of(L.SQL_ADAPTER_OPEN_ORDERS) == [("stonex",)]


async def test_adapter_auth_state(conn: FakeConn) -> None:
    store: AdapterAuthStore = A.PgAdapterAuthStore(EngineDb(FakeSource(conn), lambda: EPOCH))
    assert await store.known_bad("ctrader", "g1") is False
    conn.returns(A.SQL_AUTH_GET, ("AUTH_FAILED",), ("REARMED",))
    assert await store.known_bad("ctrader", "g1") is True
    assert await store.known_bad("ctrader", "g1") is False
    await store.record_auth_failed("ctrader", "g1", "invalid_grant" * 500)
    (args,) = conn.args_of(A.SQL_AUTH_FAILED)
    assert args[:3] == (7, "ctrader", "g1") and len(args[3]) == 2000
    conn.status_for(A.SQL_AUTH_REARM, "UPDATE 1", "UPDATE 0")
    assert await store.rearm("ctrader", "g1", "op") is True
    assert await store.rearm("ctrader", "g1", "op") is False
    conn.stored_epoch = 8
    with pytest.raises(EpochMismatch):
        await store.record_auth_failed("ctrader", "g1", "x")


async def test_token_generation_cas_is_not_fenced(conn: FakeConn) -> None:
    conn.stored_epoch = 99   # a save started before a leader change still lands
    assert FENCE not in T.SQL_TOKEN_CAS_SAVE and FENCE not in T.SQL_TOKEN_FIRST_SAVE
    conn.returns(T.SQL_TOKEN_FIRST_SAVE, (1,))
    assert await T.save_token_pair(conn, "ctrader", 0, b"a", b"r", "kms", 10) == 1
    conn.returns(T.SQL_TOKEN_CAS_SAVE, (2,))
    assert await T.save_token_pair(conn, "ctrader", 1, b"a", b"r", "kms", 10) == 2
    assert await T.save_token_pair(conn, "ctrader", 1, b"a", b"r", "kms", 10) is None   # generation moved
    assert conn.args_of(T.SQL_TOKEN_CAS_SAVE)[0][:2] == ("ctrader", 1)


async def test_token_refresh_marker_is_fenced(conn: FakeConn) -> None:
    async with epoch_transaction(conn, EPOCH) as tx:
        conn.status_for(T.SQL_TOKEN_BEGIN_REFRESH, "UPDATE 1", "UPDATE 0")
        assert await T.begin_refresh(tx, "ctrader", 4) is True
        assert await T.begin_refresh(tx, "ctrader", 4) is False   # generation 4 already in flight
    assert FENCE in T.SQL_TOKEN_BEGIN_REFRESH


async def test_read_token_hides_material(conn: FakeConn) -> None:
    assert await T.read_token(conn, "ctrader") is None
    conn.returns(T.SQL_TOKEN_GET, (3, b"secret-a", b"secret-r", "kms", 99, None, 1_700_000_000_123))
    row = await T.read_token(conn, "ctrader")
    assert row is not None and row.generation == 3 and "secret" not in repr(row)
    assert row.saved_at_ms == 1_700_000_000_123                    # 7.3: the token-age metric reads it


def test_breach_roundtrip() -> None:
    s = BreachState("r", "account_day", "ACC:2026-11-13", "ACC", Level.HARD_HALT, Cap.SOFT, "loss", Level.HARD_HALT,
                    1, 2, 3, 4, 5, True, 600, 7, 8, None, None, 10, 2)
    params = R.breach_params(s)
    assert params[1] == "ACCOUNT_DAY" and params[4] == "HARD" and params[7] == "HARD"
    assert R.breach_from_row(params) == s
    resolved = BreachState("r", "order", "a:1", "ACC", resolved_at=5, resolved_by="entity_end")
    assert R.breach_params(resolved)[4] is None and R.breach_from_row(R.breach_params(resolved)) == resolved
    with pytest.raises(ValueError):
        R.subject_from_db("SYMBOL")


def test_outbox_and_sod_roundtrip() -> None:
    key = episode_key("r", "order", "a:1", 3, Level.SOFT_HALT)
    row = OutboxRow("episode", "high", "t", "b", 5, key, "ACC")
    arrays = R.outbox_arrays([row])
    assert arrays[3] == ["HIGH"] and arrays[2] == [False]
    back = R.outbox_from_row((9, "episode", "HIGH", "t", "b", 5, arrays[1][0], "ACC"))
    assert back == OutboxRow("episode", "high", "t", "b", 5, key, "ACC", 9)
    assert R.episode_from(list(key)) == key   # decoded jsonb (list) works too
    sod = SodMark("ACC", date(2026, 11, 13), 1, Decimal("1"), Decimal("2"), Decimal("0.5"), True, 3,
                  MappingProxyType({("a", "EURUSD"): TimedMid(Decimal("1.10"), 9)}), ("late",))
    row_back = R.sod_from_row(("ACC", "2026-11-13", 1, Decimal("1"), Decimal("2"), Decimal("0.5"), True, 3,
                               R.marks_json(sod.marks), ["late"]))
    assert row_back == sod
    assert R.sod_from_row(("ACC", date(2026, 11, 13), 1, None, None, 0, False, 3, None, None)) is None


@pytest.mark.parametrize("name", [n for n in dir(W) if n.startswith("SQL_")])
def test_sweep_writer_sql_is_fenced_with_contiguous_params(name: str) -> None:
    sql = getattr(W, name)
    assert FENCE in sql
    ph = placeholders(sql)
    assert ph == set(range(1, max(ph) + 1)), name


def test_breach_sql_parameter_counts() -> None:
    n = len(W.BREACH_COL_NAMES)
    assert len(W.BREACH_TYPES) == n == len(R.breach_params(BreachState("r", "order", "s", "a")))
    assert max(placeholders(W.SQL_BREACH_INSERT)) == n + 1
    assert max(placeholders(W.SQL_BREACH_CAS)) == n + 3   # + expected last_sweep_id, clear_generation
    assert "last_sweep_id IS NOT DISTINCT FROM" in W.SQL_BREACH_CAS
