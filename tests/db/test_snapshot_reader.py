"""PgSnapshotReader: one REPEATABLE READ snapshot per account + optional Redis live keys (oversee-rules.md §2)."""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from decimal import Decimal
from typing import Any

import fakeredis
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from src.cache.scripts import RedisState
from src.db import epoch as E
from src.db import snapshot_reader as S
from src.db.epoch import EngineDb
from src.dto import AccountLive, OrderStatus, PriceDTO
from src.oversee.dayroll import latest_roll_ms, trading_day_for_roll
from src.oversee.snapshot import DependencyUnavailable
from src.oversee.types import Cap, Level

from tests.db.conftest import EPOCH, FakeConn, FakeSource

NOW = 1_794_600_000_000   # a Wednesday in Nov 2026
ROLL = latest_roll_ms(NOW)
DAY = trading_day_for_roll(ROLL)


def acct_row() -> tuple[Any, ...]:
    return ("ACC", "stonex", "USD", Decimal("10000"), None, 1_000, None, "REST", 5, 50, 1_234)


def order_row(status: str = "WORKING") -> tuple[Any, ...]:
    return (None, "B1", "ACC", "stonex", "EURUSD", "BUY", status, Decimal("1.1"), Decimal("1.09"), None,
            Decimal(1000), Decimal(1000), False, None, False, None, 1_000, None, "STREAM", 3, 30, None)


def position_row() -> tuple[Any, ...]:
    return ("P1", "ACC", "stonex", "EURUSD", "SELL", Decimal(500), Decimal("1.2"), "OPEN", False, None, None, None,
            None, Decimal("-1.5"), Decimal("0"), 2_000, 4, "STREAM", 6, 60)


def instrument_row(symbol: str = "EURUSD") -> tuple[Any, ...]:
    return ("stonex", symbol, symbol[:3], symbol[3:], symbol[3:], Decimal(1), "UNITS", Decimal("0.0001"),
            Decimal(1), True, "400616150", None, None, None)


def breach_row(subject_id: str = "stonex:B1", resolved: int | None = None) -> tuple[Any, ...]:
    return ("max_position_risk", "ORDER", subject_id, "ACC", "SOFT", "SOFT", "risk", None, None, 900, 9, None, None,
            False, 100, 950, None, resolved, None, 9, 0)


@pytest.fixture
async def redis() -> AsyncIterator[Any]:
    r = fakeredis.FakeAsyncRedis()
    yield r
    await r.aclose()


def script_pg(conn: FakeConn, *, sod: bool = True) -> None:
    conn.returns(S.SQL_ACCOUNT, acct_row())
    conn.returns_all(S.SQL_OPEN_ORDERS, [order_row()])
    conn.returns_all(S.SQL_OPEN_POSITIONS, [position_row()])
    conn.returns(S.SQL_LAST_CHANGE, (1_500,))
    conn.returns_all(S.SQL_INSTRUMENTS, [instrument_row(), instrument_row("GBPUSD")])
    conn.returns_all(S.SQL_CLEAN_MARKS, [("EURUSD", Decimal("1.1"), Decimal("1.1002"), ROLL - 1_000)])
    conn.returns_all(S.SQL_CASH_FLOWS_SINCE, [("T1", "ACC", "stonex", "DEPOSIT", Decimal(50), "USD", ROLL + 5,
                                               99, ROLL + 6)])
    if sod:
        conn.returns(S.SQL_SOD, ("ACC", DAY, ROLL, Decimal("9900"), Decimal("9950"), Decimal(0), False, ROLL + 10,
                                 '[{"adapter_id":"stonex","symbol":"EURUSD","mid":"1.1001","ts":1}]', ["late"]))
    else:
        conn.returns(S.SQL_PREV_SOD, ("ACC", date(2026, 11, 12), ROLL - 86_400_000, Decimal("9800"),
                                      Decimal("9800"), Decimal(0), False, ROLL - 86_000_000, "[]", []))
        conn.returns_all(S.SQL_CASH_FLOWS_BETWEEN, [])
        conn.returns_all(S.SQL_TERMS_NEAR, [(ROLL - 3_600_000, None)])
    conn.returns_all(S.SQL_STOP_WATCH, [("ORDER", "stonex:B1", 777)])
    conn.returns_all(S.SQL_STREAM_HEALTH, [("stonex", "orders", None), ("stonex", "account", 5)])
    conn.returns_all(S.SQL_BREACHES, [breach_row()])
    conn.returns_all(S.SQL_ACKS, [(1, "max_position_risk", "ORDER", "stonex:B1", "SOFT", "SOFT", "risk", "op",
                                   "seen", 960)])


async def test_reads_everything_in_one_snapshot(conn: FakeConn, redis: Any) -> None:
    script_pg(conn)
    state = RedisState(redis, lambda: "b")
    await state.write_price(PriceDTO(adapter_id="stonex", symbol="EURUSD", bid=Decimal("1.1001"),
                                     ask=Decimal("1.1003"), spread_pips=Decimal("0.2"), timestamp_ms=NOW - 10),
                            Decimal("1.0"))
    await state.write_account_live(AccountLive(account_id="ACC", adapter_id="stonex", currency="USD",
                                               equity=Decimal("10010"), margin_used=Decimal(100), free_margin=None,
                                               unrealised_pnl=Decimal(10), timestamp_ms=NOW - 5))
    reader = S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), redis,
                                S.SnapshotInputs(session=lambda a, s: "CLOSED" if s == "GBPUSD" else "OPEN",
                                                 pnl_source=lambda a: False))
    snap = await reader.read_account("ACC", NOW)
    assert conn.sql[0] == E.SQL_BEGIN_SNAPSHOT and conn.sql[-1] == E.SQL_ROLLBACK
    assert not any(s.startswith(("INSERT", "UPDATE", "DELETE")) for s in conn.sql)
    assert snap.adapter_id == "stonex" and snap.account is not None and snap.account.balance == Decimal("10000")
    assert [o.status for o in snap.orders] == [OrderStatus.WORKING] and snap.positions[0].accrued_swap == Decimal("-1.5")
    assert set(snap.instruments) == {("stonex", "EURUSD"), ("stonex", "GBPUSD")}
    q = snap.quotes[("stonex", "EURUSD")]
    assert q.latest is not None and q.latest.bid == Decimal("1.1001")
    assert q.clean_mid is not None and q.clean_mid.ts_ms == NOW - 10
    assert q.last_clean is not None and q.last_clean.ts_ms == ROLL - 1_000 and q.session == "OPEN"
    assert ("stonex", "GBPUSD") not in snap.quotes                      # nothing ever seen
    assert snap.account_live is not None and snap.account_live.free_margin is None
    assert snap.cash_flows[0].committed_at_ms == ROLL + 6 and snap.cash_flows[0].flow.kind == "DEPOSIT"
    assert snap.sod is not None and snap.sod.trading_day == DAY and snap.sod.partial_reasons == ("late",)
    assert snap.sod.marks[("stonex", "EURUSD")].mid == Decimal("1.1001")
    assert snap.prev_sod is None and S.SQL_PREV_SOD not in conn.sql   # only needed at the roll
    assert dict(snap.stop_watch) == {("order", "stonex:B1"): 777}
    assert [h.conn_lost_at_ms for h in snap.stream_health] == [None, 5]
    (b,) = snap.breaches
    assert (b.subject_type, b.level, b.cap, b.uncapped_ms, b.last_pass_ms) == ("order", Level.SOFT_HALT, Cap.SOFT,
                                                                                100, 950)
    assert snap.acks[0].operator == "op" and snap.acks[0].level is Level.SOFT_HALT
    assert snap.redis_ok and not snap.pnl_source and snap.transfer_source
    assert snap.last_status_or_units_change_ms == 1_500 and snap.balance_committed_at_ms == 1_234
    (subjects,) = [a[1] for s, a in conn.calls if s == S.SQL_BREACHES]
    assert subjects == ["ACC", f"ACC:{DAY.isoformat()}", "stonex:B1", "stonex:P1"]


async def test_roll_inputs_when_sod_missing(conn: FakeConn) -> None:
    script_pg(conn, sod=False)
    reader = S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), None)
    snap = await reader.read_account("ACC", NOW)
    assert snap.sod is None and snap.prev_sod is not None and snap.prev_sod.balance == Decimal("9800")
    assert not snap.leaderless_before_roll          # one term covers the 5 min before the roll
    (args,) = conn.args_of(S.SQL_CASH_FLOWS_BETWEEN)
    assert args[2:] == (ROLL - 86_400_000, ROLL)
    assert not snap.redis_ok and snap.account_live is None   # no Redis: live inputs unknown
    assert snap.quotes[("stonex", "EURUSD")].latest is None


async def test_redis_error_falls_back(conn: FakeConn) -> None:
    script_pg(conn)

    class DeadRedis:
        def pipeline(self, transaction: bool = False) -> Any:
            raise RedisConnectionError("down")

    reader = S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), DeadRedis())  # type: ignore[arg-type]
    snap = await reader.read_account("ACC", NOW)
    assert not snap.redis_ok and snap.quotes[("stonex", "EURUSD")].last_clean is not None


async def test_account_without_state_row(conn: FakeConn) -> None:
    conn.returns(S.SQL_ADAPTER_OF, ("ctrader",))
    snap = await S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), None).read_account("X", NOW)
    assert snap.account is None and snap.adapter_id == "ctrader" and snap.leaderless_before_roll


async def test_list_accounts_and_dependency_down(conn: FakeConn) -> None:
    conn.returns_all(S.SQL_LIST_ACCOUNTS, [("A",), ("B",)])
    reader = S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), None)
    assert list(await reader.list_accounts()) == ["A", "B"]
    conn.fail(S.SQL_LIST_ACCOUNTS, ConnectionRefusedError("down"))
    with pytest.raises(DependencyUnavailable):
        await reader.list_accounts()
    conn.fail(E.SQL_BEGIN_SNAPSHOT, OSError("down"))
    with pytest.raises(DependencyUnavailable):
        await reader.read_account("A", NOW)


async def test_statement_error_is_an_account_fault(conn: FakeConn) -> None:
    class Bad(Exception):
        sqlstate = "42P01"

    conn.fail(S.SQL_ACCOUNT, Bad())
    with pytest.raises(Bad):
        await S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), None).read_account("A", NOW)


def test_covered() -> None:
    assert S.covered((0, 100), [(0, None)])
    assert S.covered((0, 100), [(-5, 50), (40, 120)])
    assert not S.covered((0, 100), [(-5, 50), (60, 120)])   # gap 50..60
    assert not S.covered((0, 100), [(10, None)])            # starts late
    assert not S.covered((0, 100), [])
    assert not S.covered((0, 100), [(0, 90)])               # ends early


async def test_latest_drop_below_soft_is_loaded_after_its_entity_has_gone(conn: FakeConn) -> None:
    """cooldown-skipped-entity-end: a state resolved with its (now closed) position is no longer a live
    subject, but the resolved row that dropped below SOFT last is still loaded for the cool-down (§2)."""
    script_pg(conn)
    closed = ("missing_stop_loss", "POSITION", "stonex:GONE", "ACC", None, "NONE", "", None, None, 900, 9, None,
              None, False, 0, NOW - 1_000, NOW - 1_000, NOW - 1_000, "entity_end", 11, 0)
    conn.returns(S.SQL_LATEST_DROP, closed)
    snap = await S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), None).read_account("ACC", NOW)
    assert conn.args_of(S.SQL_LATEST_DROP) == [("ACC",)]
    gone = next(b for b in snap.breaches if b.subject_id == "stonex:GONE")
    assert gone.resolved_by == "entity_end" and gone.below_soft_since == NOW - 1_000 and not gone.active
    assert len(snap.breaches) == 2


async def test_latest_drop_already_loaded_is_not_duplicated(conn: FakeConn) -> None:
    script_pg(conn)
    conn.returns(S.SQL_LATEST_DROP, breach_row())
    snap = await S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), None).read_account("ACC", NOW)
    assert [b.subject_id for b in snap.breaches] == ["stonex:B1"]


async def test_leader_terms_are_read_from_the_earliest_held_symbol_reference_point(conn: FakeConn) -> None:
    """6.3: the sod leaderless test runs per held symbol from its own reference point (tier-persistence.md §4), so
    the terms are read from the earliest scheduled close before the roll, and handed to the snapshot."""
    script_pg(conn, sod=False)
    close = ROLL - 3_600_000
    reader = S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), None,
                                S.SnapshotInputs(scheduled_closes=lambda a, r: {("stonex", "EURUSD"): close}))
    snap = await reader.read_account("ACC", NOW)
    (args,) = conn.args_of(S.SQL_TERMS_NEAR)
    assert args == (close - 300_000, ROLL)
    assert snap.leader_terms == ((ROLL - 3_600_000, None),)      # script_pg's one open term
    assert not snap.leaderless_before_roll                       # the roll window itself is covered
    assert dict(snap.scheduled_close_before_roll_ms) == {("stonex", "EURUSD"): close}


async def test_leader_terms_are_not_read_while_the_days_sod_row_exists(conn: FakeConn) -> None:
    script_pg(conn)
    snap = await S.PgSnapshotReader(EngineDb(FakeSource(conn), lambda: EPOCH), None).read_account("ACC", NOW)
    assert snap.leader_terms is None and S.SQL_TERMS_NEAR not in conn.sql
