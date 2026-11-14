"""`python -m src.ops pinescript` end to end against a real PostgreSQL (skipped unless ORDER_SERVER_TEST_DSN points
at a DISPOSABLE database: the test truncates the entity tables and event_log). Fixture rows are written by the
engine's own repository code (PgStateStore.commit_entity: current row, event_log, outbox)."""
from __future__ import annotations

import asyncio
import io
import os
import re
import time
from decimal import Decimal
from pathlib import Path

import pytest

from src.db.epoch import EngineDb, Epoch, bump_epoch
from src.db.migrate import migrate
from src.db.pg import create_asyncpg_source
from src.db.repository import PgStateStore
from src.dto import AccountDTO, OrderDTO, OrderStatus, PositionDTO
from src.ops import cli

from tests.reports.builders import lint, pine_arrays

DSN = os.environ.get("ORDER_SERVER_TEST_DSN")
pytestmark = [pytest.mark.integration, pytest.mark.skipif(not DSN, reason="ORDER_SERVER_TEST_DSN not set")]

TABLES = "engine_epoch, orders_state, positions_state, account_state, account_snapshots, event_log, outbox_events"
EPOCH = Epoch("it-reports", 3)
H = 3_600_000
D = Decimal
UNITS, REMAINING, SWAP, COMMISSION, BALANCE = D("123457"), D("98765.4321"), D("-3.217"), D("6.543"), D("55555.55")
FORBIDDEN = ("123457", "98765", "4321", "3.217", "6.543", "55555")


def fixture(now: int) -> list[OrderDTO | PositionDTO | AccountDTO]:
    seq = [0]

    def at(ms: int) -> dict[str, int]:
        seq[0] += 1
        return {"broker_updated_at_ms": ms, "observed_seq": seq[0], "seq_id": (ms << 20) + seq[0]}

    eu = dict(account_id="ACC", adapter_id="it", symbol="EURUSD", source="STREAM")
    t0 = now - 2 * 24 * H
    o_work = OrderDTO(client_order_id="E1", broker_order_id="E1", side="BUY", status=OrderStatus.WORKING,
                      entry_price=D("1.0811"), ordered_units=UNITS, remaining_units=UNITS, raw_qty=UNITS,
                      **eu, **at(t0))  # type: ignore[arg-type]
    o_fill = OrderDTO(client_order_id="E1", broker_order_id="E1", side="BUY", status=OrderStatus.FILLED,
                      entry_price=D("1.0811"), ordered_units=UNITS, remaining_units=D(0), raw_qty=UNITS,
                      **eu, **at(t0 + H))  # type: ignore[arg-type]
    p_open = PositionDTO(position_id="E1", side="BUY", units=UNITS, entry_price=D("1.0812"), status="OPEN",
                         raw_qty=UNITS, accrued_swap=SWAP, commission=COMMISSION, opened_by_client_order_id="E1",
                         **eu, **at(t0 + H))  # type: ignore[arg-type]
    sl = dict(client_order_id="E1-SL", broker_order_id="E1-SL", side="SELL", ordered_units=UNITS,
              remaining_units=UNITS, raw_qty=UNITS, is_reduce_only=True, linked_position_id="E1")
    sl1 = OrderDTO(status=OrderStatus.WORKING, entry_price=D("1.0751"), **sl, **eu, **at(t0 + H))  # type: ignore[arg-type]
    sl2 = OrderDTO(status=OrderStatus.WORKING, entry_price=D("1.0781"), **sl, **eu, **at(t0 + 2 * H))  # type: ignore[arg-type]
    sl3 = OrderDTO(status=OrderStatus.WORKING, entry_price=D("1.0821"), **sl, **eu, **at(t0 + 3 * H))  # type: ignore[arg-type]
    sl_fill = OrderDTO(status=OrderStatus.FILLED, entry_price=D("1.0821"), **{**sl, "remaining_units": D(0)},
                       **eu, **at(t0 + 4 * H))  # type: ignore[arg-type]
    p_closed = PositionDTO(position_id="E1", side="BUY", units=D(0), entry_price=D("1.0812"), status="CLOSED",
                           raw_qty=D(0), accrued_swap=SWAP, commission=COMMISSION, opened_by_client_order_id="E1",
                           **eu, **at(t0 + 4 * H))  # type: ignore[arg-type]
    au = dict(account_id="ACC", adapter_id="it", symbol="AUD/USD", source="REST")
    c_work = OrderDTO(client_order_id="A9", broker_order_id="A9", side="SELL", status=OrderStatus.WORKING,
                      entry_price=D("0.6611"), ordered_units=UNITS, remaining_units=UNITS, **au,
                      **at(now - 30 * H))  # type: ignore[arg-type]
    c_cancel = OrderDTO(client_order_id="A9", broker_order_id="A9", side="SELL", status=OrderStatus.CANCELLED,
                        entry_price=D("0.6611"), ordered_units=UNITS, remaining_units=UNITS, **au,
                        **at(now - 29 * H))  # type: ignore[arg-type]
    old = OrderDTO(client_order_id="OLD", broker_order_id="OLD", side="BUY", status=OrderStatus.CANCELLED,
                   entry_price=D("1.0001"), ordered_units=UNITS, remaining_units=UNITS, **eu,
                   **at(now - 10 * 24 * H))  # type: ignore[arg-type]
    acct = AccountDTO(account_id="ACC", adapter_id="it", currency="USD", balance=BALANCE, realised_pnl_today=SWAP,
                      **at(now - H))
    return [old, o_work, o_fill, p_open, sl1, sl2, sl3, sl_fill, p_closed, c_work, c_cancel, acct]


async def seed(now: int) -> None:
    assert DSN is not None
    src = await create_asyncpg_source(DSN, application_name="xtnl:3:it-reports")
    try:
        async with src.acquire() as conn:
            await migrate(conn)
            await conn.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")
            await bump_epoch(conn, EPOCH, "it-node", terminate_older=False)
        store = PgStateStore(EngineDb(src, lambda: EPOCH))
        for dto in fixture(now):
            await store.commit_entity(dto)
    finally:
        await src.close()


def test_cli_exports_from_real_postgres(tmp_path: Path) -> None:
    assert DSN is not None
    now = int(time.time() * 1000)
    asyncio.run(seed(now))
    out, err = io.StringIO(), io.StringIO()
    code = cli.main(["pinescript", "--days", "7", "--adapter", "it", "--out", str(tmp_path)],
                    environ={"ORDER_SERVER_DSN": DSN}, out=out, err=err)
    assert code == 0, err.getvalue()
    files = sorted(p.name for p in tmp_path.iterdir())
    assert [f.split("_")[0] for f in files] == ["AUDUSD", "EURUSD"]
    summary = out.getvalue()
    assert re.search(r"EURUSD_\S+\.pine orders=2 positions=1 trades=1 pending=0 drawn=1\+0 lines=\d+ labels=\d+ "
                     r"boxes=3 left_out=0", summary), summary
    assert re.search(r"AUDUSD_\S+\.pine orders=1 positions=0 trades=0 pending=1 drawn=0\+1 ", summary), summary
    eur = (tmp_path / files[1]).read_text(encoding="utf-8")
    aud = (tmp_path / files[0]).read_text(encoding="utf-8")
    for text in (eur, aud):
        assert lint(text) == []
        for n in FORBIDDEN:
            assert n not in text and n not in summary, n
    assert "OLD" not in eur and "1.0001" not in eur                      # outside the 7-day window
    arr = pine_arrays(eur)
    t0 = now - 2 * 24 * H
    # one trade: the order, its position and its stop child (moved twice, then hit)
    assert arr["trHead"] == ['"T1 BUY #E1"'] and arr["trOrders"] == ['"orders #E1 #E1-SL"']
    assert (arr["trEntryT"], arr["trEntryP"], arr["trExitT"], arr["trExitP"]) == (
        [str(t0 + H)], ["1.0812"], [str(t0 + 4 * H)], ["1.0821"])
    assert arr["trExitKind"] == ["1"] and arr["trTag"] == ['"T1 BUY"']        # SL hit
    assert arr["trTagSt"] == ["0"]                     # no take-profit: the first stop-loss zone carries the tag
    steps = list(zip(arr["stLeg"], (int(x) for x in arr["stX1"]), (int(x) for x in arr["stX2"]), arr["stP"]))
    assert steps == [("1", t0 + H, t0 + 2 * H, "1.0751"), ("1", t0 + 2 * H, t0 + 3 * H, "1.0781"),
                     ("1", t0 + 3 * H, t0 + 4 * H, "1.0821"), ("3", t0, t0 + H, "1.0811")]
    events = list(zip((int(x) for x in arr["evT"]), arr["evCode"], arr["evP"]))
    assert events == [(t0, "1", "1.0811"), (t0 + H, "9", "1.0812"), (t0 + H, "3", "1.0751"),
                      (t0 + 2 * H, "4", "1.0781"), (t0 + 3 * H, "4", "1.0821"), (t0 + 4 * H, "11", "1.0821")]
    aarr = pine_arrays(aud)
    assert aarr["trHead"] == [] and aarr["pdHead"] == ['"#A9 SELL"'] and aarr["pdKind"] == ["1"]
    assert (aarr["psX1"], aarr["psX2"]) == ([str(now - 30 * H)], [str(now - 29 * H)])
    assert 'const string DATA_SYMBOL = "AUDUSD"' in aud
    # the export is read-only: running it again changes nothing and gives the same scripts
    out2 = io.StringIO()
    again = tmp_path / "again"
    assert cli.main(["pinescript", "--adapter", "it", "--out", str(again)], environ={"ORDER_SERVER_DSN": DSN},
                    out=out2, err=io.StringIO()) == 0
    stamp = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}(:\d{2}Z)?")     # generation time and window only
    first, second = sorted(tmp_path.glob("*.pine")), sorted(again.glob("*.pine"))
    assert len(first) == len(second) == 2
    for a_path, b_path in zip(first, second):
        a, b = a_path.read_text(encoding="utf-8"), b_path.read_text(encoding="utf-8")
        assert stamp.sub("T", a) == stamp.sub("T", b)


async def seed_old_live(now: int) -> None:
    """An OPEN position and a WORKING order whose rows were last committed (and changed at the broker) long
    before the window: they are still live, so they must be drawn up to now."""
    assert DSN is not None
    old = now - 20 * 24 * H
    gb = dict(account_id="ACC", adapter_id="it", symbol="GBPUSD", source="STREAM", broker_updated_at_ms=old,
              observed_seq=1, seq_id=old << 20)
    pos = PositionDTO(position_id="G1", side="SELL", units=UNITS, entry_price=D("1.2711"), status="OPEN",
                      raw_qty=UNITS, accrued_swap=SWAP, commission=COMMISSION, **gb)  # type: ignore[arg-type]
    work = OrderDTO(client_order_id="G2", broker_order_id="G2", side="BUY", status=OrderStatus.WORKING,
                    entry_price=D("1.2611"), ordered_units=UNITS, remaining_units=UNITS,
                    **{**gb, "seq_id": (old << 20) + 1})  # type: ignore[arg-type]
    src = await create_asyncpg_source(DSN, application_name="xtnl:3:it-reports")
    try:
        async with src.acquire() as conn:
            await migrate(conn)
            await conn.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")
            await bump_epoch(conn, EPOCH, "it-node", terminate_older=False)
        store = PgStateStore(EngineDb(src, lambda: EPOCH))
        for dto in (pos, work):
            await store.commit_entity(dto)
        async with src.acquire() as conn:   # test set-up only: as if no snapshot re-committed them since
            await conn.execute("UPDATE positions_state SET committed_at_ms = $1::bigint", old)
            await conn.execute("UPDATE orders_state SET committed_at_ms = $1::bigint", old)
    finally:
        await src.close()


def test_live_rows_older_than_the_window_are_drawn(tmp_path: Path) -> None:
    assert DSN is not None
    now = int(time.time() * 1000)
    asyncio.run(seed_old_live(now))
    out, err = io.StringIO(), io.StringIO()
    assert cli.main(["pinescript", "--days", "1", "--adapter", "it", "--out", str(tmp_path)],
                    environ={"ORDER_SERVER_DSN": DSN}, out=out, err=err) == 0, err.getvalue()
    summary = out.getvalue()
    assert re.search(r"GBPUSD_\S+\.pine orders=1 positions=1 trades=1 pending=1 drawn=1\+1 ", summary), summary
    text = next(tmp_path.glob("GBPUSD_*.pine")).read_text(encoding="utf-8")
    arr = pine_arrays(text)
    assert arr["trEntryP"] == ["1.2711"] and arr["trExitKind"] == ["4"] and arr["psP"] == ["1.2611"]
    assert int(arr["trExitT"][0]) >= now and int(arr["psX2"][0]) >= now   # open position / working order to now
    for n in FORBIDDEN:
        assert n not in text and n not in out.getvalue(), n
    assert lint(text) == []
