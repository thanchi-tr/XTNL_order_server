"""Pure arming rules (plan S26, design §B7, §B11, §B4): edge refusal, entry time, S0 source, position changes, the
New York roll window and the pre-warm set. Synthetic ids and levels only."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from src.db.arm_ports import EntryTime, HandleVersionRow
from src.dto import OrderDTO, OrderStatus
from src.oversee.risk import StopDetail, StopHandle
from src.riskmgr import arming as A
from src.riskmgr.types import EntryTimeSource, S0Source
from tests.riskmgr.tracker_fakes import assignment, document

D = Decimal
FILL = 1_700_000_000_000


def _ny_roll_ms(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, 17, tzinfo=ZoneInfo("America/New_York")).timestamp()) * 1000


def _row(seq: int, t: int | None, origin: str | None, level: str | None, hid: str = "P1",
         kind: str = "POSITION_FIELD") -> HandleVersionRow:
    return HandleVersionRow(handle_kind=kind, handle_id=hid, seq_id=seq, broker_time_ms=t, origin=origin,  # type: ignore[arg-type]
                            status="OPEN", level=None if level is None else D(level))


DETAIL = StopDetail(D("1.19"), (StopHandle("POSITION_FIELD", "P1", D("1.19"), D(1000)),))


# ---------------------------------------------------------------------------------------------------- edge
def test_second_enabled_edge_is_found_and_null_or_local_edge_is_not() -> None:
    assert A.second_edges(document(), "default") == ()
    doc = document(assignments=[assignment(1), assignment(2, edge="default"), assignment(3, edge="momentum"),
                                assignment(4, edge="carry", enabled=False)])
    assert A.second_edges(doc, "default") == ("momentum",)


# ---------------------------------------------------------------------------------------------------- entry time
def test_entry_time_is_the_fill_never_the_placement() -> None:
    assert A.entry_choice(EntryTime(source="BROKER", broker_time_ms=FILL, seq_id=4), first_seen_ms=FILL + 9) \
        == (FILL, EntryTimeSource.BROKER)
    assert A.entry_choice(EntryTime(source="STREAM", broker_time_ms=FILL, seq_id=1), first_seen_ms=FILL + 9) \
        == (FILL, EntryTimeSource.STREAM)
    assert A.entry_choice(None, first_seen_ms=FILL + 9) == (FILL + 9, EntryTimeSource.ESTIMATED)
    assert A.exact_entry_time(EntryTime(source="BROKER", broker_time_ms=FILL, seq_id=4))
    assert not A.exact_entry_time(None)


# ---------------------------------------------------------------------------------------------------- S0
def test_s0_at_fill_needs_a_stream_version_within_ten_seconds_and_no_later_amendment() -> None:
    rows = [_row(5, FILL + 3_000, "stream", "1.19")]
    pick = A.s0_pick(DETAIL, fill_known=True, at_fill=rows, now=rows)
    assert pick.source is S0Source.AT_FILL and pick.level == D("1.19")
    late = [_row(5, FILL + 30_000, "stream", "1.19")]
    assert A.s0_pick(DETAIL, fill_known=True, at_fill=[], now=late).source is S0Source.FIRST_SEEN
    amended = [_row(5, FILL + 3_000, "stream", "1.18"), _row(9, FILL + 60_000, "stream", "1.19")]
    at_fill = [r for r in amended if r.broker_time_ms is not None and r.broker_time_ms <= FILL + 10_000]
    assert A.s0_pick(DETAIL, fill_known=True, at_fill=at_fill, now=amended).source is S0Source.FIRST_SEEN
    snap = [_row(5, FILL + 3_000, "snapshot", "1.19")]
    assert A.s0_pick(DETAIL, fill_known=True, at_fill=snap, now=snap).source is S0Source.FIRST_SEEN
    assert A.s0_pick(DETAIL, fill_known=False, at_fill=[], now=rows).source is S0Source.FIRST_SEEN


def test_s0_from_history_is_history_last_level() -> None:
    rows = [_row(5, FILL + 3_000, "stream", "1.18"), _row(8, FILL + 50_000, "history", None)]
    assert A.s0_pick(DETAIL, fill_known=True, at_fill=rows[:1], now=rows).source is S0Source.HISTORY_LAST_LEVEL
    only = [_row(3, FILL, "history", None)]
    assert A.s0_pick(DETAIL, fill_known=True, at_fill=only, now=only).source is S0Source.HISTORY_LAST_LEVEL


# ---------------------------------------------------------------------------------------------------- changes
def _change(units: str, entry: str, *, live: bool = False, at: int = FILL) -> A.Change:
    return A.classify_change(pinned_units=D(1000), pinned_entry=D("1.2"), units=D(units), entry=D(entry),
                             fills_cover=live, changed_at_ms=at)


def test_position_changes() -> None:
    assert _change("1000", "1.2") is A.Change.NONE
    assert _change("500", "1.2") is A.Change.UNITS_DECREASE         # units decrease: the pin follows it down
    assert _change("500", "1.25") is A.Change.ENTRY_CORRECTION      # an entry change is never hidden by a decrease
    assert _change("2000", "1.21", live=True) is A.Change.FILL_PROGRESS
    assert _change("2000", "1.21") is A.Change.SUSPEND
    roll = _ny_roll_ms(2023, 11, 14)
    assert _change("1000", "1.21", at=roll + 90_000) is A.Change.ROLL_ADJUST
    assert _change("1000", "1.21", at=roll - 120_000) is A.Change.ROLL_ADJUST
    assert _change("1000", "1.21", at=roll + 121_000) is A.Change.ENTRY_CORRECTION


def test_roll_window_follows_new_york_daylight_saving() -> None:
    summer = _ny_roll_ms(2023, 7, 11)     # 17:00 EDT = 21:00 UTC
    winter = _ny_roll_ms(2023, 12, 12)    # 17:00 EST = 22:00 UTC
    assert A.near_ny_roll(summer) and A.near_ny_roll(winter)
    assert not A.near_ny_roll(summer + 3_600_000) and not A.near_ny_roll(winter - 3_600_000)


# ---------------------------------------------------------------------------------------------------- pre-warm
def test_prewarm_set_explicit_then_wildcard_and_capped() -> None:
    doc = document(assignments=[assignment(1, symbol="SYN.Z"), assignment(2, adapter=None, symbol="SYN.Y")])
    got = A.prewarm_symbols(doc, 0, held={"mock": {"SYN.A"}}, tick_symbols={}, bar_adapters={"mock"})
    assert got == {"mock": ("SYN.Y", "SYN.Z")}    # no wildcard: held symbols are not pre-warmed
    wild = document(assignments=[assignment(1), assignment(2, symbol="SYN.Z")])
    many = {f"SYN.{c}" for c in "ABCDEFGHIJ"}
    got = A.prewarm_symbols(wild, 0, held={"mock": many}, tick_symbols={"mock": {"SYN.T"}}, bar_adapters={"mock"})
    assert got["mock"][0] == "SYN.Z" and len(got["mock"]) == A.PREWARM_CAP
    assert A.prewarm_symbols(wild, 0, held={"mock": many}, tick_symbols={}, bar_adapters=set()) == {}
    assert A.prewarm_symbols(None, 0, held={}, tick_symbols={}, bar_adapters={"mock"}) == {}


def _entry(status: OrderStatus, remaining: str, *, reduce_only: bool = False) -> OrderDTO:
    return OrderDTO(client_order_id=None, broker_order_id="E1", account_id="a", adapter_id="mock", symbol="SYN.A",
                    side="BUY", status=status, entry_price=D("1.2"), ordered_units=D(2000),
                    remaining_units=D(remaining), order_kind="STOP", is_reduce_only=reduce_only)


def test_entry_fills_cover_counts_the_final_fill() -> None:
    assert A.entry_fills_cover("E1", [_entry(OrderStatus.PARTIALLY_FILLED, "1000")], D(2000))     # still working
    assert not A.entry_fills_cover("E1", [_entry(OrderStatus.PARTIALLY_FILLED, "1000")], D(2500))  # beyond size
    assert A.entry_fills_cover("E1", [_entry(OrderStatus.FILLED, "0")], D(2000))                  # the final fill
    assert A.entry_fills_cover("E1", [_entry(OrderStatus.CANCELLED, "500")], D(1500))             # last fill + cancel
    assert not A.entry_fills_cover("E1", [_entry(OrderStatus.CANCELLED, "500")], D(1600))         # beyond its fills
    assert not A.entry_fills_cover("E1", [_entry(OrderStatus.FILLED, "0", reduce_only=True)], D(2000))
    assert not A.entry_fills_cover(None, [_entry(OrderStatus.FILLED, "0")], D(2000))   # Pepperstone: no entry id


def test_roll_note_is_opaque_and_fits_the_state_reason_check() -> None:
    import re
    note = A.roll_note("mock:P1", D("1.2010"))
    assert note == A.roll_note("mock:P1", D("1.201")) != A.roll_note("mock:P1", D("1.2011"))
    assert note != A.roll_note("mock:P2", D("1.2010")) and note.startswith(A.ROLL_NOTE_PREFIX)
    assert re.fullmatch(r"[a-z][a-z0-9_]{0,63}", note) and "1201" not in note
