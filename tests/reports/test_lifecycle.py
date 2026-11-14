"""Timeline and life-cycle building (pure: no database)."""
from __future__ import annotations

import random
from decimal import Decimal

from src.reports.lifecycle import (Lifecycle, Marker, Step, StateVersion, Window, build_lifecycles, build_timeline,
                                   build_timelines, normalise_symbol, related_keys, steps_from_points,
                                   version_from_row)

from tests.reports.builders import H, NOW, T0, d, order, position


def lifecycles(*versions: StateVersion) -> dict[str, Lifecycle]:
    return {lc.key: lc for lc in build_lifecycles(build_timelines(versions), NOW)}


def stonex_round_trip() -> list[StateVersion]:
    """StoneX: entry O1 placed, filled; position id = order id; stop / limit children; the limit fills."""
    return [
        order("O1", T0, "WORKING", "1.0800"),
        order("O1", T0 + H, "FILLED", "1.0800"),
        position("O1", T0 + H, "OPEN", "1.0801", client="O1"),
        order("O1-SL", T0 + H, "WORKING", "1.0750", side="SELL", reduce_only=True, linked="O1"),
        order("O1-TP", T0 + H, "WORKING", "1.0900", side="SELL", reduce_only=True, linked="O1"),
        order("O1-TP", T0 + 3 * H, "FILLED", "1.0900", side="SELL", reduce_only=True, linked="O1"),
        order("O1-SL", T0 + 3 * H + 500, "CANCELLED", "1.0750", side="SELL", reduce_only=True, linked="O1"),
        position("O1", T0 + 3 * H, "CLOSED", "1.0801", client="O1"),
    ]


def test_placed_filled_closed() -> None:
    lcs = lifecycles(*stonex_round_trip())
    assert list(lcs) == ["stonex:O1"]          # children belong to the order's life cycle
    lc = lcs["stonex:O1"]
    assert lc.status_trail == ("WORKING", "FILLED", "CLOSED")
    assert lc.outcome == "CLOSED" and lc.position_id == "O1" and lc.children == ("O1-SL", "O1-TP")
    assert lc.placed == Marker(T0, d("1.0800"))
    assert lc.working == (Step(T0, T0 + H, d("1.0800")),)
    assert lc.fill == Marker(T0 + H, d("1.0801"))
    assert lc.position == (Step(T0 + H, T0 + 3 * H, d("1.0801")),)
    assert lc.stop_loss == (Step(T0 + H, T0 + 3 * H, d("1.0750")),)
    assert lc.take_profit == (Step(T0 + H, T0 + 3 * H, d("1.0900")),)
    assert lc.close == Marker(T0 + 3 * H, d("1.0900")) and lc.close_result == "FAVOURABLE"
    assert lc.ended is None


def test_stop_moved_twice_and_position_still_open() -> None:
    """Pepperstone: SL on the position itself, moved twice; the opening order carries no position id and is
    linked by its fill time; the position is still open (lines run to now)."""
    t1, t2, t3 = T0 + H, T0 + 2 * H, T0 + 4 * H
    lcs = lifecycles(
        order("9001", T0, "WORKING", "1.2500", adapter="pepperstone", client="cli-1"),
        order("9001", t1, "FILLED", "1.2500", adapter="pepperstone", client="cli-1"),
        position("5001", t1 + 2_000, "OPEN", "1.2502", adapter="pepperstone", sl="1.2400", tp="1.2700"),
        position("5001", t2, "OPEN", "1.2502", adapter="pepperstone", sl="1.2450", tp="1.2700"),
        position("5001", t3, "OPEN", "1.2502", adapter="pepperstone", sl="1.2510", tp="1.2700"),
    )
    lc = lcs["pepperstone:9001"]
    assert lc.position_id == "5001" and lc.outcome == "OPEN"
    assert lc.status_trail == ("WORKING", "FILLED", "OPEN")
    assert lc.stop_loss == (Step(t1 + 2_000, t2, d("1.2400")), Step(t2, t3, d("1.2450")), Step(t3, NOW, d("1.2510")))
    assert lc.take_profit == (Step(t1 + 2_000, NOW, d("1.2700")),)
    assert lc.position == (Step(t1, NOW, d("1.2502")),)
    assert lc.close is None and lc.close_result is None


def test_position_without_order_is_its_own_lifecycle() -> None:
    lcs = lifecycles(position("P9", T0, "OPEN", "150.25", side="SELL", symbol="USDJPY"))
    lc = lcs["stonex:P9"]
    assert lc.order_id is None and lc.status_trail == ("OPEN",)
    assert lc.fill == Marker(T0, d("150.25")) and lc.position == (Step(T0, NOW, d("150.25")),)


def test_order_cancelled_after_amend() -> None:
    lcs = lifecycles(
        order("C1", T0, "WORKING", "1.1000", side="SELL"),
        order("C1", T0 + H, "WORKING", "1.1050", side="SELL"),
        order("C1", T0 + 2 * H, "CANCELLED", "1.1050", side="SELL"),
    )
    lc = lcs["stonex:C1"]
    assert lc.status_trail == ("WORKING", "CANCELLED") and lc.outcome == "CANCELLED"
    assert lc.working == (Step(T0, T0 + H, d("1.1000")), Step(T0 + H, T0 + 2 * H, d("1.1050")))
    assert lc.ended == Marker(T0 + 2 * H, d("1.1050"))
    assert lc.fill is None and lc.position == () and lc.close is None


def test_order_rejected() -> None:
    lc = lifecycles(order("R1", T0, "PENDING", "1.3"), order("R1", T0 + 10, "REJECTED", "1.3"))["stonex:R1"]
    assert lc.status_trail == ("PENDING", "REJECTED") and lc.ended == Marker(T0 + 10, d("1.3"))


def test_working_order_runs_to_now() -> None:
    lc = lifecycles(order("W1", T0, "WORKING", "0.6500", sl="0.6400"))["stonex:W1"]
    assert lc.working == (Step(T0, NOW, d("0.6500")),)
    assert lc.stop_loss == (Step(T0, NOW, d("0.6400")),)
    assert lc.outcome == "WORKING"


def test_out_of_order_versions_and_duplicates() -> None:
    """Versions arrive in any order (and the current row repeats the last one): the version key decides."""
    base = [
        order("X1", T0, "WORKING", "1.1000", obs=1, seq=(T0 << 20) + 1),
        order("X1", T0 + H, "WORKING", "1.1050", obs=2, seq=(T0 << 20) + 2),
        order("X1", T0 + 2 * H, "CANCELLED", "1.1050", obs=3, seq=(T0 << 20) + 3),
    ]
    want = lifecycles(*base)
    shuffled = list(base) + [base[2]]          # the current row
    random.Random(7).shuffle(shuffled)
    assert lifecycles(*shuffled) == want
    # a broker-seq'd stream version beats a later-committed REST read of an older state
    tl = build_timeline([order("Y", T0 + 5, "FILLED", "2", broker_seq=9, obs=1),
                         order("Y", T0, "WORKING", "2", broker_seq=3, obs=7, source="REST")])
    assert [s.status for s in tl.states] == ["WORKING", "FILLED"]
    # an event time never moves backwards along the version order
    tl = build_timeline([order("Z", T0 + 100, "WORKING", "2", broker_seq=1),
                         order("Z", T0 + 50, "FILLED", "2", broker_seq=2)])
    assert tl.times == (T0 + 100, T0 + 100)


def test_catch_up_rows_use_broker_time() -> None:
    """Downtime catch-up: REST rows committed hours later, first seen already FILLED / CLOSED."""
    later = T0 + 9 * H
    lcs = lifecycles(
        order("H-RT-1", T0 + 2 * H, "FILLED", "0.6560", side="SELL", source="REST", commit=later),
        position("H-RT-1", T0 + 5 * H, "CLOSED", "0.6560", side="SELL", source="REST", commit=later,
                 client="H-RT-1"),
        order("H-RT-1-X", T0 + 5 * H, "FILLED", "0.6540", side="BUY", reduce_only=True, linked="H-RT-1",
              source="REST", commit=later),
    )
    lc = lcs["stonex:H-RT-1"]
    assert lc.placed is None and lc.working == ()      # never seen working: no placement line
    assert lc.fill == Marker(T0 + 2 * H, d("0.6560"))
    assert lc.position == (Step(T0 + 2 * H, T0 + 5 * H, d("0.6560")),)
    assert lc.close == Marker(T0 + 5 * H, d("0.6540")) and lc.close_result == "FAVOURABLE"
    assert lc.status_trail == ("FILLED", "CLOSED")
    # an untimed row falls back to its commit time
    untimed = order("U", 0, "WORKING", "1", commit=T0 + 7)
    assert untimed.event_ms == T0 + 7 and build_timeline([untimed]).times == (T0 + 7,)


def test_unfavourable_and_unknown_close() -> None:
    lcs = lifecycles(
        order("L1", T0, "FILLED", "1.2000"), position("L1", T0, "OPEN", "1.2000"),
        order("L1-S", T0, "WORKING", "1.1900", side="SELL", reduce_only=True, linked="L1"),
        order("L1-S", T0 + H, "FILLED", "1.1900", side="SELL", reduce_only=True, linked="L1"),
        position("L1", T0 + H, "CLOSED", "1.2000"),
        order("M1", T0, "FILLED", "1.5000"), position("M1", T0, "OPEN", "1.5000"),
        position("M1", T0 + H, "CLOSED", "1.5000"),
    )
    assert lcs["stonex:L1"].close_result == "UNFAVOURABLE" and lcs["stonex:L1"].close == Marker(T0 + H, d("1.19"))
    assert lcs["stonex:M1"].close_result == "UNKNOWN" and lcs["stonex:M1"].close == Marker(T0 + H, d("1.5"))


def test_newest_first_ordering() -> None:
    out = build_lifecycles(build_timelines([order("A", T0, "WORKING", "1"), order("B", T0 + 5, "WORKING", "1")]),
                           NOW)
    assert [lc.key for lc in out] == ["stonex:B", "stonex:A"]


def test_steps_from_points() -> None:
    assert steps_from_points([(10, d("1")), (20, d("1")), (30, None), (40, d("2"))], 50) == (
        Step(10, 30, d("1")), Step(40, 50, d("2")))
    assert steps_from_points([(10, d("1"))], 10) == ()


def test_version_from_row_and_keys() -> None:
    row = ("ORDER", "stonex", "ACC", "7", "EUR/USD", "SELL", "WORKING", "1.0850", None, "1.07", "true", "P1",
           "c-7", None, str(T0), "3", str(T0 << 20), str(T0 + 1), "REST")
    v = version_from_row(row)
    assert v is not None and v.side == "SELL" and v.price == d("1.0850") and v.take_profit == d("1.07")
    assert v.is_reduce_only and v.linked_position_id == "P1" and v.broker_seq is None and v.observed_seq == 3
    assert version_from_row(("ORDER", "stonex", "A", None) + row[4:]) is None      # no id
    assert version_from_row(row[:5] + ("LONG",) + row[6:]) is None                 # no side
    assert version_from_row(row[:6] + ("OPEN",) + row[7:]) is None                 # not an order status
    assert version_from_row(row[:7] + ("NaN",) + row[8:]) is not None             # a bad price is just absent
    okeys, pkeys = related_keys([v])
    assert okeys == [("stonex", "7"), ("stonex", "P1")] and pkeys == [("stonex", "7"), ("stonex", "P1")]


def test_window_and_symbols() -> None:
    w = Window.last_days(7, NOW)
    assert w.until_ms - w.since_ms == 7 * 86_400_000 and w.seq_range == (w.since_ms << 20, (NOW + 1) << 20)
    for bad in (0, 91):
        try:
            Window.last_days(bad, NOW)
        except ValueError:
            continue
        raise AssertionError(bad)
    assert normalise_symbol("eur/usd") == normalise_symbol("EUR_USD") == "EURUSD"


# ------------------------------------------------------------------------------------------------ fidelity
def test_replaced_stop_keeps_the_new_level() -> None:
    """StoneX cancel / replace: the new stop child is placed, then the old one is cancelled. The new stop must
    run on (to now); the old child's cancel ends only its own level."""
    lc = lifecycles(
        order("O1", T0, "FILLED", "1.0800"), position("O1", T0, "OPEN", "1.0800"),
        order("S1", T0, "WORKING", "1.0700", side="SELL", reduce_only=True, linked="O1"),
        order("S2", T0 + H, "WORKING", "1.0750", side="SELL", reduce_only=True, linked="O1"),
        order("S1", T0 + H + 500, "CANCELLED", "1.0700", side="SELL", reduce_only=True, linked="O1"),
    )["stonex:O1"]
    assert lc.stop_loss == (Step(T0, T0 + H, d("1.0700")), Step(T0 + H, NOW, d("1.0750")))
    # the other order (cancel first, then the new one): a gap while no stop was working, as the log says
    lc = lifecycles(
        order("O1", T0, "FILLED", "1.0800"), position("O1", T0, "OPEN", "1.0800"),
        order("S1", T0, "WORKING", "1.0700", side="SELL", reduce_only=True, linked="O1"),
        order("S1", T0 + H, "CANCELLED", "1.0700", side="SELL", reduce_only=True, linked="O1"),
        order("S2", T0 + 2 * H, "WORKING", "1.0750", side="SELL", reduce_only=True, linked="O1"),
    )["stonex:O1"]
    assert lc.stop_loss == (Step(T0, T0 + H, d("1.0700")), Step(T0 + 2 * H, NOW, d("1.0750")))


def test_terminal_protection_order_does_not_end_the_position_stop() -> None:
    """Pepperstone emits a protection (SL/TP) order only once terminal. Its cancel must not end the stop the
    position itself still carries."""
    lc = lifecycles(
        position("P1", T0, "OPEN", "1.2500", adapter="pepperstone", sl="1.2400", tp="1.2700"),
        position("P1", T0 + H, "OPEN", "1.2500", adapter="pepperstone", sl="1.2450", tp="1.2700"),
        order("X1", T0 + H + 300, "CANCELLED", "1.2400", side="SELL", adapter="pepperstone", reduce_only=True,
              linked="P1"),
    )["pepperstone:P1"]
    assert lc.stop_loss == (Step(T0, T0 + H, d("1.2400")), Step(T0 + H, NOW, d("1.2450")))
    assert lc.take_profit == (Step(T0, NOW, d("1.2700")),)
    assert lc.children == ("X1",)


def test_order_levels_hand_over_to_the_position() -> None:
    """The opening order's own SL / TP fields count until its position exists; after that a level the position
    removed is gone (a stale order field never stands in for it)."""
    t1 = T0 + H
    lc = lifecycles(
        order("9001", T0, "WORKING", "1.2500", adapter="pepperstone", sl="1.2400"),
        order("9001", t1, "FILLED", "1.2500", adapter="pepperstone", sl="1.2400"),
        position("5001", t1 + 2_000, "OPEN", "1.2502", adapter="pepperstone", sl="1.2400"),
        position("5001", t1 + H, "OPEN", "1.2502", adapter="pepperstone"),
    )["pepperstone:9001"]
    assert lc.position_id == "5001"
    assert lc.stop_loss == (Step(T0, t1 + H, d("1.2400")),)


def test_position_first_seen_closed_has_no_fill_marker() -> None:
    """A position opened before the window (or closed during downtime, a catch-up row) is first seen CLOSED:
    no fill is invented at the close time; the close (and the label) are drawn there."""
    lc = lifecycles(position("P2", T0 + 2 * H, "CLOSED", "1.2500", adapter="pepperstone", sl="1.2400",
                             source="REST", commit=T0 + 9 * H))["pepperstone:P2"]
    assert lc.fill is None and lc.position == () and lc.stop_loss == ()
    assert lc.close == Marker(T0 + 2 * H, d("1.25")) and lc.close_result == "UNKNOWN"
    assert lc.anchor == lc.close and lc.status_trail == ("CLOSED",)
    # a position first seen OPEN without its order: the marker says where it was first seen
    lc = lifecycles(position("P3", T0, "OPEN", "1.2500", adapter="pepperstone"))["pepperstone:P3"]
    assert lc.fill == Marker(T0, d("1.25")) and lc.fill_seen_only
    # StoneX: the opening order (same id) gives the real fill time, so the position line is drawn
    lc = lifecycles(order("Q1", T0, "FILLED", "1.1000"),
                    position("Q1", T0 + 3 * H, "CLOSED", "1.1001"))["stonex:Q1"]
    assert lc.fill == Marker(T0, d("1.1001")) and not lc.fill_seen_only
    assert lc.position == (Step(T0, T0 + 3 * H, d("1.1001")),)


def test_unlinked_closing_trade_is_matched_by_close_time() -> None:
    """A StoneX closing trade the adapter could not link (two candidate positions) closes the position of the
    same account / symbol / other side whose close is within FILL_MATCH_MS: the close gets its price."""
    lcs = lifecycles(
        order("A1", T0, "FILLED", "1.1000"), position("A1", T0, "OPEN", "1.1000"),
        order("B1", T0 + 60_000, "FILLED", "1.1010"), position("B1", T0 + 60_000, "OPEN", "1.1010"),
        position("A1", T0 + 2 * H, "CLOSED", "1.1000"),
        order("C9", T0 + 2 * H + 800, "FILLED", "1.1050", side="SELL", reduce_only=True),
    )
    assert "stonex:C9" not in lcs                       # not drawn as a stray order of its own
    a1, b1 = lcs["stonex:A1"], lcs["stonex:B1"]
    assert a1.children == ("C9",) and a1.close == Marker(T0 + 2 * H, d("1.105"))
    assert a1.close_result == "FAVOURABLE"
    assert b1.close is None and b1.outcome == "OPEN"
    # nothing closed near the fill: the closing trade is drawn alone (never guessed onto an open position)
    lcs = lifecycles(order("A1", T0, "FILLED", "1.1000"), position("A1", T0, "OPEN", "1.1000"),
                     order("C9", T0 + 2 * H, "FILLED", "1.1050", side="SELL", reduce_only=True))
    assert "stonex:C9" in lcs and lcs["stonex:A1"].children == ()


def test_related_keys_look_up_both_tables_for_a_position() -> None:
    okeys, pkeys = related_keys([position("P7", T0, "CLOSED", "1.1")])
    assert okeys == [("stonex", "P7")] and pkeys == [("stonex", "P7")]


# ------------------------------------------------------------------------------------------------ events
def kinds(lc: Lifecycle) -> list[tuple[int, str, Decimal | None, Decimal | None]]:
    return [(e.t_ms, e.kind, e.price, e.prev) for e in lc.events]


def test_events_round_trip() -> None:
    lc = lifecycles(*stonex_round_trip())["stonex:O1"]
    assert kinds(lc) == [(T0, "PLACED", d("1.0800"), None), (T0 + H, "FILLED", d("1.0801"), None),
                         (T0 + H, "SL_SET", d("1.0750"), None), (T0 + H, "TP_SET", d("1.0900"), None),
                         (T0 + 3 * H, "CLOSED", d("1.0900"), None)]
    assert lc.events[-1].result == "FAVOURABLE"
    # the stop child's cancel after the close is clean-up, not an adjustment
    assert not any(e.kind.endswith("REMOVED") for e in lc.events)


def test_events_every_sl_tp_and_price_change_with_its_broker_time() -> None:
    t1, t2, t3 = T0 + H, T0 + 2 * H, T0 + 4 * H
    lc = lifecycles(
        order("9001", T0, "WORKING", "1.2500", adapter="pepperstone", sl="1.2300"),
        order("9001", T0 + 1_000, "WORKING", "1.2490", adapter="pepperstone", sl="1.2300"),
        order("9001", t1, "FILLED", "1.2490", adapter="pepperstone", sl="1.2300"),
        position("5001", t1 + 2_000, "OPEN", "1.2492", adapter="pepperstone", sl="1.2400", tp="1.2700"),
        position("5001", t2, "OPEN", "1.2492", adapter="pepperstone", sl="1.2450", tp="1.2700"),
        position("5001", t3, "OPEN", "1.2492", adapter="pepperstone", sl="1.2510"),
    )["pepperstone:9001"]
    assert kinds(lc) == [
        (T0, "PLACED", d("1.2500"), None), (T0, "SL_SET", d("1.2300"), None),
        (T0 + 1_000, "PRICE_CHANGED", d("1.2490"), d("1.2500")),
        (t1, "FILLED", d("1.2492"), None),
        (t1 + 2_000, "SL_MOVED", d("1.2400"), d("1.2300")), (t1 + 2_000, "TP_SET", d("1.2700"), None),
        (t2, "SL_MOVED", d("1.2450"), d("1.2400")),
        (t3, "SL_MOVED", d("1.2510"), d("1.2450")), (t3, "TP_REMOVED", d("1.2700"), None)]


def test_events_stonex_stop_replace_and_cancel() -> None:
    lc = lifecycles(
        order("O1", T0, "FILLED", "1.0800"), position("O1", T0, "OPEN", "1.0800"),
        order("S1", T0, "WORKING", "1.0700", side="SELL", reduce_only=True, linked="O1"),
        order("S1", T0 + H, "CANCELLED", "1.0700", side="SELL", reduce_only=True, linked="O1"),
        order("S2", T0 + 2 * H, "WORKING", "1.0750", side="SELL", reduce_only=True, linked="O1"),
    )["stonex:O1"]
    assert kinds(lc) == [(T0, "FILLED", d("1.0800"), None), (T0, "SL_SET", d("1.0700"), None),
                         (T0 + H, "SL_REMOVED", d("1.0700"), None), (T0 + 2 * H, "SL_SET", d("1.0750"), None)]
    c = lifecycles(order("C1", T0, "WORKING", "1.1000", side="SELL"),
                   order("C1", T0 + H, "WORKING", "1.1050", side="SELL"),
                   order("C1", T0 + 2 * H, "CANCELLED", "1.1050", side="SELL"))["stonex:C1"]
    assert kinds(c) == [(T0, "PLACED", d("1.1000"), None), (T0 + H, "PRICE_CHANGED", d("1.1050"), d("1.1000")),
                        (T0 + 2 * H, "CANCELLED", d("1.1050"), None)]
    r = lifecycles(order("R1", T0, "PENDING", "1.3"), order("R1", T0 + 10, "REJECTED", "1.3"))["stonex:R1"]
    assert [e.kind for e in r.events] == ["PLACED", "REJECTED"]


def test_events_close_price_unknown_and_first_seen() -> None:
    lc = lifecycles(position("P2", T0 + 2 * H, "CLOSED", "1.2500", adapter="pepperstone"))["pepperstone:P2"]
    assert kinds(lc) == [(T0 + 2 * H, "CLOSED", None, None)] and lc.events[0].result == "UNKNOWN"
    lc = lifecycles(position("P3", T0, "OPEN", "1.2500", adapter="pepperstone"))["pepperstone:P3"]
    assert kinds(lc) == [(T0, "OPENED", d("1.25"), None)]
