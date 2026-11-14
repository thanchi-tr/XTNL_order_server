"""Trades (pure: no database): one position plus every order that belongs to it, numbered per symbol, with its
stop-loss / take-profit steps, its exit kind and its events; orders that never filled are pending-only items."""
from __future__ import annotations

from decimal import Decimal

from src.reports.lifecycle import (EXIT_MATCH, PendingOrder, StateVersion, Step, Trade, build_lifecycles,
                                   build_timelines, build_trades, exit_kind)

from tests.reports.builders import H, NOW, T0, d, order, position

M = 60_000


def book(*versions: StateVersion) -> tuple[dict[str, Trade], dict[str, PendingOrder]]:
    trades, pending = build_trades(build_lifecycles(build_timelines(versions), NOW), NOW)
    return {t.key: t for t in trades}, {p.key: p for p in pending}


def child(oid: str, t: int, status: str, price: str, *, linked: str = "O1", side: str = "SELL",
          **kw: object) -> StateVersion:
    return order(oid, t, status, price, side=side, reduce_only=True, linked=linked, **kw)  # type: ignore[arg-type]


def bracket_tp() -> list[StateVersion]:
    """StoneX BUY limit O1 placed, filled; SL child S1 and TP child TP1; the SL replaced (S2 placed, S1 cancelled
    half a second later); TP1 fills, S2 is cancelled with the close."""
    return [
        order("O1", T0, "WORKING", "1.0800"),
        order("O1", T0 + H, "FILLED", "1.0800"),
        position("O1", T0 + H, "OPEN", "1.0801", client="O1"),
        child("S1", T0 + H, "WORKING", "1.0750"),
        child("TP1", T0 + H, "WORKING", "1.0900"),
        child("S2", T0 + 2 * H, "WORKING", "1.0780"),
        child("S1", T0 + 2 * H + 500, "CANCELLED", "1.0750"),
        child("TP1", T0 + 3 * H, "FILLED", "1.0900"),
        child("S2", T0 + 3 * H + 300, "CANCELLED", "1.0780"),
        position("O1", T0 + 3 * H, "CLOSED", "1.0801", client="O1"),
    ]


# ------------------------------------------------------------------------------------------------ grouping
def test_entry_sl_tp_replacement_and_close_are_one_trade() -> None:
    trades, pending = book(*bracket_tp())
    assert list(trades) == ["stonex:O1"] and pending == {}
    t = trades["stonex:O1"]
    assert (t.number, t.name, t.side, t.order_id, t.position_id) == (1, "T1", "BUY", "O1", "O1")
    assert t.order_ids == ("O1", "S1", "S2", "TP1")
    assert (t.entry_ms, t.entry_price, t.entry_kind) == (T0 + H, d("1.0801"), "FILLED")
    assert t.placed is not None and t.placed.t_ms == T0 and t.working == (Step(T0, T0 + H, d("1.0800")),)
    # SL from the entry until replaced, the replacement until the exit; TP from the entry to the exit
    assert t.stop_loss == (Step(T0 + H, T0 + 2 * H, d("1.0750")), Step(T0 + 2 * H, T0 + 3 * H, d("1.0780")))
    assert t.take_profit == (Step(T0 + H, T0 + 3 * H, d("1.0900")),)
    assert (t.exit_ms, t.exit_price, t.exit_kind) == (T0 + 3 * H, d("1.0900"), "TP")
    assert t.sl_moves == 1                                   # the replacement; the level at the entry is not a move
    assert [e.kind for e in t.adjustments()] == ["SL_MOVED"]
    assert [e.kind for e in t.events] == ["PLACED", "FILLED", "SL_SET", "TP_SET", "SL_MOVED", "CLOSED"]


def test_closing_order_is_part_of_the_trade_and_a_manual_close_is_closed() -> None:
    """A manual close: both bracket legs are cancelled and a separate closing order fills between them."""
    v = [x for x in bracket_tp() if not (x.entity_id == "TP1" and x.status == "FILLED")
         and not (x.entity_id == "S2" and x.status == "CANCELLED")]
    v += [child("TP1", T0 + 3 * H + 200, "CANCELLED", "1.0900"),
          child("S2", T0 + 3 * H + 300, "CANCELLED", "1.0780"),
          child("C1", T0 + 3 * H, "FILLED", "1.0850")]
    t = book(*v)[0]["stonex:O1"]
    assert t.order_ids == ("C1", "O1", "S1", "S2", "TP1")
    assert (t.exit_price, t.exit_kind) == (d("1.0850"), "CLOSED")


def test_sl_hit_within_tolerance_and_open_trade() -> None:
    v = [x for x in bracket_tp() if x.entity_id not in ("TP1", "S2") and x.status != "CLOSED"]
    v += [child("S1", T0 + 2 * H, "FILLED", "1.07495"), child("TP1", T0 + H, "WORKING", "1.0900"),
          child("TP1", T0 + 2 * H + 100, "CANCELLED", "1.0900"),
          position("O1", T0 + 2 * H, "CLOSED", "1.0801", client="O1")]
    t = book(*v)[0]["stonex:O1"]
    assert (t.exit_price, t.exit_kind) == (d("1.07495"), "SL")      # slipped half a tick past the stop
    assert t.stop_loss == (Step(T0 + H, T0 + 2 * H, d("1.0750")),)
    # still open: the exit is the export time, no exit price; the levels run to it
    t = book(*bracket_tp()[:6])[0]["stonex:O1"]
    assert (t.exit_ms, t.exit_price, t.exit_kind) == (NOW, None, "OPEN")
    assert t.stop_loss[-1] == Step(T0 + 2 * H, NOW, d("1.0780")) and t.take_profit[-1].end_ms == NOW


def test_exit_kind_tolerance() -> None:
    levels = {"SL": [d("1.1000")], "TP": [d("1.0900")]}
    assert exit_kind(d("1.1000") + d("1.1") * EXIT_MATCH, levels) == "SL"
    assert exit_kind(d("1.1003"), levels) == "CLOSED"
    assert exit_kind(d("1.08999"), levels) == "TP" and exit_kind(None, levels) == "CLOSED"
    assert exit_kind(d("1.0950"), {"SL": [], "TP": []}) == "CLOSED"


def test_pepperstone_levels_on_the_position() -> None:
    """cTrader: the opening order carries no position id (matched by its fill time), SL / TP live on the position
    and are moved there; the position closes at its take-profit."""
    pep = dict(adapter="pepperstone", side="SELL")
    trades, _ = book(
        order("9001", T0, "WORKING", "1.2500", client="c1", **pep),         # type: ignore[arg-type]
        order("9001", T0 + H, "FILLED", "1.2500", client="c1", **pep),      # type: ignore[arg-type]
        position("5001", T0 + H + 2_000, "OPEN", "1.2498", sl="1.2600", tp="1.2300", **pep),   # type: ignore[arg-type]
        position("5001", T0 + 2 * H, "OPEN", "1.2498", sl="1.2550", tp="1.2300", **pep),       # type: ignore[arg-type]
        position("5001", T0 + 4 * H, "CLOSED", "1.2498", sl="1.2550", tp="1.2300", **pep),     # type: ignore[arg-type]
        order("9002", T0 + 4 * H, "FILLED", "1.2301", side="BUY", adapter="pepperstone", reduce_only=True,
              linked="5001"),
    )
    t = trades["pepperstone:9001"]
    assert t.position_id == "5001" and t.order_ids == ("9001", "9002")
    assert t.stop_loss == (Step(T0 + H + 2_000, T0 + 2 * H, d("1.26")), Step(T0 + 2 * H, T0 + 4 * H, d("1.255")))
    assert (t.exit_price, t.exit_kind) == (d("1.2301"), "TP")


def test_pending_only_orders_are_separate() -> None:
    trades, pending = book(*bracket_tp(),
                           order("X1", T0 + M, "WORKING", "1.0700"), order("X1", T0 + 5 * M, "CANCELLED", "1.0700"),
                           order("W1", T0 + 9 * M, "WORKING", "1.0650", sl="1.0600", tp="1.0750"),
                           order("R1", T0 + 9 * M, "PENDING", "1.3"), order("R1", T0 + 9 * M + 10, "REJECTED", "1.3"))
    assert list(trades) == ["stonex:O1"]
    assert set(pending) == {"stonex:X1", "stonex:W1", "stonex:R1"}
    x, w, r = pending["stonex:X1"], pending["stonex:W1"], pending["stonex:R1"]
    assert (x.outcome, x.first_ms, x.end_ms, x.price, x.placed_seen) == ("CANCELLED", T0 + M, T0 + 5 * M,
                                                                         d("1.07"), True)
    assert x.steps == (Step(T0 + M, T0 + 5 * M, d("1.07")),)
    assert (w.outcome, w.end_ms, w.stop_loss, w.take_profit) == ("WORKING", NOW, d("1.06"), d("1.075"))
    assert r.outcome == "REJECTED"


def test_numbering_per_symbol_in_entry_order() -> None:
    v: list[StateVersion] = []
    for i, (sym, t) in enumerate((("EURUSD", T0 + 3 * H), ("EURUSD", T0), ("USDJPY", T0 + H), ("EUR/USD", T0 + H))):
        oid = f"N{i}"
        v += [order(oid, t, "FILLED", "1.1", symbol=sym), position(oid, t, "OPEN", "1.1", symbol=sym)]
    trades, _ = book(*v)
    assert {k: t.name for k, t in trades.items()} == {"stonex:N1": "T1", "stonex:N3": "T2", "stonex:N0": "T3",
                                                       "stonex:N2": "T1"}
    ordered = build_trades(build_lifecycles(build_timelines(v), NOW), NOW)[0]
    assert [t.key for t in ordered] == ["stonex:N0", "stonex:N2", "stonex:N3", "stonex:N1"]   # newest entry first


# ------------------------------------------------------------------------------------------------ catch-up shapes
def catch_up_trade(base: int, oid: int, *, side: str = "SELL") -> list[StateVersion]:
    """What a downtime catch-up reads for a StoneX bracket that hit its stop (the real week's shape): the opening
    trade marked reduce-only and linked to itself, stamped with the close time; the stop leg seen only filled;
    the take-profit leg seen only cancelled, unlinked; the position closed with no entry price."""
    o, sl, tp = str(oid), str(oid + 1), str(oid + 2)
    other = "BUY" if side == "SELL" else "SELL"
    rest = dict(source="REST", commit=base + 9 * H)
    return [
        order(o, base, "FILLED", "1.10225", side=side, reduce_only=True, linked=o, **rest),     # type: ignore[arg-type]
        order(sl, base, "FILLED", "1.10234", side=other, reduce_only=True, linked=o, **rest),   # type: ignore[arg-type]
        order(tp, base, "CANCELLED", "1.09764", side=other, **rest),                            # type: ignore[arg-type]
        position(o, base, "CLOSED", None, side=side, client=o, **rest),                          # type: ignore[arg-type]
    ]


def test_catch_up_bracket_is_one_trade_with_an_unrecorded_entry_time() -> None:
    trades, pending = book(*catch_up_trade(T0, 1038153117), *catch_up_trade(T0 + 1_500, 1038153178))
    assert pending == {} and set(trades) == {"stonex:1038153117", "stonex:1038153178"}
    t = trades["stonex:1038153117"]
    assert t.order_ids == ("1038153117", "1038153118", "1038153119") and t.side == "SELL"
    assert (t.entry_ms, t.entry_price, t.entry_kind) == (T0, d("1.10225"), "NOT_RECORDED")
    assert (t.exit_ms, t.exit_price, t.exit_kind) == (T0, d("1.10234"), "SL")
    # no time span: the levels resting at the exit, one step each (drawn at a minimum width by the script)
    assert t.stop_loss == (Step(T0, T0, d("1.10234")),) and t.take_profit == (Step(T0, T0, d("1.09764")),)
    # the opening trade's row carries the close time: no 'filled <time>' event, and the stop-loss changes while
    # the trade ran were not seen (unknown, not zero)
    assert [e.kind for e in t.events] == ["CLOSED"] and t.sl_moves is None
    # the two closes 1.5 s apart: each cancelled leg goes to the nearest close (then the nearest id)
    assert trades["stonex:1038153178"].order_ids == ("1038153178", "1038153179", "1038153180")


def test_position_seen_open_before_a_late_fill_row() -> None:
    """The live trade of the screenshot: the position first seen OPEN (catch-up), its opening trade row stamped
    with the close time; the SL leg set later and hit; the TP leg linked only in its first version (the catch-up
    row that cancelled it dropped the link)."""
    rest = dict(source="REST")
    t0 = T0 + 10 * H
    trades, pending = book(
        position("936", t0, "OPEN", "1.10400", side="SELL", client="936", **rest),             # type: ignore[arg-type]
        order("938", t0, "WORKING", "1.10120", side="BUY", reduce_only=True, linked="936", **rest),  # type: ignore[arg-type]
        order("937", t0 + 27 * M, "WORKING", "1.10450", side="BUY", reduce_only=True, linked="936", **rest),  # type: ignore[arg-type]
        order("937", t0 + 100 * M, "FILLED", "1.10450", side="BUY", reduce_only=True, linked="936", **rest),  # type: ignore[arg-type]
        order("938", t0 + 100 * M, "CANCELLED", "1.10120", side="BUY", **rest),                 # type: ignore[arg-type]
        order("936", t0 + 100 * M, "FILLED", "1.10400", side="SELL", reduce_only=True, linked="936", **rest),  # type: ignore[arg-type]
        position("936", t0 + 100 * M, "CLOSED", "1.10400", side="SELL", client="936", **rest),  # type: ignore[arg-type]
    )
    assert pending == {}
    t = trades["stonex:936"]
    assert (t.entry_ms, t.entry_kind, t.order_ids) == (t0, "SEEN_OPEN", ("936", "937", "938"))
    assert t.stop_loss == (Step(t0 + 27 * M, t0 + 100 * M, d("1.10450")),)
    assert t.take_profit == (Step(t0, t0 + 100 * M, d("1.10120")),)
    assert (t.exit_ms, t.exit_price, t.exit_kind, t.sl_moves) == (t0 + 100 * M, d("1.10450"), "SL", 1)
    assert [e.kind for e in t.events] == ["OPENED", "TP_SET", "SL_SET", "CLOSED"]


def test_pending_catch_up_bracket_is_one_item() -> None:
    """A bracket that never filled, read by a catch-up (no links): entry X, legs X+1 / X+2 on the other side,
    cancelled together -> one pending item with its SL / TP levels."""
    rest = dict(source="REST")
    _, pending = book(
        order("1038129421", T0, "CANCELLED", "1.10000", side="SELL", **rest),     # type: ignore[arg-type]
        order("1038129422", T0, "CANCELLED", "1.10059", side="BUY", **rest),      # type: ignore[arg-type]
        order("1038129423", T0, "CANCELLED", "1.09591", side="BUY", **rest),      # type: ignore[arg-type]
        # not a leg: same side as the entry
        order("1038129500", T0 + H, "CANCELLED", "1.10000", side="SELL", **rest),  # type: ignore[arg-type]
        order("1038129501", T0 + H, "CANCELLED", "1.10100", side="SELL", **rest),  # type: ignore[arg-type]
        # not a leg: cancelled at another time
        order("1038129600", T0 + 2 * H, "CANCELLED", "1.10000", side="SELL", **rest),  # type: ignore[arg-type]
        order("1038129601", T0 + 3 * H, "CANCELLED", "1.10100", side="BUY", **rest),   # type: ignore[arg-type]
    )
    assert set(pending) == {"stonex:1038129421", "stonex:1038129500", "stonex:1038129501", "stonex:1038129600",
                            "stonex:1038129601"}
    p = pending["stonex:1038129421"]
    assert p.legs == ("1038129422", "1038129423") and not p.placed_seen and p.steps == ()
    assert (p.price, p.stop_loss, p.take_profit, p.outcome) == (d("1.1"), d("1.10059"), d("1.09591"), "CANCELLED")


def test_unrelated_order_cancelled_far_from_a_close_stays_pending() -> None:
    v = catch_up_trade(T0, 1038153117)
    v = [x for x in v if x.entity_id != "1038153119"]
    v.append(order("77", T0 + H, "CANCELLED", "1.09764", side="BUY", source="REST"))
    trades, pending = book(*v)
    assert trades["stonex:1038153117"].order_ids == ("1038153117", "1038153118")
    assert set(pending) == {"stonex:77"}
    # the stop leg's fill without a cancelled other leg: the exit is a close, not a proven stop
    assert trades["stonex:1038153117"].exit_kind == "CLOSED"


def test_pending_bracket_cancelled_with_a_close_is_not_taken_for_its_leg() -> None:
    """A catch-up bracket that never filled (BUY entry, SELL legs), cancelled a second after a SELL trade closed:
    it stays one pending item, and the trade keeps only its own three orders."""
    rest = dict(source="REST")
    trades, pending = book(
        *catch_up_trade(T0, 1038153117),
        order("1038153130", T0 + 1_000, "CANCELLED", "1.09900", side="BUY", **rest),   # type: ignore[arg-type]
        order("1038153131", T0 + 1_000, "CANCELLED", "1.09850", side="SELL", **rest),  # type: ignore[arg-type]
        order("1038153132", T0 + 1_000, "CANCELLED", "1.10200", side="SELL", **rest),  # type: ignore[arg-type]
    )
    assert trades["stonex:1038153117"].order_ids == ("1038153117", "1038153118", "1038153119")
    assert set(pending) == {"stonex:1038153130"}
    assert pending["stonex:1038153130"].legs == ("1038153131", "1038153132")


def test_closing_fill_without_its_position_is_a_pending_item() -> None:
    _, pending = book(order("C9", T0, "FILLED", "1.1050", side="SELL", reduce_only=True))
    assert pending["stonex:C9"].outcome == "FILLED"


def test_position_first_seen_closed_without_an_order() -> None:
    trades, _ = book(position("P2", T0 + 2 * H, "CLOSED", "1.2500", adapter="pepperstone", source="REST"))
    t = trades["pepperstone:P2"]
    assert (t.entry_kind, t.entry_price, t.exit_price, t.exit_kind) == ("NOT_RECORDED", None, None, "CLOSED")
    assert t.stop_loss == () and t.order_ids == ()


def test_sl_moves_counts_changes_inside_the_trade_only() -> None:
    t = book(*bracket_tp())[0]["stonex:O1"]
    assert t.sl_moves == 1 and isinstance(t.events[0].price, Decimal)
