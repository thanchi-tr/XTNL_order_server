"""Regression (xtnl_live, 2026-11-14: 0 boxes): a StoneX trade whose stop-loss / take-profit come only from the
downtime catch-up history now draws its red / green zones from the entry to the exit, because the history gives
each leg a WORKING version at its placement and links it to its entry. The rows go the real way: StoneX history
rows -> the adapter's pure mapping -> event_log payloads (APPLIED, in submit order) -> the report read's rows ->
trades -> the Pine drawing. No network, no database."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from src.adapters.stonex.history import map_stop_limit_history, map_trade_history_window
from src.dto import OrderDTO, PositionDTO
from src.reports.lifecycle import Step, build_lifecycles, build_timelines, build_trades, version_from_row
from src.reports.pinescript import trade_elements
from tests.adapters.test_stonex_history_versions import X, sl_row, trade_row
from tests.reports.builders import T0, log_row

M = 60_000
PLACED = T0 + 10 * M
FILLED = PLACED + 6 * M + 29_000
CLOSED = PLACED + 102 * M
NOW = T0 + 24 * 60 * M


def history_versions() -> list[OrderDTO | PositionDTO]:
    """The live bracket 1038153117 as the catch-up reads it: SELL stop entry X with BUY stop-loss X+1 (hit) and BUY
    take-profit X+2 (cancelled), all placed in one second; trade rows for the entry fill and the stop-loss fill."""
    stops = [("C", sl_row(X, direction="Sell", type_id=2, status=9, created=PLACED, changed=FILLED, level=1.0850)),
             ("C", sl_row(X + 1, direction="Buy", type_id=2, status=9, created=PLACED, changed=CLOSED, level=1.0900)),
             ("C", sl_row(X + 2, direction="Buy", type_id=3, status=4, created=PLACED, changed=CLOSED, level=1.0750))]
    trades = [("C", trade_row(X, direction="Sell", executed=FILLED, changed=CLOSED, opening=[X], price=1.0849)),
              ("C", trade_row(X + 1, direction="Buy", executed=CLOSED, opening=[X], pnl=-1.0, price=1.0901))]
    out = map_trade_history_window(trades, [], since_ms=T0, lookup=lambda _m: None)
    map_stop_limit_history(stops, since_ms=T0, until_ms=NOW, lookup=lambda _m: None, known_parent=lambda _o: None,
                           out=out, trade_rows=trades)
    return [*out.order_versions(), *out.positions.values()]


def logged(rows: list[OrderDTO | PositionDTO]) -> list[tuple[object, ...]]:
    """Each version as the consumers commit it (labelled at the fetch's send time) and the report reads it back."""
    send = (NOW << 20) + 1
    return [log_row(replace(dto, observed_seq=send, seq_id=send + 1 + i), NOW) for i, dto in enumerate(rows)]


def test_a_trade_whose_levels_come_only_from_the_history_draws_its_zones_from_entry_to_exit() -> None:
    versions = [v for v in (version_from_row(r) for r in logged(history_versions())) if v is not None]
    lifecycles = build_lifecycles(build_timelines(versions), NOW)
    trades, pending = build_trades(lifecycles, NOW)
    assert pending == ()
    (t,) = trades
    assert t.order_ids == (str(X), str(X + 1), str(X + 2)) and t.side == "SELL"
    assert (t.entry_ms, t.entry_price, t.entry_kind) == (FILLED, Decimal("1.0849"), "FILLED")
    assert (t.exit_ms, t.exit_price, t.exit_kind) == (CLOSED, Decimal("1.0901"), "SL")
    assert t.stop_loss == (Step(FILLED, CLOSED, Decimal("1.09")),)
    assert t.take_profit == (Step(FILLED, CLOSED, Decimal("1.075")),)
    # the legs were placed with the entry order, before it filled: the life cycle keeps the placement time
    (lc,) = lifecycles
    assert lc.stop_loss[0].start_ms == PLACED and lc.take_profit[0].start_ms == PLACED
    assert lc.placed is not None and lc.placed.t_ms == PLACED
    els = trade_elements(t, since_ms=T0)
    assert els.boxes == 2                                    # one red (stop-loss) and one green (take-profit) zone
