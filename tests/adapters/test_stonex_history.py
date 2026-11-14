"""StoneXAdapter.fetch_history: orders placed and finished while the engine was off are reconstructed from
order/tradehistory (+ the optional stop/limit order history), the open positions and the active orders, read-only
and at RestPriority.RECONCILE. No network I/O (fake CIAPI)."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

from src.adapters.base import cash_flows_partial
from src.adapters.ratelimit import RestBudget, RestPriority, rest_priority
from src.adapters.stonex.adapter import StoneXAdapter
from src.dto import Decision, OrderStatus, decide

from .conftest import AUDUSD_ID, EURUSD_ID, TRADING, FakeStoneX, make_adapter, wcf

T0 = 1_767_225_600_000                    # last known active time (the engine went off here)
UNTIL = T0 + 3_600_000                     # the engine starts again an hour later


def trade(order_id: int, ts: int, *, direction: str = "Buy", market: str = EURUSD_ID, qty: int = 10000,
          opening: list[int] | None = None, pnl: float | None = None, price: float = 1.087) -> dict[str, Any]:
    row: dict[str, Any] = {"OrderId": order_id, "OpeningOrderIds": opening or [], "MarketId": int(market),
                           "Direction": direction, "OriginalQuantity": qty, "Quantity": qty, "Price": price,
                           "TradingAccountId": int(TRADING), "ExecutedDateTimeUtc": wcf(ts),
                           "LastChangedDateTimeUtc": wcf(ts)}
    if pnl is not None:
        row["RealisedPnl"] = pnl
        row["RealisedPnlCurrency"] = "AUD"
    return row


def open_position(order_id: int, ts: int, *, direction: str = "Buy", market: str = EURUSD_ID,
                  qty: int = 10000) -> dict[str, Any]:
    return {"OrderId": order_id, "MarketId": int(market), "Direction": direction, "Quantity": qty, "Price": 1.087,
            "TradingAccountId": int(TRADING), "LastChangedDateTimeUtc": wcf(ts)}


def offline_book(fake: FakeStoneX) -> None:
    """While off: 9001 bought and still open; 9002 sold and later closed by 9003 (no RealisedPnl, live finding)."""
    fake.history = [
        trade(9001, T0 + 60_000),
        trade(9002, T0 + 120_000, direction="Sell", market=AUDUSD_ID, qty=20000, price=0.656),
        trade(9003, T0 + 300_000, direction="Buy", market=AUDUSD_ID, qty=20000, opening=[9002], price=0.654),
    ]
    fake.positions = [open_position(9001, T0 + 60_000)]


async def connected(fake: FakeStoneX, **kwargs: Any) -> StoneXAdapter:
    adapter = make_adapter(fake, **kwargs)
    await adapter.connect()
    return adapter


async def test_orders_filled_and_positions_closed_while_off_are_reconstructed(fake_broker: FakeStoneX) -> None:
    offline_book(fake_broker)
    adapter = await connected(fake_broker)
    h = await adapter.fetch_history(T0, UNTIL)
    orders = {o.broker_order_id: o for o in h.orders}
    assert set(orders) == {"9001", "9002", "9003"}
    assert all(o.status == OrderStatus.FILLED and o.remaining_units == 0 and o.source == "REST"
               for o in orders.values())
    assert orders["9001"].symbol == "EURUSD" and orders["9001"].ordered_units == Decimal(10000)
    assert not orders["9002"].is_reduce_only and orders["9002"].side == "SELL"
    closing = orders["9003"]
    assert closing.is_reduce_only and closing.linked_position_id == "9002"     # without RealisedPnl too
    positions = {p.position_id: p for p in h.positions}
    assert positions["9001"].status == "OPEN" and positions["9001"].units == Decimal(10000)
    closed = positions["9002"]
    assert closed.status == "CLOSED" and closed.units == 0 and closed.side == "SELL" and closed.symbol == "AUDUSD"
    assert closed.broker_updated_at_ms == T0 + 300_000 and closed.entry_price == Decimal("0.656")
    # each historic row carries the broker time of the event it describes
    assert orders["9001"].broker_updated_at_ms == T0 + 60_000
    assert orders["9003"].broker_updated_at_ms == T0 + 300_000
    # fail closed: without the stop/limit history, stop/limit orders cancelled while off cannot be listed
    assert not h.complete
    assert h.notes == ("stop/limit orders cancelled while offline cannot be listed "
                       "(StoneX stop/limit order history is off)",)
    assert h.covered_from_ms == T0 and (h.since_ms, h.until_ms) == (T0, UNTIL)
    assert adapter.unmapped_rows["orders"] == 0
    assert not h.retryable                       # a switch / broker limit: a re-read would repeat it
    assert "/TradingAPI/order/stoplimitorderhistory" not in fake_broker.paths()
    tradehistory = [r for r in fake_broker.requests if r.url.path == "/TradingAPI/order/tradehistory"]
    assert tradehistory[0].url.params["from"] == str(T0 // 1000)
    assert tradehistory[0].url.params["TradingAccountId"] == TRADING
    # the open positions are read after the history (the newer truth decides open vs closed)
    paths = fake_broker.paths()
    assert paths.index("/TradingAPI/order/openpositions") > paths.index("/TradingAPI/order/tradehistory")
    assert adapter.history_retention_ms is None
    await adapter.close()


async def test_closing_trade_without_realised_pnl_keeps_rows_and_only_cash_flows_go_partial(
        fake_broker: FakeStoneX) -> None:
    offline_book(fake_broker)
    adapter = await connected(fake_broker, stop_limit_history=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert h.complete and h.notes == ()
    assert {p.position_id: p.status for p in h.positions} == {"9001": "OPEN", "9002": "CLOSED"}
    flows = await adapter.fetch_cash_flows(T0)
    assert cash_flows_partial(flows) and "9003" in cash_flows_partial(flows)[0]
    await adapter.close()


async def test_stop_order_placed_and_cancelled_while_off_comes_from_the_stop_limit_history(
        fake_broker: FakeStoneX) -> None:
    fake_broker.stop_limit_history = [
        {"OrderId": 9100, "MarketId": int(EURUSD_ID), "Direction": "Sell", "OriginalQuantity": 5000,
         "TriggerPrice": 1.079, "TradingAccountId": int(TRADING), "StatusId": 4,
         "LastChangedDateTimeUtc": wcf(T0 + 600_000)},
        {"OrderId": 9101, "MarketId": int(EURUSD_ID), "Direction": "Buy", "OriginalQuantity": 5000,
         "TriggerPrice": 1.09, "TradingAccountId": int(TRADING), "StatusId": 5,
         "LastChangedDateTimeUtc": wcf(T0 - 600_000)},                       # ended before the window
    ]
    adapter = await connected(fake_broker, stop_limit_history=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert h.complete
    assert [(o.broker_order_id, o.status) for o in h.orders] == [("9100", OrderStatus.CANCELLED)]
    stop = h.orders[0]
    assert stop.entry_price == Decimal("1.079") and stop.ordered_units == Decimal(5000)
    assert stop.broker_updated_at_ms == T0 + 600_000 and not stop.is_reduce_only
    req = next(r for r in fake_broker.requests if r.url.path == "/TradingAPI/order/stoplimitorderhistory")
    assert req.url.params["TradingAccountId"] == TRADING
    await adapter.close()


def stop_row(order_id: int, ts: int) -> dict[str, Any]:
    return {"OrderId": order_id, "MarketId": int(EURUSD_ID), "Direction": "Sell", "OriginalQuantity": 5000,
            "TriggerPrice": 1.079, "TradingAccountId": int(TRADING), "StatusId": 4,
            "LastChangedDateTimeUtc": wcf(ts)}


async def test_stop_limit_history_is_read_from_the_window_start_and_paged_oldest_first(
        fake_broker: FakeStoneX) -> None:
    """Live-confirmed (2026-11-14): ``from`` filters by last change and a capped answer is oldest-first, so a
    full page is followed from its last row's second until a page is not full; overlaps are deduplicated."""
    fake_broker.stop_limit_history = [stop_row(9100 + i, T0 + 600_000 + 2_000 * i) for i in range(5)]
    fake_broker.stop_limit_history.append(stop_row(9090, T0 - 600_000))       # before the window: never read
    adapter = await connected(fake_broker, stop_limit_history=True, history_page_size=2,
                              history_paging_confirmed=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert h.complete and h.covered_from_ms == T0
    assert sorted(o.broker_order_id for o in h.orders) == ["9100", "9101", "9102", "9103", "9104"]
    reqs = [r for r in fake_broker.requests if r.url.path == "/TradingAPI/order/stoplimitorderhistory"]
    assert reqs[0].url.params["from"] == str(T0 // 1000) and len(reqs) >= 3      # followed page by page
    await adapter.close()


async def test_stop_limit_history_that_never_stops_being_full_is_incomplete(fake_broker: FakeStoneX) -> None:
    fake_broker.stop_limit_history = [stop_row(9100 + i, T0 + 600_000 + 2_000 * i) for i in range(10)]
    adapter = await connected(fake_broker, stop_limit_history=True, history_page_size=2, history_max_pages=2)
    h = await adapter.fetch_history(T0, UNTIL)
    assert not h.complete and not h.retryable
    assert any("stop/limit order history" in n and "still full" in n for n in h.notes)
    await adapter.close()


async def test_a_full_stop_limit_page_with_an_untimed_row_is_incomplete(fake_broker: FakeStoneX) -> None:
    untimed = stop_row(9112, T0 + 600_000)
    del untimed["LastChangedDateTimeUtc"]
    adapter = await connected(fake_broker, stop_limit_history=True, history_page_size=2)

    async def full_page(trading_id: str, from_s: int, size: int = 200) -> list[dict[str, Any]]:
        return [untimed, stop_row(9110, T0 + 700_000)]

    adapter._rest.stop_limit_order_history = full_page        # type: ignore[method-assign]  # noqa: SLF001
    h = await adapter.fetch_history(T0, UNTIL)
    assert not h.complete and any("untimed" in n for n in h.notes)
    await adapter.close()


async def test_a_working_order_placed_while_off_is_listed(fake_broker: FakeStoneX) -> None:
    fake_broker.actives = [
        {"OrderId": 9200, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 3000, "TriggerPrice": 1.07,
         "TradingAccountId": int(TRADING), "LastChangedDateTimeUtc": wcf(T0 + 900_000)},
        {"OrderId": 9201, "MarketId": int(EURUSD_ID), "Direction": "Buy", "Quantity": 3000, "TriggerPrice": 1.06,
         "TradingAccountId": int(TRADING), "LastChangedDateTimeUtc": wcf(T0 - 86_400_000)},   # unchanged since
    ]
    adapter = await connected(fake_broker, stop_limit_history=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert [(o.broker_order_id, o.status) for o in h.orders] == [("9200", OrderStatus.WORKING)]
    await adapter.close()


async def test_unmappable_history_rows_are_counted_and_make_it_incomplete(fake_broker: FakeStoneX) -> None:
    offline_book(fake_broker)
    bad = trade(9009, T0 + 30_000)
    del bad["Direction"]
    untimed = trade(9010, T0 + 30_000)
    del untimed["ExecutedDateTimeUtc"], untimed["LastChangedDateTimeUtc"]
    fake_broker.history = [*fake_broker.history, bad, untimed]
    adapter = await connected(fake_broker, stop_limit_history=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert not h.complete and "2 history row(s) could not be mapped" in h.notes
    # the read's own channel, never the live-stream counter (else the stream would look interrupted)
    assert h.unmapped == 2 and h.retryable and adapter.unmapped_rows["orders"] == 0
    assert {o.broker_order_id for o in h.orders} == {"9001", "9002", "9003"}   # the good rows still map
    await adapter.close()


async def test_a_position_neither_open_nor_closed_by_a_trade_is_never_closed_on_absence(
        fake_broker: FakeStoneX) -> None:
    fake_broker.history = [trade(9001, T0 + 60_000)]
    fake_broker.positions = []                     # not open any more, yet no closing trade names it
    adapter = await connected(fake_broker, stop_limit_history=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert [o.broker_order_id for o in h.orders] == ["9001"] and h.positions == ()
    assert not h.complete
    assert h.notes == ("position 9001 opened in the window is not open and no closing trade names it",)
    assert h.retryable                           # evidence that did not add up between reads: read again
    await adapter.close()


async def test_a_closing_trade_without_opening_ids_is_a_broker_limit_never_a_retry(
        fake_broker: FakeStoneX) -> None:
    """The row itself lacks OpeningOrderIds: every re-read returns it again. Retryable would hold adapter_coverage
    on this window for good (every restart and every hourly retry re-reads it and pages again, never complete)."""
    fake_broker.history = [trade(9001, T0 + 60_000),
                           trade(9005, T0 + 90_000, direction="Sell", pnl=1.0)]     # a close naming nothing
    fake_broker.positions = []
    adapter = await connected(fake_broker, stop_limit_history=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert not h.complete
    assert any("closing trade 9005 names no opening order" in n for n in h.notes)
    # 9001 is unexplained, but an unnamed close of the same window may be its close: the same broker limit
    assert any("position 9001 opened in the window is not open" in n for n in h.notes)
    assert h.retryable is False
    await adapter.close()


async def test_an_unnamed_close_alone_is_a_broker_limit(fake_broker: FakeStoneX) -> None:
    fake_broker.history = [trade(9007, T0 + 60_000, direction="Sell", pnl=-2.0)]
    adapter = await connected(fake_broker, stop_limit_history=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert not h.complete and h.retryable is False
    assert [o.broker_order_id for o in h.orders] == ["9007"]
    await adapter.close()


async def test_a_partial_close_leaves_the_position_open(fake_broker: FakeStoneX) -> None:
    fake_broker.history = [trade(9001, T0 + 60_000),
                           trade(9005, T0 + 90_000, direction="Sell", qty=4000, opening=[9001], pnl=1.0)]
    fake_broker.positions = [open_position(9001, T0 + 90_000, qty=6000)]
    adapter = await connected(fake_broker, stop_limit_history=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert h.complete
    assert [(p.position_id, p.status, p.units) for p in h.positions] == [("9001", "OPEN", Decimal(6000))]
    await adapter.close()


async def test_a_closing_trade_of_a_position_opened_before_the_window(fake_broker: FakeStoneX) -> None:
    fake_broker.history = [trade(9007, T0 + 60_000, direction="Sell", opening=[8000])]
    adapter = await connected(fake_broker, stop_limit_history=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert h.complete
    [closed] = h.positions
    assert (closed.position_id, closed.status, closed.side, closed.entry_price) == ("8000", "CLOSED", "BUY", None)
    await adapter.close()


async def test_oldest_first_pages_are_followed_until_the_window_is_covered(fake_broker: FakeStoneX) -> None:
    fake_broker.history_paging = "asc"
    fake_broker.history = [trade(9300 + i, T0 + 1_000 * (i + 1), opening=[8000 + i], direction="Sell")
                           for i in range(5)]
    adapter = await connected(fake_broker, stop_limit_history=True, history_page_size=2,
                              history_paging_confirmed=True)
    h = await adapter.fetch_history(T0, UNTIL)
    assert h.complete
    assert {o.broker_order_id for o in h.orders} == {str(9300 + i) for i in range(5)}
    froms = [int(r.url.params["from"]) for r in fake_broker.requests if r.url.path.endswith("/tradehistory")]
    assert froms == sorted(froms) and len(froms) >= 3 and froms[0] == T0 // 1000
    await adapter.close()


async def test_following_a_page_is_incomplete_until_the_from_filter_time_is_confirmed(
        fake_broker: FakeStoneX) -> None:
    """Which time 'from' filters on is unconfirmed: a row executed before a page boundary but changed after it
    could fall between two pages, so a paged answer is not proven complete (fail closed)."""
    fake_broker.history_paging = "asc"
    fake_broker.history = [trade(9300 + i, T0 + 1_000 * (i + 1), opening=[8000 + i], direction="Sell")
                           for i in range(5)]
    adapter = await connected(fake_broker, stop_limit_history=True, history_page_size=2)
    h = await adapter.fetch_history(T0, UNTIL)
    assert {o.broker_order_id for o in h.orders} == {str(9300 + i) for i in range(5)}     # still read in full
    assert not h.complete and h.covered_from_ms == T0
    assert not h.retryable                       # the unconfirmed paging rule: a broker limit
    assert sum("paged by time while the 'from' filter time is unconfirmed" in n for n in h.notes) == 1
    await adapter.close()


async def test_a_single_full_page_needs_no_paging_note(fake_broker: FakeStoneX) -> None:
    fake_broker.history_paging = "asc"
    fake_broker.history = [trade(9300 + i, T0 + 1_000 * (i + 1), opening=[8000 + i], direction="Sell")
                           for i in range(2)]
    adapter = await connected(fake_broker, stop_limit_history=True, history_page_size=3)
    h = await adapter.fetch_history(T0, UNTIL)
    assert h.complete and h.notes == ()
    await adapter.close()


async def test_a_full_newest_first_page_reaching_back_to_the_window_start_covers_it(
        fake_broker: FakeStoneX) -> None:
    fake_broker.history_paging = "desc"
    fake_broker.history = [trade(9400, T0, opening=[8100], direction="Sell"),
                           trade(9401, T0 + 1_000, opening=[8101], direction="Sell")]
    adapter = await connected(fake_broker, stop_limit_history=True, history_page_size=2)
    h = await adapter.fetch_history(T0, UNTIL)
    assert h.complete and h.covered_from_ms == T0 and h.notes == ()
    assert {o.broker_order_id for o in h.orders} == {"9400", "9401"}
    await adapter.close()


async def test_a_full_newest_first_page_is_incomplete_and_covers_only_its_rows(fake_broker: FakeStoneX) -> None:
    fake_broker.history_paging = "desc"
    fake_broker.history = [trade(9300 + i, T0 + 1_000 * (i + 1), opening=[8000 + i], direction="Sell")
                           for i in range(5)]
    adapter = await connected(fake_broker, stop_limit_history=True, history_page_size=2)
    h = await adapter.fetch_history(T0, UNTIL)
    assert not h.complete and h.covered_from_ms == T0 + 4_000
    assert {o.broker_order_id for o in h.orders} == {"9303", "9304"}
    await adapter.close()


async def test_the_page_limit_leaves_the_window_incomplete(fake_broker: FakeStoneX) -> None:
    fake_broker.history_paging = "asc"
    fake_broker.history = [trade(9300 + i, T0 + 1_000 * (i + 1), opening=[8000 + i], direction="Sell")
                           for i in range(6)]
    adapter = await connected(fake_broker, stop_limit_history=True, history_page_size=2, history_max_pages=2)
    h = await adapter.fetch_history(T0, UNTIL)
    assert not h.complete and any("still full after 2 pages" in n for n in h.notes)
    assert not h.retryable
    await adapter.close()


async def test_history_reads_run_at_reconcile_priority_on_the_adapter_budget(fake_broker: FakeStoneX) -> None:
    offline_book(fake_broker)
    budget = RestBudget("stonex", rate_per_s=1000.0, burst=1000)
    adapter = await connected(fake_broker, rest_budget=budget)
    before = budget.stats().requests
    with rest_priority(RestPriority.POLL):         # even when the caller tags a background class
        await adapter.fetch_history(T0, UNTIL)
    after = budget.stats().requests
    assert after["reconcile"] - before["reconcile"] >= 3 and after["poll"] == before["poll"]
    await adapter.close()


async def test_a_historic_row_never_beats_a_stream_update_it_predates(fake_broker: FakeStoneX) -> None:
    offline_book(fake_broker)
    adapter = await connected(fake_broker)
    h = await adapter.fetch_history(T0, UNTIL)
    historic = next(p for p in h.positions if p.position_id == "9001")
    order = next(o for o in h.orders if o.broker_order_id == "9001")
    # a stream frame of a later broker change of the same entities (e.g. after the engine is back)
    stream_pos = replace(historic, units=Decimal(5000), raw_qty=Decimal(5000),
                         broker_updated_at_ms=T0 + 60_000 + 1, source="STREAM")
    assert decide(stream_pos, historic) == Decision.DROP_STALE
    stream_order = replace(order, entry_price=Decimal("1.0875"), broker_updated_at_ms=order.broker_updated_at_ms + 1,
                           source="STREAM")
    assert decide(stream_order, order) == Decision.DROP_STALE
    await adapter.close()


async def test_no_history_read_touches_an_order_changing_path(fake_broker: FakeStoneX) -> None:
    offline_book(fake_broker)
    adapter = await connected(fake_broker, stop_limit_history=True)
    await adapter.fetch_history(T0, UNTIL)
    sent = {(r.method, r.url.path) for r in fake_broker.requests}
    assert all(method == "GET" or path in ("/v2/Session", "/v2/market/information") for method, path in sent)
    await adapter.close()


async def test_a_close_of_a_position_the_adapter_knew_keeps_its_entry_and_side(fake_broker: FakeStoneX) -> None:
    """Resume after sleep: the position was seen (snapshot or stream) before it closed, its opening trade is older
    than the window. The CLOSED row keeps the known side and entry price instead of blanking the entry (the
    history row may tie with, and so replace, the stream's own CLOSED row)."""
    fake_broker.positions = [open_position(8000, T0 - 86_400_000, direction="Sell")]
    adapter = await connected(fake_broker, stop_limit_history=True)
    await adapter.fetch_positions()                       # learnt before the PC slept
    fake_broker.positions = []
    fake_broker.history = [trade(9007, T0 + 60_000, direction="Buy", opening=[8000])]
    h = await adapter.fetch_history(T0, UNTIL)
    [closed] = h.positions
    assert (closed.position_id, closed.status, closed.side, closed.entry_price, closed.symbol) == (
        "8000", "CLOSED", "SELL", Decimal("1.087"), "EURUSD")
    assert closed.broker_updated_at_ms == T0 + 60_000
    await adapter.close()


async def test_an_opening_trade_keeps_its_execution_time_after_its_position_closed(fake_broker: FakeStoneX) -> None:
    """Live finding (2026-11-14): once a position closes, StoneX moves its opening trade's LastChangedDateTimeUtc to
    the close time; ExecutedDateTimeUtc still holds the fill time. The FILLED order must carry the fill time, or the
    entry is stamped with the exit (4 of 5 real trades lost their entry time)."""
    opening = trade(9002, T0 + 120_000, direction="Sell", market=AUDUSD_ID, qty=20000, price=0.656)
    opening["LastChangedDateTimeUtc"] = wcf(T0 + 300_000)                       # moved to the close
    fake_broker.history = [opening,
                           trade(9003, T0 + 300_000, direction="Buy", market=AUDUSD_ID, qty=20000, opening=[9002],
                                 price=0.654)]
    adapter = await connected(fake_broker, stop_limit_history=True)
    h = await adapter.fetch_history(T0, UNTIL)
    orders = {o.broker_order_id: o for o in h.orders}
    assert orders["9002"].broker_updated_at_ms == T0 + 120_000                  # filled at entry, not at the close
    assert orders["9003"].broker_updated_at_ms == T0 + 300_000
    assert {p.position_id: p.broker_updated_at_ms for p in h.positions} == {"9002": T0 + 300_000}
    await adapter.close()
