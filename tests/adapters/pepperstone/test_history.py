"""PepperstoneAdapter.fetch_history: deals and orders by time range (one-week windows, hasMore splits) rebuild
the orders and positions placed and finished while the engine was off, read-only. No network I/O (fake cTrader)."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

import pytest

from src.adapters.pepperstone.adapter import DAY_MS, WEEK_MS, PepperstoneAdapter
from src.adapters.pepperstone.protocol import OUTBOUND_ALLOW_LIST
from src.adapters.ratelimit import RestBudget, RestPriority, rest_priority
from src.dto import Decision, OrderStatus, decide

from .conftest import EURUSD, T0, FakeCTrader, make_adapter, order, position

SINCE = T0 - 3_600_000
UNTIL = T0 + 60_000        # == the fake wall clock


def deal(deal_id: int, order_id: int, position_id: int, ts: int, *, side: str = "BUY", volume: int = 1_000_000,
         price: float = 1.0855, close: dict[str, Any] | None = None, symbol: int = EURUSD) -> dict[str, Any]:
    row: dict[str, Any] = {"dealId": deal_id, "orderId": order_id, "positionId": position_id, "volume": volume,
                           "filledVolume": volume, "symbolId": symbol, "createTimestamp": ts,
                           "executionTimestamp": ts, "utcLastUpdateTimestamp": ts, "executionPrice": price,
                           "tradeSide": side, "dealStatus": "FILLED", "commission": -35, "moneyDigits": 2}
    if close is not None:
        row["closePositionDetail"] = close
    return row


def filled(oid: int, ts: int, *, side: str = "BUY", position_id: int, **extra: Any) -> dict[str, Any]:
    return order(oid, order_type="MARKET", status="ORDER_STATUS_FILLED", volume=1_000_000, side=side, ts=ts,
                 executedVolume=1_000_000, executionPrice=1.0855, positionId=position_id, **extra)


def offline_book(fake: FakeCTrader) -> None:
    """While off: position 501 opened (order 701) and closed (order 702); position 502 opened (order 703, missing
    from the order list) and still open; stop order 704 placed and cancelled."""
    fake.deals = [
        deal(1, 701, 501, T0 - 3_000_000),
        deal(2, 702, 501, T0 - 1_000_000, side="SELL", price=1.0870,
             close={"entryPrice": 1.0855, "closedVolume": 1_000_000, "grossProfit": 1500, "swap": 0,
                    "commission": -35, "balance": 1_001_500, "balanceVersion": 43, "moneyDigits": 2}),
        deal(3, 703, 502, T0 - 500_000),
    ]
    fake.order_history = [
        filled(701, T0 - 3_000_000, position_id=501),
        filled(702, T0 - 1_000_000, side="SELL", position_id=501, closingOrder=True),
        order(704, order_type="STOP", status="ORDER_STATUS_CANCELLED", volume=500_000, side="SELL",
              ts=T0 - 2_000_000, stopPrice=1.079),
    ]
    fake.order_details = {703: filled(703, T0 - 500_000, position_id=502)}
    fake.positions = [position(502, ts=T0 - 500_000)]


async def connected(fake: FakeCTrader, **kwargs: Any) -> PepperstoneAdapter:
    adapter = make_adapter(fake, **kwargs)
    await adapter.connect()
    return adapter


async def test_orders_and_positions_finished_while_off_are_reconstructed(fake: FakeCTrader) -> None:
    offline_book(fake)
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert h.complete and h.notes == () and h.covered_from_ms == SINCE
    orders = {o.broker_order_id: o for o in h.orders}
    assert {k: o.status for k, o in orders.items()} == {
        "701": OrderStatus.FILLED, "702": OrderStatus.FILLED, "703": OrderStatus.FILLED, "704": OrderStatus.CANCELLED}
    assert orders["702"].is_reduce_only and orders["702"].linked_position_id == "501"
    assert orders["701"].ordered_units == Decimal(10000)                 # volume / 100
    assert orders["704"].entry_price == Decimal("1.079") and orders["704"].remaining_units == Decimal(5000)
    assert orders["703"].broker_updated_at_ms == T0 - 500_000             # read by id: a deal named it
    positions = {p.position_id: p for p in h.positions}
    closed = positions["501"]
    assert (closed.status, closed.units, closed.side) == ("CLOSED", Decimal(0), "BUY")
    assert closed.broker_updated_at_ms == T0 - 1_000_000                  # the closing deal's broker time
    assert positions["502"].status == "OPEN" and positions["502"].units == Decimal(10000)
    assert all(o.source == "REST" for o in h.orders)
    assert adapter.history_retention_ms is None
    sent = fake.types_sent()
    assert set(sent) <= OUTBOUND_ALLOW_LIST
    assert 2175 in sent and 2133 in sent and 2181 in sent and 2179 in sent
    for f in fake.sent:
        if int(f["payloadType"]) in (2133, 2175):
            p = f["payload"]
            assert p["toTimestamp"] - p["fromTimestamp"] <= WEEK_MS
    # the reconcile is read after the lists: it is the newest truth for "still open"
    assert sent.index(2124, sent.index(2175)) > sent.index(2175)
    await adapter.close()


async def test_has_more_splits_the_window(fake: FakeCTrader) -> None:
    offline_book(fake)
    fake.page_limit = 1
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert h.complete
    assert {o.broker_order_id for o in h.orders} == {"701", "702", "703", "704"}
    assert fake.types_sent().count(2133) > 1 and fake.types_sent().count(2175) > 1
    await adapter.close()


async def test_a_still_truncated_window_is_incomplete(fake: FakeCTrader) -> None:
    offline_book(fake)
    fake.deals.append(deal(4, 705, 503, T0 - 3_000_000))     # same broker time as deal 1: cannot be split apart
    fake.positions.append(position(503, ts=T0 - 3_000_000))
    fake.page_limit = 1
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert not h.complete and any(n.startswith("deal list still truncated") for n in h.notes)
    assert not h.retryable                       # the broker's own paging limit: a re-read would repeat it
    await adapter.close()


async def test_a_failed_page_makes_the_history_incomplete(fake: FakeCTrader) -> None:
    offline_book(fake)
    fake.errors[2175] = ["INTERNAL_ERROR"]
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert not h.complete and h.retryable
    assert any(n.startswith("order list page") and n.endswith("failed: INTERNAL_ERROR") for n in h.notes)
    assert {p.position_id for p in h.positions} == {"501", "502"}         # the deals still place the positions
    await adapter.close()


@pytest.mark.parametrize("code", ["INTERNAL_ERROR", "SERVER_IS_UNDER_MAINTENANCE", "REQUEST_FREQUENCY_EXCEEDED"])
async def test_a_failed_page_alone_makes_the_history_retryable(fake: FakeCTrader, code: str) -> None:
    """Skeptic review: a transient page failure (an error code, a throttle, maintenance; a timeout or a refused
    budget take the same path) is not a broker limit: the catch-up must hold coverage back and re-read the
    window, never move past it. Here the failed deal-list page is the answer's only gap."""
    offline_book(fake)
    fake.errors[2133] = [code] * 10                      # every attempt at that page fails
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert not h.complete and h.retryable
    assert len(h.notes) == 1 and h.notes[0].startswith("deal list page") and h.notes[0].endswith(f"failed: {code}")
    await adapter.close()


async def test_a_long_window_is_read_in_one_week_requests(fake: FakeCTrader) -> None:
    offline_book(fake)
    adapter = await connected(fake)
    h = await adapter.fetch_history(T0 - 10 * DAY_MS, UNTIL)
    assert h.complete
    pages = [f["payload"] for f in fake.sent if int(f["payloadType"]) == 2133]
    assert len(pages) == 2 and pages[0]["fromTimestamp"] == T0 - 10 * DAY_MS
    assert all(p["toTimestamp"] - p["fromTimestamp"] <= WEEK_MS for p in pages)
    assert pages[0]["toTimestamp"] == pages[1]["fromTimestamp"] and pages[-1]["toTimestamp"] == UNTIL
    await adapter.close()


async def test_a_very_long_window_is_capped_and_incomplete(fake: FakeCTrader) -> None:
    adapter = await connected(fake)
    h = await adapter.fetch_history(T0 - 400 * DAY_MS, UNTIL)
    assert not h.complete and h.covered_from_ms > T0 - 400 * DAY_MS
    assert not h.retryable                       # the 13-week request cap: a re-read would repeat it
    assert len([t for t in fake.types_sent() if t == 2133]) <= 14
    await adapter.close()


async def test_a_position_opened_before_the_lookback_is_closed_from_its_closing_deal(fake: FakeCTrader) -> None:
    fake.deals = [deal(9, 709, 600, T0 - 600_000, side="SELL",
                       close={"entryPrice": 1.0811, "closedVolume": 1_000_000, "grossProfit": 10, "moneyDigits": 2})]
    fake.order_history = [filled(709, T0 - 600_000, side="SELL", position_id=600, closingOrder=True)]
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert h.complete
    [closed] = h.positions
    assert (closed.position_id, closed.status, closed.side, closed.entry_price) == ("600", "CLOSED", "BUY",
                                                                                     Decimal("1.0811"))
    await adapter.close()


async def test_deals_that_show_a_position_open_but_absent_never_close_it(fake: FakeCTrader) -> None:
    fake.deals = [deal(5, 710, 777, T0 - 600_000)]       # opened in the window, no closing deal, not reconciled
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert h.positions == () and not h.complete
    assert "position 777 is not in the reconcile but its deals show it open" in h.notes
    assert h.retryable                           # evidence that did not add up between reads: read again
    await adapter.close()


async def test_unmappable_history_orders_are_counted(fake: FakeCTrader) -> None:
    offline_book(fake)
    bad = order(799, status="ORDER_STATUS_CANCELLED", ts=T0 - 100_000)
    del bad["tradeData"]["tradeSide"]
    fake.order_history.append(bad)
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert not h.complete and "1 history row(s) could not be mapped" in h.notes
    # the read's own channel, never the live-stream counter (else the stream would look interrupted)
    assert h.unmapped == 1 and h.retryable and adapter.unmapped_rows["orders"] == 0
    await adapter.close()


async def test_a_deal_without_its_position_id_is_counted(fake: FakeCTrader) -> None:
    offline_book(fake)
    orphan = deal(8, 708, 0, T0 - 200_000)
    del orphan["positionId"]
    fake.deals.append(orphan)
    fake.order_details[708] = filled(708, T0 - 200_000, position_id=0)
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert not h.complete and "1 history row(s) could not be mapped" in h.notes
    # the read's own channel, never the live-stream counter (else the stream would look interrupted)
    assert h.unmapped == 1 and h.retryable and adapter.unmapped_rows["orders"] == 0
    assert {p.position_id for p in h.positions} == {"501", "502"}
    await adapter.close()


async def test_working_protection_orders_are_not_emitted_terminal_ones_are(fake: FakeCTrader) -> None:
    fake.order_history = [
        order(801, order_type="STOP_LOSS_TAKE_PROFIT", status="ORDER_STATUS_ACCEPTED", ts=T0 - 100_000,
              stopPrice=1.08, positionId=502),
        order(802, order_type="STOP_LOSS_TAKE_PROFIT", status="ORDER_STATUS_CANCELLED", ts=T0 - 90_000,
              stopPrice=1.07, positionId=502),
    ]
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert [(o.broker_order_id, o.status, o.is_reduce_only) for o in h.orders] == [
        ("802", OrderStatus.CANCELLED, True)]
    await adapter.close()


async def test_history_runs_at_reconcile_priority(fake: FakeCTrader) -> None:
    offline_book(fake)
    budget = RestBudget("pepperstone", rate_per_s=1000.0, burst=1000)
    adapter = await connected(fake, rest_budget=budget)
    before = budget.stats().requests
    with rest_priority(RestPriority.POLL):
        await adapter.fetch_history(SINCE, UNTIL)
    after = budget.stats().requests
    assert after["reconcile"] - before["reconcile"] >= 4 and after["poll"] == before["poll"]
    await adapter.close()


async def test_a_historic_row_never_beats_a_later_stream_update(fake: FakeCTrader) -> None:
    offline_book(fake)
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    historic = next(o for o in h.orders if o.broker_order_id == "704")
    later = replace(historic, broker_updated_at_ms=historic.broker_updated_at_ms + 1, source="STREAM",
                    entry_price=Decimal("1.078"))
    assert decide(later, historic) == Decision.DROP_STALE
    await adapter.close()


async def test_a_rejected_deal_moves_no_position_and_does_not_make_the_history_incomplete(
        fake: FakeCTrader) -> None:
    """A market order rejected while off: its REJECTED order is listed; its rejected deal names a position that
    never existed, which is not a position 'not proven closed'."""
    offline_book(fake)
    rejected = deal(11, 711, 511, T0 - 400_000)
    rejected["dealStatus"] = "REJECTED"
    rejected["filledVolume"] = 0
    fake.deals.append(rejected)
    fake.order_history.append(order(711, order_type="MARKET", status="ORDER_STATUS_REJECTED", volume=1_000_000,
                                    ts=T0 - 400_000))
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert h.complete and h.notes == ()
    assert {o.broker_order_id: o.status for o in h.orders}["711"] == OrderStatus.REJECTED
    assert {p.position_id for p in h.positions} == {"501", "502"}
    assert not any(f["payload"].get("positionId") == 511 for f in fake.sent if int(f["payloadType"]) == 2179)
    await adapter.close()


async def test_a_deal_without_a_status_still_counts_as_a_position_touch(fake: FakeCTrader) -> None:
    fake.deals = [deal(12, 712, 512, T0 - 400_000)]
    del fake.deals[0]["dealStatus"]
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert not h.complete and "position 512 is not in the reconcile and its deals do not prove it closed" in h.notes
    await adapter.close()


async def test_a_rejected_closing_deal_never_closes_an_absent_position(fake: FakeCTrader) -> None:
    close = deal(13, 713, 613, T0 - 300_000, side="SELL",
                 close={"entryPrice": 1.0811, "closedVolume": 1_000_000, "moneyDigits": 2})
    close["dealStatus"] = "REJECTED"
    fake.deals = [close]
    adapter = await connected(fake)
    h = await adapter.fetch_history(SINCE, UNTIL)
    assert h.positions == ()
    await adapter.close()


async def test_a_capped_window_reads_a_closed_positions_deals_from_the_covered_start(fake: FakeCTrader) -> None:
    """A 400-day window is capped to 13 weeks: the by-position deal read starts one lookback before the covered
    start, not before the requested start (a bounded number of requests per closed position)."""
    fake.deals = [deal(1, 701, 501, T0 - 3_000_000),
                  deal(2, 702, 501, T0 - 1_000_000, side="SELL",
                       close={"entryPrice": 1.0855, "closedVolume": 1_000_000, "moneyDigits": 2})]
    adapter = await connected(fake)
    h = await adapter.fetch_history(T0 - 400 * DAY_MS, UNTIL)
    assert [(p.position_id, p.status) for p in h.positions] == [("501", "CLOSED")]
    by_position = [f["payload"] for f in fake.sent if int(f["payloadType"]) == 2179]
    assert by_position and min(p["fromTimestamp"] for p in by_position) >= h.covered_from_ms - WEEK_MS
    assert len(by_position) <= 15
    await adapter.close()


async def test_an_unmappable_by_id_answer_inside_a_history_read_raises_never_answers_not_found(
        fake: FakeCTrader) -> None:
    """Skeptic review: the catch-up's by-id re-reads run inside `history_read`, where an unmappable row counts on
    the read, not on `unmapped_rows`. `fetch_order_status` / `fetch_position_status` used the growth of
    `unmapped_rows` to tell "unmappable" (raise) from "nothing proved" (None), so inside the read an unmappable
    answer came back as None: reported as a broker "not found" instead of a failed re-read."""
    from src.adapters.base import BrokerError, history_read

    bad = order(805, status="ORDER_STATUS_ACCEPTED", ts=T0 - 100_000)
    del bad["tradeData"]["tradeSide"]
    fake.order_details[805] = bad
    bad_pos = position(605, ts=T0 - 100_000)
    del bad_pos["tradeData"]["tradeSide"]
    fake.positions = [bad_pos]
    adapter = await connected(fake)
    try:
        with history_read() as read:
            with pytest.raises(BrokerError, match="unmappable"):
                await adapter.fetch_order_status("805")
            with pytest.raises(BrokerError, match="unmappable"):
                await adapter.fetch_position_status("605")
        assert read.unmapped == 2 and adapter.unmapped_rows["orders"] == 0
        with pytest.raises(BrokerError, match="unmappable"):         # outside a read: the live counter, as before
            await adapter.fetch_order_status("805")
        assert adapter.unmapped_rows["orders"] == 1
    finally:
        await adapter.close()
