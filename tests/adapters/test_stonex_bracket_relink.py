"""Live finding 2026-11-20: a sell-STOP entry (itself a stop order, so held for its own parent read) and its if-done
stop / take-profit children were committed with no link, so Oversee read the entry and both children as missing a
stop (SOFT_HALT missing_stop_loss). The fix (adapter-isolation.md §2 StoneX):
- an order is a bracket-parent candidate from its first frame's arrival (``note_frame``), keyed by trading account,
  market and first LastChangedTime; a bracket links only to the one valid candidate (opposite side, not itself a
  child, a bracket or a closing trade) and never guesses between several, or while a candidate's side is unknown;
- the entry's own REST row teaches its IfDone children's parent;
- a held bracket with no known parent stays held until every candidate of its key whose read is running has been
  mapped (and a child whose known parent is held waits for it), so each child is mapped once, after its parent;
- a snapshot maps an if-done child nested in a working entry's row Pending at the entry row's broker time, so it
  agrees with the stream and heals a child the stream committed unlinked."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

from src.adapters.base import StreamEventKind
from src.adapters.stonex.adapter import StoneXAdapter
from src.adapters.stonex.orders import OrderItem, OrderStreamMapper
from src.dto import OrderDTO, OrderStatus

from .conftest import CLIENT, EURUSD_ID, TRADING, FakeStoneX, FakeStreamTransport, make_adapter, wait_until, wcf
from .test_stonex_adapter import order_frame
from .test_stonex_orders import frame, mapper, orders, run

T0 = 1_767_225_600_000


def item(values: dict[str, str | None]) -> OrderItem:
    it = OrderItem.from_frame(values)
    assert it is not None
    return it


def entry_frame(oid: str = "800", ts: int = T0, direction: str = "0") -> dict[str, str | None]:
    # a sell-stop entry: Type Stop (2), Accepted (2), Direction sell (0)
    return frame(oid, "2", type_="2", direction=direction, ts=ts, open_price="1.08000")


def child_frame(oid: str, type_: str, ts: int = T0, status: str = "1", direction: str = "1") -> dict[str, str | None]:
    # an if-done child: Pending (1), on the buy side
    return frame(oid, status, type_=type_, direction=direction, ts=ts,
                 open_price="1.08300" if type_ == "2" else "1.07500")


# ---- mapper: candidates and the bracket rule ---------------------------------------------------------------
def test_a_held_entry_noted_at_arrival_links_children_mapped_before_it() -> None:
    m = mapper()
    m.note_frame(item(entry_frame()))          # the entry's frame arrived, then was held for its parent read
    (stop,) = orders(run(m, child_frame("801", "2")))
    (limit,) = orders(run(m, child_frame("802", "3")))
    assert (stop.is_reduce_only, stop.linked_position_id, stop.order_kind) == (True, "800", "STOP")
    assert (limit.is_reduce_only, limit.linked_position_id, limit.order_kind) == (True, "800", "LIMIT")
    (entry,) = orders(run(m, entry_frame()))
    assert not entry.is_reduce_only and entry.linked_position_id is None


def test_without_the_entry_noted_the_bracket_stays_unlinked() -> None:
    m = mapper()
    (orphan,) = orders(run(m, child_frame("811", "3")))
    assert orphan.is_reduce_only and orphan.linked_position_id is None


def test_a_later_item_matches_on_the_brackets_first_time() -> None:
    m = mapper()
    m.note_frame(item(entry_frame("830", ts=T0)))
    (first,) = orders(run(m, child_frame("831", "2", ts=T0)))
    assert first.linked_position_id == "830"
    m2 = mapper()                       # the child is first mapped before its entry was noted: unlinked
    (orphan,) = orders(run(m2, child_frame("841", "2", ts=T0)))
    assert orphan.linked_position_id is None
    m2.note_frame(item(entry_frame("840", ts=T0)))
    # the user moves the stop 14 s later: matched on the child's first time, not the new item's
    (later,) = orders(run(m2, child_frame("841", "2", ts=T0 + 14_000)))
    assert later.linked_position_id == "840" and later.broker_updated_at_ms == T0 + 14_000


def test_a_held_order_that_turns_out_to_be_a_child_is_never_a_bracket_parent() -> None:
    m = mapper()
    m.note_frame(item(frame("931", "2", type_="2", direction="0", ts=T0)))   # a stop added to a position, held
    m.learn_parent("931", "930")                                             # its read names the position
    m.note_frame(item(entry_frame("940", ts=T0)))                            # same second: a sell-stop entry
    (child,) = orders(run(m, child_frame("941", "2", ts=T0)))
    assert child.linked_position_id == "940"


def test_two_candidate_entries_in_one_second_are_never_guessed() -> None:
    m = mapper()
    m.note_frame(item(entry_frame("800", ts=T0)))
    m.note_frame(item(entry_frame("810", ts=T0)))                     # a second sell entry, same second
    (child,) = orders(run(m, child_frame("801", "2", ts=T0)))
    assert child.is_reduce_only and child.linked_position_id is None
    m.learn_parent("801", "800")                                      # REST settles it: the next item is linked
    (later,) = orders(run(m, child_frame("801", "2", ts=T0 + 1)))
    assert later.linked_position_id == "800"


def test_an_oco_straddle_child_only_matches_the_opposite_side_entry() -> None:
    m = mapper()
    m.note_frame(item(entry_frame("900", ts=T0)))                                 # sell-stop entry
    m.note_frame(item(entry_frame("910", ts=T0, direction="1")))                  # buy-stop entry
    (sell_child,) = orders(run(m, child_frame("911", "2", ts=T0, direction="0")))  # a SELL stop child
    assert sell_child.linked_position_id == "910"


def test_a_same_side_order_is_never_a_parent_and_an_unknown_side_is_undecided() -> None:
    m = mapper()
    m.note_frame(item(frame("650", "3", type_="1", direction="1", ts=T0)))   # an unrelated BUY market fill
    (child,) = orders(run(m, child_frame("601", "2", ts=T0)))                # a BUY stop: same side
    assert child.linked_position_id is None
    m = mapper()
    m.note_frame(item(entry_frame("700", ts=T0) | {"Direction": None}))      # the entry's side is not known yet
    m.note_frame(item(frame("900", "3", type_="1", direction="0", ts=T0 + 3000)))   # unrelated SELL, 3 s later
    run(m, child_frame("701", "2", ts=T0))
    (moved,) = orders(run(m, child_frame("701", "2", ts=T0 + 3000)))
    assert moved.linked_position_id is None      # never the unrelated order at the item's time
    m.learn_side("700", "SELL")
    (later,) = orders(run(m, child_frame("701", "2", ts=T0 + 4000)))
    assert later.linked_position_id == "700"


def test_a_bracket_never_links_across_trading_accounts() -> None:
    spec = mapper()._lookup(EURUSD_ID)
    m = OrderStreamMapper(lookup=lambda mk: spec if mk == EURUSD_ID else None,
                          account_for=lambda t: CLIENT if t in (TRADING, "3002") else None)
    m.note_frame(item(entry_frame("700", ts=T0)))
    (child,) = orders(run(m, child_frame("711", "2", ts=T0) | {"TradingAccountId": "3002"}))
    assert child.linked_position_id is None


def test_a_rest_known_order_is_not_a_candidate_from_its_stream_frame() -> None:
    m = mapper()
    m.learn_side("860", "SELL")                 # a REST read made its memo first
    m.note_frame(item(entry_frame("860", ts=T0 + 7)))
    (child,) = orders(run(m, child_frame("861", "2", ts=T0 + 7)))
    assert child.linked_position_id is None


def test_bracket_candidates_name_what_a_held_bracket_waits_for() -> None:
    m = mapper()
    m.note_frame(item(entry_frame("800", ts=T0)))
    m.note_frame(item(entry_frame("810", ts=T0 + 1)))
    assert m.bracket_candidates(item(child_frame("801", "2", ts=T0))) == ["800"]
    assert m.bracket_candidates(item(entry_frame("805", ts=T0))) == []          # not a Pending bracket
    m.learn_parent("801", "800")
    assert m.bracket_candidates(item(child_frame("801", "2", ts=T0))) == []     # its parent is known


# ---- adapter -----------------------------------------------------------------------------------------------
async def _subscribed(fake_broker: FakeStoneX, fake_stream: FakeStreamTransport,
                      slow: dict[str, float] | None = None) -> tuple[StoneXAdapter, AsyncIterator[object]]:
    adapter = make_adapter(fake_broker, fake_stream)
    if slow:
        real = adapter._rest.order_active_stop_limit

        async def delayed(order_id: str) -> Any:
            await asyncio.sleep(slow.get(order_id, 0.0))
            return await real(order_id)
        adapter._rest.order_active_stop_limit = delayed  # type: ignore[method-assign]
    await adapter.connect()
    gen = adapter.stream_order_updates()
    first = asyncio.ensure_future(gen.__anext__())
    await wait_until(lambda: len(fake_stream.subs) == 1)
    fake_stream.event("ORDERS", StreamEventKind.SUBSCRIBED)
    await asyncio.wait_for(first, 2)
    return adapter, gen


async def _take_until(gen: AsyncIterator[object], done: Callable[[list[object]], bool],
                      timeout: float = 3.0) -> list[object]:
    got: list[object] = []

    async def pump() -> None:
        while not done(got):
            got.append(await gen.__anext__())
    await asyncio.wait_for(pump(), timeout)
    return got


def _dtos(got: list[object]) -> list[OrderDTO]:
    return [o for o in got if isinstance(o, OrderDTO)]


def _entry_row(oid: int, children: list[tuple[str, int]], direction: str = "sell") -> dict[str, object]:
    if_done: list[dict[str, object]] = [{key: {"OrderId": cid, "TriggerPrice": 1.083 if key == "Stop" else 1.075}}
                                        for key, cid in children]
    return {"OrderId": oid, "MarketId": int(EURUSD_ID), "Direction": direction, "Quantity": 10000,
            "TriggerPrice": 1.08, "Type": 2, "ParentOrderId": None,
            "TradingAccountId": int(TRADING), "IfDone": if_done, "LastChangedDateTimeUTC": wcf(T0)}


def _emit_bracket(fake_stream: FakeStreamTransport, entry: str, stop: str, limit: str,
                  entry_direction: str | None = "0") -> None:
    fake_stream.emit("ORDERS", order_frame(entry, "2", Type="2", Direction=entry_direction, LastChangedTime=wcf(T0)))
    fake_stream.emit("ORDERS", order_frame(stop, "1", Type="2", Direction="1", LastChangedTime=wcf(T0)))
    fake_stream.emit("ORDERS", order_frame(limit, "1", Type="3", Direction="1", LastChangedTime=wcf(T0)))


def _assert_once_after_parent(dtos: list[OrderDTO], entry: str, children: tuple[str, ...]) -> None:
    ids = [o.broker_order_id for o in dtos]
    assert ids.count(entry) == 1 and all(ids.count(c) == 1 for c in children), ids
    for c in children:
        assert ids.index(entry) < ids.index(c), ids
        (dto,) = [o for o in dtos if o.broker_order_id == c]
        assert dto.is_reduce_only and dto.linked_position_id == entry
    (e,) = [o for o in dtos if o.broker_order_id == entry]
    assert not e.is_reduce_only


async def _collect(gen: AsyncIterator[object], ids: set[str]) -> list[OrderDTO]:
    got = await _take_until(gen, lambda g: ids <= {o.broker_order_id for o in _dtos(g)})
    try:                                   # nothing more (no second, unlinked or re-sent version) follows
        got.append(await asyncio.wait_for(gen.__anext__(), 0.3))
    except asyncio.TimeoutError:
        pass
    return _dtos(got)


async def test_the_incident_children_are_mapped_once_linked_after_the_entry(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    fake_broker.order_active["900"] = _entry_row(900, [("Stop", 901), ("Limit", 902)])
    adapter, gen = await _subscribed(fake_broker, fake_stream, slow={"900": 0.3})
    _emit_bracket(fake_stream, "900", "901", "902")
    dtos = await _collect(gen, {"900", "901", "902"})
    _assert_once_after_parent(dtos, "900", ("901", "902"))
    assert {o.broker_order_id: o.order_kind for o in dtos} == {"900": "STOP", "901": "STOP", "902": "LIMIT"}
    await adapter.close()


async def test_without_ifdone_the_bracket_rule_links_after_the_slow_entry(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    fake_broker.order_active["950"] = _entry_row(950, [])
    adapter, gen = await _subscribed(fake_broker, fake_stream, slow={"950": 0.3})
    _emit_bracket(fake_stream, "950", "951", "952")
    _assert_once_after_parent(await _collect(gen, {"950", "951", "952"}), "950", ("951", "952"))
    await adapter.close()


async def test_an_entry_first_seen_without_direction_still_gets_its_children(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    fake_broker.order_active["960"] = _entry_row(960, [])     # its read gives the side; no IfDone
    adapter, gen = await _subscribed(fake_broker, fake_stream, slow={"960": 0.3})
    _emit_bracket(fake_stream, "960", "961", "962", entry_direction=None)
    _assert_once_after_parent(await _collect(gen, {"960", "961", "962"}), "960", ("961", "962"))
    await adapter.close()


async def test_a_snapshot_maps_an_entrys_ifdone_children_pending_at_the_entry_time(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    fake_broker.actives = [_entry_row(970, [("Stop", 971), ("Limit", 972)])]
    adapter = make_adapter(fake_broker, fake_stream)
    await adapter.connect()
    by_id = {o.broker_order_id: o for o in await adapter.fetch_open_orders()}
    for cid, kind in (("971", "STOP"), ("972", "LIMIT")):
        child = by_id[cid]
        assert (child.status, child.broker_updated_at_ms, child.order_kind) == (OrderStatus.PENDING, T0, kind)
        assert child.is_reduce_only and child.linked_position_id == "970"
    assert by_id["970"].status == OrderStatus.WORKING and not by_id["970"].is_reduce_only
    await adapter.close()


# ---- review round 3 (2026-11-21) ---------------------------------------------------------------------------
def test_a_later_items_time_never_finds_a_parent() -> None:
    m = mapper()
    run(m, frame("400", "3", type_="1", direction="1", ts=T0 - 60_000))      # position 400 (BUY)
    (stop,) = orders(run(m, child_frame("401", "2", ts=T0, direction="0")))  # its stop, parent not known
    assert stop.linked_position_id is None
    m.note_frame(item(frame("500", "3", type_="1", direction="1", ts=T0 + 30_000)))   # an unrelated BUY, later
    (moved,) = orders(run(m, child_frame("401", "2", ts=T0 + 30_000, direction="0")))
    assert moved.linked_position_id is None


async def test_a_childs_finished_read_waits_behind_its_entrys_queued_frame(
        fake_broker: FakeStoneX, fake_stream: FakeStreamTransport) -> None:
    """The channel hands a finished read over before queued broker frames: with a busy consumer the child's read
    can finish while its entry's frame is still queued. The entry is registered first all the same."""
    fake_broker.order_active["900"] = _entry_row(900, [("Stop", 901)])
    adapter, gen = await _subscribed(fake_broker, fake_stream, slow={"900": 0.3})
    fake_stream.emit("ORDERS", order_frame("500", "2", Type="1", Direction="1", LastChangedTime=wcf(T0 - 5000)))
    fake_stream.emit("ORDERS", order_frame("901", "1", Type="2", Direction="1", LastChangedTime=wcf(T0)))
    fake_stream.emit("ORDERS", order_frame("777", "2", Type="1", Direction="1", LastChangedTime=wcf(T0 - 4000)))
    fake_stream.emit("ORDERS", order_frame("900", "2", Type="2", Direction="0", LastChangedTime=wcf(T0)))
    got: list[object] = []

    async def busy() -> None:          # a consumer that commits slowly, so the loop runs between items
        while not {"900", "901"} <= {o.broker_order_id for o in _dtos(got)}:
            got.append(await gen.__anext__())
            await asyncio.sleep(0.05)
    await asyncio.wait_for(busy(), 3.0)
    _assert_once_after_parent([o for o in _dtos(got) if o.broker_order_id in ("900", "901")], "900", ("901",))
    await adapter.close()
