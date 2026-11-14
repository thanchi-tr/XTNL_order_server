"""SM2 DlqRecoveryWorker (tier-persistence.md §5): every row transition with fakes."""
from __future__ import annotations

import asyncio
from dataclasses import replace

from src.integrity.dlq_recovery import DlqConfig, DlqOutcome, DlqRecoveryWorker, is_deterministic
from src.integrity.ports import DlqStatus
from src.pubsub.dlq_rows import dlq_row
from tests.integrity.fakes import T0, FakeDlqBroker, FakeDlqStore, ManualClock, Pages, account, cash_flow, order


class Env:
    def __init__(self) -> None:
        self.clock = ManualClock()
        self.store = FakeDlqStore(self.clock)
        self.broker = FakeDlqBroker()
        self.pages = Pages()
        self.w = DlqRecoveryWorker(self.store, self.broker, clock_ms=self.clock, pager=self.pages,
                                   owner="w1", jitter=lambda: 0.0)

    def status(self, dlq_id: int) -> DlqStatus:
        return self.store.rows[dlq_id].status


async def test_leases_only_streaming_adapters() -> None:
    e = Env()
    a = e.store.park("ORDER", "B1", "QueueFull")
    b = e.store.park("ORDER", "B2", "QueueFull", adapter_id="ctrader")
    e.broker.entities["B1"] = order("B1")
    done = await e.w.run_once()
    assert done == [(a.dlq_id, DlqOutcome.RESOLVED)]
    assert e.status(b.dlq_id) is DlqStatus.QUEUED
    e.broker.streaming.clear()
    e.store.park("ORDER", "B3", "QueueFull")
    assert await e.w.run_once() == []


async def test_converged_order_is_resubmitted_and_resolved() -> None:
    e = Env()
    r = e.store.park("ORDER", "B1", "QueueFull")
    e.broker.entities["B1"] = order("B1")
    await e.w.run_once()
    assert e.broker.calls == [("reread", "ORDER", "B1")]
    assert e.status(r.dlq_id) is DlqStatus.RESOLVED


async def test_broker_without_the_row_still_converges() -> None:
    """An explicit broker "not found" (the adapter's note_absent) is absence: the parked payload is dropped."""
    e = Env()
    r = e.store.park("POSITION", "P9", "timeout")
    e.broker.absent.add("P9")
    await e.w.run_once()
    assert e.broker.calls == [("reread", "POSITION", "P9")]
    assert e.status(r.dlq_id) is DlqStatus.RESOLVED


async def test_a_none_without_an_explicit_not_found_fails_closed() -> None:
    """IMPLEMENTATION_PLAN 8.1: a read that returns no row without the adapter recording an explicit "not found"
    for that id (an unparsable body, a row the broker could not prove, another entity's not-found) never
    converges: the row backs off, then is quarantined and paged instead of being silently dropped."""
    for event_type in ("ORDER", "POSITION", "ACCOUNT"):
        e = Env()
        e.broker.absent.add("OTHER")                      # a not-found for another id proves nothing about this one
        r = e.store.park(event_type, "X1", "QueueFull")
        assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.BACKOFF)]
        row = e.store.rows[r.dlq_id]
        assert row.status is DlqStatus.BACKOFF and row.retry_count == 1
        last = e.store.park(event_type, "X2", "QueueFull", retry_count=4)
        e.clock.advance(60_000)
        out = dict(await e.w.run_once())
        assert out[last.dlq_id] is DlqOutcome.QUARANTINED
        assert e.store.quarantined == [(last.dlq_id, "broker returned no row without an explicit not found after 5 "
                                                     "attempts")]
        assert "dlq_quarantined" in e.pages.codes()


async def test_an_illegal_transition_row_needs_a_row_or_an_explicit_not_found() -> None:
    e = Env()
    r = e.store.park("ORDER", "B1", "illegal_transition")
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.BACKOFF)]      # None, no explicit not-found
    assert e.broker.calls == [("read", "ORDER", "B1")]
    e.broker.absent.add("B1")
    e.clock.advance(60_000)
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.QUARANTINED)]
    assert e.store.quarantined == [(r.dlq_id, "converged after illegal_transition")]


async def test_converged_illegal_transition_is_quarantined_not_resubmitted() -> None:
    e = Env()
    r = e.store.park("ORDER", "B1", "illegal_transition")
    e.broker.entities["B1"] = order("B1")
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.QUARANTINED)]
    assert e.broker.calls == [("read", "ORDER", "B1")]
    assert e.store.quarantined == [(r.dlq_id, "converged after illegal_transition")]
    assert e.pages.codes() == ["dlq_quarantined"]


async def test_read_error_backs_off_with_retry_count() -> None:
    e = Env()
    r = e.store.park("ACCOUNT", "ACC", "QueueFull")
    e.broker.entities["ACC"] = account("ACC")
    e.broker.errors.append(ConnectionError("timeout"))
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.BACKOFF)]
    row = e.store.rows[r.dlq_id]
    assert row.status is DlqStatus.BACKOFF and row.retry_count == 1
    assert row.next_retry_at_ms == T0 + 2_000                  # min(60, 2**1 + 0) s
    assert await e.w.run_once() == []                          # not due yet
    e.clock.advance(2_000)
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.RESOLVED)]


async def test_backoff_is_capped_at_60s() -> None:
    e = Env()
    r = e.store.park("ORDER", "B1", "QueueFull", retry_count=3)
    e.w = DlqRecoveryWorker(e.store, e.broker, clock_ms=e.clock, owner="w1", jitter=lambda: 0.0,
                            config=DlqConfig(max_retries=20))
    e.store.rows[r.dlq_id] = replace(e.store.rows[r.dlq_id], retry_count=10)
    e.broker.errors.append(RuntimeError("boom"))
    await e.w.run_once()
    assert e.store.rows[r.dlq_id].next_retry_at_ms == T0 + 60_000


async def test_adapter_leaving_streaming_requeues_without_retry() -> None:
    e = Env()
    r = e.store.park("ORDER", "B1", "QueueFull")

    def drop() -> None:
        e.broker.streaming.clear()

    e.broker.on_read = drop
    e.broker.errors.append(ConnectionError("socket closed"))
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.REQUEUED)]
    row = e.store.rows[r.dlq_id]
    assert row.status is DlqStatus.QUEUED and row.retry_count == 0


async def test_not_streaming_at_process_time_requeues() -> None:
    e = Env()
    r = e.store.park("ORDER", "B1", "QueueFull")
    [leased] = await e.store.lease("w1", e.clock(), 30_000, {"stonex"}, 10)
    e.broker.streaming.clear()
    assert await e.w.process(leased) is DlqOutcome.REQUEUED
    assert e.store.rows[r.dlq_id].status is DlqStatus.QUEUED and e.broker.calls == []


async def test_lease_expiry_returns_row_without_retry() -> None:
    e = Env()
    r = e.store.park("ORDER", "B1", "QueueFull")
    await e.store.lease("crashed-worker", e.clock(), 1_000, {"stonex"}, 10)
    e.clock.advance(1_001)
    e.broker.streaming.clear()
    await e.w.run_once()
    row = e.store.rows[r.dlq_id]
    assert row.status is DlqStatus.QUEUED and row.retry_count == 0


async def test_quarantine_after_five_failures() -> None:
    e = Env()
    r = e.store.park("ORDER", "B1", "QueueFull", retry_count=4)
    e.broker.errors.append(RuntimeError("500"))
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.QUARANTINED)]
    assert e.store.quarantined[0][0] == r.dlq_id and "after 5 attempts" in e.store.quarantined[0][1]


async def test_cash_flow_converges_only_when_found() -> None:
    e = Env()
    payload = dlq_row(cash_flow("T1", ts=T0 - 5_000), "QueueFull").raw_payload
    r = e.store.park("CASH_FLOW", "T1", "QueueFull", payload=payload)
    e.store.cash_since = T0 - 60_000
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.BACKOFF)]
    assert e.broker.calls == [("cash_flow", "T1", T0 - 60_000)]   # min(last committed, oldest parked)
    e.broker.flows["T1"] = cash_flow("T1")
    e.clock.advance(60_000)
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.RESOLVED)]


async def test_cash_flow_since_falls_back_to_payload_then_lookback() -> None:
    e = Env()
    payload = dlq_row(cash_flow("T1", ts=T0 - 5_000), "QueueFull").raw_payload
    e.store.park("CASH_FLOW", "T1", "QueueFull", payload=payload)
    e.store.park("CASH_FLOW", "T2", "QueueFull", payload="not json")
    await e.w.run_once()
    assert e.broker.calls == [("cash_flow", "T1", T0 - 5_000), ("cash_flow", "T2", T0 - 7 * 24 * 3_600_000)]


async def test_cash_flow_never_found_is_quarantined_and_day_unverified() -> None:
    e = Env()
    payload = dlq_row(cash_flow("T1"), "QueueFull").raw_payload
    r = e.store.park("CASH_FLOW", "T1", "QueueFull", payload=payload, retry_count=4)
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.QUARANTINED)]
    assert e.store.unverified == [("stonex", "ACC")]
    assert e.pages.alerts[-1].code == "dlq_cash_flow_unverified" and e.pages.alerts[-1].severity == "critical"


async def test_deterministic_lineage_accumulates_and_quarantines() -> None:
    e = Env()
    e.broker.entities["B1"] = order("B1")
    origin = e.store.park("ORDER", "B1", "future_timestamp")
    await e.w.run_once()
    assert e.status(origin.dlq_id) is DlqStatus.RESOLVED
    for n in range(1, 5):
        again = e.store.park("ORDER", "B1", "future_timestamp")      # the orchestrator re-parked it
        out = await e.w.run_once()
        assert (again.dlq_id, DlqOutcome.MERGED) in out
        assert e.store.rows[again.dlq_id].origin_dlq_id == origin.dlq_id
        assert e.store.rows[origin.dlq_id].retry_count == n
        if n < 4:
            e.clock.advance(60_000)
            assert (origin.dlq_id, DlqOutcome.RESOLVED) in await e.w.run_once()
    e.clock.advance(60_000)
    await e.w.run_once()
    last = e.store.park("ORDER", "B1", "future_timestamp")
    await e.w.run_once()
    assert e.status(origin.dlq_id) is DlqStatus.QUARANTINED
    assert e.store.rows[last.dlq_id].origin_dlq_id == origin.dlq_id
    assert "dlq_quarantined" in e.pages.codes()


async def test_transient_reason_starts_fresh_and_pages_after_15_min() -> None:
    e = Env()
    e.broker.entities["B1"] = order("B1")
    origin = e.store.park("ORDER", "B1", "QueueFull")
    await e.w.run_once()
    e.clock.advance(16 * 60_000)
    again = e.store.park("ORDER", "B1", "QueueFull")
    assert await e.w.run_once() == [(again.dlq_id, DlqOutcome.RESOLVED)]
    assert e.store.rows[again.dlq_id].origin_dlq_id is None
    assert e.store.rows[origin.dlq_id].retry_count == 0
    assert e.pages.codes() == []              # origin resolved 16 min ago: outside the lineage window
    e2 = Env()
    e2.broker.entities["B1"] = order("B1")
    o2 = e2.store.park("ORDER", "B1", "QueueFull")
    await e2.w.run_once()
    e2.clock.advance(9 * 60_000)
    e2.store.rows[o2.dlq_id] = replace(e2.store.rows[o2.dlq_id], parked_at_ms=T0 - 10 * 60_000)
    e2.store.park("ORDER", "B1", "QueueFull")
    await e2.w.run_once()
    assert e2.pages.codes() == ["dlq_parked_15min"]
    e2.store.park("ORDER", "B1", "QueueFull")
    await e2.w.run_once()
    assert e2.pages.codes() == ["dlq_parked_15min"]   # paged once per entity


def test_reason_classes() -> None:
    assert is_deterministic("schema_invalid") and is_deterministic("illegal_transition")
    assert not is_deterministic("QueueFull") and not is_deterministic("timeout")


async def test_lost_lease_is_reported() -> None:
    e = Env()
    r = e.store.park("ORDER", "B1", "QueueFull")

    def steal() -> None:
        e.store.rows[r.dlq_id] = replace(e.store.rows[r.dlq_id], status=DlqStatus.QUEUED, lease_owner=None)

    e.broker.on_read = steal
    assert await e.w.run_once() == [(r.dlq_id, DlqOutcome.LOST_LEASE)]


async def test_run_loop() -> None:
    e = Env()
    e.w.cfg = DlqConfig(poll_interval_s=0.01)
    r = e.store.park("ORDER", "B1", "QueueFull")
    e.broker.entities["B1"] = order("B1")
    stop = asyncio.Event()
    task = asyncio.create_task(e.w.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 1.0)
    assert e.status(r.dlq_id) is DlqStatus.RESOLVED and e.w.liveness_ms == T0
    assert e.w.outcomes[DlqOutcome.RESOLVED] == 1


async def test_lineage_is_adapter_scoped() -> None:
    """dlq-lineage-not-adapter-scoped: cTrader's order B1 parked with the same deterministic reason is
    never merged into StoneX's resolved B1 row; it is checked against the broker on its own."""
    e = Env()
    e.broker.streaming.add("ctrader")
    e.broker.entities["B1"] = order("B1")
    stonex = e.store.park("ORDER", "B1", "schema_invalid")
    await e.w.run_once()
    assert e.status(stonex.dlq_id) is DlqStatus.RESOLVED
    e.broker.calls.clear()
    ctrader = e.store.park("ORDER", "B1", "schema_invalid", adapter_id="ctrader")
    out = await e.w.run_once()
    assert (ctrader.dlq_id, DlqOutcome.MERGED) not in out
    assert e.store.rows[ctrader.dlq_id].origin_dlq_id is None
    assert e.store.rows[stonex.dlq_id].retry_count == 0
    assert e.broker.calls != []                       # check-before-retry ran for the cTrader row


async def test_transient_page_chain_is_adapter_scoped() -> None:
    """A new cTrader episode for order B1 must not reset StoneX's B1 transient chain (entity ids are per
    adapter): StoneX's B1, parked again and again since T0, still pages once it has been parked 15 min."""
    e = Env()
    e.broker.streaming.add("ctrader")
    e.broker.entities["B1"] = order("B1")
    e.store.park("ORDER", "B1", "QueueFull")                          # StoneX chain starts at T0
    await e.w.run_once()
    for step in range(3):
        e.clock.advance(6 * 60_000 if step < 2 else 4 * 60_000)       # T0+6, T0+12, T0+16 min
        if step == 0:
            e.store.park("ORDER", "B1", "QueueFull")
            await e.w.run_once()
            e.store.park("ORDER", "B1", "QueueFull", adapter_id="ctrader")   # a fresh cTrader episode
            await e.w.run_once()
            continue
        e.store.park("ORDER", "B1", "QueueFull")
        await e.w.run_once()
    assert e.pages.codes() == ["dlq_parked_15min"]                    # 16 min since T0: paged
