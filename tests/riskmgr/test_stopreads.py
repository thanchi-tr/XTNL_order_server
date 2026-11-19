"""PR9 stop-read schedule (S27, design PR9 tests, R11-M20): one list read per adapter per anchor, the PRE window at
offsets 30 s (the narrowest the config accepts, S38c) and 45 s with the BARS bucket idle and saturated, the DELAYED POST's
two reads, staggering, and `XTNL_ARM_STOP_READS=0`."""
from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from src.marketdata.ports import HandleRef, StopHandleRead
from src.marketdata.timing import COLD_SKEW, TIMING_TABLE, pre_fire_ms, pre_valid_until_ms
from src.marketdata.types import M15, Transient
from src.riskmgr.barclock import DELAYED, PRE_READ_MISSING, NotReady
from src.riskmgr.config import ArmSettings
from src.riskmgr.stopreads import (
    Anchor,
    ReadBook,
    StopReads,
    pre_queue_ms,
)
from src.riskmgr.types import ArmEvent
from tests.riskmgr.barclock_fakes import (
    ADAPTER,
    SETTLE,
    T1,
    T2,
    FakeClock,
    FakeStopPort,
    arm_row,
    bc_rig,
    handles_for,
)

ROW = TIMING_TABLE[M15]


@pytest.mark.parametrize("leaves_after_ms", [None, 15_000, 15_400],
                         ids=["bars_idle", "bars_saturated", "bars_saturated_slow_answer"])
@pytest.mark.parametrize("offset", [30, 31, 45])
async def test_pre_is_never_stop_stale_when_its_read_returns(offset: int, leaves_after_ms: int | None) -> None:
    """Saturated: the BARS bucket holds the request for 15 s, so it leaves (and is stamped) at the fire time; the
    slow case answers 400 ms after that. The passes land 100 ms after the fire, never on it."""
    r = bc_rig(arm_row(cursor=T1), settings=ArmSettings(stop_reads=True, pre_offset_s=offset), stop_reads=True,
               gated=leaves_after_ms is not None)
    queue_at = pre_queue_ms(T2, COLD_SKEW, offset)
    fire = pre_fire_ms(T2, COLD_SKEW, offset)
    until = pre_valid_until_ms(fire, T2, ROW, COLD_SKEW)
    assert queue_at == fire - 15_000
    await r.step(queue_at)
    assert r.port is not None
    pre_calls = [c for c in r.port.calls if c[2] == queue_at]
    assert len(pre_calls) == 1
    if leaves_after_ms == 15_000:
        await r.release_reads(queue_at + leaves_after_ms)
    await r.step(fire + 100)
    if leaves_after_ms == 15_400:
        assert r.store.rows_of() == []                        # still waiting for its read, within the window
        await r.release_reads(queue_at + leaves_after_ms)
        await r.step(fire + 500)
    [pre] = [q for q in r.eval.requests if q.event is ArmEvent.PRE]
    current = pre.stop_reads.current
    assert current is not None and current.sent_ms is not None and current.sent_ms <= fire + 400
    assert current.sent_ms >= (queue_at if leaves_after_ms is None else fire)
    assert pre.stop_reads.fresh(ArmEvent.PRE, until)          # still within 30 s at the latest valid-until
    assert [d.event for d in r.store.rows_of()] == ["PRE"]
    assert r.reads.stale_counts() == {}


async def test_eight_positions_with_two_handles_cost_one_list_read_per_anchor() -> None:
    pids = [f"P{i}" for i in range(1, 9)]
    r = bc_rig(*(arm_row(p) for p in pids), stop_reads=True, handles=handles_for(*pids, per=2), now=T1)
    await r.step(T1)                                           # POST anchor of T1 at T + p50
    await r.step(T1 + SETTLE + 1_000)                          # POSTs decided on the anchor read (6 s old)
    await r.step(pre_queue_ms(T2, COLD_SKEW, 40))              # PRE anchor of T2
    await r.step(pre_fire_ms(T2, COLD_SKEW, 40))
    assert r.port is not None
    assert [(c[0], len(c[1])) for c in r.port.calls] == [(ADAPTER, 16), (ADAPTER, 16)]
    assert sum(1 for d in r.store.decisions.values() if d.event == "POST") == 8
    assert sum(1 for d in r.store.decisions.values() if d.event == "PRE") == 8


async def test_delayed_post_uses_the_bar_end_read_and_a_fresh_read_for_the_post_guards() -> None:
    final_at = T1 + 180_000

    def rule(req):  # type: ignore[no-untyped-def]
        return NotReady("broker_bar_not_final") if req.event is ArmEvent.POST and req.now_ms < final_at else None

    r = bc_rig(rule=rule, stop_reads=True, now=T1)
    await r.step(T1)
    await r.step(T1 + SETTLE + 1_000)
    await r.step(final_at)                                     # ready, but the newest read is 180 s old: re-read
    assert r.store.rows_of() == []
    assert [q for q in r.eval.requests if q.now_ms == final_at] == []   # not prepared while it waits for the read
    await r.step(final_at + 250)
    [d] = r.store.rows_of()
    assert DELAYED in d.caveats
    req = r.eval.requests[-1]
    at_t, now_read = req.stop_reads.at_bar_end, req.stop_reads.current
    assert at_t is not None and (at_t.anchor, at_t.sent_ms) == (Anchor.POST, T1)
    assert now_read is not None and (now_read.anchor, now_read.sent_ms) == (Anchor.REREAD, final_at)
    assert req.stop_reads.fresh(ArmEvent.POST, final_at + 250)


async def test_stop_reads_off_queues_nothing() -> None:
    clock = FakeClock(T1)
    port = FakeStopPort(clock)
    reads = StopReads(port, handles_for("P1"), enabled=False, pre_offset_s=40)
    book = ReadBook(ADAPTER, COLD_SKEW, T1, T2, T2 + SETTLE)
    for at in (T1, pre_queue_ms(T2, COLD_SKEW, 40), T2):
        clock.now = at
        reads.tick(at, [book])
        assert reads.request(ADAPTER, ArmEvent.LATE, at)
    await reads.drain()
    assert port.calls == [] and reads.view(ADAPTER, T1).current is None
    assert reads.pre_state(ADAPTER, T2, f"{ADAPTER}:P1") == "off"
    r = bc_rig(stop_reads=False, now=T1)
    await r.step(T1 + SETTLE + 1_000)
    assert len(r.store.rows_of()) == 1 and r.reads.counters.reads == 0


async def test_reopen_reads_are_staggered_and_on_demand_reads_coalesced() -> None:
    clock = FakeClock(T1)
    port = FakeStopPort(clock, fail=True)
    reads = StopReads(port, handles_for("P1", "P2"), enabled=True, pre_offset_s=40)
    assert not reads.request(ADAPTER, ArmEvent.REOPEN, T1)
    assert not reads.request(ADAPTER, ArmEvent.REOPEN, T1 + 5_000)
    await reads.drain()
    assert not reads.request(ADAPTER, ArmEvent.REOPEN, T1 + 10_000)
    await reads.drain()
    assert len(port.calls) == 2 and reads.counters.failed == 2
    port.fail = False
    clock.now = T1 + 20_000
    assert not reads.request(ADAPTER, ArmEvent.LATE, T1 + 20_000)
    assert not reads.request(ADAPTER, ArmEvent.CORRECTED, T1 + 20_000)   # same lane, in flight: coalesced
    await reads.drain()
    assert len(port.calls) == 3
    assert reads.request(ADAPTER, ArmEvent.LATE, T1 + 21_000)            # fresh now
    assert reads.counters.reopen_reads == 2 and reads.counters.on_demand == 1


async def test_a_read_without_a_send_time_counts_as_failed() -> None:
    class NoStamp:
        async def read_stop_handles(self, adapter_id: str,
                                    handles: Sequence[HandleRef]) -> StopHandleRead | Transient:
            return StopHandleRead(adapter_id, (), None)

    reads = StopReads(NoStamp(), handles_for("P1"), enabled=True, pre_offset_s=40)
    assert not reads.request(ADAPTER, ArmEvent.LATE, T1)
    await reads.drain()
    assert (reads.counters.failed, reads.counters.no_send_time) == (1, 1)
    assert reads.view(ADAPTER, T1).current is None


class HangingPort:
    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.calls = 0

    async def read_stop_handles(self, adapter_id: str, handles: Sequence[HandleRef]) -> StopHandleRead | Transient:
        self.calls += 1
        await self.release.wait()
        return Transient("late")


async def test_pre_whose_read_never_returns_is_not_sent_and_counts_stop_stale() -> None:
    port = HangingPort()
    r = bc_rig(arm_row(cursor=T1), stop_reads=True, read_port=port)
    reads = r.reads
    fire = pre_fire_ms(T2, COLD_SKEW, 40)
    r.clock.now = pre_queue_ms(T2, COLD_SKEW, 40)
    await r.clock_.step()
    r.clock.now = fire
    await r.clock_.step()
    r.clock.now = pre_valid_until_ms(fire, T2, ROW, COLD_SKEW) + 1
    await r.clock_.step()
    assert r.store.rows_of() == [] and r.eval.requests == []
    assert r.clock_.counters.pre_dropped == {PRE_READ_MISSING: 1}
    assert reads.stale_counts() == {f"{ADAPTER}:P1": 1}
    reads.close()
