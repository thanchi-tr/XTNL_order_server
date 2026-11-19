"""S39b gate `entry_time_source` (ARM side): an `ESTIMATED` entry time is upgraded to `BROKER` from the broker's
trade-history execution time (`ExecutionTimePort`), read in its own task, retried at most every minute and a bounded
number of times, and only while bar 0's `decision_cap` has not passed. Synthetic ids and times only."""
from __future__ import annotations

import asyncio

from src.riskmgr.ports import HistoryFill
from src.riskmgr.tracker import HISTORY_MAX_ATTEMPTS, HISTORY_RETRY_MS
from tests.riskmgr.tracker_fakes import ADAPTER, T0, image, position, rig

M15_MS = 15 * 60_000
HOUR = 3_600_000


class FakeHistory:
    def __init__(self, fill_ms: int | None = None, *, fail: int = 0, supported: bool = True) -> None:
        self.fill_ms = fill_ms
        self.fail = fail
        self.supported = supported
        self.calls: list[tuple[str, str, int, int]] = []

    def supports(self, adapter_id: str) -> bool:
        return self.supported

    async def execution_time(self, adapter_id: str, order_id: str, since_ms: int, until_ms: int) -> HistoryFill:
        self.calls.append((adapter_id, order_id, since_ms, until_ms))
        if self.fail > 0:
            self.fail -= 1
            raise ConnectionError("history read failed")
        return HistoryFill(fill_ms=self.fill_ms, fills=0 if self.fill_ms is None else 1)


async def _pass(r: object, advance_ms: int = 2_000) -> None:
    r.clock.now += advance_ms                                     # type: ignore[attr-defined]
    await r.tracker.step()                                        # type: ignore[attr-defined]
    await asyncio.sleep(0)                                        # the lookup task runs
    await asyncio.sleep(0)


async def test_an_estimated_entry_time_is_upgraded_to_broker_from_the_history() -> None:
    r = rig()
    hist = FakeHistory(fill_ms=T0 - 3 * 60_000)                   # the fill was in the previous bucket
    r.tracker._exec_times = hist                                  # noqa: SLF001  (the runtime passes exec_times=)
    r.snapshot.images = [image(position("P1", opened_by="E1"))]
    await r.tracker.step()
    row = r.store.row()
    assert (row.entry_time_source, row.entry_time_ms) == ("ESTIMATED", r.clock.now)
    await _pass(r)                                                # event_log has nothing: the history lookup starts
    assert [c[:2] for c in hist.calls] == [(ADAPTER, "E1")] and r.store.row().entry_time_source == "ESTIMATED"
    since, until = hist.calls[0][2:]
    assert since < row.entry_time_ms <= until
    await _pass(r)                                                # the finished lookup upgrades the pin
    row = r.store.row()
    assert (row.entry_time_source, row.entry_time_ms) == ("BROKER", T0 - 3 * 60_000)
    assert row.last_decided_bar == T0 - M15_MS and r.tracker.counters.history_upgrades == 1
    await _pass(r)
    assert len(hist.calls) == 1                                   # exact now: no more reads


async def test_a_failed_or_empty_lookup_is_retried_bounded_and_never_after_bar_zero_cap() -> None:
    r = rig()
    hist = FakeHistory(fill_ms=None, fail=1)
    r.tracker._exec_times = hist                                  # noqa: SLF001
    r.snapshot.images = [image(position("P1"))]
    await r.tracker.step()
    await _pass(r)
    await _pass(r)                                                # the failure is counted; no retry within a minute
    assert len(hist.calls) == 1 and r.tracker.counters.history_errors == 1
    assert hist.calls[0][1] == "P1"                               # no entry order id: the position's own id
    for _ in range(HISTORY_MAX_ATTEMPTS + 2):
        await _pass(r, HISTORY_RETRY_MS)
    assert len(hist.calls) == HISTORY_MAX_ATTEMPTS
    assert r.store.row().entry_time_source == "ESTIMATED"
    late = rig()
    late_hist = FakeHistory(fill_ms=T0)
    late.tracker._exec_times = late_hist                          # noqa: SLF001
    late.snapshot.images = [image(position("P1"))]
    await late.tracker.step()
    await _pass(late, 2 * HOUR)                                   # past bar 0's decision_cap: never looked up
    assert late_hist.calls == [] and late.store.row().entry_time_source == "ESTIMATED"


async def test_close_cancels_a_running_lookup() -> None:
    gate = asyncio.Event()

    class Slow(FakeHistory):
        async def execution_time(self, adapter_id: str, order_id: str, since_ms: int, until_ms: int) -> HistoryFill:
            await gate.wait()
            return HistoryFill(fill_ms=T0)

    r = rig()
    r.tracker._exec_times = Slow()                                # noqa: SLF001
    r.snapshot.images = [image(position("P1"))]
    await r.tracker.step()
    await _pass(r)
    tasks = list(r.tracker._history.values())                     # noqa: SLF001
    assert len(tasks) == 1
    r.tracker.close()
    await asyncio.sleep(0)
    assert tasks[0].cancelled() and r.tracker._history == {}      # noqa: SLF001


async def test_an_adapter_whose_history_cannot_match_is_never_looked_up() -> None:
    r = rig()
    hist = FakeHistory(fill_ms=T0, supported=False)
    r.tracker._exec_times = hist                                  # noqa: SLF001
    r.snapshot.images = [image(position("P1", opened_by="E1"))]
    await r.tracker.step()
    for _ in range(3):
        await _pass(r, HISTORY_RETRY_MS)
    assert hist.calls == [] and r.tracker.counters.history_lookups == 0
    assert r.tracker._history == {} and r.tracker._history_tries == {}    # noqa: SLF001


async def test_a_lookup_left_when_bar_zero_cap_passes_is_dropped_and_its_exception_consumed() -> None:
    r = rig()
    hist = FakeHistory(fill_ms=None, fail=1)
    r.tracker._exec_times = hist                                  # noqa: SLF001
    r.snapshot.images = [image(position("P1"))]
    await r.tracker.step()
    await _pass(r)                                                # the lookup starts and fails (not yet collected)
    assert len(hist.calls) == 1
    task = r.tracker._history[f"{ADAPTER}:P1"]                    # noqa: SLF001
    assert task.done() and isinstance(task.exception(), ConnectionError)
    await _pass(r, 2 * HOUR)                                      # past bar 0's cap: forgotten, no new read
    assert r.tracker._history == {} and r.tracker._history_tries == {}    # noqa: SLF001
    assert len(hist.calls) == 1
