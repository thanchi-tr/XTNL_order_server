"""TaskSupervisor: restart with backoff, liveness stamps, per-task circuit, half-open, reset_task,
critical hard exit; LoopWatchdog and sd_notify (operations.md §3)."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from src.runtime.sdnotify import SdNotifier
from src.runtime.supervisor import LoopWatchdog, TaskContext, TaskSpec, TaskSupervisor
from tests.runtime.fakes import Exits, FakeClock


@dataclass
class Pages:
    items: list[tuple[str, str, str]] = field(default_factory=list)

    def page(self, severity: str, title: str, body: str) -> None:
        self.items.append((severity, title, body))


async def crash(ctx: TaskContext) -> None:
    await asyncio.sleep(0)
    raise RuntimeError("boom")


async def forever(ctx: TaskContext) -> None:
    while True:
        ctx.stamp()
        await asyncio.sleep(0.001)


async def settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


def sup(*specs: TaskSpec) -> tuple[TaskSupervisor, FakeClock, Exits, Pages, list[tuple[str, str, str]]]:
    clk, exits, pages, audit = FakeClock(), Exits(), Pages(), []
    s = TaskSupervisor(list(specs), clock=clk, exit_fn=exits, pager=pages,
                       audit=lambda op, name, who: audit.append((op, name, who)))
    return s, clk, exits, pages, audit


async def test_restart_with_exponential_backoff() -> None:
    s, clk, _, _, _ = sup(TaskSpec("t", crash, max_stamp_age_s=5, backoff_base_s=1, backoff_max_s=4))
    s.start()
    delays = []
    for _ in range(4):
        await settle()
        s.poll()                                  # death noticed, restart scheduled
        assert not s.running("t")
        start = clk.t
        while not s.running("t"):
            clk.advance(0.5)
            s.poll()
        delays.append(clk.t - start)
    assert delays == [1.0, 2.0, 4.0, 4.0]          # capped at backoff_max
    assert s.restarts("t") == 4 and s.degraded() == []
    await s.stop_all()


async def test_circuit_opens_after_more_than_n_deaths_then_half_open_closes_after_clean_run() -> None:
    quarantined: list[str] = []
    behaviour = {"crash": True}

    async def flaky(ctx: TaskContext) -> None:
        if behaviour["crash"]:
            raise RuntimeError("bad input")
        await forever(ctx)

    s, clk, exits, pages, _ = sup(TaskSpec("feed", flaky, max_stamp_age_s=30, max_deaths=3, backoff_base_s=0.1,
                                           backoff_max_s=0.1, clean_run_s=60,
                                           on_quarantine=lambda: quarantined.append("feed")))
    s.start()
    for _ in range(4):
        await settle()
        s.poll()
        clk.advance(0.2)
        s.poll()
    await settle()
    s.poll()
    assert s.degraded() == ["feed"] and quarantined == ["feed"] and exits.reasons == []
    assert pages.items[-1][0] == "critical" and "circuit open" in pages.items[-1][1]
    clk.advance(599)
    s.poll()
    assert not s.running("feed")                   # no restart while open
    behaviour["crash"] = False
    clk.advance(1)
    s.poll()
    assert s.running("feed")                       # half-open: one retry
    await settle()
    clk.advance(59)
    await asyncio.sleep(0.01)                      # let it stamp at the new fake time
    s.poll()
    assert s.degraded() == ["feed"]
    clk.advance(1)
    await asyncio.sleep(0.01)
    s.poll()
    assert s.degraded() == [] and s.deaths("feed") == 0
    await s.stop_all()


async def test_half_open_death_keeps_circuit_open_for_another_10_min() -> None:
    s, clk, _, pages, _ = sup(TaskSpec("t", crash, max_stamp_age_s=5, max_deaths=0))
    s.start()
    await settle()
    s.poll()
    assert s.degraded() == ["t"]
    clk.advance(600)
    s.poll()
    await settle()
    s.poll()
    assert s.degraded() == ["t"] and "still open" in pages.items[-1][1]
    clk.advance(599)
    s.poll()
    assert not s.running("t")
    clk.advance(1)
    s.poll()
    assert s.running("t")
    await s.stop_all()


async def test_deaths_outside_the_window_do_not_count() -> None:
    s, clk, _, _, _ = sup(TaskSpec("t", crash, max_stamp_age_s=5, max_deaths=2, backoff_base_s=0.1,
                                   backoff_max_s=0.1))
    s.start()
    for _ in range(6):
        await settle()
        s.poll()
        clk.advance(400)                           # deaths spread over > 10 min
        s.poll()
    assert s.degraded() == []
    await s.stop_all()


async def test_reset_task_closes_the_circuit_and_is_audited() -> None:
    s, clk, _, _, audit = sup(TaskSpec("t", crash, max_stamp_age_s=5, max_deaths=0))
    s.start()
    await settle()
    s.poll()
    assert s.degraded() == ["t"] and s.circuit_open_age_s("t") == 0.0
    clk.advance(30)
    assert s.circuit_open_age_s("t") == 30.0
    s.reset_task("t", "alice")
    assert audit == [("reset_task", "t", "alice")] and s.degraded() == [] and s.running("t")
    assert s.circuit_open_age_s("t") is None and s.circuit_open_age_s("nope") is None
    await s.stop_all()


async def test_stale_stamp_pages_and_restarts_but_exempt_waits_do_not() -> None:
    gate = asyncio.Event()

    async def stuck(ctx: TaskContext) -> None:
        await gate.wait()

    async def polite(ctx: TaskContext) -> None:
        with ctx.exempt_wait("postgres"):
            await gate.wait()

    async def backing_off(ctx: TaskContext) -> None:
        ctx.set_exempt("BACKOFF")
        await gate.wait()

    s, clk, exits, pages, _ = sup(TaskSpec("stuck", stuck, max_stamp_age_s=5),
                                  TaskSpec("polite", polite, max_stamp_age_s=5),
                                  TaskSpec("backoff", backing_off, max_stamp_age_s=5))
    s.start()
    await settle()
    clk.advance(6)
    s.poll()
    assert [p[1] for p in pages.items] == ["task stuck stale"]
    assert s.running("polite") and s.running("backoff") and s.deaths("polite") == 0
    ctx = s.context("backoff")
    assert ctx is not None and ctx.exempt and ctx.exempt_reason == "BACKOFF"
    ctx.set_exempt(None)
    assert not ctx.exempt
    assert exits.reasons == []                     # a stale stamp never kills the process
    clk.advance(1)
    s.poll()
    assert s.running("stuck")
    await s.stop_all()


async def test_critical_task_death_hard_exits() -> None:
    s, _, exits, _, _ = sup(TaskSpec("orchestrator", crash, max_stamp_age_s=5, critical=True))
    s.start()
    await settle()
    s.poll()
    assert exits.reasons and exits.reasons[0].startswith("critical_task: orchestrator")
    await s.stop_all()


async def test_add_and_run_loop() -> None:
    clk = FakeClock()
    s = TaskSupervisor([], clock=clk, exit_fn=Exits(), poll_interval_s=0.001)
    stop = asyncio.Event()
    runner = asyncio.create_task(s.run(stop))
    await asyncio.sleep(0.005)
    s.add(TaskSpec("late", forever, max_stamp_age_s=5))
    await asyncio.sleep(0.005)
    assert s.running("late")
    stop.set()
    await asyncio.wait_for(runner, 1)
    assert not s.running("late")


def test_unique_names() -> None:
    import pytest
    with pytest.raises(ValueError):
        TaskSupervisor([TaskSpec("a", forever, 1), TaskSpec("a", forever, 1)])


class FakeNotifier(SdNotifier):
    def __init__(self) -> None:
        super().__init__(notify_socket="")
        self.sent: list[str] = []

    def notify(self, state: str) -> bool:
        self.sent.append(state)
        return True


def test_watchdog_feeds_only_while_lease_ok_and_alerts_on_lag() -> None:
    n, lags = FakeNotifier(), []
    lease = {"ok": True}
    wd = LoopWatchdog(n, lease_ok=lambda: lease["ok"], on_lag=lags.append)
    wd.beat(0.01)
    wd.beat(0.25)
    lease["ok"] = False
    wd.beat(0.0)
    assert n.sent == ["WATCHDOG=1", "WATCHDOG=1"] and wd.withheld == 1
    assert lags == [0.25] and wd.max_lag_s == 0.25


async def test_watchdog_run_sends_ready_and_beats() -> None:
    n = FakeNotifier()
    wd = LoopWatchdog(n, interval_s=0.005)
    stop = asyncio.Event()
    task = asyncio.create_task(wd.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert n.sent[0] == "READY=1" and wd.beats >= 2


def test_sd_notify_is_a_noop_without_socket() -> None:
    n = SdNotifier(notify_socket="")
    assert not n.enabled and n.notify("WATCHDOG=1") is False
    n.close()
