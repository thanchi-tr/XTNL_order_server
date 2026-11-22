"""Review round 6 (final review): the per-source views of the dead-man receiver.

1. A stopped leader counted as a running second engine when only one of its beats arrived after the new leader's
   first beat (a late delivery). Its durations then ran on to `now` for 40 s, so a clean failover could page about
   an engine that had already stopped. Now another source interleaves only with at least TWO_ENGINE_MIN_BEATS beats
   strictly after the newest source's first beat, or while `two_engines` is active or held.
2. When a stalled receiver handled the old term's last beat and the new term's first beat in the same millisecond,
   the round-6 handover was skipped (strict `<`), giving a false resolved notice and a second page. Equal receive
   times now count as "stopped before".
3. Source views were pruned on the receiver's wall clock, so a wall step back kept a stopped source's conditions
   active for the size of the step plus 40 s. They now use the monotonic clock.
"""
from __future__ import annotations

from typing import Any

from src.deadman_receiver.evaluate import SOURCE_LIVE_MS, BeatError, Evaluator, parse_beat
from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import ReceiverService
from src.deadman_receiver.settings import ReceiverSettings
from src.runtime.deadman import AdapterBeat
from tests.deadman_receiver.test_evaluate import T0, body, payload
from tests.deadman_receiver.test_service import ENV, Clock, FakeTransport
from tests.deadman_receiver.test_service import payload as svc_payload
from tests.deadman_receiver.test_two_engines_conditions_round5 import S, flips, interleaved

BACKOFF = {"stonex": AdapterBeat("BACKOFF", True, None)}
NOT_STREAMING = "adapter_not_streaming:stonex"
CIRCUIT = "circuit_open:integrity"

# (receive second, sent second, payload keywords)
Plan = list[tuple[int, int, dict[str, Any]]]


def run(plan: Plan, key: str, *, until_s: int, step_at_s: int | None = None,
        step_ms: int = 0) -> list[tuple[int, bool]]:
    """(second, now active) at each change of `key` on a 1 s tick. From `step_at_s` the receiver's wall clock is
    `step_ms` behind its monotonic clock (the engines' clocks stay right)."""
    plan = sorted(plan, key=lambda x: x[0])
    ev = Evaluator(started_ms=T0)
    i, active, out = 0, False, []
    for sec in range(until_s):
        mono = T0 + sec * S
        wall = mono - (step_ms if step_at_s is not None and sec >= step_at_s else 0)
        while i < len(plan) and T0 + plan[i][0] * S <= mono:
            _, sent, kw = plan[i]
            try:
                ev.accept(parse_beat(body(payload(sent_at_ms=T0 + sent * S, **kw))), wall, rx_ms=mono)
            except BeatError:
                pass
            i += 1
        now = key in ev.active(wall, rx_ms=mono)
        if now != active:
            out.append((sec, now))
            active = now
    return out


# ---------------------------------------------------------------- 1. one late beat of a stopped leader
def failover(*, last_rx_s: int) -> Plan:
    """aws-a epoch 5 beats every 15 s up to sent 300 s, spill non-empty from 270 s; its last beat is received at
    `last_rx_s`. aws-b epoch 6 beats cleanly from 302 s."""
    plan: Plan = [(s, s, {"node": "aws-a", "epoch": "5", "spill_bytes": 100 if s >= 270 else 0})
                  for s in range(0, 300, 15)]
    plan.append((last_rx_s, 300, {"node": "aws-a", "epoch": "5", "spill_bytes": 100}))
    plan += [(s, s, {"node": "aws-b", "epoch": "6"}) for s in range(302, 600, 15)]
    return plan


def test_one_late_beat_of_a_stopped_leader_never_pages() -> None:
    """The finding's reproduction: the last beat (sent 300 s) arrives at 304 s, after aws-b's first (302 s)."""
    assert run(failover(last_rx_s=304), "spill_nonempty", until_s=600) == []


def test_control_on_time_last_beat_never_pages() -> None:
    assert run(failover(last_rx_s=300), "spill_nonempty", until_s=600) == []


async def test_late_beat_through_the_receiver_service() -> None:
    """The same failover through ReceiverService.tick: no spill page and no resolved notice (it sent a critical
    page at 330 s and a resolved notice at 344 s)."""
    clock = Clock()
    t = FakeTransport()
    svc = ReceiverService(ReceiverSettings.from_env(ENV), Pager([t], clock), clock_ms=clock)
    start = clock.ms
    plan = sorted(failover(last_rx_s=304), key=lambda x: x[0])
    i = 0
    for sec in range(500):
        clock.ms = start + sec * S
        while i < len(plan) and plan[i][0] <= sec:
            _, sent, kw = plan[i]
            svc.on_beat(svc_payload(start + sent * S, **kw).to_json().encode())
            i += 1
        await svc.tick()
    assert not [m for m in t.sent if "spill" in m.subject]
    assert "spill_nonempty" not in svc.evaluator._episodes      # round 9: one episode per key, ended


def test_a_real_second_engine_still_counts_after_two_beats() -> None:
    """Two engines whose beats interleave: the spare's spill duration still runs to now and pages at 60 s."""
    plan = interleaved({"node": "pc1", "epoch": "5"}, {"node": "pc1", "epoch": "3", "spill_bytes": 10},
                       until_s=300)
    assert flips(plan, "spill_nonempty", until_s=300) == [(68, True)]


def test_a_second_engine_whose_clock_runs_behind_still_counts() -> None:
    """The spare's beats are all refused as not newer (its clock 5 s behind) but still recorded: once two of its
    beats came after the main box's first, its durations run to now."""
    plan: Plan = [(s, s, {"node": "pc1", "epoch": "5"}) for s in range(0, 300, 15)]
    plan += [(s, s - 5, {"node": "pc1", "epoch": "3", "spill_bytes": 10}) for s in range(8, 300, 15)]
    assert run(plan, "spill_nonempty", until_s=300) == [(68, True)]


# ---------------------------------------------------------------- 2. same-millisecond handover
def stalled_restart(*, new_first_rx_s: int) -> Plan:
    """aws-a epoch 5 (BACKOFF) beats every 15 s, its last beat (sent 300 s) handled at 312 s; aws-b epoch 6
    (BACKOFF), first beat sent 310 s handled at `new_first_rx_s`, then every 15 s."""
    a = {"node": "aws-a", "epoch": "5", "adapters": BACKOFF}
    b = {"node": "aws-b", "epoch": "6", "adapters": BACKOFF}
    plan: Plan = [(s, s, a) for s in range(0, 300, 15)]
    plan.append((312, 300, a))
    plan.append((new_first_rx_s, 310, b))
    plan += [(new_first_rx_s + 15 * k, 310 + 15 * k, b) for k in range(1, 40)]
    return plan


def test_same_millisecond_handover_pages_once() -> None:
    """The finding's reproduction: both beats handled at 312 s (A first). One activation, no false resolve."""
    assert run(stalled_restart(new_first_rx_s=312), NOT_STREAMING, until_s=900) == [(120, True)]


def test_same_millisecond_handover_new_term_first() -> None:
    """The stalled receiver handles the new term's beat first and the old one (refused) in the same millisecond."""
    plan = stalled_restart(new_first_rx_s=312)
    plan.sort(key=lambda x: (x[0], x[2]["node"] == "aws-a"))       # aws-b's beat before aws-a's at 312 s
    ev = Evaluator(started_ms=T0)
    seen = []
    i = 0
    for sec in range(900):
        t = T0 + sec * S
        while i < len(plan) and T0 + plan[i][0] * S <= t:
            _, sent, kw = plan[i]
            try:
                ev.accept(parse_beat(body(payload(sent_at_ms=T0 + sent * S, **kw))), T0 + plan[i][0] * S,
                          rx_ms=T0 + plan[i][0] * S)
            except BeatError:
                pass
            i += 1
        seen.append(NOT_STREAMING in ev.active(t, rx_ms=t))
    assert seen[120:] == [True] * (900 - 120)


def test_control_one_millisecond_apart_pages_once() -> None:
    assert run(stalled_restart(new_first_rx_s=313), NOT_STREAMING, until_s=900) == [(120, True)]


# ---------------------------------------------------------------- 3. wall clock step on the receiver
def stepped() -> Plan:
    plan: Plan = [(s, s, {"node": "aws-a", "epoch": "5", "degraded": ["integrity"]}) for s in range(0, 301, 15)]
    plan += [(s, s, {"node": "aws-b", "epoch": "6"}) for s in range(302, 700, 15)]
    return plan


def test_wall_step_back_does_not_keep_a_stopped_source() -> None:
    """The finding's reproduction: the receiver's wall clock steps back 240 s at 310 s. The stopped aws-a's open
    circuit resolves SOURCE_LIVE_MS after its last beat (300 s), as without the step."""
    control = run(stepped(), CIRCUIT, until_s=700)
    step = run(stepped(), CIRCUIT, until_s=700, step_at_s=310, step_ms=240 * S)
    assert control == step == [(0, True), (300 + SOURCE_LIVE_MS // S + 1, False)]
