"""Scenario matrix for the dead-man receiver's condition tracking (invariants I1-I10).

Every case drives the REAL `ReceiverService` (and through it the real `Evaluator`) with a fake pager transport and a
deterministic simulated clock: the monotonic clock is the simulation time, the wall clock is the monotonic clock plus
an offset that only the I8 cases step. Beats are POST bodies built with the engine's own `DeadmanPayload`
(`tests.deadman_receiver.test_service.payload`), handed to `ReceiverService.on_beat` at their receive time; the 1 s
tick runs at every whole second. A transition is a key entering or leaving `ReceiverService.active` after a tick,
which is exactly what the service pages (new key: an episode page; key gone: a resolved notice); the harness checks
that the pages the fake transport received agree.

Parametrised over the condition kind (three duration conditions and one age condition), the engine cadence
(15.0 / 15.3 / 18 / 22 s, and a seeded jitter of 15-22 s between beats with 0.05-1.5 s delivery delay) and the
scenario shapes of I1-I10. Each case asserts the exact transition list (key, on/off, time window); a failure prints
the whole observed list.
"""
from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

import pytest

from src.deadman_receiver.evaluate import OVERLAP_MIN_SPAN_MS, SOURCE_LIVE_MS
from src.deadman_receiver.pager import Pager
from src.deadman_receiver.service import ReceiverService
from src.deadman_receiver.settings import ReceiverSettings
from src.runtime.deadman import AdapterBeat
from tests.deadman_receiver.test_service import ENV, FakeTransport, payload

T0 = int(datetime(2026, 11, 13, 10, 0, tzinfo=UTC).timestamp() * 1000)   # far from the 22:00 UTC test page
SEED = 20261122
LIVE_S = SOURCE_LIVE_MS / 1000
WINDOW_S = 60.0                                   # default XTNL_RECEIVER_TWO_ENGINES_WINDOW_S
TWO_ENGINES_RESOLVE_S = OVERLAP_MIN_SPAN_MS / 1000 + WINDOW_S   # look-back, then the Hold (evaluate.py docstring)
NODE_CHANGE_HOLD_S = 300.0


# ------------------------------------------------------------------------------------------ parameters
@dataclass(frozen=True)
class Cad:
    name: str
    period: float | None                          # None: seeded jitter

    @property
    def cmax(self) -> float:
        """The longest interval between two beats (one beat period of the bounds)."""
        return self.period if self.period is not None else 22.0

    @property
    def half(self) -> float:
        return (self.period or 18.5) / 2


CADS = [Cad("15.0", 15.0), Cad("15.3", 15.3), Cad("18", 18.0), Cad("22", 22.0), Cad("jitter", None)]
CAD_IDS = [c.name for c in CADS]


@dataclass(frozen=True)
class Kind:
    name: str
    key: str
    kw: dict[str, Any] = field(hash=False)
    need: float                                   # seconds the condition must hold before it pages


KINDS = [
    Kind("adapter_not_streaming", "adapter_not_streaming:stonex",
         {"adapters": {"stonex": AdapterBeat("BACKOFF", True, None)}}, 120.0),
    Kind("sm8_not_ready", "sm8_not_ready", {"sm8_state": "REBUILDING"}, 60.0),
    Kind("spill_nonempty", "spill_nonempty", {"spill_bytes": 4096}, 60.0),
    Kind("feed_lag", "feed_lag", {"oldest_unacked_feed_age_ms": 45_000}, 0.0),      # age condition: at once
]
KIND_IDS = [k.name for k in KINDS]


# ------------------------------------------------------------------------------------------ events
@dataclass(frozen=True)
class Beat:
    rx_ms: int                                    # receiver monotonic time after T0
    send_ms: int                                  # engine clock after T0 (the engine's clock is right)
    node: str
    epoch: str
    kw: dict[str, Any] = field(hash=False)

    @property
    def rx_s(self) -> float:
        return self.rx_ms / 1000

    @property
    def send_s(self) -> float:
        return self.send_ms / 1000

    def body(self) -> bytes:
        return payload(T0 + self.send_ms, node=self.node, epoch=self.epoch, **self.kw).to_json().encode()


@dataclass(frozen=True)
class Bad:
    """A malformed or skewed POST from a second sender."""

    rx_ms: int
    how: str                                      # missing_field | not_json | skewed

    def body(self) -> bytes:
        if self.how == "not_json":
            return b"{not json"
        if self.how == "skewed":                  # an old desktop (same env file) whose clock is 400 s behind
            return payload(T0 + self.rx_ms - 400_000, node="pc1", epoch="3").to_json().encode()
        if self.how == "skewed_ahead":            # the same old desktop with its clock 400 s ahead
            return payload(T0 + self.rx_ms + 400_000, node="pc1", epoch="3").to_json().encode()
        d = json.loads(payload(T0 + self.rx_ms, node="old-pc", epoch="1").to_json())
        del d["degraded"]                         # an older checkout: a field missing
        return json.dumps(d).encode()


Event = Beat | Bad
ALWAYS: Callable[[float], bool] = lambda s: True  # noqa: E731


def engine(node: str, epoch: str, start_s: float, stop_s: float, cad: Cad, *, kind: Kind | None = None,
           show: Callable[[float], bool] = ALWAYS, delay_s: float | None = None) -> list[Beat]:
    """Beats of one engine term sent from `start_s` while <= `stop_s`; those whose send time passes `show` carry the
    condition `kind`. Fixed cadences arrive 0.25 s after sending; the jitter cadence draws 15-22 s intervals and a
    0.05-1.5 s delivery delay from a seeded generator (deterministic per engine term)."""
    rng = random.Random(f"{SEED}:{node}:{epoch}:{start_s}:{cad.name}")
    out: list[Beat] = []
    s = start_s
    while s <= stop_s + 1e-9:
        d = delay_s if delay_s is not None else (0.25 if cad.period is not None else rng.uniform(0.05, 1.5))
        kw = dict(kind.kw) if kind is not None and show(s) else {}
        out.append(Beat(round((s + d) * 1000), round(s * 1000), node, epoch, kw))
        s += cad.period if cad.period is not None else rng.uniform(15.0, 22.0)
    return out


def bad_posts(start_s: float, stop_s: float, cad: Cad, how: str) -> list[Bad]:
    rng = random.Random(f"{SEED}:bad:{how}:{cad.name}")
    out: list[Bad] = []
    s = start_s
    while s <= stop_s + 1e-9:
        out.append(Bad(round(s * 1000), how))
        s += cad.period if cad.period is not None else rng.uniform(15.0, 22.0)
    return out


def first_shown(beats: Sequence[Beat]) -> Beat:
    return next(b for b in beats if b.kw)


def last_shown(beats: Sequence[Beat]) -> Beat:
    return [b for b in beats if b.kw][-1]


# ------------------------------------------------------------------------------------------ simulation
Transition = tuple[int, str, bool]


@dataclass
class Sim:
    transitions: list[Transition]
    episodes: int
    resolved: int
    events: list[Event]                           # as handled (after a stall moved them)


async def _simulate(events: Iterable[Event], until_s: float, *, stall: tuple[float, float] | None = None,
                    steps: Sequence[tuple[float, float]] = ()) -> Sim:
    """Run the receiver from T0 to `until_s`. `stall` (start, end in s): the receiver's event loop is stalled, every
    POST received in it is handled at `end` in arrival order and no tick runs inside it. `steps`: (at s, delta s)
    receiver wall clock steps (the monotonic clock is not touched)."""
    mono = [T0]

    def wall_at(m: int) -> int:
        return m + sum(round(d * 1000) for at, d in steps if m - T0 >= at * 1000)

    def mono_ms() -> int:
        return mono[0]

    def wall_ms() -> int:
        return wall_at(mono[0])

    transport = FakeTransport()
    settings = ReceiverSettings.from_env({**ENV, "XTNL_RECEIVER_REPAGE_S": "86400"})
    svc = ReceiverService(settings, Pager([transport], wall_ms), clock_ms=wall_ms, mono_ms=mono_ms)
    evs: list[Event] = sorted(events, key=lambda e: e.rx_ms)
    s0 = s1 = 0
    if stall is not None:
        s0, s1 = round(stall[0] * 1000), round(stall[1] * 1000)
        evs = sorted((replace(e, rx_ms=s1) if s0 <= e.rx_ms < s1 else e for e in evs), key=lambda e: e.rx_ms)
    prev: set[str] = set()
    out: list[Transition] = []
    i = 0
    for k in range(int(until_s) + 1):
        t = k * 1000
        while i < len(evs) and evs[i].rx_ms <= t:
            mono[0] = T0 + evs[i].rx_ms
            svc.on_beat(evs[i].body())
            i += 1
        if stall is not None and s0 < t < s1:
            continue
        mono[0] = T0 + t
        await svc.tick()
        now = set(svc.active)
        out += [(k, key, True) for key in sorted(now - prev)]
        out += [(k, key, False) for key in sorted(prev - now)]
        prev = now
    episodes = sum(1 for m in transport.sent if m.kind == "episode")
    resolved = sum(1 for m in transport.sent if m.subject.startswith("[XTNL DEADMAN] resolved:"))
    return Sim(out, episodes, resolved, evs)


def run(events: Iterable[Event], until_s: float, **kw: Any) -> Sim:
    sim = asyncio.run(_simulate(events, until_s, **kw))
    # harness self-check: the transitions are what was paged
    assert sim.episodes == sum(1 for t in sim.transitions if t[2]), sim.transitions
    assert sim.resolved == sum(1 for t in sim.transitions if not t[2]), sim.transitions
    return sim


Spec = tuple[str, bool, float, float]


def check(sim: Sim, spec: Sequence[Spec], *, optional: Sequence[tuple[str, bool]] = ()) -> None:
    """Every spec (key, on, earliest s, latest s) matches one distinct transition; each `optional` (key, on) may
    match at most one more; nothing else may happen."""
    left = list(sim.transitions)
    missing: list[Spec] = []
    for key, on, lo, hi in spec:
        hit = next((x for x in left if x[1] == key and x[2] == on and lo <= x[0] <= hi), None)
        if hit is None:
            missing.append((key, on, lo, hi))
        else:
            left.remove(hit)
    for key, on in optional:
        hit = next((x for x in left if x[1] == key and x[2] == on), None)
        if hit is not None:
            left.remove(hit)
    assert not missing and not left, f"observed={sim.transitions} missing={missing} unexpected={left}"


def activation(kind: Kind, start_rx_s: float, slack_s: float = 0.0) -> Spec:
    """The condition pages at its threshold from `start_rx_s` (the next whole-second tick), `slack_s` later at most."""
    return kind.key, True, start_rx_s + kind.need, start_rx_s + kind.need + 1 + slack_s


# ============================================================================================ I1 / I2
@pytest.mark.parametrize("gap_s", [1.0, 20.0, 45.0])
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", KINDS, ids=KIND_IDS)
def test_i1_condition_across_engine_restart_pages_once(kind: Kind, cad: Cad, gap_s: float) -> None:
    """I1: same node, new epoch, gap shorter than the silence threshold; the condition holds throughout."""
    old = engine("pc1", "5", 0, 300, cad, kind=kind)
    b0 = old[-1].send_s + gap_s
    new = engine("pc1", "6", b0, b0 + 400, cad, kind=kind)
    sim = run(old + new, b0 + 420)
    check(sim, [activation(kind, old[0].rx_s)])


@pytest.mark.parametrize("gap_s", [1.0, 20.0, 45.0])
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", KINDS, ids=KIND_IDS)
def test_i2_condition_across_failover_pages_once(kind: Kind, cad: Cad, gap_s: float) -> None:
    """I2: failover to another node; only the node_changed notice (and its expiry) may be added."""
    old = engine("aws-a", "5", 0, 300, cad, kind=kind)
    b0 = old[-1].send_s + gap_s
    new = engine("aws-b", "9", b0, b0 + 400, cad, kind=kind)
    sim = run(old + new, b0 + 420)
    n0 = new[0].rx_s
    check(sim, [activation(kind, old[0].rx_s), ("node_changed", True, n0, n0 + 1),
                ("node_changed", False, n0 + NODE_CHANGE_HOLD_S, n0 + NODE_CHANGE_HOLD_S + 1)])


# ============================================================================================ I3
@pytest.mark.parametrize("shape", ["plain", "after_restart", "at_restart", "at_failover"])
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", KINDS, ids=KIND_IDS)
def test_i3_real_end_resolves_once_within_bound(kind: Kind, cad: Cad, shape: str) -> None:
    """I3: the condition really ends; it resolves within one beat period + SOURCE_LIVE_MS of the last beat that
    showed it, and does not come back."""
    if shape == "plain":
        beats = engine("pc1", "5", 0, 600, cad, kind=kind, show=lambda s: s < 300)
        until = 600.0
    elif shape == "after_restart":
        old = engine("pc1", "5", 0, 200, cad, kind=kind)
        b0 = old[-1].send_s + 10
        beats = old + engine("pc1", "6", b0, 700, cad, kind=kind, show=lambda s: s < 400)
        until = 700.0
    else:
        node2 = ("pc1", "6") if shape == "at_restart" else ("aws-b", "9")
        old = engine("pc1" if shape == "at_restart" else "aws-a", "5", 0, 300, cad, kind=kind)
        b0 = old[-1].send_s + 10
        beats = old + engine(*node2, b0, b0 + 280, cad)
        until = b0 + 280
    end = last_shown(beats).rx_s
    spec = [activation(kind, first_shown(beats).rx_s),
            (kind.key, False, end, end + cad.cmax + LIVE_S + 1)]
    optional: list[tuple[str, bool]] = []
    if shape == "at_failover":
        n0 = [b for b in beats if b.node == "aws-b"][0].rx_s
        spec.append(("node_changed", True, n0, n0 + 1))
        optional.append(("node_changed", False))
    check(run(beats, until), spec, optional=optional)


# ============================================================================================ I4
@pytest.mark.parametrize("beats_before", [0, 1])
@pytest.mark.parametrize("change", ["restart", "failover"])
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", KINDS, ids=KIND_IDS)
def test_i4_condition_starting_just_before_handover_is_not_late(kind: Kind, cad: Cad, change: str,
                                                                beats_before: int) -> None:
    """I4: the condition is first shown by the old term's last beat (or the one before it), then by the new term.
    It pages no later than its threshold from its first showing beat plus one beat, exactly once."""
    a, b = (("pc1", "5"), ("pc1", "6")) if change == "restart" else (("aws-a", "5"), ("aws-b", "9"))
    plain = engine(*a, 0, 200, cad)
    c_start = plain[-1 - beats_before].send_s
    old = engine(*a, 0, 200, cad, kind=kind, show=lambda s: s >= c_start)
    b0 = old[-1].send_s + 20
    new = engine(*b, b0, b0 + 300, cad, kind=kind)
    sim = run(old + new, b0 + 250)
    r0 = first_shown(old).rx_s
    spec: list[Spec] = [(kind.key, True, r0 + kind.need, r0 + kind.need + cad.cmax + 1)]
    if change == "failover":
        spec.append(("node_changed", True, new[0].rx_s, new[0].rx_s + 1))
    check(sim, spec)


# ============================================================================================ I5
PAIRINGS = {"nodes": (("pc1", "5"), ("pc2", "3")), "epochs": (("pc1", "5"), ("pc1", "3")),
            "shared": (("pc1", "5"), ("pc1", "5"))}


def two_engines(pairing: str, cad: Cad, kind: Kind | None, shower: str,
                cad_b: Cad | None = None) -> tuple[list[Beat], list[Beat], float]:
    """Engine A beats from 0 to 700 s, engine B (interleaved by half a period; at `cad_b` when given) from ~100 to
    460 s."""
    a, b = PAIRINGS[pairing]
    ea = engine(*a, 0, 700, cad, kind=kind if shower == "A" else None)
    eb = engine(*b, 100 + cad.half, 460, cad_b or cad, kind=kind if shower == "B" else None)
    return ea, eb, eb[-1].rx_s + 200


def two_engines_spec(pairing: str, eb: Sequence[Beat]) -> tuple[list[Spec], list[tuple[str, bool]]]:
    b_last = eb[-1].rx_s
    spec: list[Spec] = [("two_engines", True, eb[0].rx_s, b_last),
                        ("two_engines", False, b_last, b_last + TWO_ENGINES_RESOLVE_S + 2)]
    optional: list[tuple[str, bool]] = []
    if pairing == "nodes":
        spec.append(("node_changed", True, eb[0].rx_s, eb[0].rx_s + 1))
        optional.append(("node_changed", False))
    return spec, optional


@pytest.mark.parametrize("shower", ["A", "B"])
@pytest.mark.parametrize("pairing", list(PAIRINGS))
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", KINDS, ids=KIND_IDS)
def test_i5_two_engines_once_and_single_engine_condition_never_flaps(kind: Kind, cad: Cad, pairing: str,
                                                                     shower: str) -> None:
    """I5: two_engines activates once while both beat and resolves once after B stops (within the look-back plus
    the Hold); a condition only one engine shows pages once on time and never resolves while that engine beats
    (B's condition resolves once, after B stopped)."""
    _i5(kind, cad, cad, pairing, shower)


MIXED = [(Cad("15.0", 15.0), Cad("22", 22.0)), (Cad("22", 22.0), Cad("15.0", 15.0))]


@pytest.mark.parametrize("shower", ["A", "B"])
@pytest.mark.parametrize("pairing", list(PAIRINGS))
@pytest.mark.parametrize("cads", MIXED, ids=["A15-B22", "A22-B15"])
@pytest.mark.parametrize("kind", KINDS, ids=KIND_IDS)
def test_i5_mixed_cadences(kind: Kind, cads: tuple[Cad, Cad], pairing: str, shower: str) -> None:
    """I5 with the two engines at different fixed cadences (one at 15 s, one at the ~22 s of an engine whose
    PostgreSQL is slow): one engine then beats twice between two beats of the other."""
    _i5(kind, cads[0], cads[1], pairing, shower)


def _i5(kind: Kind, cad: Cad, cad_b: Cad, pairing: str, shower: str) -> None:
    ea, eb, until = two_engines(pairing, cad, kind, shower, cad_b)
    spec, optional = two_engines_spec(pairing, eb)
    shown = ea if shower == "A" else eb
    r0 = first_shown(shown).rx_s
    cmax = max(cad.cmax, cad_b.cmax)
    spec.append((kind.key, True, r0 + kind.need, r0 + kind.need + cmax + 1))
    if shower == "B":
        b_last = eb[-1].rx_s
        spec.append((kind.key, False, b_last, b_last + TWO_ENGINES_RESOLVE_S + 2))
    check(run(ea + eb, until), spec, optional=optional)


@pytest.mark.parametrize("pairing", list(PAIRINGS))
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
def test_i5_two_clean_engines_page_two_engines_once(cad: Cad, pairing: str) -> None:
    ea, eb, until = two_engines(pairing, cad, None, "A")
    spec, optional = two_engines_spec(pairing, eb)
    check(run(ea + eb, until), spec, optional=optional)


# ============================================================================================ I6
@pytest.mark.parametrize("restart_at", [50, 300])
@pytest.mark.parametrize("queued", [1, 2])
@pytest.mark.parametrize("change", ["restart", "failover"])
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", [*KINDS, None], ids=[*KIND_IDS, "none"])
def test_i6_handover_behind_stalled_receiver(kind: Kind | None, cad: Cad, change: str, queued: int,
                                             restart_at: int) -> None:
    """I6: the receiver stalls; the old term's last `queued` beats and the new term's first `queued` beats are
    handled in the same millisecond. No two_engines, no false resolve, the carried condition pages once."""
    a, b = (("pc1", "5"), ("pc1", "6")) if change == "restart" else (("aws-a", "5"), ("aws-b", "9"))
    old = engine(*a, 0, restart_at, cad, kind=kind)
    b0 = old[-1].send_s + 20
    new = engine(*b, b0, b0 + 400, cad, kind=kind)
    stall = (old[-queued].rx_s - 0.5, new[queued - 1].rx_s + 0.5)
    sim = run(old + new, b0 + 250, stall=stall)
    s_end = stall[1]
    spec: list[Spec] = []
    if kind is not None:
        beats = [e for e in sim.events if isinstance(e, Beat)]
        r0 = first_shown(beats).rx_s
        due = r0 + kind.need
        if stall[0] < due <= s_end:
            due = s_end                            # no tick runs inside the stall
        spec.append((kind.key, True, due, due + 1))
    if change == "failover":
        spec.append(("node_changed", True, s_end, s_end + 1))
    check(sim, spec)


# ============================================================================================ I7
@pytest.mark.parametrize("pattern", ["stream", "single"])
@pytest.mark.parametrize("how", ["missing_field", "not_json", "skewed"])
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", [*KINDS, None], ids=[*KIND_IDS, "none"])
def test_i7_malformed_posts_do_not_flip_other_conditions(kind: Kind | None, cad: Cad, how: str,
                                                         pattern: str) -> None:
    """I7: malformed / skewed POSTs from a second sender between 200 and 400 s (or one at 200 s) mixed into a
    normal stream: payload_invalid pages once and resolves once after they stop; the other condition is untouched."""
    main = engine("pc1", "5", 0, 600, cad, kind=kind)
    bads = bad_posts(200 + cad.half, 400 if pattern == "stream" else 200 + cad.half, cad, how)
    sim = run([*main, *bads], 600)
    first_bad, last_bad = bads[0].rx_ms / 1000, bads[-1].rx_ms / 1000
    spec: list[Spec] = [("payload_invalid", True, first_bad, first_bad + 1),
                        ("payload_invalid", False, last_bad + LIVE_S, last_bad + LIVE_S + cad.cmax + 1)]
    if kind is not None:
        spec.append(activation(kind, main[0].rx_s))
    check(sim, spec)


# ============================================================================================ I8
@pytest.mark.parametrize("step_at", [30, 200])
@pytest.mark.parametrize("step_s", [600, -600, 240, -240, 90, -90])
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", [*KINDS, None], ids=[*KIND_IDS, "none"])
def test_i8_wall_clock_step_changes_nothing(kind: Kind | None, cad: Cad, step_s: int, step_at: int) -> None:
    """I8: the receiver's wall clock steps by `step_s` at `step_at` (monotonic clock and the engine's clock
    unaffected); the condition (shown throughout) pages once on time, nothing else happens."""
    main = engine("pc1", "5", 0, 600, cad, kind=kind)
    sim = run(main, 600, steps=[(step_at, step_s)])
    check(sim, [] if kind is None else [activation(kind, main[0].rx_s)])


# ============================================================================================ I9
@pytest.mark.parametrize("shape", ["steady", "restart", "failover"])
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
def test_i9_clean_engine_pages_nothing(cad: Cad, shape: str) -> None:
    """I9: a clean single engine pages nothing (a failover adds only the node_changed notice and its expiry)."""
    if shape == "steady":
        beats = engine("pc1", "5", 0, 900, cad)
    else:
        a, b = (("pc1", "5"), ("pc1", "6")) if shape == "restart" else (("aws-a", "5"), ("aws-b", "9"))
        old = engine(*a, 0, 300, cad)
        beats = old + engine(*b, old[-1].send_s + 15, 900, cad)
    sim = run(beats, 900)
    spec: list[Spec] = []
    if shape == "failover":
        n0 = [x for x in beats if x.node == "aws-b"][0].rx_s
        spec = [("node_changed", True, n0, n0 + 1),
                ("node_changed", False, n0 + NODE_CHANGE_HOLD_S, n0 + NODE_CHANGE_HOLD_S + 1)]
    check(sim, spec)


# ============================================================================================ I10
@pytest.mark.parametrize("shower", ["old", "both"])
@pytest.mark.parametrize("timing", ["late", "overlap_one"])
@pytest.mark.parametrize("change", ["restart", "failover"])
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", KINDS, ids=KIND_IDS)
def test_i10_stopped_leader_late_beat(kind: Kind, cad: Cad, change: str, timing: str, shower: str) -> None:
    """I10: the old leader's last beat arrives after the new leader's first beat: `late` (sent 2 s before the new
    leader's first, delivered 5 s after sending: refused as not newer) or `overlap_one` (sent 3 s after the new
    leader's first: accepted). Not a second engine; an old-only condition resolves within the I3 bound."""
    a, b = (("pc1", "5"), ("pc1", "6")) if change == "restart" else (("aws-a", "5"), ("aws-b", "9"))
    old = engine(*a, 0, 300, cad, kind=kind)
    a_last = old[-1]
    if timing == "late":
        b0 = a_last.send_s + 2
        old[-1] = replace(a_last, rx_ms=a_last.send_ms + 5_000)
    else:
        b0 = a_last.send_s - 3
    new = engine(*b, b0, b0 + 400, cad, kind=kind if shower == "both" else None)
    sim = run(old + new, b0 + 200)
    spec: list[Spec] = [activation(kind, old[0].rx_s)]
    if shower == "old":
        end = old[-1].rx_s
        spec.append((kind.key, False, end, end + cad.cmax + LIVE_S + 1))
    if change == "failover":
        spec.append(("node_changed", True, new[0].rx_s, new[0].rx_s + 1))
    check(sim, spec)


# ============================================================================================ round 9 breaker
# Concrete sequences found by the adversarial round 9 (two-engine incidents, shared sources, malformed POSTs and
# receiver clock steps). Each one violates an invariant with the evaluator as it stands; see the case docstrings.
def _i5_at(kind: Kind, cad: Cad, cad_b: Cad, pairing: str, shower: str, b_start_s: float) -> None:
    """`_i5` with engine B's first beat sent at `b_start_s` (instead of half a period after one of A's)."""
    a, b = PAIRINGS[pairing]
    ea = engine(*a, 0, 700, cad, kind=kind if shower == "A" else None)
    eb = engine(*b, b_start_s, 460, cad_b, kind=kind if shower == "B" else None)
    spec, optional = two_engines_spec(pairing, eb)
    shown = ea if shower == "A" else eb
    r0 = first_shown(shown).rx_s
    spec.append((kind.key, True, r0 + kind.need, r0 + kind.need + max(cad.cmax, cad_b.cmax) + 1))
    if shower == "B":
        b_last = eb[-1].rx_s
        spec.append((kind.key, False, b_last, b_last + TWO_ENGINES_RESOLVE_S + 2))
    check(run(ea + eb, eb[-1].rx_s + 200), spec, optional=optional)


CAD_BY_NAME = {c.name: c for c in CADS}
SHARED_LATE_FIRST = [("15.0", 118.5), ("22", 123.0), ("22", 127.0)]


@pytest.mark.parametrize("cad_start", SHARED_LATE_FIRST, ids=["15-gap13", "22-gap13", "22-gap17"])
@pytest.mark.parametrize("kind", KINDS, ids=KIND_IDS)
def test_r9_i5_shared_source_first_beat_not_fast(kind: Kind, cad_start: tuple[str, float]) -> None:
    """I5, shared source (same node and epoch): engine B's first beat comes 13-17 s after A's latest, so it is not a
    fast pair and the source is not yet counted as shared. That one beat of B drops every duration start time of
    A's condition: an active condition resolves falsely and pages again a full threshold later (an age condition
    flips off and on), and one not yet active pages a full threshold late."""
    cad = CAD_BY_NAME[cad_start[0]]
    _i5_at(kind, cad, cad, "shared", "A", cad_start[1])


@pytest.mark.parametrize("pairing", ["nodes", "epochs"])
@pytest.mark.parametrize("case", [("15.3", "15.0", 106.5), ("18", "15.0", 106.5), ("15.3", "jitter", 120.5)],
                         ids=["A15.3-B15", "A18-B15", "A15.3-Bjitter"])
def test_r9_i5_condition_reaching_threshold_before_interleave_flaps(pairing: str,
                                                                    case: tuple[str, str, float]) -> None:
    """I5, two sources: A's condition reaches its threshold (120 s) in the seconds after B's first beat, before B
    counts as interleaving. While A's beat is the newest the condition counts to now and activates; at B's next
    beat A becomes "another source" whose durations stop at its last beat, so it resolves, and A's next beat brings
    it back: an activation, a false resolved notice and a second page within seconds."""
    ca, cb, b_start = case
    _i5_at(KINDS[0], CAD_BY_NAME[ca], CAD_BY_NAME[cb], pairing, "A", b_start)


@pytest.mark.parametrize("case", [("15.3", "22", 118.5), ("18", "jitter", 104.5)], ids=["A15.3-B22", "A18-Bjitter"])
@pytest.mark.parametrize("kind", KINDS[:3], ids=KIND_IDS[:3])
def test_r9_i5_shared_source_refused_beat_drops_condition(kind: Kind, case: tuple[str, str, float]) -> None:
    """I5, shared source: one of B's beats is refused as "not newer" (A's beat with the same or a later
    `sent_at_ms` was handled first: an equal millisecond, or B's delivery delay). A refused beat of the newest
    beat's own source is not recorded at all, so B's ~22 s cadence leaves a hole of ~44 s in the view, longer than
    SOURCE_LIVE_MS: B's condition resolves falsely while B still beats and pages again."""
    ca, cb, b_start = case
    _i5_at(kind, CAD_BY_NAME[ca], CAD_BY_NAME[cb], "shared", "B", b_start)


@pytest.mark.parametrize("step", [(150, -600), (250, -600), (250, -360)], ids=["before-600", "during-600",
                                                                               "during-360"])
@pytest.mark.parametrize("kind", [KINDS[1], None], ids=["sm8_not_ready", "none"])
def test_r9_i7_i8_skewed_sender_hidden_by_wall_step(kind: Kind | None, step: tuple[float, float]) -> None:
    """I7 + I8: a second sender 400 s behind (refused as skewed: `payload_invalid`) while the receiver's wall clock
    steps back. Its `sent_at_ms` is then within 5 min of the stepped wall clock, so the skew gate's first test lets
    it through and it is refused only as "not newer" (409, no `payload_invalid`): the page is missed (step before
    the stream) or resolves falsely while the bad POSTs continue (step during it)."""
    cad = CADS[0]
    main = engine("pc1", "5", 0, 600, cad, kind=kind)
    bads = bad_posts(200 + cad.half, 400, cad, "skewed")
    sim = run([*main, *bads], 600, steps=[step])
    first_bad, last_bad = bads[0].rx_ms / 1000, bads[-1].rx_ms / 1000
    spec: list[Spec] = [("payload_invalid", True, first_bad, first_bad + 1),
                        ("payload_invalid", False, last_bad + LIVE_S, last_bad + LIVE_S + cad.cmax + 1)]
    if kind is not None:
        spec.append(activation(kind, main[0].rx_s))
    check(sim, spec)


@pytest.mark.parametrize("step", [(100, 600), (100, 240), (250, 600)], ids=["before+600", "before+240",
                                                                            "during+600"])
@pytest.mark.parametrize("kind", [KINDS[1], None], ids=["sm8_not_ready", "none"])
def test_r9_i7_i8_skewed_ahead_sender_after_wall_step_locks_out_engine(kind: Kind | None,
                                                                      step: tuple[float, float]) -> None:
    """I7 + I8: a second sender 400 s AHEAD posts for ~90 s after the receiver's wall clock stepped forward. Within
    5 min of the stepped wall clock, its beats are accepted as the newest, so every beat of the healthy engine is
    refused ("not newer", or skewed against the engine clock carried on from the bad sender): no or a short
    `payload_invalid`, the healthy engine's condition resolves falsely, and a false `silence` page follows once the
    bad sender stops (for 400 s, or for good while the wall step stays)."""
    cad = CADS[0]
    main = engine("pc1", "5", 0, 900, cad, kind=kind)
    bads = bad_posts(200 + cad.half, 300, cad, "skewed_ahead")
    sim = run([*main, *bads], 900, steps=[step])
    first_bad, last_bad = bads[0].rx_ms / 1000, bads[-1].rx_ms / 1000
    spec: list[Spec] = [("payload_invalid", True, first_bad, first_bad + 1),
                        ("payload_invalid", False, last_bad + LIVE_S, last_bad + LIVE_S + cad.cmax + 1)]
    if kind is not None:
        spec.append(activation(kind, main[0].rx_s))
    check(sim, spec)


# ============================================================================================ breaker round (I2 / I3 / I10)
FIXED_CADS = [c for c in CADS if c.period is not None]
FIXED_CAD_IDS = [c.name for c in FIXED_CADS]


@pytest.mark.parametrize("skew_ms", [500, 2000])
@pytest.mark.parametrize("cad", FIXED_CADS, ids=FIXED_CAD_IDS)
@pytest.mark.parametrize("kind", KINDS, ids=KIND_IDS)
def test_i2_i10_new_leader_clock_behind_first_beat_refused(kind: Kind, cad: Cad, skew_ms: int) -> None:
    """I2 / I10: failover aws-a -> aws-b, aws-b's clock `skew_ms` behind aws-a's (well under the 15 s cadence). aws-b
    starts beating skew/2 after aws-a's second-last beat, so its first beat reads earlier than that beat and is
    refused as not newer (it only seeds aws-b's view, its duration start time at that instant). aws-a then sends its
    last beat (the only one after aws-b's first, accepted), and aws-b's second beat is refused too (it reads before
    aws-a's last). When aws-b's third beat becomes the newest, aws-a's last beat came after aws-b's first, so the
    handover does not carry the start time: aws-a's view (already past the threshold) is dropped SOURCE_LIVE_MS after
    its last beat while aws-b's own start time has not reached the threshold yet. Observed: a false resolved notice
    and a second critical page of a condition that held throughout (duration kinds; not 22 s for the 60 s ones)."""
    old = engine("aws-a", "5", 0, 300, cad, kind=kind)
    b0 = old[-2].send_s + skew_ms / 2000
    new = [replace(x, send_ms=x.send_ms - skew_ms) for x in engine("aws-b", "9", b0, b0 + 400, cad, kind=kind)]
    sim = run(old + new, b0 + 300)
    n0 = new[0].rx_s
    check(sim, [activation(kind, old[0].rx_s), ("node_changed", True, n0, n0 + 1)],
          optional=[("node_changed", False)])


@pytest.mark.parametrize("delay_s", [20, 30, 45])
@pytest.mark.parametrize("change", ["restart", "failover"])
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", KINDS, ids=KIND_IDS)
def test_i3_i10_very_late_last_beat_does_not_reactivate(kind: Kind, cad: Cad, change: str, delay_s: int) -> None:
    """I3 / I10: the stopped leader's last beat (sent 2 s before the new leader's first) sat in the socket behind
    `tailscale serve` and is delivered `delay_s` after sending (refused as not newer). The condition only the old
    leader showed resolves once and must not come back. Observed for the age condition (feed_lag): when the delay
    outlives SOURCE_LIVE_MS from the old leader's previous beat, the old source's view is already dropped (resolved),
    the late beat re-creates it and the condition pages again (then resolves a second time 40 s later)."""
    a, b = (("pc1", "5"), ("pc1", "6")) if change == "restart" else (("aws-a", "5"), ("aws-b", "9"))
    old = engine(*a, 0, 300, cad, kind=kind)
    a_last = old[-1]
    b0 = a_last.send_s + 2
    old[-1] = replace(a_last, rx_ms=a_last.send_ms + delay_s * 1000)
    new = engine(*b, b0, b0 + 400, cad)
    sim = run(old + new, b0 + 300)
    spec: list[Spec] = [activation(kind, old[0].rx_s),
                        (kind.key, False, old[-2].rx_s, old[-1].rx_s + cad.cmax + LIVE_S + 1)]
    optional: list[tuple[str, bool]] = []
    if change == "failover":
        spec.append(("node_changed", True, new[0].rx_s, new[0].rx_s + 1))
        optional.append(("node_changed", False))
    check(sim, spec, optional=optional)


# ============================================================================================ breaker round (I3 / I10, delayed newest beat)
# (cadence, new leader's first beat sent g s after the stopped leader's last, its delivery delay, the late beat's delay)
LATE_BEHIND_DELAYED_FIRST = [("15.0", 12.0, 11.5, 26.5), ("15.3", 12.0, 11.5, 26.5), ("18", 8.0, 8.5, 23.5),
                             ("22", 2.0, 5.0, 19.5), ("22", 8.0, 4.5, 19.5)]


@pytest.mark.parametrize("change", ["restart", "failover"])
@pytest.mark.parametrize("case", LATE_BEHIND_DELAYED_FIRST, ids=["15-d11.5", "15.3-d11.5", "18-d8.5", "22-d5",
                                                                 "22-d4.5"])
def test_i3_i10_late_beat_judged_against_delayed_first_beat(case: tuple[str, float, float, float],
                                                           change: str) -> None:
    """I3 / I10: the stopped leader's last beat (showing an age condition the new leader does not show) is delivered
    late (19.5-26.5 s after sending), and the new leader's first beat, the newest accepted when the late beat arrives,
    was itself delivered late (4.5 s at the 22 s cadence: the engine's 5 s POST timeout; more at 15-18 s). LATE_BEAT_MS
    is measured against the engine clock carried on from that delayed beat, which lags real time by its delivery
    delay, so the late beat counts for the conditions although its episode already ended SOURCE_LIVE_MS after the
    previous showing beat. Observed: the condition resolves, pages again one tick later and resolves a second time
    40 s after (a false critical page and a second resolved notice)."""
    cad_name, g, d_first, d_late = case
    cad, kind = CAD_BY_NAME[cad_name], KINDS[3]
    a, b = (("pc1", "5"), ("pc1", "6")) if change == "restart" else (("aws-a", "5"), ("aws-b", "9"))
    old = engine(*a, 0, 300, cad, kind=kind)
    a_last = old[-1]
    old[-1] = replace(a_last, rx_ms=a_last.send_ms + round(d_late * 1000))
    new = engine(*b, a_last.send_s + g, a_last.send_s + g + 400, cad)
    new[0] = replace(new[0], rx_ms=new[0].send_ms + round(d_first * 1000))
    sim = run(old + new, new[0].send_s + 300)
    spec: list[Spec] = [activation(kind, old[0].rx_s),
                        (kind.key, False, old[-2].rx_s, old[-1].rx_s + cad.cmax + LIVE_S + 1)]
    optional: list[tuple[str, bool]] = []
    if change == "failover":
        spec.append(("node_changed", True, new[0].rx_s, new[0].rx_s + 1))
        optional.append(("node_changed", False))
    check(sim, spec, optional=optional)


# ============================================================================================ breaker round 10 (I5-I8)
# Concrete sequences found by the adversarial round 10 (two-engine incidents, shared sources, stalled-receiver bunching
# and receiver clock steps). Each one violates an invariant with the evaluator as it stands; see the case docstrings.
@pytest.mark.parametrize("shower", ["A", "B"])
@pytest.mark.parametrize("pairing", list(PAIRINGS))
@pytest.mark.parametrize("cad", CADS, ids=CAD_IDS)
@pytest.mark.parametrize("kind", KINDS[:3], ids=KIND_IDS[:3])
def test_r10_i5_i6_stall_during_two_engines_restarts_condition(kind: Kind, cad: Cad, pairing: str,
                                                               shower: str) -> None:
    """I5 + stalled receiver: two engines beat interleaved and one of them shows a duration condition (already
    active). The receiver stalls for ~30-35 s (shorter than the I6 stalls) from just before the OTHER engine's beat
    until SOURCE_LIVE_MS after the showing engine's last handled beat. At the end of the stall the queued beats are
    handled in arrival order at one rx ms: the other engine's beat comes first and counts as "a later beat that did
    not show it", so when the showing engine's queued beats follow, `_record` finds the episode ended (no showing beat
    for > 40 s on the receive clock: a stall artefact) and starts a new one. Observed: a false resolved notice at the
    end of the stall and a second critical page one full threshold later, while the engine showed the condition
    throughout (with the condition not yet active, the page is a full threshold late or missed)."""
    ea, eb, until = two_engines(pairing, cad, kind, shower)
    shown, other = (ea, eb) if shower == "A" else (eb, ea)
    x = next(b for b in shown if b.rx_s >= 300)                  # the showing engine's last beat before the stall
    y = next(b for b in other if b.rx_s > x.rx_s)                # the other engine's next beat: first after it
    nxt = next(b for b in shown if b.rx_s > x.rx_s)
    assert y.rx_s < nxt.rx_s
    stall = (y.rx_s - 0.5, x.rx_s + LIVE_S + 2)
    sim = run(ea + eb, until, stall=stall)
    spec, optional = two_engines_spec(pairing, eb)
    r0 = first_shown(shown).rx_s
    spec.append(activation(kind, r0, cad.cmax))
    if shower == "B":
        b_last = eb[-1].rx_s
        spec.append((kind.key, False, b_last, b_last + TWO_ENGINES_RESOLVE_S + 2))
    check(sim, spec, optional=optional)


@pytest.mark.parametrize("pairing", list(PAIRINGS))
@pytest.mark.parametrize("cad", FIXED_CADS, ids=FIXED_CAD_IDS)
@pytest.mark.parametrize("kind", KINDS[:3], ids=KIND_IDS[:3])
def test_r10_i5_sub_threshold_condition_during_two_engines_pages(kind: Kind, cad: Cad, pairing: str) -> None:
    """I5: during a two-engine incident engine A shows a duration condition for less than its threshold and then
    clears it: its last showing beat is more than `need - SOURCE_LIVE_MS` after its first, and its first beat without
    it comes before `need` (e.g. 30 s of a 60 s condition at 15 s, then clear at 45 s). With `two_engines` active (or
    a shared source) a duration condition counts "up to now" for as long as its episode lasts, i.e. SOURCE_LIVE_MS
    past its last showing beat, so `since + need` is reached after the condition already ended. Observed: a critical
    page for a condition that never held for its threshold, then a resolved notice a few seconds later (an
    activation and a resolve of a single-engine condition during the incident). The same A beats alone page
    nothing (checked first)."""
    a, b = PAIRINGS[pairing]
    t0 = next(x.send_s for x in engine(*a, 0, 700, cad) if x.send_s >= 300)
    end = t0 + kind.need - cad.cmax - 1                       # the first beat without it comes before `need`
    ea = engine(*a, 0, 700, cad, kind=kind, show=lambda s: t0 <= s <= end)
    shown = [x for x in ea if x.kw]
    assert shown[-1].send_s - shown[0].send_s > kind.need - LIVE_S
    check(run(ea, 700), [])                                   # one engine: nothing pages
    eb = engine(*b, 100 + cad.half, 460, cad)
    spec, optional = two_engines_spec(pairing, eb)
    check(run(ea + eb, eb[-1].rx_s + 200), spec, optional=optional)


@pytest.mark.parametrize("gap_s", [1.0, 5.0])
@pytest.mark.parametrize("kind", KINDS[1:3], ids=KIND_IDS[1:3])
def test_r10_i3_shared_source_restart_pages_ended_condition(kind: Kind, gap_s: float) -> None:
    """I3, single engine: the engine restarts within a few seconds and its new term beats under the SAME (node,
    epoch) (a fresh etcd volume restarts from the same low revision, evaluate.py docstring). The old term showed a
    60 s condition for 45 s (255-300 s); the new term does not show it, so it really ended at the restart. The old
    term's last beat and the new term's first are a "fast pair" (< 12 s on both clocks), which marks the source as
    shared for a window, and the duration then counts up to now: the condition pages ~15 s AFTER it ended, then
    resolves 40 s after its last showing beat. Observed: a false critical page and a resolved notice (no page with a
    new epoch)."""
    cad = CADS[0]
    old = engine("pc1", "5", 0, 300, cad, kind=kind, show=lambda s: s >= 250)
    b0 = old[-1].send_s + gap_s
    new = engine("pc1", "5", b0, b0 + 300, cad)
    check(run(old + new, b0 + 250), [])


@pytest.mark.parametrize("step_s", [600, -600, 360, -360])
@pytest.mark.parametrize("kind", [KINDS[1], None], ids=["sm8_not_ready", "none"])
def test_r10_i8_wall_step_before_first_accepted_beat(kind: Kind | None, step_s: int) -> None:
    """I8: the receiver (re)starts while the engine is live; its wall clock steps by `step_s` at 5 s, before the
    engine's next beat (12 s) is the first accepted one (monotonic clock and engine clock unaffected). Before the first
    accepted beat the skew gate judges against the stepped wall clock alone, although the receiver's monotonic clock
    and its start wall time still give the unstepped time. Observed: every beat of the healthy engine is refused as
    skewed for good: a false `payload_invalid` page at once and a false `silence` page at 61 s, neither of which
    clears while the step stays (the condition never pages)."""
    beats = engine("pc1", "5", 12, 600, CADS[0], kind=kind)
    sim = run(beats, 600, steps=[(5, step_s)])
    check(sim, [] if kind is None else [activation(kind, beats[0].rx_s)])
