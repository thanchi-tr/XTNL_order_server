"""S39b gate `receipt_gap_feed`: the S14 receipt-gap detector is fed live and turned on.

* The orchestrator calls its per-adapter frame hook (`on_stream_frame`) on every prices-stream HEARTBEAT while the
  subscription is SUBSCRIBED and not lost; a raising hook never reaches the socket path.
* The tick hook's watch is turned on per adapter (`watch_receipt_gaps`); heartbeats keep a quiet market out of a gap;
  a silence (no tick, no heartbeat) longer than `RECEIPT_GAP_MS` blinds the TICK buckets it overlaps, never
  `complete`; a market seeded when the hook binds is watched from the bind, not from its interval's start.
* The runtime binds it: `EngineSession.set_md_frame_hook` forwards the orchestrator's calls.

Synthetic instruments, prices and times only.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.adapters.base import StreamEvent, StreamEventKind
from src.marketdata.clockstep import RECEIPT_GAP_MS, ReceiptGapDetector
from src.marketdata.types import M1, Quality
from src.runtime.session import EngineSession
from tests.marketdata.test_ticks import AD, H0, MIN, SYM, Clock, closed, dto, one
from src.marketdata.ticks import MdHook
from tests.pubsub.conftest import ManualClock
from tests.pubsub.test_orchestrator import make, subscribe_all

HB = 5_000


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


def watched_hook(*, start: int = H0 - MIN, seeded_since: int | None = None) -> tuple[MdHook, Clock]:
    clock = Clock(start)
    h = MdHook(clock_ms=clock, unfiltered=(AD,))
    h.watch_receipt_gaps((AD,), RECEIPT_GAP_MS)
    h.seed_subscriptions(AD, {SYM: start if seeded_since is None else seeded_since})
    return h, clock


def test_heartbeats_keep_a_quiet_market_out_of_a_gap() -> None:
    h, clock = watched_hook()
    h.on_price(dto("1.10000", H0 + 1_000, received_ms=H0 + 1_000))
    for t in range(H0 + HB, H0 + 3 * MIN, HB):            # no tick for minutes, a heartbeat every 5 s
        clock.now = t
        h.note_frame(AD, None, t)
        h.advance(t)
        assert not h.in_receipt_gap(AD, SYM)
    assert h.receipt_gaps == 0


def test_a_silence_blinds_the_tick_buckets_it_overlaps() -> None:
    """No tick and no heartbeat for longer than the keepalive (the price stream died with the socket still up): the
    minute the silence overlaps is `blind`, never `complete`, even when ticks come back inside it; a resumed heartbeat
    feed ends the gap."""
    h, clock = watched_hook()
    for i in range(4):
        h.on_price(dto(f"1.1000{i}", H0 + i * 10_000 + 1, received_ms=H0 + i * 10_000 + 1))
    clock.now = H0 + 30_001 + RECEIPT_GAP_MS + 1
    h.advance(clock.now)                                    # the silence started after the last tick
    assert h.in_receipt_gap(AD, SYM) and h.receipt_gaps == 1
    h.on_price(dto("1.10009", H0 + 55_000, received_ms=H0 + 55_000))   # ticks again, still in minute 0
    assert not h.in_receipt_gap(AD, SYM)
    clock.now = H0 + 2 * MIN
    h.advance(clock.now)
    assert one(closed(h), M1, H0).bar.quality is Quality.BLIND
    clock.now = H0 + 2 * MIN + RECEIPT_GAP_MS + 1_000
    h.advance(clock.now)                                    # silent again
    assert h.in_receipt_gap(AD, SYM) and h.receipt_gaps == 2
    h.note_frame(AD, None, clock.now + 1)                   # the heartbeat feed resumes: the gap ends there
    assert not h.in_receipt_gap(AD, SYM)


def test_a_seeded_market_is_watched_from_the_bind_not_from_its_interval_start() -> None:
    h, clock = watched_hook(start=H0, seeded_since=H0 - 3_600_000)   # subscribed an hour before the hook bound
    clock.now = H0 + 5_000
    h.advance(clock.now)
    assert not h.in_receipt_gap(AD, SYM) and h.receipt_gaps == 0


def test_an_unwatched_adapter_never_reports_a_gap_and_bad_keepalives_refuse() -> None:
    h = MdHook(clock_ms=Clock(H0), unfiltered=(AD,))
    h.seed_subscriptions(AD, {SYM: H0})
    h.advance(H0 + 10 * MIN)
    assert h.receipt_gaps == 0
    g = ReceiptGapDetector()
    try:
        g.set_keepalive(AD, 0)
    except ValueError:
        pass
    else:
        raise AssertionError("a zero keepalive must be refused")


# ---------------------------------------------------------------------------------------------------- the feed
async def test_the_orchestrator_feeds_prices_heartbeats_to_the_frame_hook(tmp_path: Path, clock: ManualClock) -> None:
    frames: list[tuple[str, str | None, int]] = []
    env = make(tmp_path, clock, on_stream_frame=lambda a, m, t: frames.append((a, m, t)))
    rt = env.orch.runtime("mock")
    hb = StreamEvent(StreamEventKind.HEARTBEAT, "prices", "", clock.ms)
    env.orch._on_control(rt, hb)                             # noqa: SLF001  (not subscribed yet: nothing)
    assert frames == []
    subscribe_all(env)
    rt.subs["prices"].lost = False                           # reconciled: SUBSCRIBED and no gap
    env.orch._on_control(rt, hb)                             # noqa: SLF001
    env.orch._on_control(rt, StreamEvent(StreamEventKind.HEARTBEAT, "orders", "", clock.ms))   # noqa: SLF001
    assert frames == [("mock", None, clock())]               # prices only, adapter-wide, the orchestrator's clock
    rt.subs["prices"].lost = True
    env.orch._on_control(rt, hb)                             # noqa: SLF001  (a lost stream: no proof of life)
    assert len(frames) == 1


async def test_a_raising_frame_hook_never_reaches_the_socket_path(tmp_path: Path, clock: ManualClock) -> None:
    def boom(a: str, m: str | None, t: int) -> None:
        raise RuntimeError("hook")

    env = make(tmp_path, clock, on_stream_frame=boom)
    subscribe_all(env)
    rt = env.orch.runtime("mock")
    rt.subs["prices"].lost = False
    env.orch._on_control(rt, StreamEvent(StreamEventKind.HEARTBEAT, "prices", "", clock.ms))   # noqa: SLF001
    assert rt.frame_hook_logged_ms > 0 and rt.subs["prices"].subscribed and not rt.subs["prices"].lost


def test_the_session_forwards_the_frame_hook() -> None:
    got: list[tuple[str, str | None, int]] = []
    session = EngineSession.__new__(EngineSession)
    session._md_frame_hook = None                            # noqa: SLF001
    session._on_stream_frame("mock", None, 1)                # noqa: SLF001  (unbound: nothing)
    session.set_md_frame_hook(lambda a, m, t: got.append((a, m, t)))
    session._on_stream_frame("mock", None, 2)                # noqa: SLF001
    session.set_md_frame_hook(None)
    session._on_stream_frame("mock", None, 3)                # noqa: SLF001
    assert got == [("mock", None, 2)]
