"""Feed observer (oversee-rules.md §1, §3) and NotificationDispatcher (§10)."""
from __future__ import annotations

from dataclasses import replace

from src.oversee.feed import FeedObserver, InMemoryFeedStream, RecordingQuarantine
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import (
    EmailChannel,
    MobilePushChannel,
    NotificationDispatcher,
    OutboxRow,
    alerts_for_transitions,
    repages_due,
)
from src.oversee.snapshot import BreachAck
from src.oversee.types import BreachState, Cap, Level, Transition


class Clock:
    def __init__(self, t: int = 0) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t


class Signals:
    def __init__(self) -> None:
        self.woken: list[str] = []

    def sweep_now(self, account_id: str) -> None:
        self.woken.append(account_id)


def observer(boot: str | None = "run:1") -> tuple[FeedObserver, InMemoryFeedStream, Signals, RecordingQuarantine,
                                                   Clock]:
    clock = Clock(1000)
    stream = InMemoryFeedStream(boot=boot, clock=clock)
    sig, q = Signals(), RecordingQuarantine()
    return FeedObserver(stream, sig, lambda: "run:1", q, clock), stream, sig, q, clock


async def test_feed_wakes_sweep_then_acks() -> None:
    obs, stream, sig, _, _ = observer()
    a, b = stream.add("A1"), stream.add("A2", "ACCOUNT")
    assert await obs.poll_once() == 2
    assert sig.woken == ["A1", "A2"] and stream.acked == [a, b] and stream.pending == {}


async def test_feed_paused_until_boot_id_matches() -> None:
    obs, stream, sig, _, _ = observer(boot="run:0")
    stream.add("A1")
    assert await obs.poll_once() == 0 and sig.woken == [] and stream.acked == []
    stream.boot = "run:1"
    assert await obs.poll_once() == 1 and sig.woken == ["A1"]


async def test_feed_autoclaim_and_quarantine_after_five_deliveries() -> None:
    obs, stream, sig, q, clock = observer()
    entry = stream.add("A1")
    await stream.read_group(10, 0)            # delivered once to a consumer that died
    for _ in range(3):
        clock.t += 31_000
        await stream.autoclaim(30_000, 10)     # redelivered without ack (deliveries 2..4)
    clock.t += 31_000
    assert await obs.claim_once() == 1         # 5th delivery -> quarantine, then XACK
    assert q.rows and q.rows[0][1] == "max_deliveries" and stream.acked == [entry] and sig.woken == []


async def test_feed_idle_entry_is_reclaimed_and_signalled() -> None:
    obs, stream, sig, _, clock = observer()
    stream.add("A1")
    await stream.read_group(10, 0)
    clock.t += 31_000
    assert await obs.claim_once() == 1 and sig.woken == ["A1"]


async def test_feed_malformed_entry_quarantined() -> None:
    obs, stream, sig, q, _ = observer()
    stream.entries.append(("9-0", {"entity_type": "ORDER"}))
    await obs.poll_once()
    assert q.rows[0][1] == "malformed_entry" and stream.acked == ["9-0"]


def tr(kind: str, level: Level = Level.SOFT_HALT, activation: int = 5) -> Transition:
    return Transition("agg", "account", "A1", "A1", kind, 5, 100, level, Cap.NONE, "r", activation)  # type: ignore[arg-type]


def soft_state(**kw: object) -> BreachState:
    return BreachState("agg", "account", "A1", "A1", level=Level.SOFT_HALT, reason="r", active_since=100,
                       activation_id=5, **kw)  # type: ignore[arg-type]


async def test_dispatcher_episode_dedupe_and_trip_bypass() -> None:
    db = InMemoryOverseeDb()
    email, push = EmailChannel(), MobilePushChannel()
    disp = NotificationDispatcher(db, db, [email, push], escalate_after_ms=900_000)
    rows = alerts_for_transitions([tr("activated")])
    await db.add(rows + rows)                                          # replayed transition
    await db.add([OutboxRow("trip", "critical", "HALT HARD", "x", 100)] * 2)
    assert await disp.drain(200) == 3                                   # 1 episode + 2 trips
    assert len(email.sent) == 3 and len(push.sent) == 3


async def test_repage_every_escalate_after_unless_acknowledged() -> None:
    db = InMemoryOverseeDb()
    email = EmailChannel()
    disp = NotificationDispatcher(db, db, [email], escalate_after_ms=1000)
    await db.add(alerts_for_transitions([tr("activated")]))
    await disp.drain(100)
    s = soft_state()
    assert await disp.queue_repages([s], [], 500) == 0
    assert await disp.queue_repages([s], [], 1100) == 1
    await disp.drain(1100)
    assert [r.kind for r in email.sent] == ["episode", "repage"]
    ack = BreachAck(1, "agg", "account", "A1", Level.SOFT_HALT, Cap.NONE, "r", "op", "known", 150)
    assert repages_due([s], [ack], {k: 0 for k in [("agg", "account", "A1", 5, 2)]}, 5000, 1000) == []
    changed = replace(s, reason="other")      # an ack is void once the reason changes
    assert len(repages_due([changed], [ack], {("agg", "account", "A1", 5, 2): 0}, 5000, 1000)) == 1


async def test_dispatcher_sends_nothing_without_lease() -> None:
    db = InMemoryOverseeDb()
    email = EmailChannel()
    await db.add([OutboxRow("trip", "critical", "HALT", "x", 1)])
    assert await NotificationDispatcher(db, db, [email], 1000, lease_valid=lambda: False).drain(2) == 0
    assert email.sent == [] and len(await db.pending(10)) == 1


async def test_redis_feed_stream_with_fakeredis() -> None:
    import fakeredis

    from src.oversee.feed import FEED_STREAM, RedisFeedStream

    r = fakeredis.FakeAsyncRedis()
    stream = RedisFeedStream(r, consumer="node-a")
    await stream.ensure_group()
    await stream.ensure_group()                     # BUSYGROUP is tolerated
    await r.set("redis:boot_id", "run:1")
    await r.xadd(FEED_STREAM, {"account_id": "A1", "entity_type": "ORDER"})
    sig, q = Signals(), RecordingQuarantine()
    obs = FeedObserver(stream, sig, lambda: "run:1", q, Clock(0))
    assert await obs.poll_once() == 1 and sig.woken == ["A1"]
    assert (await r.xpending(FEED_STREAM, "oversee"))["pending"] == 0
