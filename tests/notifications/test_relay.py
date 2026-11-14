"""OutboxRelay: routing, episode dedupe, re-pages, per-channel status, retries with back-off, delivered flag."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

import pytest

from src.notifications.errors import ChannelError
from src.notifications.relay import OutboxRelay, RelayPolicy, VolatileRetryOutbox
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import (
    EmailChannel,
    NotificationDispatcher,
    OutboxRow,
    alerts_for_transitions,
    episode_key,
)
from src.oversee.snapshot import DependencyUnavailable
from src.oversee.types import BreachState, Cap, Level, Transition

SECRET = "s3cr3t-value-never-shown"
POLICY = RelayPolicy(backoff_base_ms=1_000, backoff_max_ms=8_000, max_attempts=4, escalate_after_ms=10_000)


class Clock:
    def __init__(self, t: int = 1_000) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t


@dataclass
class Flaky:
    name: str
    fail: int = 0                               # fail this many sends, then succeed
    exc: BaseException = field(default_factory=lambda: ChannelError("down"))
    sent: list[OutboxRow] = field(default_factory=list)
    calls: int = 0

    async def send(self, row: OutboxRow) -> None:
        self.calls += 1
        if self.fail > 0:
            self.fail -= 1
            raise self.exc
        self.sent.append(row)


def tr(level: Level = Level.SOFT_HALT, activation: int = 5) -> Transition:
    return Transition("agg", "account", "A1", "A1", "activated", 5, 100, level, Cap.NONE, "r", activation)


def relay(db: InMemoryOverseeDb, *channels: Flaky, clock: Clock, disabled: tuple[str, ...] = (),
          lease: list[bool] | None = None, **kw: object) -> OutboxRelay:
    flag = lease if lease is not None else [True]
    return OutboxRelay(db, db, list(channels), disabled=disabled, policy=POLICY, clock_ms=clock,
                       lease_valid=lambda: flag[0], **kw)  # type: ignore[arg-type]


def outcome(db: InMemoryOverseeDb, oid: int) -> bool | None:
    s = db.t.outbox_sent.get(oid)
    return None if s is None else s[1]


async def test_routing_flag_email_soft_hard_email_and_push() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Flaky("email"), Flaky("mobile_push")
    await db.add(alerts_for_transitions([tr(Level.FLAG, 1), tr(Level.SOFT_HALT, 2), tr(Level.HARD_HALT, 3)]))
    await db.add([OutboxRow("info", "info", "HALT cleared", "cool_down", 50)])
    res = await relay(db, email, push, clock=clock).tick()
    assert res.delivered == 4 and res.due == 4
    assert [r.title.split()[0] for r in email.sent] == ["FLAG", "SOFT_HALT", "HARD_HALT", "HALT"]
    assert [r.title.split()[0] for r in push.sent] == ["SOFT_HALT", "HARD_HALT"]
    assert all(outcome(db, i) is True for i in (1, 2, 3, 4))


async def test_episode_dedupe_and_level_change_always_alerted() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Flaky("email"), Flaky("mobile_push")
    rows = alerts_for_transitions([tr()])
    await db.add(rows + rows)                                          # replayed transition
    await db.add([OutboxRow("trip", "critical", "HALT HARD", "x", 100)] * 2)
    res = await relay(db, email, push, clock=clock).tick()
    assert res.delivered == 3 and res.suppressed == 1
    assert outcome(db, 2) is False                                     # the duplicate episode row
    assert len(email.sent) == 3 and len(push.sent) == 3
    assert await db.last_sent(episode_key("agg", "account", "A1", 5, Level.SOFT_HALT)) == 1_000


async def test_repage_of_unacknowledged_halt_every_escalate_after() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Flaky("email"), Flaky("mobile_push")
    r = relay(db, email, push, clock=clock)
    disp = NotificationDispatcher(db, db, [], escalate_after_ms=POLICY.escalate_after_ms)
    await db.add(alerts_for_transitions([tr()]))
    await r.tick()
    state = BreachState("agg", "account", "A1", "A1", level=Level.SOFT_HALT, reason="r", active_since=100,
                        activation_id=5)
    assert await disp.queue_repages([state], [], 5_000) == 0
    clock.t = 11_000
    assert await disp.queue_repages([state], [], clock.t) == 1
    await r.tick()
    assert [x.kind for x in push.sent] == ["episode", "repage"]
    assert await disp.queue_repages([state], [], 12_000) == 0          # next re-page is 10 s later


async def test_one_channel_down_other_delivers_then_retry_only_missing_channel() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Flaky("email"), Flaky("mobile_push", fail=1)
    r = relay(db, email, push, clock=clock)
    await db.add(alerts_for_transitions([tr()]))
    res = await r.tick()
    assert res.partial == 1 and len(email.sent) == 1 and push.sent == []
    st = r.status()
    assert st["email"].state == "up" and st["mobile_push"].state == "down"
    assert st["mobile_push"].last_error == "down" and st["mobile_push"].retry_at_ms == 3_000   # 2^1 s
    assert outcome(db, 1) is None and r.oldest_undelivered_ms() == 100
    clock.t = 2_999
    assert (await r.tick()).due == 0                                   # back-off not over
    clock.t = 3_000
    res = await r.tick()
    assert res.delivered == 1 and len(email.sent) == 1 and len(push.sent) == 1   # email not re-sent
    assert outcome(db, 1) is True and r.status()["mobile_push"].state == "up"
    assert r.oldest_undelivered_ms() is None


async def test_down_channel_not_hammered_within_a_tick() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Flaky("email"), Flaky("mobile_push", fail=100)
    await db.add([OutboxRow("trip", "critical", f"HALT {i}", "x", 100) for i in range(5)])
    res = await relay(db, email, push, clock=clock).tick()
    assert res.partial == 5 and len(email.sent) == 5 and push.calls == 1


async def test_retry_backoff_then_give_up_undelivered() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email = Flaky("email", fail=100)
    r = relay(db, email, clock=clock)
    await db.add([OutboxRow("info", "info", "t", "b", 100)])
    times = []
    for _ in range(10):
        res = await r.tick()
        if res.gave_up:
            break
        times.append(clock.t)
        clock.t = max(clock.t + 1, r.status()["email"].retry_at_ms)
    assert email.calls == POLICY.max_attempts and outcome(db, 1) is False
    assert [b - a for a, b in zip(times, times[1:])] == [2_000, 4_000]   # 2^n s; the 4th attempt gives up


async def test_retry_after_failed_first_attempt_is_not_deduplicated() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email = Flaky("email", fail=1)
    r = relay(db, email, clock=clock)
    await db.add(alerts_for_transitions([tr(Level.FLAG)]))
    await r.tick()                                     # failed, but this row now owns the episode
    await db.add(alerts_for_transitions([tr(Level.FLAG)]))   # a duplicate queued meanwhile
    clock.t = 3_000
    res = await r.tick()
    assert res.delivered == 1 and res.suppressed == 1 and len(email.sent) == 1
    assert outcome(db, 1) is True and outcome(db, 2) is False


async def test_disabled_channel_reported_and_skipped() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email = Flaky("email")
    r = relay(db, email, clock=clock, disabled=("mobile_push",), issues=("mobile push disabled: x",))
    assert r.status()["mobile_push"].state == "disabled"
    assert any("mobile_push is disabled" in w for w in r.warnings)
    await db.add(alerts_for_transitions([tr(Level.HARD_HALT)]))
    assert (await r.tick()).delivered == 1 and len(email.sent) == 1


async def test_no_channel_configured_never_crashes(caplog: pytest.LogCaptureFixture) -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    r = relay(db, clock=clock, disabled=("email", "mobile_push"))
    with caplog.at_level(logging.WARNING):
        w = r.report_startup()
    assert w[0].startswith("no alert channel") and "no alert channel" in caplog.text
    await db.add([OutboxRow("trip", "critical", "HALT HARD", "x", 1)])
    res = await r.tick()
    assert res.gave_up == 1 and outcome(db, 1) is False


async def test_nothing_sent_without_lease_and_lease_checked_per_send() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Flaky("email"), Flaky("mobile_push")
    lease = [False]
    r = relay(db, email, push, clock=clock, lease=lease)
    await db.add([OutboxRow("trip", "critical", "HALT", "x", 1)])
    assert (await r.tick()).lease_lost and email.calls == 0 and outcome(db, 1) is None

    class LoseLease(Flaky):
        async def send(self, row: OutboxRow) -> None:
            await super().send(row)
            lease[0] = False

    lease[0] = True
    first = LoseLease("email")
    r2 = relay(db, first, push, clock=clock, lease=lease)
    res = await r2.tick()
    assert res.lease_lost and len(first.sent) == 1 and push.calls == 0   # push not sent after the lease went


async def test_auth_error_hook_and_logs_hold_no_secret_or_body(caplog: pytest.LogCaptureFixture) -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email = Flaky("email", fail=1, exc=ChannelError("smtp auth rejected (535)", auth=True))
    push = Flaky("mobile_push", fail=1, exc=RuntimeError(f"url https://x/{SECRET}"))
    auth: list[str] = []
    r = relay(db, email, push, clock=clock, on_auth_error=auth.append)
    await db.add([OutboxRow("trip", "critical", "HALT HARD", f"body with {SECRET}", 1)])
    with caplog.at_level(logging.DEBUG):
        await r.tick()
    assert auth == ["email"]
    assert r.status()["mobile_push"].last_error == "RuntimeError"
    assert SECRET not in caplog.text and "down" in caplog.text


async def test_run_loop_stamps_survives_db_outage_and_stops() -> None:
    db, clock = InMemoryOverseeDb(), Clock()

    class Outage(VolatileRetryOutbox):
        fails = 2

        async def due(self, now_ms: int, limit: int) -> list[OutboxRow]:
            if self.fails:
                self.fails -= 1
                raise DependencyUnavailable("postgres unavailable")
            return await super().due(now_ms, limit)

    email = Flaky("email")
    r = OutboxRelay(Outage(db), db, [email], policy=RelayPolicy(interval_s=0.001), clock_ms=clock)
    await db.add([OutboxRow("info", "info", "t", "b", 1)])
    stop = asyncio.Event()
    stamps: list[int] = []

    def stamp() -> None:
        stamps.append(1)
        if email.sent:
            stop.set()

    await asyncio.wait_for(r.run(stop, stamp), 2)
    assert len(email.sent) == 1 and len(stamps) >= 3


async def test_real_email_channel_with_transport_counts_without_growing() -> None:
    sent: list[str] = []

    class T:
        backend = "fake"

        async def deliver(self, msg: object) -> None:
            sent.append(getattr(msg, "subject"))

    db, clock = InMemoryOverseeDb(), Clock()
    ch = EmailChannel(transport=T())
    r = OutboxRelay(db, db, [ch], policy=POLICY, clock_ms=clock)
    await db.add([OutboxRow("info", "info", "HALT cleared", "b", 1)])
    await r.tick()
    assert sent == ["[XTNL INFO] HALT cleared"] and ch.delivered == 1 and ch.sent == []
    assert r.status()["email"].backend == "fake"


def test_duplicate_channel_names_rejected() -> None:
    db = InMemoryOverseeDb()
    with pytest.raises(ValueError):
        OutboxRelay(db, db, [Flaky("email"), Flaky("email")])
