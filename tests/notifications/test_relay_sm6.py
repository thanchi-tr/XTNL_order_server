"""Item 5.4: SM6 (min(60, 2^n) s back-off, escalation to the other channel after 5 failures, per-channel
independence, delivered flag), relay re-page cadence that follows the ruleset, and channel credentials that
follow the secrets source (rotation + re-read on an auth error). No network: fake channels and transports."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import httpx
import pytest

from src.notifications.errors import ChannelError
from src.notifications.factory import ChannelSet
from src.notifications.relay import Escalation, OutboxRelay, RelayPolicy
from src.notifications.rotation import RotatingChannels, settings_from_secrets
from src.notifications.settings import NotificationSettings
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import (
    ESCALATED_TOKEN,
    OutboxRow,
    decode_channels,
    encode_channels,
    episode_key,
)
from src.oversee.types import Level
from src.secrets.source import EnvSecrets

SM6 = RelayPolicy()          # the production defaults are the SM6 numbers
SECRET = "rotated-secret-value-never-logged"


class Clock:
    def __init__(self, t: int = 1_000) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t


@dataclass
class Scripted:
    """A channel that fails `fail` times (with `exc`), then accepts."""

    name: str
    fail: int = 0
    exc: BaseException = field(default_factory=lambda: ChannelError("down"))
    sent: list[OutboxRow] = field(default_factory=list)
    calls: int = 0
    backend: str = "fake"

    async def send(self, row: OutboxRow) -> None:
        self.calls += 1
        if self.fail > 0:
            self.fail -= 1
            raise self.exc
        self.sent.append(row)


def outcome(db: InMemoryOverseeDb, oid: int) -> bool | None:
    s = db.t.outbox_sent.get(oid)
    return None if s is None else s[1]


async def drive(r: OutboxRelay, clock: Clock, until: int, *, step: int = 1_000) -> None:
    while clock.t < until:
        await r.tick()
        clock.t += step


# ---- back-off ------------------------------------------------------------------------------------
def test_sm6_backoff_is_min_60_2n_seconds() -> None:
    assert [SM6.backoff_ms(n) for n in range(0, 9)] == [0, 2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000,
                                                         60_000]
    assert SM6.escalate_after_failures == 5


async def test_sm6_retry_times_follow_the_backoff() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email = Scripted("email", fail=100)
    r = OutboxRelay(db, db, [email], policy=SM6, clock_ms=clock)
    await db.add([OutboxRow("info", "info", "t", "b", 100)])
    attempts: list[int] = []
    while clock.t < 200_000:
        before = email.calls
        await r.tick()
        if email.calls > before:
            attempts.append(clock.t)
        clock.t += 500
    gaps = [b - a for a, b in zip(attempts, attempts[1:])]
    assert gaps[:7] == [2_000, 4_000, 8_000, 16_000, 32_000, 60_000, 60_000]


# ---- escalation ----------------------------------------------------------------------------------
async def test_sm6_email_only_alert_escalates_to_push_after_5_failures() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Scripted("email", fail=100), Scripted("mobile_push")
    seen: list[Escalation] = []
    r = OutboxRelay(db, db, [email, push], policy=SM6, clock_ms=clock, on_escalation=seen.append)
    await db.add([OutboxRow("info", "info", "FLAG max_position_risk", "risk 1.2 %", 100, account_id="A1")])
    await drive(r, clock, 1_000 + 2_000 + 4_000 + 8_000 + 16_000)        # attempts 1-4 at t+0, 2, 6, 14
    assert email.calls == 4 and push.sent == [] and seen == []
    await drive(r, clock, 31_001)                                        # 5th attempt at t+30 s
    assert email.calls == 5 and len(push.sent) == 1
    assert "escalated: not delivered by email" in push.sent[0].body and push.sent[0].title == "FLAG max_position_risk"
    assert len(seen) == 1 and seen[0].failed == ("email",) and seen[0].to == ("mobile_push",)
    assert seen[0].reason == "5 failed attempts" and seen[0].account_id == "A1"
    assert r.recent_escalations() == tuple(seen) and r.status()["email"].escalations == 1
    assert outcome(db, 1) is None                       # email still owed: not delivered, keeps retrying
    await drive(r, clock, 400_000)
    assert email.calls > 8 and len(push.sent) == 1 and len(seen) == 1    # escalated once only
    email.fail = 0
    await drive(r, clock, 470_000)
    assert len(email.sent) == 1 and outcome(db, 1) is True   # every channel accepted: delivered flag set
    assert len(push.sent) == 1


async def test_sm6_escalation_tick_result_and_dashboard_log(caplog: pytest.LogCaptureFixture) -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email = Scripted("email", fail=1, exc=ChannelError("550 mailbox unavailable", retryable=False))
    push = Scripted("mobile_push")
    r = OutboxRelay(db, db, [email, push], policy=SM6, clock_ms=clock)
    await db.add([OutboxRow("info", "info", "t", f"body {SECRET}", 1)])
    with caplog.at_level(logging.ERROR):
        res = await r.tick()
    # a provider rejection is not retried into the ground: it escalates at once
    assert res.escalated == 1 and res.partial == 1 and len(push.sent) == 1
    assert "escalated" in caplog.text and "rejected by the provider" in caplog.text and SECRET not in caplog.text
    clock.t += 59_999
    assert (await r.tick()).due == 0                      # a permanent failure waits the 60 s cap
    clock.t += 1
    res = await r.tick()
    assert res.delivered == 1 and outcome(db, 1) is True and len(push.sent) == 1


async def test_sm6_soft_alert_both_routed_escalation_resends_nothing() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Scripted("email", fail=100), Scripted("mobile_push")
    seen: list[Escalation] = []
    r = OutboxRelay(db, db, [email, push], policy=SM6, clock_ms=clock, on_escalation=seen.append)
    await db.add([OutboxRow("trip", "critical", "HALT HARD", "x", 1)])
    await drive(r, clock, 40_000)
    assert email.calls == 5 and len(push.sent) == 1                       # push got it once, at the start
    assert len(seen) == 1 and seen[0].to == () and seen[0].failed == ("email",)
    assert outcome(db, 1) is None


async def test_sm6_unconfigured_route_goes_to_the_other_channel() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    push = Scripted("mobile_push")
    r = OutboxRelay(db, db, [push], disabled=("email",), policy=SM6, clock_ms=clock)
    await db.add([OutboxRow("info", "info", "HALT cleared", "cool_down", 1)])
    res = await r.tick()
    assert res.escalated == 1 and res.delivered == 1 and len(push.sent) == 1
    assert "not delivered by email" in push.sent[0].body and outcome(db, 1) is True


async def test_sm6_per_channel_independence_after_escalation() -> None:
    """The row is due when its earliest channel is: a push that failed once retries after 2 s while the
    email (5 failures) waits its own 32 s; the email is not tried (or counted) early."""
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Scripted("email", fail=100), Scripted("mobile_push", fail=1)
    r = OutboxRelay(db, db, [email, push], policy=SM6, clock_ms=clock)
    await db.add([OutboxRow("info", "info", "t", "b", 1)])
    await drive(r, clock, 31_001)
    t_esc = 31_000
    assert email.calls == 5 and push.calls == 1 and push.sent == []      # escalated push failed once
    clock.t = t_esc + 2_000
    await r.tick()
    assert push.calls == 2 and len(push.sent) == 1 and email.calls == 5
    clock.t = t_esc + 31_000
    await r.tick()
    assert email.calls == 5                                               # 32 s not over
    clock.t = t_esc + 32_000
    await r.tick()
    assert email.calls == 6 and len(push.sent) == 1


async def test_sm6_gives_up_after_max_attempts_undelivered_even_if_escalation_delivered() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Scripted("email", fail=1_000), Scripted("mobile_push")
    r = OutboxRelay(db, db, [email, push], policy=RelayPolicy(max_attempts=7), clock_ms=clock)
    await db.add([OutboxRow("info", "info", "t", "b", 1)])
    await drive(r, clock, 400_000)
    assert email.calls == 7 and len(push.sent) == 1 and outcome(db, 1) is False


# ---- channel codec and durable retry state -------------------------------------------------------
def test_channels_codec_round_trip() -> None:
    assert decode_channels(["email"]) == (("email",), (), False)          # the INSERT default
    enc = encode_channels(["email", "mobile_push"], {"email": 5, "mobile_push": 0}, True)
    assert enc == ["email:5", "mobile_push", ESCALATED_TOKEN]
    assert decode_channels(enc) == (("email", "mobile_push"), (("email", 5),), True)
    row = OutboxRow("info", "info", "t", "b", 1, channels=("email",), channel_failures=(("email", 3),))
    assert row.failures_of("email") == 3 and row.failures_of("mobile_push") == 0


# ---- re-page cadence follows the ruleset ---------------------------------------------------------
async def test_relay_repage_cadence_follows_the_installed_ruleset() -> None:
    db, clock = InMemoryOverseeDb(), Clock(1_000_000)
    push, email = Scripted("mobile_push"), Scripted("email")
    key = episode_key("agg", "account", "A1", 1, Level.SOFT_HALT)
    cadence = [900_000]
    r = OutboxRelay(db, db, [email, push], policy=SM6, clock_ms=clock)
    r.follow_cadence(lambda: cadence[0])
    await db.record(key, clock.t - 300_000)
    await db.add([OutboxRow("repage", "high", "still SOFT", "r", clock.t, key)])
    assert (await r.tick()).suppressed == 1                              # 300 s < 900 s
    cadence[0] = 300_000                                                 # ruleset reload shortens it
    await db.add([OutboxRow("repage", "high", "still SOFT", "r", clock.t, key)])
    assert (await r.tick()).delivered == 1 and r.escalate_after_ms == 300_000
    r2 = OutboxRelay(db, db, [email], escalate_after_ms=lambda: 42)
    assert r2.escalate_after_ms == 42


# ---- credentials follow the secrets source -------------------------------------------------------
class Source:
    def __init__(self, sets: list[ChannelSet]) -> None:
        self.sets = sets
        self.polls = 0
        self.invalidations = 0

    def poll(self) -> ChannelSet | None:
        self.polls += 1
        return self.sets.pop(0) if self.sets else None

    def invalidate(self) -> None:
        self.invalidations += 1


async def test_rebuilt_channel_drops_the_old_credentials_backoff() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    old = Scripted("email", fail=100, exc=ChannelError("smtp auth rejected (535)", auth=True))
    src = Source([])
    r = OutboxRelay(db, db, [old], policy=SM6, clock_ms=clock, channel_source=src)
    for _ in range(3):                                # three failures: channel down for 8 s
        await db.add([OutboxRow("info", "info", "t", "b", 1)])
        await r.tick()
        clock.t = r.status()["email"].retry_at_ms
    assert r.status()["email"].consecutive_failures == 3
    src.sets.append(ChannelSet((Scripted("email"),), (), ()))
    await r.tick()
    st = r.status()["email"]
    assert st.consecutive_failures == 0 and st.failed == 3 and st.state == "up"


async def test_relay_swaps_channels_on_rotated_credentials_and_rereads_on_auth_error() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    old = Scripted("email", fail=100, exc=ChannelError("smtp auth rejected (535)", auth=True))
    new = Scripted("email")
    src = Source([])
    auth: list[str] = []
    r = OutboxRelay(db, db, [old], policy=SM6, clock_ms=clock, channel_source=src, on_auth_error=auth.append)
    await db.add([OutboxRow("info", "info", "t", "b", 1)])
    await r.tick()
    assert auth == ["email"] and src.invalidations == 1 and r.status()["email"].state == "down"
    polls = src.polls
    clock.t += 1_000
    await r.tick()
    assert src.polls == polls + 1                     # fast polling after an auth error (not every 5 s)
    src.sets.append(ChannelSet((new,), ("mobile_push",), ()))
    clock.t += 1_000                                  # the row's own 2 s back-off is over
    res = await r.tick()                              # rebuilt channel used at once
    st = r.status()
    assert st["email"].retry_at_ms == 0 and st["email"].failed == 1 and st["mobile_push"].state == "disabled"
    assert st["email"].state == "up"
    assert res.delivered == 1 and len(new.sent) == 1 and old.calls == 1 and outcome(db, 1) is True


async def test_relay_polls_the_source_every_credentials_poll_ms_and_survives_errors() -> None:
    db, clock = InMemoryOverseeDb(), Clock()

    class Broken(Source):
        def poll(self) -> ChannelSet | None:
            self.polls += 1
            raise RuntimeError(SECRET)

    src = Broken([])
    r = OutboxRelay(db, db, [Scripted("email")], policy=SM6, clock_ms=clock, channel_source=src)
    for _ in range(10):
        await r.tick()
        clock.t += 1_000
    assert src.polls == 2 and "email" in r.status()   # every 5 s; a failing poll keeps the channels


def test_rotating_channels_rebuild_only_when_settings_change(caplog: pytest.LogCaptureFixture) -> None:
    env = {"XTNL_PUSH_PROVIDER": "ntfy", "XTNL_PUSH_URL": "https://ntfy.example/topic",
           "XTNL_PUSH_TOKEN": "tok-1"}
    invalidated: list[int] = []
    client = httpx.AsyncClient()
    try:
        rc = RotatingChannels(settings_from_secrets(EnvSecrets(), env), client,
                              invalidate=lambda: invalidated.append(1))
        assert [c.name for c in rc.current.channels] == ["mobile_push"] and rc.current.disabled == ("email",)
        assert rc.poll() is None
        env["XTNL_PUSH_TOKEN"] = SECRET                                  # rotated
        with caplog.at_level(logging.DEBUG):
            cs = rc.poll()
        assert cs is not None and cs is rc.current and rc.rebuilds == 1 and rc.poll() is None
        assert SECRET not in caplog.text
        rc.invalidate()
        assert invalidated == [1]
    finally:
        import asyncio
        asyncio.run(client.aclose())


def test_rotating_channels_keep_the_last_good_set_when_the_secret_is_unreadable() -> None:
    calls = [0]

    def load() -> NotificationSettings:
        calls[0] += 1
        if calls[0] > 1:
            raise RuntimeError("the secret has not been read yet")
        return NotificationSettings()

    client = httpx.AsyncClient()
    try:
        rc = RotatingChannels(load, client)
        before = rc.current
        assert rc.poll() is None and rc.current is before
    finally:
        import asyncio
        asyncio.run(client.aclose())


# ---- skeptic review (5.4): backlog behind a dead channel, lease, durable retry state -------------
async def test_sm6_backlog_behind_a_dead_email_escalates_every_row_not_only_the_probe() -> None:
    """Only the oldest due row probes a backing-off channel, so the rows queued behind it never collect
    their own 5 failures. Once the channel has failed 5 times in a row they escalate too (all within the
    probe's 5 attempts), and still get the email (delivered flag) once it recovers."""
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Scripted("email", fail=10**9), Scripted("mobile_push")
    seen: list[Escalation] = []
    r = OutboxRelay(db, db, [email, push], policy=SM6, clock_ms=clock, on_escalation=seen.append)
    await db.add([OutboxRow("info", "info", f"FLAG {i}", "b", 100) for i in range(10)])
    await drive(r, clock, 32_000)
    assert email.calls == 5                                   # the down channel is not hammered
    assert sorted(e.outbox_id for e in seen) == list(range(1, 11)) and len(push.sent) == 10
    assert all(e.to == ("mobile_push",) for e in seen)
    assert {e.reason for e in seen[1:]} == {"channel down after 5+ consecutive failures"}
    await drive(r, clock, 600_000)
    assert len(push.sent) == 10 and len(seen) == 10           # once per row
    email.fail = 0
    await drive(r, clock, 700_000)
    assert all(outcome(db, i) is True for i in range(1, 11)) and len(email.sent) == 10


async def test_sm6_no_escalation_once_the_lease_is_lost_in_the_pass() -> None:
    db, clock = InMemoryOverseeDb(), Clock()
    email, push = Scripted("email", fail=10**9), Scripted("mobile_push")
    seen: list[Escalation] = []
    lease = {"left": 10**9}

    def lease_valid() -> bool:
        lease["left"] -= 1
        return lease["left"] >= 0

    r = OutboxRelay(db, db, [email, push], policy=SM6, clock_ms=clock, on_escalation=seen.append,
                    lease_valid=lease_valid)
    await db.add([OutboxRow("info", "info", "t", "b", 1)])
    await drive(r, clock, 31_001)
    assert len(seen) == 1 and r.status()["email"].consecutive_failures == 5
    await db.add([OutboxRow("info", "high", "SOFT", "b", 2)])     # email known dead, push next
    lease["left"] = 2                                             # tick start + row loop, then lost at push
    res = await r.tick()
    assert res.lease_lost and res.escalated == 0 and len(seen) == 1


class TokenStore:
    """A RelayOutboxStore that keeps the retry state only as the encoded `channels` tokens, like
    notification_outbox, so a second relay (a new leader) has to decode it."""

    def __init__(self) -> None:
        self.rows: dict[int, OutboxRow] = {}
        self.tokens: dict[int, list[str]] = {}
        self.next_ms: dict[int, int] = {}
        self.attempts: dict[int, int] = {}
        self.sent: dict[int, bool] = {}

    async def add(self, rows: list[OutboxRow]) -> None:
        for row in rows:
            oid = len(self.rows) + 1
            self.rows[oid] = OutboxRow(row.kind, row.severity, row.title, row.body, row.created_ms, row.episode,
                                       row.account_id, oid)
            from src.oversee.notify import channels_for
            self.tokens[oid], self.next_ms[oid], self.attempts[oid] = list(channels_for(row.severity)), 0, 0

    async def pending(self, limit: int) -> list[OutboxRow]:
        return [r for i, r in self.rows.items() if i not in self.sent][:limit]

    async def mark_sent(self, outbox_id: int, at_ms: int, delivered: bool) -> None:
        self.sent[outbox_id] = delivered

    async def due(self, now_ms: int, limit: int) -> list[OutboxRow]:
        from dataclasses import replace
        out = []
        for i, r in self.rows.items():
            if i in self.sent or self.next_ms[i] > now_ms:
                continue
            names, fails, esc = decode_channels(self.tokens[i])
            out.append(replace(r, channels=names, channel_failures=fails, escalated=esc, attempts=self.attempts[i]))
        return out[:limit]

    async def reschedule(self, outbox_id: int, *, remaining: list[str], next_attempt_ms: int, error: str | None,
                         attempted: bool, failures: dict[str, int] | None = None, escalated: bool = False) -> None:
        self.tokens[outbox_id] = encode_channels(remaining, failures or {}, escalated)
        self.next_ms[outbox_id] = next_attempt_ms
        self.attempts[outbox_id] += 1 if attempted else 0

    async def finish(self, outbox_id: int, at_ms: int, *, delivered: bool, error: str | None) -> None:
        self.sent[outbox_id] = delivered

    async def oldest_undelivered_ms(self) -> int | None:
        rows = await self.pending(1)
        return rows[0].created_ms if rows else None


async def test_sm6_failure_count_survives_a_leader_change() -> None:
    """3 failures under one relay; the next leader's relay continues at n = 4 (16 s back-off) and escalates
    on its own 2nd attempt, the 5th overall."""
    store, clock = TokenStore(), Clock()
    email, push = Scripted("email", fail=10**9), Scripted("mobile_push")
    old = OutboxRelay(store, db_dedupe := InMemoryOverseeDb(), [email, push], policy=SM6, clock_ms=clock)
    await store.add([OutboxRow("info", "info", "t", "b", 1)])
    await drive(old, clock, 7_001)                                       # attempts at +0, +2, +6 s
    assert email.calls == 3 and store.tokens[1] == ["email:3"]
    seen: list[Escalation] = []
    new = OutboxRelay(store, db_dedupe, [email, push], policy=SM6, clock_ms=clock, on_escalation=seen.append)
    await drive(new, clock, 15_001)                                      # 4th failure at +14 s (8 s back-off)
    assert email.calls == 4 and seen == [] and store.next_ms[1] == 1_000 + 14_000 + 16_000
    await drive(new, clock, 31_001)
    assert email.calls == 5 and len(seen) == 1 and len(push.sent) == 1
    assert store.tokens[1] == ["email:5", ESCALATED_TOKEN]
