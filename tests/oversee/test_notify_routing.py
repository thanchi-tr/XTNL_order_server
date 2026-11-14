"""Alert routing, rendering and transport-backed channels in src/oversee/notify.py (§10)."""
from __future__ import annotations

from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import (
    AlertMessage,
    EmailChannel,
    MobilePushChannel,
    NotificationDispatcher,
    OutboxRow,
    RecordingChannel,
    alerts_for_transitions,
    channels_for,
    render_message,
)
from src.oversee.types import Cap, Level, Transition


def tr(level: Level, activation: int) -> Transition:
    return Transition("agg", "account", "A1", "A1", "activated", 5, 100, level, Cap.NONE, "r", activation)


def test_channels_for_severity() -> None:
    assert channels_for("info") == ("email",)
    assert channels_for("high") == ("email", "mobile_push")
    assert channels_for("critical") == ("email", "mobile_push")
    row = OutboxRow("episode", "high", "t", "b", 1)
    assert row.route == ("email", "mobile_push")
    assert OutboxRow("episode", "high", "t", "b", 1, channels=("mobile_push",)).route == ("mobile_push",)


def test_flag_episode_is_email_only_and_soft_hard_page() -> None:
    rows = alerts_for_transitions([tr(Level.FLAG, 1), tr(Level.SOFT_HALT, 2), tr(Level.HARD_HALT, 3)])
    assert [r.route for r in rows] == [("email",), ("email", "mobile_push"), ("email", "mobile_push")]


def test_render_message() -> None:
    m = render_message(OutboxRow("trip", "critical", "HALT HARD", "daily loss", 7, None, "A1", 12))
    assert m == AlertMessage("[XTNL CRITICAL] HALT HARD", m.body, "critical", "trip", 12, "A1")
    assert m.body.startswith("daily loss\n") and "account: A1" in m.body and "alert id: 12" in m.body


async def test_dispatcher_routes_flag_to_email_only() -> None:
    db = InMemoryOverseeDb()
    email, push, other = EmailChannel(), MobilePushChannel(), RecordingChannel("audit")
    await db.add(alerts_for_transitions([tr(Level.FLAG, 1), tr(Level.SOFT_HALT, 2)]))
    assert await NotificationDispatcher(db, db, [email, push, other], 1000).drain(10) == 2
    assert len(email.sent) == 2 and len(push.sent) == 1 and len(other.sent) == 2


async def test_channel_with_transport_delivers_rendered_message() -> None:
    got: list[AlertMessage] = []

    class T:
        backend = "fake"

        async def deliver(self, msg: AlertMessage) -> None:
            got.append(msg)

    ch = MobilePushChannel(transport=T())
    await ch.send(OutboxRow("trip", "high", "HALT SOFT", "b", 1))
    assert got[0].subject == "[XTNL HIGH] HALT SOFT" and ch.delivered == 1 and ch.sent == []
    assert ch.backend == "fake" and EmailChannel().backend == "recording"
    assert "transport" not in repr(ch)
