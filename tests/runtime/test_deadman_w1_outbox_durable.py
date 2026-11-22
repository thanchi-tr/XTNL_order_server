"""W1 review round 2: the receiver-down page counts as paged only once its outbox row committed.

`queue_page` hands the outbox insert to a background task and only logs its failure. The W1 episode uses
`queue_page_durable` instead, which awaits the insert: when it fails, the episode is neither marked paged nor
recorded in notification_dedupe, and the next failed beat pages again.
"""
from __future__ import annotations

import dataclasses
from typing import Any

from src.notifications.settings import DeadmanSettings
from src.runtime.deadman import ACK_LOST_TITLE, ack_episode_keys
from src.runtime.supervisor import TaskSupervisor
from tests.runtime.test_deadman_receiver_alert_w1 import BEAT_MS, HOST, T0, URL, _Receiver, _settle
from tests.runtime.test_session_56 import Built, built  # noqa: F401  (the session fixture)


async def test_a_failed_outbox_insert_is_paged_again_on_the_next_failed_beat(built: Built) -> None:
    s = built.session
    s.ctx = dataclasses.replace(s.ctx, deadman=DeadmanSettings(URL, "tok-0123456789abcdef"), deadman_alert_after_s=60)
    s.init_deps(TaskSupervisor([]))
    s._add = lambda spec: None                # type: ignore[method-assign]  (no background beat: the test drives it)
    s._start_notifications()
    sender = s.deadman
    assert sender is not None
    outbox: Any = s.ports.outbox
    real_add = outbox.add
    failing = [1]                             # the first insert fails (a statement timeout, a dropped connection)

    async def add(rows: Any) -> Any:
        if failing[0] > 0:
            failing[0] -= 1
            raise TimeoutError("statement timeout")
        return await real_add(rows)

    outbox.add = add
    receiver = _Receiver()
    receiver.up = False
    sender._transport = receiver
    clock = [T0]
    sender._clock_ms = lambda: clock[0]
    sender._mono = lambda: clock[0] / 1000
    lost, _ = ack_episode_keys(HOST)
    for _ in range(5):                        # the 5th failed beat is 60 s into the streak: the page is due
        await sender.send_once()
        clock[0] += BEAT_MS
    await _settle()
    assert failing[0] == 0                    # the page was tried, and its insert failed
    assert not sender.ack_paged and sender.ack_episodes == 0
    assert lost not in s.ports.dedupe.t.dedupe           # type: ignore[attr-defined]  (no durable 'lost' row)
    assert not [r for r in s.ports.outbox.t.outbox if r.title == ACK_LOST_TITLE]   # type: ignore[attr-defined]
    await sender.send_once()                  # the next failed beat pages again, and this insert commits
    await _settle()
    rows = [r for r in s.ports.outbox.t.outbox if r.title == ACK_LOST_TITLE]       # type: ignore[attr-defined]
    assert [(r.severity, r.title) for r in rows] == [("high", ACK_LOST_TITLE)]
    assert sender.ack_paged and sender.ack_episodes == 1
    assert lost in s.ports.dedupe.t.dedupe                # type: ignore[attr-defined]


async def test_queue_page_durable_answers_false_without_a_lease(built: Built) -> None:
    s = built.session
    s.ctx.lease.valid = False                 # type: ignore[attr-defined]  (FakeLease: not the leader any more)
    assert await s.queue_page_durable("high", ACK_LOST_TITLE, "body") is False
    await _settle()
    assert not s.ports.outbox.t.outbox        # type: ignore[attr-defined]
