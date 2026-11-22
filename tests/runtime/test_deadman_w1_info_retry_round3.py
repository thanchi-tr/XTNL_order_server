"""Home-server plan W1, review round 3: a retried or inherited info page keeps the episode's real figures.

The acknowledged beat resets the failure counters whether or not the info page could be queued, so the body is fixed
at the first attempt; a page path answering False is logged with both of its causes (no lease, or the outbox insert
failed).
"""
from __future__ import annotations

import logging

import pytest

from src.runtime.deadman import ACK_BACK_TITLE
from tests.runtime.test_deadman_receiver_alert_w1 import HOST, rig
from tests.runtime.test_deadman_w1_episode_durable import World


async def test_a_retried_info_page_keeps_the_body_of_its_first_attempt(caplog: pytest.LogCaptureFixture) -> None:
    r = rig(alert_after_s=60)
    pages = r.pages
    refuse = [False]

    def page(sev: str, title: str, body: str) -> bool:
        if refuse[0]:
            refuse[0] = False                 # only the first info attempt fails
            return False
        pages.append((sev, title, body))
        return True
    r.sender._page = page
    r.receiver.up = False
    await r.beat(6)                           # paged at the 5th failure (60 s), one more failure after it
    assert [p[0] for p in pages] == ["high"]
    r.receiver.up = True
    refuse[0] = True
    with caplog.at_level(logging.WARNING, logger="src.runtime.deadman"):
        await r.beat()                        # the info page cannot be queued: the episode stays open
    assert [p[0] for p in pages] == ["high"] and r.sender.ack_paged
    assert any("no valid lease or outbox insert failed" in m for m in caplog.messages)
    await r.beat()                            # retried on the next acknowledged beat
    assert [(p[0], p[1]) for p in pages] == [("high", pages[0][1]), ("info", ACK_BACK_TITLE)]
    assert pages[1][2] == f"receiver {HOST}: acknowledged again after 6 failed beats over 90 s"
    assert not r.sender.ack_paged
    await r.beat(3)
    assert len(pages) == 2


async def test_a_new_episode_after_a_closed_one_builds_a_fresh_body() -> None:
    r = rig(alert_after_s=60)
    r.receiver.up = False
    await r.beat(5)
    r.receiver.up = True
    await r.beat()
    r.receiver.up = False
    await r.beat(7)
    r.receiver.up = True
    await r.beat()
    bodies = [p[2] for p in r.pages if p[0] == "info"]
    assert bodies == [f"receiver {HOST}: acknowledged again after 5 failed beats over 75 s",
                      f"receiver {HOST}: acknowledged again after 7 failed beats over 105 s"]


async def test_an_inherited_episode_with_failures_in_the_new_term_gives_both_parts() -> None:
    w = World()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 21)                      # paged at the 21st failure
    t2 = w.term()
    await w.beat(t2, 3)                       # 3 failures in the new term; the good beat comes 45 s after the first
    w.receiver.up = True
    await w.beat(t2)
    body = t2.pages[0][2]
    assert body == (f"receiver {HOST}: acknowledged again; 3 failed beats over 45 s in this leader term; "
                    f"the episode was paged in an earlier term 60 s before this beat "
                    f"(failed beats of earlier terms not known)")
    assert "/" not in body and "AUD" not in body
