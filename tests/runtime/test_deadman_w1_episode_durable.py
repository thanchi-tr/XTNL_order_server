"""Home-server plan W1, review round 1: the receiver-down episode is durable and timed on the monotonic clock.

* operations.md §9 (dedupe state in PostgreSQL): the high page and the info page record their times in
  `notification_dedupe`, so a new leader term neither pages the same outage again nor leaves it without its info page.
* A wall-clock step never moves the page (the streak runs on the monotonic clock).
"""
from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from types import MappingProxyType

import pytest

from src.db.fence import EpochMismatch
from src.oversee.notify import EpisodeKey
from src.runtime.deadman import (
    ACK_BACK_TITLE, ACK_LOST_TITLE, DeadmanSender, ack_episode_keys,
)
from tests.runtime.test_deadman_receiver_alert_w1 import BEAT_MS, HOST, T0, _Receiver, _Src

LOST, BACK = ack_episode_keys(HOST)


class _Dedupe:
    """In-memory DedupeStore (the same contract as PgDedupeStore)."""

    def __init__(self) -> None:
        self.rows: dict[EpisodeKey, int] = {}
        self.fail_reads = 0
        self.fence_lost = False

    async def last_sent(self, key: EpisodeKey) -> int | None:
        return self.rows.get(key)

    async def record(self, key: EpisodeKey, at_ms: int) -> None:
        if self.fence_lost:
            raise EpochMismatch("fence lost")
        self.rows[key] = at_ms

    async def snapshot(self) -> Mapping[EpisodeKey, int]:
        return MappingProxyType(dict(self.rows))

    async def last_sent_of(self, keys: Sequence[EpisodeKey]) -> Mapping[EpisodeKey, int]:
        if self.fail_reads:
            self.fail_reads -= 1
            raise ConnectionError("postgres down")
        return MappingProxyType({k: self.rows[k] for k in keys if k in self.rows})


@dataclasses.dataclass
class Term:
    sender: DeadmanSender
    pages: list[tuple[str, str, str]]


class World:
    """One receiver, one durable store, and a wall clock plus a monotonic clock that tests can move apart."""

    def __init__(self) -> None:
        self.wall = [T0]
        self.mono = [1000.0]
        self.receiver = _Receiver()
        self.dedupe = _Dedupe()

    def term(self, alert_after_s: float = 300) -> Term:
        pages: list[tuple[str, str, str]] = []
        sender = DeadmanSender(None, _Src(), lease_valid=lambda: True, is_leader=lambda: True,
                               clock_ms=lambda: self.wall[0], monotonic=lambda: self.mono[0],
                               transport=self.receiver,
                               page=lambda sev, title, body: pages.append((sev, title, body)),
                               alert_after_s=alert_after_s, episodes=self.dedupe)
        return Term(sender, pages)

    async def beat(self, t: Term, n: int = 1) -> None:
        for _ in range(n):
            await t.sender.send_once()
            self.wall[0] += BEAT_MS
            self.mono[0] += BEAT_MS / 1000


async def test_the_high_page_and_the_info_page_are_recorded() -> None:
    w = World()
    t = w.term()
    w.receiver.up = False
    await w.beat(t, 21)
    assert [p[1] for p in t.pages] == [ACK_LOST_TITLE] and LOST in w.dedupe.rows and BACK not in w.dedupe.rows
    w.receiver.up = True
    await w.beat(t)
    assert [p[1] for p in t.pages] == [ACK_LOST_TITLE, ACK_BACK_TITLE]
    assert w.dedupe.rows[BACK] > w.dedupe.rows[LOST]


async def test_a_new_term_does_not_page_an_open_episode_again() -> None:
    """Case A of the finding: term 1 paged, the engine restarts, the receiver is still down at t=900 s."""
    w = World()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 25)
    assert [p[0] for p in t1.pages] == ["high"]
    t2 = w.term()                              # restart / takeover: a fresh sender, the same PostgreSQL
    await w.beat(t2, 40)                       # 600 s more of failures in the new term
    assert t2.pages == [] and t2.sender.ack_paged and t2.sender.ack_inherited


async def test_a_new_term_sends_the_info_page_of_an_inherited_episode() -> None:
    """Case B: the receiver comes back while term 2's own streak is short (or empty)."""
    w = World()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 25)
    t2 = w.term()
    await w.beat(t2, 3)                        # 45 s: under the threshold in this term
    w.receiver.up = True
    await w.beat(t2)
    assert [(p[0], p[1]) for p in t2.pages] == [("info", ACK_BACK_TITLE)]
    assert "earlier term" in t2.pages[0][2] and "/" not in t2.pages[0][2]
    t3 = w.term()                              # the episode is closed: a later term stays quiet
    await w.beat(t3, 5)
    assert t3.pages == [] and not t3.sender.ack_paged


async def test_a_new_term_whose_first_beat_is_acknowledged_closes_the_episode() -> None:
    w = World()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 21)
    w.receiver.up = True
    t2 = w.term()
    await w.beat(t2)
    assert [p[0] for p in t2.pages] == ["info"]
    # round-3 review: never '0 failed beats over 0 s'; the earlier term's count is unknown, the time since its
    # stored 'lost' row is given instead (term 1 paged at its 21st failure, 300 s in; term 2 beats 15 s later)
    body = t2.pages[0][2]
    assert "0 failed beats" not in body and "over 0 s" not in body
    assert body == (f"receiver {HOST}: acknowledged again; the first beat of this leader term was acknowledged; "
                    f"the episode was paged in an earlier term 15 s before this beat "
                    f"(failed beats of earlier terms not known)")


async def test_a_closed_episode_lets_a_new_outage_page_again() -> None:
    w = World()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 21)
    w.receiver.up = True
    await w.beat(t1)
    w.receiver.up = False
    t2 = w.term()
    await w.beat(t2, 21)
    assert [p[0] for p in t2.pages] == ["high"]


async def test_an_unreadable_store_is_retried_and_never_stops_the_beat() -> None:
    w = World()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 21)
    w.dedupe.fail_reads = 2
    t2 = w.term()
    await w.beat(t2, 2)                        # both reads fail; the beats still go out
    assert w.receiver.posts == 23 and not t2.sender.ack_paged
    await w.beat(t2)                           # read succeeds: the open episode is adopted, no second high page
    assert t2.sender.ack_paged and t2.pages == []


async def test_a_lost_fence_while_recording_propagates() -> None:
    w = World()
    t = w.term()
    w.receiver.up = False
    await w.beat(t, 20)
    w.dedupe.fence_lost = True
    with pytest.raises(EpochMismatch):
        await t.sender.send_once()


async def test_a_wall_clock_step_back_does_not_delay_the_page() -> None:
    w = World()
    t = w.term()
    w.receiver.up = False
    await w.beat(t, 10)
    w.wall[0] -= 600_000                       # w32time steps the clock back 10 min mid-streak
    await w.beat(t, 11)                        # the 21st failure, 300 s after the first on the monotonic clock
    assert [p[0] for p in t.pages] == ["high"]
    assert "over 300 s" in t.pages[0][2]


async def test_a_wall_clock_step_forward_does_not_hasten_the_page() -> None:
    w = World()
    t = w.term()
    w.receiver.up = False
    await w.beat(t)
    w.wall[0] += 3_600_000                     # a forward step of an hour after one failure
    await w.beat(t, 2)
    assert t.pages == []
