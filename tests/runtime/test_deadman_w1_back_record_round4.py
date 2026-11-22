"""Home-server plan W1, review round 4: a stale 'open' episode never silences a later, unrelated receiver outage.

* The 'back' row is retried on every following beat until it is written (its first attempt can time out under a
  slow PostgreSQL), so the durable state does not stay 'open' after the info page went out.
* The 'back' row is written after the 'lost' row it closes, even when the wall clock stepped back between them.
* An inherited open episode older than INHERITED_EPISODE_MAX_AGE_MS no longer stops a new term from paging its own
  outage (the 'back' record can still be lost with the process).
"""
from __future__ import annotations

from src.oversee.notify import EpisodeKey
from src.runtime.deadman import ACK_BACK_TITLE, ACK_LOST_TITLE, INHERITED_EPISODE_MAX_AGE_MS
from tests.runtime.test_deadman_w1_episode_durable import BACK, LOST, World, _Dedupe


class _FlakyDedupe(_Dedupe):
    """Fails the next `fail_records` writes (a PostgreSQL timeout), then works."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_records = 0

    async def record(self, key: EpisodeKey, at_ms: int) -> None:
        if self.fail_records:
            self.fail_records -= 1
            raise TimeoutError("postgres slow")
        await super().record(key, at_ms)


def flaky_world() -> tuple[World, _FlakyDedupe]:
    w = World()
    store = _FlakyDedupe()
    w.dedupe = store
    return w, store


async def test_a_failed_back_record_is_retried_until_it_lands() -> None:
    w, store = flaky_world()
    t = w.term()
    w.receiver.up = False
    await w.beat(t, 21)                        # the high page, recorded as 'lost'
    assert LOST in store.rows
    w.receiver.up = True
    store.fail_records = 2                     # the 'back' insert times out, and its first retry too
    await w.beat(t)
    assert [p[1] for p in t.pages] == [ACK_LOST_TITLE, ACK_BACK_TITLE] and BACK not in store.rows
    await w.beat(t)
    assert BACK not in store.rows              # still failing
    await w.beat(t)
    assert store.rows[BACK] > store.rows[LOST]
    assert [p[1] for p in t.pages] == [ACK_LOST_TITLE, ACK_BACK_TITLE]    # the info page is not sent again


async def test_a_failed_back_record_then_a_new_term_with_the_receiver_down_still_pages_high() -> None:
    """Scenario (a) of the review: after the retry lands, the next term starts with the receiver down and pages."""
    w, store = flaky_world()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 21)
    w.receiver.up = True
    store.fail_records = 1
    await w.beat(t1, 2)                        # info page; the back record fails once, then lands
    w.receiver.up = False
    t2 = w.term()                              # the next morning: Tailscale or the VPS not up yet
    await w.beat(t2, 21)
    assert not t2.sender.ack_inherited
    assert [p[1] for p in t2.pages] == [ACK_LOST_TITLE]


async def test_a_back_record_never_written_does_not_silence_the_next_days_outage() -> None:
    """The process dies before the retry lands: the next term inherits the stale 'open' row, but once that row is
    older than INHERITED_EPISODE_MAX_AGE_MS its own outage pages high after alert_after_s."""
    w, store = flaky_world()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 21)
    w.receiver.up = True
    store.fail_records = 1
    await w.beat(t1)                           # info page sent, back record lost with the process
    assert BACK not in store.rows
    w.wall[0] += INHERITED_EPISODE_MAX_AGE_MS + 3_600_000    # restarted the next morning
    w.receiver.up = False
    t2 = w.term()
    await w.beat(t2, 21)
    assert [(p[0], p[1]) for p in t2.pages] == [("high", ACK_LOST_TITLE)]
    assert t2.sender.ack_paged and not t2.sender.ack_inherited
    w.receiver.up = True
    await w.beat(t2)
    assert [p[1] for p in t2.pages] == [ACK_LOST_TITLE, ACK_BACK_TITLE]
    assert "earlier term" not in t2.pages[1][2]               # this term's own episode
    assert store.rows[BACK] > store.rows[LOST]


async def test_a_recent_inherited_episode_still_is_not_paged_twice() -> None:
    """The bound does not undo round 1: a restart inside an ongoing outage stays quiet."""
    w = World()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 25)
    t2 = w.term()
    await w.beat(t2, 40)
    assert t2.pages == [] and t2.sender.ack_inherited


async def test_back_is_written_after_lost_when_the_wall_clock_stepped_back() -> None:
    """Scenario (b): 'lost' written with the clock ahead, then w32time steps it back more than the episode."""
    w = World()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 21)
    lost_ms = w.dedupe.rows[LOST]
    w.wall[0] -= 3_600_000                     # stepped back an hour
    w.receiver.up = True
    await w.beat(t1)
    assert w.dedupe.rows[BACK] > lost_ms
    w.receiver.up = False
    t2 = w.term()                              # the next term reads the episode as closed and pages its own outage
    await w.beat(t2, 21)
    assert not t2.sender.ack_inherited and [p[1] for p in t2.pages] == [ACK_LOST_TITLE]


async def test_back_is_written_after_an_inherited_lost_row_too() -> None:
    w = World()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 21)
    lost_ms = w.dedupe.rows[LOST]
    w.wall[0] -= 3_600_000
    w.receiver.up = True
    t2 = w.term()
    await w.beat(t2)
    assert [p[0] for p in t2.pages] == ["info"] and w.dedupe.rows[BACK] > lost_ms


async def test_a_new_episode_drops_an_unwritten_back_of_the_previous_one() -> None:
    w, store = flaky_world()
    t = w.term(alert_after_s=60)
    w.receiver.up = False
    await w.beat(t, 5)
    w.receiver.up = True
    store.fail_records = 1
    await w.beat(t)                            # info page; back record fails
    store.fail_records = 100                   # PostgreSQL stays down: retries fail
    w.receiver.up = False
    await w.beat(t, 5)                         # a new episode pages high
    assert [p[1] for p in t.pages] == [ACK_LOST_TITLE, ACK_BACK_TITLE, ACK_LOST_TITLE]
    assert t.sender._back_record_pending_ms is None
