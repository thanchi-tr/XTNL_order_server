"""Home-server plan W1, review round 5: the 'lost' row of the durable episode is as safe as the 'back' row.

* A 'lost' row whose first write fails (the outbox committed, the dedupe write timed out) is retried on every
  following beat, so a takeover mid-outage still inherits the episode that was already paged.
* A pending 'lost' row is written before the 'back' row that closes it.
* A new episode's 'lost' row sorts after the previous episode's 'back' row, even after a wall-clock step back.
"""
from __future__ import annotations

from src.runtime.deadman import ACK_BACK_TITLE, ACK_LOST_TITLE
from tests.runtime.test_deadman_w1_back_record_round4 import flaky_world
from tests.runtime.test_deadman_w1_episode_durable import BACK, LOST, World


async def test_a_failed_lost_record_is_retried_and_a_new_term_inherits_the_episode() -> None:
    w, store = flaky_world()
    t1 = w.term()
    w.receiver.up = False
    await w.beat(t1, 20)
    store.fail_records = 2                     # the 'lost' write and its first retry time out
    await w.beat(t1)                           # the high page goes out
    assert [p[1] for p in t1.pages] == [ACK_LOST_TITLE] and LOST not in store.rows
    await w.beat(t1)
    assert LOST not in store.rows              # still failing
    await w.beat(t1)
    assert LOST in store.rows and t1.sender._lost_record_pending_ms is None
    t2 = w.term()                              # takeover mid-outage
    await w.beat(t2, 40)
    assert t2.pages == [] and t2.sender.ack_inherited  # not paged high a second time
    w.receiver.up = True
    await w.beat(t2)
    assert [p[0] for p in t2.pages] == ["info"]        # the high page still gets its info page


async def test_a_pending_lost_row_is_written_before_back() -> None:
    w, store = flaky_world()
    t = w.term()
    w.receiver.up = False
    await w.beat(t, 20)
    store.fail_records = 100                   # PostgreSQL down for the rest of the outage
    await w.beat(t, 3)
    w.receiver.up = True
    await w.beat(t)                            # info page; neither row can be written
    assert [p[1] for p in t.pages] == [ACK_LOST_TITLE, ACK_BACK_TITLE]
    assert LOST not in store.rows and BACK not in store.rows
    assert t.sender._lost_record_pending_ms is not None and t.sender._back_record_pending_ms is not None
    order: list[str] = []
    record = store.record

    async def spy(key, at_ms):                 # type: ignore[no-untyped-def]
        order.append("lost" if key == LOST else "back")
        await record(key, at_ms)

    store.record = spy                         # type: ignore[method-assign]
    store.fail_records = 0
    await w.beat(t)
    assert order == ["lost", "back"] and store.rows[BACK] > store.rows[LOST]
    assert t.sender._lost_record_pending_ms is None and t.sender._back_record_pending_ms is None
    t2 = w.term()                              # the episode reads as closed
    await w.beat(t2)
    assert not t2.sender.ack_inherited and t2.pages == []


async def test_a_new_lost_row_sorts_after_the_previous_back_after_a_clock_step_back() -> None:
    w = World()
    t1 = w.term(alert_after_s=60)
    w.receiver.up = False
    await w.beat(t1, 5)
    w.receiver.up = True
    await w.beat(t1)                           # episode 1 closed
    back_ms = w.dedupe.rows[BACK]
    w.wall[0] -= 600_000                       # w32time steps the clock back 10 min
    w.receiver.up = False
    await w.beat(t1, 5)                        # episode 2 pages high
    assert [p[1] for p in t1.pages] == [ACK_LOST_TITLE, ACK_BACK_TITLE, ACK_LOST_TITLE]
    assert w.dedupe.rows[LOST] > back_ms
    t2 = w.term(alert_after_s=60)              # takeover mid-outage: the episode is still open
    await w.beat(t2, 10)
    assert t2.pages == [] and t2.sender.ack_inherited
    w.receiver.up = True
    await w.beat(t2)
    assert [p[0] for p in t2.pages] == ["info"] and w.dedupe.rows[BACK] > w.dedupe.rows[LOST]


async def test_a_new_term_guards_lost_against_a_back_row_it_read() -> None:
    """The guard also covers a 'back' row written by an earlier term and read by this one."""
    w = World()
    t1 = w.term(alert_after_s=60)
    w.receiver.up = False
    await w.beat(t1, 5)
    w.receiver.up = True
    await w.beat(t1)
    back_ms = w.dedupe.rows[BACK]
    w.wall[0] -= 600_000
    t2 = w.term(alert_after_s=60)
    await w.beat(t2)                           # reads the closed episode
    w.receiver.up = False
    await w.beat(t2, 5)
    assert [p[1] for p in t2.pages] == [ACK_LOST_TITLE] and w.dedupe.rows[LOST] > back_ms


async def test_a_term_that_paged_before_it_could_read_the_store_rewrites_lost_after_back() -> None:
    w = World()
    t1 = w.term(alert_after_s=60)
    w.receiver.up = False
    await w.beat(t1, 5)
    w.receiver.up = True
    await w.beat(t1)
    back_ms = w.dedupe.rows[BACK]
    w.wall[0] -= 600_000
    w.receiver.up = False
    w.dedupe.fail_reads = 100                  # the new term cannot read the episode yet
    t2 = w.term(alert_after_s=60)
    await w.beat(t2, 5)                        # pages high; its 'lost' row is written below the old 'back'
    assert [p[1] for p in t2.pages] == [ACK_LOST_TITLE] and w.dedupe.rows[LOST] < back_ms
    w.dedupe.fail_reads = 0
    await w.beat(t2)                           # the read lands and the 'lost' row is moved after 'back'
    assert w.dedupe.rows[LOST] > back_ms
    t3 = w.term(alert_after_s=60)
    await w.beat(t3, 10)
    assert t3.pages == [] and t3.sender.ack_inherited
