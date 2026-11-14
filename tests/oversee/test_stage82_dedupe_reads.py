"""IMPLEMENTATION_PLAN 8.2: the RiskSweep's re-page check reads only the `notification_dedupe` rows it needs.

Before: `NotificationDispatcher.queue_repages` read the whole table (`DedupeStore.snapshot`) on every account
pass, and the table only grows (one row per episode ever paged). Now it reads the episodes of this pass's
candidate states only: active, not acknowledged, and not already queued within the cadence."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace

from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import EpisodeKey, NotificationDispatcher, episode_key
from src.oversee.snapshot import BreachAck
from src.oversee.types import Cap, Level
from tests.db.test_oversee_conformance import state

CADENCE = 900_000


def key_of(s: object) -> EpisodeKey:
    return episode_key(s.rule_id, s.subject_type, s.subject_id, s.activation_id, s.level)  # type: ignore[attr-defined]


class NoFullRead(InMemoryOverseeDb):
    async def snapshot(self) -> Mapping[EpisodeKey, int]:
        raise AssertionError("the re-page check must not read the whole notification_dedupe table")


async def test_only_the_candidates_rows_are_read_and_the_outcome_is_unchanged() -> None:
    db = NoFullRead()
    for i in range(500):                                         # old episodes of other subjects
        db.t.dedupe[("max_position_risk", "order", f"stonex:OLD{i}", i, int(Level.SOFT_HALT))] = 1
    due = state(subject_id="stonex:DUE", activation_id=1)                     # last sent long ago: re-pages
    fresh = state(subject_id="stonex:FRESH", activation_id=2)                 # sent recently: not due
    never = state(subject_id="stonex:NEVER", activation_id=3, active_since=1_000)   # never sent: from active_since
    gone = replace(state(subject_id="stonex:GONE", activation_id=4), resolved_at=2_000)  # not active
    acked = state(subject_id="stonex:ACK", activation_id=5)
    db.t.dedupe[key_of(due)] = 10_000
    db.t.dedupe[key_of(fresh)] = 2_000_000
    db.t.dedupe[key_of(acked)] = 10_000
    acks = [BreachAck(1, acked.rule_id, acked.subject_type, acked.subject_id, acked.level, Cap.NONE, acked.reason,
                      "op", "seen", 5_000)]
    d = NotificationDispatcher(db, db, (), CADENCE)
    now = 2_000_000 + 1
    assert await d.queue_repages([due, fresh, never, gone, acked], acks, now) == 2
    assert db.dedupe_reads == [tuple(sorted({key_of(due), key_of(fresh), key_of(never)}, key=repr))]
    assert {r.episode for r in db.t.outbox} == {key_of(due), key_of(never)}
    # Queued episodes are not read again within the cadence; nothing at all is read when nothing is a candidate.
    assert await d.queue_repages([due, fresh, never], acks, now + 1) == 0
    assert db.dedupe_reads[-1] == (key_of(fresh),)
    reads = len(db.dedupe_reads)
    assert await d.queue_repages([gone, acked], acks, now + 2) == 0
    assert len(db.dedupe_reads) == reads


async def test_a_raised_level_is_a_new_episode_with_its_own_row() -> None:
    db = NoFullRead()
    soft = state(subject_id="stonex:R", activation_id=7, level=Level.SOFT_HALT)
    hard = replace(soft, level=Level.HARD_HALT)
    db.t.dedupe[key_of(soft)] = 10                               # the SOFT episode's delivery, long ago
    db.t.dedupe[key_of(hard)] = 1_000_000                        # the HARD episode was delivered recently
    d = NotificationDispatcher(db, db, (), CADENCE)
    assert await d.queue_repages([hard], [], 1_500_000) == 0     # its own row decides (not the SOFT one)
    assert db.dedupe_reads == [(key_of(hard),)]


async def test_the_memory_store_keeps_a_bounded_read_log() -> None:
    db = InMemoryOverseeDb()
    keys: Sequence[EpisodeKey] = [("r", "account", "A", 1, 2)]
    for _ in range(250):
        await db.last_sent_of(keys)
    assert len(db.dedupe_reads) == 100
