"""S08 (design PR4, §C3): the Oversee relay's pending and due reads never return an ARM notice (kind 'arm'), and the
dead-man's oldest-undelivered age never counts one: in memory (src/oversee/memory.py, through VolatileRetryOutbox)
and over the PostgreSQL store's exact SQL (PgEmulator)."""
from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

from src.db.epoch import EngineDb, Epoch
from src.db.notify_store import PgOutboxStore
from src.notifications.relay import VolatileRetryOutbox
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.notify import ARM_OUTBOX_KIND, OutboxRow

from tests.db.pg_emulator import PgEmulator

INFO = OutboxRow("info", "info", "oversee info", "b", 100)


def test_the_arm_kind_is_the_one_the_sql_skips() -> None:
    assert ARM_OUTBOX_KIND == "arm"


async def test_memory_store_never_returns_an_arm_row() -> None:
    mem = InMemoryOverseeDb()
    arm = replace(INFO, kind=cast(Any, ARM_OUTBOX_KIND), title="arm notice", created_ms=50,
                  channels=("arm_email", "arm_push"))
    await mem.add([arm, INFO, arm])
    assert [r.title for r in await mem.pending(10)] == ["oversee info"]
    relay = VolatileRetryOutbox(mem)
    assert [r.title for r in await relay.due(10_000, 10)] == ["oversee info"]
    assert await relay.oldest_undelivered_ms() == 100          # the older ARM row is not counted
    await relay.finish(2, 200, delivered=True, error=None)
    assert await mem.pending(10) == [] and await relay.due(10_000, 10) == []
    assert await relay.oldest_undelivered_ms() is None          # only ARM rows left unsent


async def test_pg_store_never_returns_an_arm_row() -> None:
    emu = PgEmulator(epoch=7)
    store = PgOutboxStore(EngineDb(emu, lambda: Epoch("c", 7)))
    emu.add_outbox_row(kind="arm", channels=["arm_email:2", "arm_push"], created=50, title="arm notice")
    await store.add([INFO])
    emu.add_outbox_row(kind="arm", channels=["arm_push"], created=60, title="arm notice 2")
    assert [r.title for r in await store.pending(10)] == ["oversee info"]
    due = await store.due(10_000, 10)
    assert [r.title for r in due] == ["oversee info"] and due[0].route == ("email",)
    assert await store.oldest_undelivered_ms() == 100
    await store.finish(due[0].outbox_id or 0, 300, delivered=True, error=None)
    assert await store.pending(10) == [] and await store.due(10_000, 10) == []
    assert await store.oldest_undelivered_ms() is None
    # the ARM rows are still there, unsent, for the ARM relay's own store (S29)
    assert [r["title"] for r in emu.t.outbox if r["sent_at"] is None] == ["arm notice", "arm notice 2"]


async def test_a_limit_counts_only_oversee_rows() -> None:
    emu = PgEmulator(epoch=7)
    store = PgOutboxStore(EngineDb(emu, lambda: Epoch("c", 7)))
    for i in range(5):
        emu.add_outbox_row(kind="arm", channels=["arm_email"], created=i)
    await store.add([INFO, replace(INFO, title="second", created_ms=101)])
    assert [r.title for r in await store.pending(2)] == ["oversee info", "second"]
    assert [r.title for r in await store.due(10_000, 1)] == ["oversee info"]
