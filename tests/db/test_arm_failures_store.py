"""S24: the FAILED record, ping, re-ping, probe notes, open episodes and the clear (design §B14; R11-M05, RT-M5)."""
from __future__ import annotations

from dataclasses import replace

import pytest

from src.db import arm_failures_store as F
from src.db.arm_ports import ClearedFailure
from src.db.epoch import EngineDb, Epoch
from src.db.fence import EpochMismatch, FencedTx
from src.db.notify_store import SQL_OUTBOX_INSERT
from tests.db.arm_emulator import ArmEmulator
from tests.db.arm_rows import failure, ping


@pytest.fixture
def emu() -> ArmEmulator:
    return ArmEmulator(epoch=7)


@pytest.fixture
def db(emu: ArmEmulator) -> EngineDb:
    return EngineDb(emu, lambda: Epoch("c", 7))


async def test_record_writes_the_row_and_both_ping_rows(emu: ArmEmulator, db: EngineDb) -> None:
    assert await F.PgArmFailuresStore(db).record(failure(), ping()) is True
    assert list(emu.t.failures) == ["f-1"]
    assert emu.outbox_kinds() == [("info", "CRITICAL"), ("arm", "CRITICAL")]
    crit, arm = emu.t.outbox
    assert crit["channels"] == ["email", "mobile_push"] and arm["channels"] == ["arm_email", "arm_push"]
    assert crit["title"] == arm["title"] and "ping-1" in crit["title"] and crit["body"] == arm["body"]
    assert emu.t.failures["f-1"]["position_ids"] == ["syn:P1"]


@pytest.mark.parametrize("failing", [F.SQL_ARM_PING_INSERT, SQL_OUTBOX_INSERT, F.SQL_FAILURE_INSERT])
async def test_the_failure_transaction_writes_all_three_rows_or_none(emu: ArmEmulator, db: EngineDb,
                                                                     failing: str) -> None:
    emu.fail_on[failing] = ConnectionResetError("lost")
    with pytest.raises(ConnectionResetError):
        await F.PgArmFailuresStore(db).record(failure(), ping())
    assert emu.t.failures == {} and emu.t.outbox == []
    assert await F.PgArmFailuresStore(db).record(failure(), ping()) is True        # retried from memory
    assert len(emu.t.failures) == 1 and len(emu.t.outbox) == 2


async def test_a_replay_or_a_second_open_episode_queues_no_second_ping(emu: ArmEmulator, db: EngineDb) -> None:
    store = F.PgArmFailuresStore(db)
    assert await store.record(failure(), ping()) is True
    assert await store.record(failure(), ping()) is False                          # spool replay, same id
    assert await store.record(failure(failure_id="f-2", ping_id="ping-2"), ping("ping-2")) is False  # same scope
    assert len(emu.t.outbox) == 2


async def test_a_lost_fence_writes_nothing(emu: ArmEmulator, db: EngineDb) -> None:
    emu.epoch = 8
    with pytest.raises(EpochMismatch):
        await F.PgArmFailuresStore(db).record(failure(), ping())
    assert emu.t.failures == {} and emu.t.outbox == []


@pytest.mark.parametrize("bad", [
    {"probe_kind": "fresh_bar"},                    # not the probe of invalid_bar
    {"scope_kind": "NODE"},
    {"failure_id": "has space"},
    {"position_ids": ("P1",)},                      # must be adapter_id:position_id
    {"counters": (("Bad-Name", 1),)},
    {"counters": (("count", True),)},
])
async def test_invalid_records_are_refused(db: EngineDb, bad: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        await F.PgArmFailuresStore(db).record(failure(**bad), ping())


async def test_the_ping_id_is_in_the_title_and_the_body_is_bounded(emu: ArmEmulator, db: EngineDb) -> None:
    with pytest.raises(ValueError, match="ping id"):
        await F.PgArmFailuresStore(db).record(failure(), replace(ping(), title="ARM FAILED"))
    long_body = "positions " + ", ".join(f"syn:P{i}" for i in range(2_000))
    assert await F.PgArmFailuresStore(db).record(failure(), ping(body=long_body)) is True
    assert all(len(r["body"]) == F.MAX_PING_BODY for r in emu.t.outbox)


async def test_reping_probe_and_open_episodes(emu: ArmEmulator, db: EngineDb) -> None:
    store = F.PgArmFailuresStore(db)
    await store.record(failure(), ping())
    assert await store.reping("f-1", "ping-1", ping(created_ms=9_000)) is True
    assert await store.reping("f-1", "ping-9", ping("ping-9")) is False            # not this episode's ping
    assert await store.note_probe("f-1", 9_500, "refused:bars_invalid") is True
    [ep] = await store.open_episodes()
    assert ep.record == failure() and ep.epoch == 7 and ep.ping_count == 2 and ep.last_ping_at_ms == 9_000
    assert ep.last_probe_result == "refused:bars_invalid"
    assert len(emu.t.outbox) == 4


async def test_the_clear_needs_the_lock_and_a_matching_request(emu: ArmEmulator, db: EngineDb) -> None:
    store, clear = F.PgArmFailuresStore(db), F.PgArmFailedClear(db)
    await store.record(failure(), ping())
    emu.add_request(11, kind="arm_disarm", target="SERIES:syn:SYN1:15m:bid")
    emu.add_request(12, kind="arm_clear_failed", target="SERIES:other")
    emu.add_request(13, kind="arm_clear_failed", target="SERIES:syn:SYN1:15m:bid", epoch=6)
    emu.add_request(14, kind="arm_clear_failed", target="SERIES:syn:SYN1:15m:bid")
    for rid in (11, 12, 13, 99):
        assert await clear.clear(rid, "f-1", 10_000) is None
    seen: list[ClearedFailure] = []

    async def restore(tx: FencedTx, done: ClearedFailure) -> None:
        seen.append(done)

    done = await clear.clear(14, "f-1", 10_000, after=restore)
    assert done == ClearedFailure(failure_id="f-1", scope_kind="SERIES", scope_id="syn:SYN1:15m:bid",
                                  cleared_at_ms=10_000, request_id=14)
    assert seen == [done]
    assert await store.open_episodes() == []
    assert await clear.clear(14, "f-1", 11_000) is None                            # already cleared
    await store.record(failure(failure_id="f-2"), ping())
    assert await clear.clear(14, "f-2", 12_000) is None                            # one request clears one episode


async def test_a_failing_after_hook_keeps_the_episode_open(emu: ArmEmulator, db: EngineDb) -> None:
    await F.PgArmFailuresStore(db).record(failure(), ping())
    emu.add_request(14, kind="arm_clear_failed", target="SERIES:syn:SYN1:15m:bid")

    async def boom(tx: FencedTx, done: ClearedFailure) -> None:
        raise RuntimeError("restore failed")

    with pytest.raises(RuntimeError):
        await F.PgArmFailedClear(db).clear(14, "f-1", 10_000, after=boom)
    assert [e.record.failure_id for e in await F.PgArmFailuresStore(db).open_episodes()] == ["f-1"]


async def test_the_clear_writes_the_counter_reset_in_its_own_transaction(emu: ArmEmulator, db: EngineDb) -> None:
    """S38f (R14 review): the counters a clear resets commit with the clear, or not at all."""
    from src.db.arm_budget_store import SQL_BUDGET_UPSERT
    from src.db.arm_ports import BudgetCounterRow
    from src.db.epoch import SQL_COMMIT
    key = ("SERIES", "syn:SYN1:15m:bid", "unresolved")
    emu.t.budget[key] = (4, 1_000, 900_000, 7, 2_000)
    reset = [BudgetCounterRow(scope_kind=key[0], scope_id=key[1], counter=key[2], count=0)]
    await F.PgArmFailuresStore(db).record(failure(), ping())
    emu.add_request(13, kind="arm_clear_failed", target="SERIES:other")
    assert await F.PgArmFailedClear(db).clear(13, "f-1", 10_000, reset=reset) is None
    assert emu.t.budget[key][0] == 4                                   # a refused clear resets nothing
    emu.add_request(14, kind="arm_clear_failed", target="SERIES:syn:SYN1:15m:bid")
    mark = len(emu.sql)
    assert await F.PgArmFailedClear(db).clear(14, "f-1", 10_000, reset=reset) is not None
    assert emu.t.budget[key] == (0, None, None, 7, 10_000)
    sql = emu.sql[mark:]
    clear_at, upsert_at = sql.index(F.SQL_FAILURE_CLEAR), sql.index(SQL_BUDGET_UPSERT)
    assert clear_at < upsert_at < sql.index(SQL_COMMIT)
    # a failing reset write rolls the clear back with it
    await F.PgArmFailuresStore(db).record(failure(failure_id="f-2"), ping())
    emu.add_request(15, kind="arm_clear_failed", target="SERIES:syn:SYN1:15m:bid")
    emu.fail_on[SQL_BUDGET_UPSERT] = ConnectionResetError("lost")
    with pytest.raises(ConnectionResetError):
        await F.PgArmFailedClear(db).clear(15, "f-2", 11_000, reset=reset)
    assert [e.record.failure_id for e in await F.PgArmFailuresStore(db).open_episodes()] == ["f-2"]


def test_only_the_clear_statement_sets_cleared_at_ms() -> None:
    owner_only = [name for name in dir(F) if name.startswith("SQL_") and "cleared_at_ms =" in getattr(F, name)]
    assert owner_only == ["SQL_FAILURE_CLEAR"]
    assert "pg_advisory_xact_lock" in F.SQL_ARM_FAILED_LOCK
    assert "operator_audit" in F.SQL_FAILURE_CLEAR and "'arm_clear_failed'" in F.SQL_FAILURE_CLEAR
    assert "RETURNING" not in F.SQL_ARM_PING_INSERT                    # xtnl_arm has INSERT only on the outbox


async def test_a_folded_episode_records_the_row_without_ping_rows(emu: ArmEmulator, db: EngineDb) -> None:
    """S30a collapsed ping: `queue_ping=False` writes the row (under the collapsing ping's id) and no outbox row."""
    store = F.PgArmFailuresStore(db)
    assert await store.record(failure(), ping(), queue_ping=False) is True
    assert list(emu.t.failures) == ["f-1"] and emu.t.outbox == []
    assert emu.t.failures["f-1"]["ping_id"] == "ping-1"
    assert await store.record(failure(), ping(), queue_ping=False) is False      # a replay writes nothing
