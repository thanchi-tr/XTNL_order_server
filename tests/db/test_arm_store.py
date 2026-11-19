"""S24: the ARM state store (positions, decisions, handles, notices, correction audit) over the emulator."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from src.db import arm_store as A
from src.db.arm_ports import (
    LOST_FENCE_ERRORS,
    CorrectionAuditRow,
    DecisionRef,
    DecisionWriteStatus,
    NoticeRow,
    PositionCas,
    PositionKey,
    UnnoticedPage,
    is_lost_fence,
)
from src.db.arm_schema import SUPPRESSED_NOTICE_ID
from src.db.epoch import EngineDb, Epoch, EpochFenced
from src.db.fence import SQL_ROLLBACK_TO_SAVEPOINT, SQL_SAVEPOINT, EpochMismatch
from tests.db.arm_emulator import ArmEmulator, InFailedTransaction
from tests.db.arm_rows import KEY, decision_row, handle_row, position_row


@pytest.fixture
def emu() -> ArmEmulator:
    return ArmEmulator(epoch=7)


@pytest.fixture
def store(emu: ArmEmulator) -> A.PgArmStateStore:
    return A.PgArmStateStore(EngineDb(emu, lambda: Epoch("c", 7)))


def cas(expected: int = 0, **kw: object) -> PositionCas:
    return PositionCas(row=position_row(state="TRACKING", decision_seq=1, last_decided_bar=900_000,
                                        last_decided_hash="01" + "d" * 32, **kw), expected_row_version=expected)


async def test_position_insert_is_idempotent_and_round_trips(store: A.PgArmStateStore) -> None:
    pos = position_row(pre_basis_json='{"levels": [["POSITION_FIELD", "9.0"]]}', pre_basis_bar=900_000)
    async with store.transaction() as txn:
        assert await txn.insert_position(pos) is True
        assert await txn.insert_position(replace(pos, state="TRACKING")) is False   # ON CONFLICT DO NOTHING
    assert await store.position(KEY) == replace(pos, epoch=7)
    assert await store.position(PositionKey(adapter_id="other", position_id="P1")) is None


async def test_two_adapters_may_share_a_position_id(store: A.PgArmStateStore) -> None:
    other = position_row(key=PositionKey(adapter_id="syn2", position_id="P1"))
    async with store.transaction() as txn:
        assert await txn.insert_position(position_row()) is True
        assert await txn.insert_position(other) is True
    assert [p.key.adapter_id for p in await store.live_positions()] == ["syn", "syn2"]


async def test_cas_hits_once_and_misses_on_a_moved_version(store: A.PgArmStateStore, emu: ArmEmulator) -> None:
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        assert await txn.update_position(cas(0)) is True
        assert await txn.update_position(cas(0)) is False          # row_version is 1 now: a miss, no raise
    got = await store.position(KEY)
    assert got is not None and got.row_version == 1 and got.state == "TRACKING" and got.decision_seq == 1


async def test_cas_never_changes_an_ended_row(store: A.PgArmStateStore) -> None:
    async with store.transaction() as txn:
        await txn.insert_position(position_row(state="CLOSED", ended_at_ms=5))
        assert await txn.update_position(cas(0)) is False
    assert await store.live_positions() == []


async def test_decision_handles_and_cas_commit_together(store: A.PgArmStateStore, emu: ArmEmulator) -> None:
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        w = await txn.record_decision(decision_row(), [handle_row(), handle_row(handle_id="O9",
                                      handle_kind="CHILD_ORDER", handle_role="CHILD_ORDER:0")], position=cas(0))
    assert w.status is DecisionWriteStatus.INSERTED and w.ref == DecisionRef(decision_id=1, bar_end_ms=900_000)
    assert [h["handle_role"] for h in emu.t.handles] == ["POSITION_FIELD", "CHILD_ORDER:0"]
    assert emu.t.positions[("syn", "P1", "STOP_LEVEL", "position")]["row_version"] == 1
    assert emu.sql.count(SQL_SAVEPOINT) == 1


async def test_decision_memory_reads_this_bars_pre_adjust_and_the_newest_bar_end_decision(
        store: A.PgArmStateStore) -> None:
    """S36b review: the bar-end test re-reads this bar's committed PRE `ADJUST` levels (`this_bar_pre`) and the
    newest committed bar-end decision (`prev_final_at_target`) from PostgreSQL, so a new term decides the same."""
    empty = await store.decision_memory(KEY, "15m", 1_800_000)
    assert empty.pre_levels == () and empty.last_outcome is None
    child = handle_row(handle_id="O9", handle_kind="CHILD_ORDER", handle_role="CHILD_ORDER:0",
                       level_at_pre=Decimal("8.5"), level_at_bar_end=None, target_level=None)
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        await txn.record_decision(decision_row(outcome="AT_TARGET_UNCONFIRMED", category="INFO", decision_seq=1))
        await txn.record_decision(decision_row(event="PRE", bar_end_ms=1_800_000, decision_seq=2, advice=False),
                                  [handle_row(level_at_pre=Decimal("9.0"), level_at_bar_end=None), child])
        await txn.record_decision(decision_row(event="PRE", bar_end_ms=2_700_000, decision_seq=3, advice=False,
                                               outcome="STRATEGY_REASON", category="INFO"),
                                  [handle_row(level_at_pre=Decimal("9.0"))])
    got = await store.decision_memory(KEY, "15m", 1_800_000)
    assert got.pre_levels == (("CHILD_ORDER:0", Decimal("8.5")), ("POSITION_FIELD", Decimal("9.0")))
    assert (got.last_outcome, got.last_reason) == ("AT_TARGET_UNCONFIRMED", None)   # PRE rows never count
    assert (await store.decision_memory(KEY, "15m", 2_700_000)).pre_levels == ()     # not an ADJUST
    assert (await store.decision_memory(KEY, "5m", 1_800_000)).last_outcome is None
    assert "8.5" not in repr(got) and "9.0" not in repr(got)


async def test_decision_memory_skips_no_broker_bar_so_an_at_target_run_is_not_broken(
        store: A.PgArmStateStore) -> None:
    """S38c review: a `NO_BROKER_BAR` bucket changes no state, so the newest bar-end decision the next bar reads
    (`prev_final_at_target`) is the one before it."""
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        await txn.record_decision(decision_row(outcome="AT_TARGET_UNCONFIRMED", category="INFO", decision_seq=1))
        await txn.record_decision(decision_row(outcome="NO_BROKER_BAR", category="INFO", decision_seq=2,
                                               bar_end_ms=1_800_000, advice=False))
    got = await store.decision_memory(KEY, "15m", 2_700_000)
    assert got.last_outcome == "AT_TARGET_UNCONFIRMED"
    assert "NO_BROKER_BAR" in A.SQL_LAST_BAR_END_DECISION


async def test_a_withdrawn_final_adjust_is_no_advice_for_a_corrected_rerun(store: A.PgArmStateStore) -> None:
    """Round 14 review: bar N-3's POST ADJUST to X, bar N-2's POST withdrew it, bar N's own POST ADJUST set
    ADVICE_OUTSTANDING. A CORRECTED re-run of bar N reads no outstanding advice (`prior_final_adjust` None: the newest
    final decision before N is not an ADJUST), although the newest earlier ADJUST (the POST/LATE read) is X; an
    unwithdrawn earlier ADJUST, or a revision of it, is the target."""
    n = 4 * 900_000
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        await txn.record_decision(decision_row(bar_end_ms=n - 3 * 900_000, decision_seq=1,
                                               proposed_stop=Decimal("10.0")))
        await txn.record_decision(decision_row(bar_end_ms=n - 2 * 900_000, decision_seq=2, outcome="STRATEGY_REASON",
                                               category="INFO", advice=False, proposed_stop=None))
        await txn.record_decision(decision_row(bar_end_ms=n, decision_seq=3, proposed_stop=Decimal("10.5")))
    got = await store.decision_memory(KEY, "15m", n)
    assert got.final_advice_target == Decimal("10.0") and got.prior_final_adjust is None
    # bar N-1 gives a final ADJUST: that unwithdrawn advice is the target of the re-run
    async with store.transaction() as txn:
        await txn.record_decision(decision_row(bar_end_ms=n - 900_000, decision_seq=4, proposed_stop=Decimal("10.2")))
    assert (await store.decision_memory(KEY, "15m", n)).prior_final_adjust == Decimal("10.2")
    # a CORRECTED of bar N-1 that withdraws it outranks its POST (by bar end, then decision_seq)
    async with store.transaction() as txn:
        await txn.record_decision(decision_row(bar_end_ms=n - 900_000, event="CORRECTED", decision_seq=5,
                                               outcome="STRATEGY_REASON", category="INFO", advice=False,
                                               proposed_stop=None))
    assert (await store.decision_memory(KEY, "15m", n)).prior_final_adjust is None
    assert "10.0" not in repr(got) and "10.2" not in repr(got)


async def test_a_replayed_decision_is_already_decided_without_a_fence_error(store: A.PgArmStateStore,
                                                                         emu: ArmEmulator) -> None:
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        await txn.record_decision(decision_row(), [handle_row()], position=cas(0))
        again = await txn.record_decision(decision_row(), [handle_row()], position=cas(1))
    assert again.status is DecisionWriteStatus.ALREADY_DECIDED
    assert len(emu.t.decisions) == 1 and len(emu.t.handles) == 1
    assert emu.t.positions[("syn", "P1", "STOP_LEVEL", "position")]["row_version"] == 1   # no CAS on a conflict


async def test_a_cas_miss_rolls_back_only_its_savepoint(store: A.PgArmStateStore, emu: ArmEmulator) -> None:
    other = PositionKey(adapter_id="syn", position_id="P2")
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        await txn.insert_position(position_row(key=other))
        miss = await txn.record_decision(decision_row(), [handle_row()], position=cas(5))
        hit = await txn.record_decision(decision_row(key=other), [handle_row(handle_id="P2")],
                                        position=PositionCas(row=position_row(key=other, state="TRACKING"),
                                                             expected_row_version=0))
    assert miss.status is DecisionWriteStatus.CAS_MISS and miss.ref is None
    assert hit.status is DecisionWriteStatus.INSERTED
    assert [d["position_id"] for d in emu.t.decisions.values()] == ["P2"]
    assert [h["handle_id"] for h in emu.t.handles] == ["P2"]
    assert emu.sql.count(SQL_ROLLBACK_TO_SAVEPOINT) == 1
    assert len(emu.t.positions) == 2                                 # the event's transaction committed


async def test_a_transient_error_in_one_savepoint_leaves_the_others(store: A.PgArmStateStore,
                                                                   emu: ArmEmulator) -> None:
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        emu.fail_on[A.SQL_HANDLES_INSERT] = TimeoutError("statement timeout")
        with pytest.raises(TimeoutError):
            await txn.record_decision(decision_row(), [handle_row()], position=cas(0))
        ok = await txn.record_decision(decision_row(revision=1), [handle_row()], position=cas(0))
    assert ok.status is DecisionWriteStatus.INSERTED
    assert [d["revision"] for d in emu.t.decisions.values()] == [1]


def _cas_of(pid: str) -> object:
    """A `fail_when` that fails the position CAS of `pid` once (a statement timeout)."""
    at = 1 + len(A.POSITION_MUTABLE_COLUMNS) + 1              # $1 epoch, the mutable columns, adapter_id, position_id
    done: list[bool] = []

    def fail(sql: str, args: tuple[object, ...]) -> BaseException | None:
        if sql == A.SQL_POSITION_CAS and args[at] == pid and not done:
            done.append(True)
            return TimeoutError("statement timeout")
        return None
    return fail


async def test_an_error_in_a_cas_alone_rolls_back_only_its_savepoint(store: A.PgArmStateStore,
                                                                    emu: ArmEmulator) -> None:
    """S27 review: the `pending_event` CAS (no decision row) between two decisions of one (event, bar end)
    transaction; its error must not abort the transaction (PostgreSQL would refuse every later statement)."""
    keys = {p: PositionKey(adapter_id="syn", position_id=p) for p in ("P1", "P2", "P3")}
    async with store.transaction() as txn:
        for k in keys.values():
            await txn.insert_position(position_row(key=k))
    emu.fail_when = _cas_of("P2")  # type: ignore[assignment]
    async with store.transaction() as txn:
        one = await txn.record_decision(decision_row(key=keys["P1"]), position=PositionCas(
            row=position_row(key=keys["P1"], state="TRACKING"), expected_row_version=0))
        with pytest.raises(TimeoutError):
            await txn.update_position(PositionCas(row=position_row(key=keys["P2"], pending_event="POST",
                                                                   pending_bar_end_ms=900_000),
                                                  expected_row_version=0))
        three = await txn.record_decision(decision_row(key=keys["P3"]), position=PositionCas(
            row=position_row(key=keys["P3"], state="TRACKING"), expected_row_version=0))
    assert one.status is three.status is DecisionWriteStatus.INSERTED
    assert sorted(d["position_id"] for d in emu.t.decisions.values()) == ["P1", "P3"]
    rows = {k[1]: r for k, r in emu.t.positions.items()}
    assert (rows["P1"]["row_version"], rows["P2"]["row_version"], rows["P3"]["row_version"]) == (1, 0, 1)


async def test_the_emulator_aborts_a_transaction_after_an_error_outside_a_savepoint(
        store: A.PgArmStateStore, emu: ArmEmulator) -> None:
    emu.fail_on[A.SQL_NOTICE_INSERT] = TimeoutError("statement timeout")
    with pytest.raises(InFailedTransaction):
        async with store.transaction() as txn:
            with pytest.raises(TimeoutError):
                await txn.insert_notice(NoticeRow(route="r1", bar_spec="15m", event="POST", bar_end_ms=900_000,
                                                  created_at_ms=902_000))
            await txn.insert_position(position_row())
    assert emu.t.positions == {}
    async with store.transaction() as txn:                       # the next transaction starts clean
        assert await txn.insert_position(position_row()) is True


def test_lost_fence_errors_are_recognised_by_name() -> None:
    assert LOST_FENCE_ERRORS == {f"{c.__module__}.{c.__qualname__}" for c in (EpochMismatch, EpochFenced)}
    assert is_lost_fence(EpochMismatch("x")) and is_lost_fence(EpochFenced("x"))
    assert is_lost_fence(type("Sub", (EpochMismatch,), {})("x"))
    assert not is_lost_fence(RuntimeError("x")) and not is_lost_fence(TimeoutError())


async def test_a_lost_fence_raises_on_a_conflict_free_write(store: A.PgArmStateStore, emu: ArmEmulator) -> None:
    emu.epoch = 8
    with pytest.raises(EpochMismatch):
        async with store.transaction() as txn:
            await txn.insert_position(position_row())
    assert emu.t.positions == {}
    with pytest.raises(EpochMismatch):
        async with store.transaction() as txn:
            await txn.record_decision(decision_row(), [handle_row()])


async def test_the_cas_position_must_be_the_decided_one(store: A.PgArmStateStore) -> None:
    with pytest.raises(ValueError, match="decided position"):
        async with store.transaction() as txn:
            await txn.record_decision(decision_row(), position=cas(0, key=PositionKey(adapter_id="x",
                                                                                         position_id="P1")))


async def test_notices_attach_and_the_unnoticed_read(store: A.PgArmStateStore, emu: ArmEmulator) -> None:
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        await txn.insert_position(position_row(key=PositionKey(adapter_id="syn", position_id="P3"),
                                               account_id="ACC2"))
        a = await txn.record_decision(decision_row())
        b = await txn.record_decision(decision_row(event="LATE", decided_at_ms=950_000))
        c = await txn.record_decision(decision_row(key=PositionKey(adapter_id="syn", position_id="P3")))
        route = [("syn", "ACC1")]
        pending = await txn.unnoticed_decisions(route, "15m", since_bar_end_ms=0)
        assert [d.ref for d in pending.decisions] == [a.ref, b.ref]          # ACC2 (c) is another route
        assert pending.truncated is False
        assert pending.decisions[0].row == replace(decision_row(), epoch=7)
        nid = await txn.insert_notice(NoticeRow(route="r1", bar_spec="15m", event="POST", bar_end_ms=900_000,
                                                decision_count=1, created_at_ms=902_000))
        assert nid == 1
        assert await txn.insert_notice(NoticeRow(route="r1", bar_spec="15m", event="POST", bar_end_ms=900_000,
                                                 created_at_ms=903_000)) is None      # key exists: no outbox row
        assert a.ref is not None and b.ref is not None and c.ref is not None
        assert await txn.attach_notice(nid, [a.ref]) == 1
        assert await txn.suppress([b.ref]) == 1
        assert await txn.attach_notice(nid, [b.ref]) == 0                     # never re-attached once set
        none = UnnoticedPage(decisions=(), truncated=False)
        assert await txn.unnoticed_decisions(route, "15m", since_bar_end_ms=0) == none
        other = await txn.unnoticed_decisions([("syn", "ACC2")], "15m", since_bar_end_ms=0)
        assert [d.ref for d in other.decisions] == [c.ref]
        assert await txn.unnoticed_decisions([], "15m", since_bar_end_ms=0) == none
        with pytest.raises(ValueError, match="sentinel"):
            await txn.attach_notice(SUPPRESSED_NOTICE_ID, [c.ref])
    assert emu.t.decisions[(2, 900_000)]["notice_id"] == SUPPRESSED_NOTICE_ID


async def test_more_unnoticed_decisions_than_the_limit_are_flagged_and_drained(store: A.PgArmStateStore,
                                                                               emu: ArmEmulator) -> None:
    """§B8: no committed decision is left out. A page holds at most `limit` decisions and says when more exist; the
    notice transaction suppresses / attaches each page and reads again until `truncated` is False."""
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        made = [await txn.record_decision(decision_row(bar_end_ms=900_000 * (i + 1), decision_seq=i + 1))
                for i in range(7)]
        route = [("syn", "ACC1")]
        seen = []
        while True:
            page = await txn.unnoticed_decisions(route, "15m", since_bar_end_ms=0, limit=3)
            seen.append(len(page.decisions))
            assert await txn.suppress([d.ref for d in page.decisions]) == len(page.decisions)
            if not page.truncated:
                break
        assert seen == [3, 3, 1]
        exact = await txn.unnoticed_decisions(route, "15m", since_bar_end_ms=0, limit=3)
        assert exact == UnnoticedPage(decisions=(), truncated=False)
        with pytest.raises(ValueError, match="limit"):
            await txn.unnoticed_decisions(route, "15m", since_bar_end_ms=0, limit=0)
    assert all(m.ref is not None and emu.t.decisions[(m.ref.decision_id, m.ref.bar_end_ms)]["notice_id"]
               == SUPPRESSED_NOTICE_ID for m in made)
    assert emu.sql.count(A.SQL_DECISIONS_UNNOTICED) == 4


async def test_a_page_of_exactly_the_limit_is_not_truncated(store: A.PgArmStateStore) -> None:
    async with store.transaction() as txn:
        await txn.insert_position(position_row())
        for i in range(3):
            await txn.record_decision(decision_row(bar_end_ms=900_000 * (i + 1), decision_seq=i + 1))
        page = await txn.unnoticed_decisions([("syn", "ACC1")], "15m", since_bar_end_ms=0, limit=3)
        assert len(page.decisions) == 3 and page.truncated is False


async def test_correction_audit_is_idempotent(store: A.PgArmStateStore, emu: ArmEmulator) -> None:
    row = CorrectionAuditRow(ref=DecisionRef(decision_id=4, bar_end_ms=900_000), adapter_id="syn", position_id="P1",
                             reason="bars_missing", recorded_at_ms=1)
    async with store.transaction() as txn:
        assert await txn.insert_correction_audit(row) is True
        assert await txn.insert_correction_audit(row) is False
    assert list(emu.t.audits) == [(4, 900_000)]


def test_insert_args_follow_the_column_lists() -> None:
    pos = position_row()
    assert len(A.position_insert_args(pos)) == len(A.POSITION_INSERT_COLUMNS)
    assert len(A.position_cas_args(cas(3))) == len(A.POSITION_MUTABLE_COLUMNS) + 5
    assert A.position_cas_args(cas(3))[-1] == 3
    assert len(A.decision_args(decision_row())) == len(A.DECISION_COLUMNS)
    assert len(A.handle_arrays([handle_row()])) == len(A.HANDLE_COLUMNS)
    assert A.SQL_POSITION_INSERT.count("$") == len(A.POSITION_INSERT_COLUMNS) + 2   # + $1 twice (FENCE, epoch)


def test_levels_and_params_stay_out_of_repr() -> None:
    text = repr(position_row(entry_level=Decimal("12.345"), s0_level=Decimal("11.234"))) + repr(
        decision_row(proposed_stop=Decimal("12.345"))) + repr(handle_row(level_at_bar_end=Decimal("11.234")))
    assert "12.345" not in text and "11.234" not in text and '"k"' not in text
