"""LEADER_INIT step 1 statements and SAFE_HOLD (operations.md §4)."""
from __future__ import annotations

import pytest

from src.db import leader as L
from src.db.fence import FENCE, EpochMismatch, FencedTx

from tests.db.conftest import EPOCH, FakeConn

NOW = 1_800_000_000_000


async def test_fence_closes_predecessors_and_opens_term(conn: FakeConn) -> None:
    conn.status_for(L.SQL_FENCE_ALL_STREAMS, "UPDATE 6")
    conn.status_for(L.SQL_CLOSE_PREDECESSORS, "UPDATE 1")
    conn.returns(L.SQL_READ_CLUSTER_STATE, (False, None))
    conn.returns(L.SQL_COUNT_ABNORMAL, (1,))
    conn.returns(L.SQL_COUNT_EPOCH_CHANGES, (2,))
    res = await L.leader_init_fence(conn, EPOCH, "node-a", NOW)
    assert res == L.LeaderInitFence(streams_fenced=6, predecessors_closed=1, safe_hold=False)
    assert conn.sql[:3] == [L.SQL_FENCE_ALL_STREAMS, L.SQL_CLOSE_PREDECESSORS, L.SQL_INSERT_TERM]
    assert conn.args_of(L.SQL_FENCE_ALL_STREAMS) == [(NOW,)]
    assert conn.args_of(L.SQL_INSERT_TERM) == [("cluster-a", 7, "node-a", NOW)]
    assert L.SQL_SET_SAFE_HOLD not in conn.sql
    assert "COALESCE(conn_lost_at" in L.SQL_FENCE_ALL_STREAMS


async def test_three_abnormal_ends_set_safe_hold_and_abort_term(conn: FakeConn) -> None:
    conn.returns(L.SQL_READ_CLUSTER_STATE, (False, NOW - 3_600_000))
    conn.returns(L.SQL_COUNT_ABNORMAL, (3,))
    res = await L.leader_init_fence(conn, EPOCH, "node-a", NOW)
    assert res.safe_hold and "abnormal" in (res.safe_hold_reason or "")
    assert conn.args_of(L.SQL_COUNT_ABNORMAL) == [(NOW - 15 * 60_000, NOW - 3_600_000)]
    assert conn.args_of(L.SQL_END_TERM) == [("cluster-a", 7, NOW, "init_failed")]
    assert L.SQL_SET_SAFE_HOLD in conn.sql


def test_abnormal_count_needs_the_term_to_start_inside_the_window() -> None:
    """A predecessor closed now by step 1 (`ended_at` = now) after a kill hours ago is no flap: the count
    requires started_at in the window too (false SAFE_HOLD after a PC shutdown, 2026-11-18)."""
    assert "started_at >= $1::bigint" in L.SQL_COUNT_ABNORMAL
    assert "ended_at >= $1::bigint" in L.SQL_COUNT_ABNORMAL


async def test_epoch_change_budget(conn: FakeConn) -> None:
    conn.returns(L.SQL_READ_CLUSTER_STATE, (False, None))
    conn.returns(L.SQL_COUNT_ABNORMAL, (0,))
    conn.returns(L.SQL_COUNT_EPOCH_CHANGES, (7,))
    res = await L.leader_init_fence(conn, EPOCH, "node-a", NOW)
    assert res.safe_hold


async def test_existing_safe_hold_aborts_without_rewriting(conn: FakeConn) -> None:
    conn.returns(L.SQL_READ_CLUSTER_STATE, (True, None))
    res = await L.leader_init_fence(conn, EPOCH, "node-a", NOW)
    assert res.safe_hold
    assert L.SQL_SET_SAFE_HOLD not in conn.sql and L.SQL_END_TERM in conn.sql


async def test_term_bookkeeping(conn: FakeConn) -> None:
    tx = FencedTx(conn, EPOCH.token)
    await L.mark_reached_leader(tx, EPOCH, NOW)
    await L.record_end_reason(tx, EPOCH, "switchover")
    assert conn.args_of(L.SQL_REACHED_LEADER) == [(7, "cluster-a", 7, NOW)]
    assert conn.args_of(L.SQL_RECORD_END_REASON) == [(7, "cluster-a", 7, "switchover")]


def test_term_bookkeeping_statements_are_fenced() -> None:
    """leader-terms-writes-unfenced: LEADER entry / DRAINING writes are engine state writes (§4 fence)."""
    for sql in (L.SQL_REACHED_LEADER, L.SQL_RECORD_END_REASON):
        assert FENCE in sql and "$1" in sql


async def test_stale_leader_reached_leader_raises_epoch_mismatch(conn: FakeConn) -> None:
    """A paused LEADER_INIT node resumes after the successor bumped the epoch: no write, EpochMismatch."""
    conn.stored_epoch = 8                      # the successor's epoch
    with pytest.raises(EpochMismatch):
        await L.mark_reached_leader(FencedTx(conn, EPOCH.token), EPOCH, NOW)


async def test_stale_leader_end_reason_raises_epoch_mismatch(conn: FakeConn) -> None:
    conn.stored_epoch = 8
    with pytest.raises(EpochMismatch):
        await L.record_end_reason(FencedTx(conn, EPOCH.token), EPOCH, "abdicate")


async def test_end_reason_already_set_is_not_an_error(conn: FakeConn) -> None:
    conn.status_for(L.SQL_RECORD_END_REASON, "UPDATE 0")   # successor already closed it, fence still ours
    await L.record_end_reason(FencedTx(conn, EPOCH.token), EPOCH, "switchover")
