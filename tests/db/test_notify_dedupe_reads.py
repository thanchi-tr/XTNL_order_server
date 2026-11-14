"""IMPLEMENTATION_PLAN 8.2: PgDedupeStore.last_sent_of reads only the requested `notification_dedupe` rows."""
from __future__ import annotations

from src.db import notify_store as N
from src.db.epoch import EngineDb
from src.oversee.notify import episode_key
from src.oversee.types import Level
from tests.db.conftest import FakeConn

A = episode_key("agg", "account", "A1", 5, Level.SOFT_HALT)
B = episode_key("max_position_risk", "order", "stonex:B1", 9, Level.HARD_HALT)


async def test_last_sent_of_reads_only_the_named_keys(db: EngineDb, conn: FakeConn) -> None:
    conn.returns_all(N.SQL_DEDUPE_SOME, [(N.dedupe_key(A), 1_234), ("[\"other\",\"x\",\"y\",1,1]", 9)])
    got = await N.PgDedupeStore(db).last_sent_of([A, B])
    assert dict(got) == {A: 1_234}                         # B has no row; a stray row is ignored
    assert conn.args_of(N.SQL_DEDUPE_SOME) == [([N.dedupe_key(A), N.dedupe_key(B)],)]
    assert conn.args_of(N.SQL_DEDUPE_ALL) == []
    assert "= ANY($1::text[])" in N.SQL_DEDUPE_SOME and "WHERE" in N.SQL_DEDUPE_SOME


async def test_no_keys_no_query(db: EngineDb, conn: FakeConn) -> None:
    assert dict(await N.PgDedupeStore(db).last_sent_of([])) == {}
    assert conn.calls == []
