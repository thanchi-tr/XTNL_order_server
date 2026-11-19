"""S16 store parts: the lease index in `LEASE`, the `INDEX` script, `StoreReader` and the writer's new hand-offs
(`quarantine_stored`, the in-memory rollback set). fakeredis + lupa; synthetic instrument and prices."""
from __future__ import annotations

import inspect
from typing import Any

import pytest

from src.marketdata.store import keys as K
from src.marketdata.store import reads as R
from src.marketdata.store import scripts as S
from src.marketdata.store.reads import StoreReader
from src.marketdata.store.runner import ScriptOk, ScriptRefused, ScriptRunner, StoreUnavailable
from src.marketdata.types import M1, SeriesId, Side, Source

from .conftest import SENTINEL, SERIES, T0, TOKEN, bar, hash_of

TICK_1M = SeriesId("mock", "SYN1", M1, Side.BID, Source.TICK)


async def test_lease_writes_and_releases_its_index_entry(md_redis: Any, runner: ScriptRunner) -> None:
    assert await runner.run_one(S.lease_call(TOKEN, "arm", "specA", 30_000, "ARMED", registry_field="specA",
                                             registry_value=SERIES.canonical)) == ScriptOk(1)
    assert await md_redis.hget(K.LEASE_INDEX, "arm:specA") == b"ARMED"
    assert await StoreReader(md_redis).live_leases() == [("arm", "specA", "ARMED")]
    await md_redis.delete(K.demand("arm", "specA"))                       # the TTL ran out
    assert await StoreReader(md_redis).live_leases() == []                 # an expired index entry is skipped
    assert await runner.run_one(S.lease_call(TOKEN, "arm", "specA", 0, "")) == ScriptOk(0)
    assert await md_redis.hget(K.LEASE_INDEX, "arm:specA") is None


async def test_index_needs_the_token_and_takes_ids_only(md_redis: Any, runner: ScriptRunner) -> None:
    fields = [SERIES.canonical, K.market_field("mock", "SYN1")]
    assert await runner.run_one(S.index_call(TOKEN, fields)) == ScriptOk(2)
    assert await StoreReader(md_redis).stored_index() == {f: "1" for f in fields}
    stale = K.WriterToken(TOKEN.cluster_id, 6, "node-b", "old")
    assert await runner.run_one(S.index_call(stale, fields)) == ScriptRefused("NO_WRITER")
    with pytest.raises(ValueError):
        S.index_call(TOKEN, [])
    with pytest.raises(ValueError):
        S.index_call(TOKEN, ["no-colon"])


async def test_reader_pages_ranges_and_newest(md_redis: Any, runner: ScriptRunner) -> None:
    bars = [bar(T0 + k * 900_000) for k in range(4)]
    flush = S.flush_call(TOKEN, SENTINEL, SERIES.adapter_id, SERIES.symbol, T0 + 5 * 900_000, [],
                         [S.SeriesFlush(SERIES, closed=tuple(S.ClosedBarWrite(b, hash_of(b)) for b in bars))],
                         rollback_n=8)
    assert isinstance((await runner.run([flush]))[0], ScriptOk)
    reader = StoreReader(md_redis)
    assert [s for s, _ in await reader.bars_range(SERIES, T0 + 900_000, T0 + 2 * 900_000)] == [
        T0 + 900_000, T0 + 2 * 900_000]
    assert [s for s, _ in await reader.bars_newest(SERIES, 2)] == [T0 + 2 * 900_000, T0 + 3 * 900_000]
    assert [s for s, _ in await reader.bars_page(SERIES, T0 + 1, 2)] == [T0 + 900_000, T0 + 2 * 900_000]
    assert [e.write_seq for e in await reader.rollback(SERIES)] == [1, 2, 3, 4]
    metas = await reader.metas([SERIES, TICK_1M])
    assert metas[SERIES]["write_seq"] == "4" and metas[TICK_1M] == {}
    head = await reader.head()
    assert head.boot is not None and head.writer == TOKEN.text


async def test_reader_errors_are_store_unavailable() -> None:
    class Broken:
        async def get(self, key: str) -> Any:
            raise ConnectionError("down")

    with pytest.raises(StoreUnavailable) as e:
        await StoreReader(Broken()).boot()                                  # type: ignore[arg-type]
    assert e.value.kind == "connection"


def test_reader_never_names_keys_or_scan() -> None:
    source = inspect.getsource(R)
    for banned in (".keys(", ".scan(", "scan_iter", "KEYS", "SCAN"):
        assert banned not in source.replace("never `KEYS` / `SCAN`", "")


async def test_the_lease_script_refuses_a_malformed_index_field(md_redis: Any, runner: ScriptRunner) -> None:
    call = S.lease_call(TOKEN, "arm", "specA", 30_000, "ARMED")
    bad = S.ScriptCall(call.script, call.keys, (*call.args[:-1], "bad field"))
    reply = await runner.run_one(bad)
    assert reply == ScriptRefused("BAD_ARGS")
    assert await md_redis.exists(K.demand("arm", "specA")) == 0
