"""The `redis-md` write scripts and the script runner on fakeredis + lupa (plan S13; design §A3, §A7, §A8, §A13).

Synthetic instrument `mock:SYN1`; no price appears in an assertion message."""
from __future__ import annotations

import re
from dataclasses import replace
from typing import Any

import fakeredis
import pytest

from src.marketdata.codec import DecodedMember, decode_member, encode_member
from src.marketdata.errors import redis_error_code, safe_redis_error
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.runner import ScriptOk, ScriptRefused, ScriptRunner, StoreUnavailable, unavailable
from src.marketdata.types import M15, MdGeneration, SeriesId, Side, Source

from .conftest import RUN_ID, SENTINEL, SERIES, T0, TOKEN, RunIds, bar, hash_of

ADAPTER, SYMBOL = SERIES.adapter_id, SERIES.symbol


def _ticks(*received: int) -> list[S.TickWrite]:
    return [S.TickWrite(r, "1.2345", "1.2347", r - 40, "BROKER", "TRADEABLE") for r in received]


def _closed(*bars_: Any, silent: bool = False) -> S.SeriesFlush:
    return S.SeriesFlush(SERIES, closed=tuple(S.ClosedBarWrite(b, hash_of(b), silent) for b in bars_))


def _flush(series: list[S.SeriesFlush], ticks: list[S.TickWrite] | None = None, *, now: int = T0 + 960_000,
           rollback_n: int = 8, token: K.WriterToken = TOKEN, boot: K.BootSentinel | str = SENTINEL) -> S.ScriptCall:
    return S.flush_call(token, boot, ADAPTER, SYMBOL, now, ticks or [], series, rollback_n=rollback_n)


async def _snapshot(r: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in sorted(await r.keys("md:*")):
        kind = (await r.type(key)).decode()
        if kind == "string":
            out[key.decode()] = await r.get(key)
        elif kind == "hash":
            out[key.decode()] = await r.hgetall(key)
        elif kind == "zset":
            out[key.decode()] = await r.zrange(key, 0, -1, withscores=True)
        elif kind == "stream":
            out[key.decode()] = await r.xrange(key)
    return out


# ---------------------------------------------------------------------------------------------------- flush
async def test_flush_writes_ticks_forming_closed_bar_rollback_meta_and_event(md_redis: Any,
                                                                           runner: ScriptRunner) -> None:
    b = bar(T0)
    forming = replace(bar(T0 + 900_000), final_at_ms=None)
    call = _flush([S.SeriesFlush(SERIES, forming=S.forming_write(forming), closed=(S.ClosedBarWrite(b, hash_of(b)),))],
                  _ticks(T0 + 5, T0 + 5, T0 + 3))
    (reply,) = await runner.run([call])
    assert isinstance(reply, ScriptOk)
    parsed = S.parse_flush_reply(reply.value)
    assert (parsed.ticks_added, parsed.backward_steps) == (3, 1)
    assert parsed.bars == ((S.CLOSED, 1),)
    ids = [i.decode() for i, _ in await md_redis.xrange(K.ticks(ADAPTER, SYMBOL))]
    assert ids == [f"{T0 + 5}-0", f"{T0 + 5}-1", f"{T0 + 5}-2"]   # a backward step uses last id + 1
    (member, score), = await md_redis.zrange(K.bars(SERIES), 0, -1, withscores=True)
    assert score == T0
    decoded = decode_member(SERIES, member)
    assert isinstance(decoded, DecodedMember) and decoded.bar == b
    assert (await md_redis.hget(K.forming(SERIES), "start")).decode() == str(forming.bucket_start_ms)
    meta = await md_redis.hgetall(K.meta(SERIES))
    assert meta[b"write_seq"] == b"1" and meta[b"first_bucket"] == meta[b"last_bucket"] == str(T0).encode()
    assert meta[b"last_write_ms"] == str(T0 + 960_000).encode()
    (rb_member, ws), = await md_redis.zrange(K.rollback(SERIES), 0, -1, withscores=True)
    assert ws == 1 and rb_member.decode() == S.rollback_member(1, b.bucket_end_ms, encode_member(b, hash_of(b)))
    (_, fields), = await md_redis.xrange(K.EVENTS)
    assert fields == {b"k": b"bar_closed", b"s": SERIES.canonical.encode(), b"e": str(b.bucket_end_ms).encode(),
                      b"h": hash_of(b).encode(), b"w": b"1"}   # wake-up only: id and hash, never a price


async def test_identical_rewrite_is_idempotent(md_redis: Any, runner: ScriptRunner) -> None:
    b = bar(T0)
    await runner.run([_flush([_closed(b)])])
    before = await _snapshot(md_redis)
    (reply,) = await runner.run([_flush([_closed(b)])])
    assert isinstance(reply, ScriptOk) and S.parse_flush_reply(reply.value).bars == ((S.UNCHANGED, 0),)
    after = await _snapshot(md_redis)
    assert after == before   # same member: no event, no rollback entry, no write_seq (only last_write_ms is equal)


async def test_revision_of_an_acknowledged_bar_gets_a_new_higher_write_seq(md_redis: Any,
                                                                          runner: ScriptRunner) -> None:
    first, second = bar(T0, "1.2345"), bar(T0, "1.2399", revision=1)
    await runner.run([_flush([_closed(first)])])
    (reply,) = await runner.run([_flush([_closed(second)], now=T0 + 970_000)])
    assert isinstance(reply, ScriptOk) and S.parse_flush_reply(reply.value).bars == ((S.REVISED, 2),)
    members = await md_redis.zrange(K.bars(SERIES), 0, -1)
    assert members == [encode_member(second, hash_of(second)).encode()]
    (old, score), = await md_redis.zrange(K.bars_rev(SERIES), 0, -1, withscores=True)
    assert score == T0 and old.decode() == f"{encode_member(first, hash_of(first))}|{T0 + 970_000:013d}"
    rb = await md_redis.zrange(K.rollback(SERIES), 0, -1, withscores=True)
    assert [s for _, s in rb] == [1, 2]   # the same id twice, distinct entries, rising write_seq
    kinds = [f[b"k"] for _, f in await md_redis.xrange(K.EVENTS)]
    assert kinds == [b"bar_closed", b"bar_revised"]


async def test_rollback_set_holds_n_with_a_monotonic_write_seq(md_redis: Any, runner: ScriptRunner) -> None:
    for k in range(6):
        await runner.run([_flush([_closed(bar(T0 + k * 900_000))], rollback_n=3)])
    rb = await md_redis.zrange(K.rollback(SERIES), 0, -1, withscores=True)
    assert [s for _, s in rb] == [4, 5, 6]
    assert [m.decode()[:20] for m, _ in rb] == [f"{s:020d}" for s in (4, 5, 6)]
    assert await md_redis.hget(K.meta(SERIES), "write_seq") == b"6"
    assert await md_redis.zcard(K.bars(SERIES)) == 6


async def test_silent_rewrite_adds_no_event_and_no_rollback_entry(md_redis: Any, runner: ScriptRunner) -> None:
    b = bar(T0)
    await runner.run([_flush([_closed(b)])])
    rehashed = S.ClosedBarWrite(b, "04" + hash_of(b)[2:], silent=True)   # another key id: an md_rehash re-write
    (reply,) = await runner.run([_flush([S.SeriesFlush(SERIES, closed=(rehashed,))])])
    assert isinstance(reply, ScriptOk) and S.parse_flush_reply(reply.value).bars == ((S.SILENT, 0),)
    assert await md_redis.zcard(K.rollback(SERIES)) == 1 and await md_redis.xlen(K.EVENTS) == 1


async def test_flush_is_atomic_when_a_later_argument_is_bad(md_redis: Any, runner: ScriptRunner) -> None:
    before = await _snapshot(md_redis)
    good = _flush([_closed(bar(T0), bar(T0 + 900_000))], _ticks(T0 + 1, T0 + 2))
    args = list(good.args)
    args[-3] = "zz" * 17   # the second bar's hash: not hex
    (reply,) = await runner.run([S.ScriptCall(good.script, good.keys, tuple(args))])
    assert reply == ScriptRefused("BAD_ARGS")
    assert await _snapshot(md_redis) == before   # no tick, no bar, no meta: checks run before the first write


async def test_an_ex_leader_token_is_refused(md_redis: Any, runner: ScriptRunner) -> None:
    await md_redis.set(K.WRITER, K.WriterToken("1234567890", 8, "node-b", "other").text)   # takeover
    before = await _snapshot(md_redis)
    (reply,) = await runner.run([_flush([_closed(bar(T0))], _ticks(T0 + 1))])
    assert reply == ScriptRefused("NO_WRITER")
    assert await _snapshot(md_redis) == before


@pytest.mark.parametrize("stored", [
    None,                                                                             # missing (FLUSHALL)
    K.BootSentinel(RUN_ID, "1234567890", "node-a", MdGeneration(7, 0)).text,          # older (older AOF)
    K.BootSentinel(RUN_ID, "1234567890", "node-b", MdGeneration(7, 1)).text,          # foreign (other node)
    K.BootSentinel(RUN_ID, "1234567890", "node-a", MdGeneration(8, 1)).text,          # newer term
])
async def test_no_boot_on_a_missing_older_or_foreign_sentinel(md_redis: Any, runner: ScriptRunner,
                                                              stored: str | None) -> None:
    if stored is None:
        await md_redis.delete(K.BOOT)
    else:
        await md_redis.set(K.BOOT, stored)
    before = await _snapshot(md_redis)
    (reply,) = await runner.run([_flush([_closed(bar(T0))], _ticks(T0 + 1))])
    assert reply == ScriptRefused("NO_BOOT")
    assert await _snapshot(md_redis) == before


async def test_no_boot_on_a_run_id_mismatch_after_a_restart_with_the_aof_reloaded(
        md_redis: Any, runner: ScriptRunner, run_ids: RunIds) -> None:
    (ok,) = await runner.run([_flush([_closed(bar(T0))])])
    assert isinstance(ok, ScriptOk)
    run_ids.value = "f00dfeed99"     # Redis restarted: md:boot came back from the AOF unchanged, the run_id did not
    runner.mark_reconnected()        # the client's connect callback
    (reply,) = await runner.run([_flush([_closed(bar(T0 + 900_000))])])
    assert reply == ScriptRefused("NO_BOOT") and runner.run_id == "f00dfeed99"
    assert await md_redis.zcard(K.bars(SERIES)) == 1


async def test_tick_ids_stay_monotonic_after_the_stream_was_trimmed_empty(md_redis: Any,
                                                                         runner: ScriptRunner) -> None:
    await runner.run([_flush([], _ticks(T0 + 50))])
    await md_redis.xtrim(K.ticks(ADAPTER, SYMBOL), minid=f"{T0 + 100}-0")
    (reply,) = await runner.run([_flush([], _ticks(T0 + 10))])
    assert isinstance(reply, ScriptOk) and S.parse_flush_reply(reply.value).backward_steps == 1
    assert [i for i, _ in await md_redis.xrange(K.ticks(ADAPTER, SYMBOL))] == [f"{T0 + 50}-1".encode()]


async def test_flush_refuses_series_of_another_symbol() -> None:
    other = SeriesId("mock", "SYN2", M15, Side.BID, Source.BROKER)
    with pytest.raises(ValueError):
        S.flush_call(TOKEN, SENTINEL, ADAPTER, SYMBOL, T0, [], [S.SeriesFlush(other)], rollback_n=8)
    with pytest.raises(ValueError):
        S.flush_call(TOKEN, SENTINEL, ADAPTER, SYMBOL, T0, [], [S.SeriesFlush(SERIES, meta=(("write_seq", "9"),))],
                     rollback_n=8)


# ---------------------------------------------------------------------------------------------------- lease
async def test_lease_writes_need_only_the_token(md_redis: Any, runner: ScriptRunner) -> None:
    await md_redis.delete(K.BOOT)   # NO_BOOT for ordinary writes, but a lease re-assertion lands at once
    reply = await runner.run_one(S.lease_call(TOKEN, "arm", "spec1", 30_000, "ARMED", registry_field="spec1",
                                              registry_value=SERIES.canonical))
    assert reply == ScriptOk(1)
    assert await md_redis.get(K.demand("arm", "spec1")) == b"ARMED"
    assert 0 < await md_redis.pttl(K.demand("arm", "spec1")) <= 30_000
    assert await md_redis.hget(K.REGISTRY_SERIES, "spec1") == SERIES.canonical.encode()
    assert await runner.run_one(S.lease_call(TOKEN, "arm", "spec1", 0, "")) == ScriptOk(0)
    assert await md_redis.exists(K.demand("arm", "spec1")) == 0
    stale = K.WriterToken("1234567890", 6, "node-b", "old")
    assert await runner.run_one(S.lease_call(stale, "arm", "spec1", 30_000, "ARMED")) == ScriptRefused("NO_WRITER")
    assert await md_redis.exists(K.demand("arm", "spec1")) == 0


# ---------------------------------------------------------------------------------------------------- bootstrap
async def test_boot_lock_renew_release(md_redis: Any, runner: ScriptRunner) -> None:
    assert await runner.run_one(S.boot_lock_call(TOKEN, "lockA")) == ScriptOk(1)
    assert await runner.run_one(S.boot_lock_call(TOKEN, "lockB")) == ScriptOk(0)
    assert 0 < await md_redis.pttl(K.BOOT_LOCK) <= K.BOOT_LOCK_PX
    assert await runner.run_one(S.renew_call(TOKEN, "lockA")) == ScriptOk(1)
    assert await runner.run_one(S.renew_call(TOKEN, "lockB")) == ScriptRefused("NO_LOCK")
    assert await runner.run_one(S.release_call("lockB")) == ScriptOk(0)
    assert await runner.run_one(S.release_call("lockA")) == ScriptOk(1)
    stale = K.WriterToken("1234567890", 6, "node-b", "old")
    assert await runner.run_one(S.boot_lock_call(stale, "lockC")) == ScriptRefused("NO_WRITER")
    assert await md_redis.exists(K.BOOT_LOCK) == 0


async def test_boot_write_and_finish_under_the_lock(md_redis: Any, runner: ScriptRunner) -> None:
    b = bar(T0)
    await runner.run([_flush([S.SeriesFlush(SERIES, forming=S.forming_write(bar(T0 + 900_000)),
                                            closed=(S.ClosedBarWrite(b, hash_of(b)),))])])
    await md_redis.delete(K.BOOT)   # e.g. FLUSHALL then the token re-SET: the bootstrap runs without md:boot
    lost = S.rollback_member(5, b.bucket_end_ms + 900_000, encode_member(bar(T0 + 900_000), hash_of(bar(T0 + 900_000))))
    boot_write = S.boot_write_call(TOKEN, "lockA", [S.SeriesBoot(SERIES, meta=(("sm12", "GAPPED"),),
                                                                 entries=((5, lost),))],
                                   rollback_n=8, registry=((SERIES.canonical, "BROKER 15m"),))
    assert await runner.run_one(boot_write) == ScriptRefused("NO_LOCK")   # step 2 first
    assert await runner.run_one(S.boot_lock_call(TOKEN, "lockA")) == ScriptOk(1)
    assert await runner.run_one(boot_write) == ScriptOk(b"OK")
    assert await md_redis.exists(K.forming(SERIES)) == 0
    meta = await md_redis.hgetall(K.meta(SERIES))
    assert meta[b"sm12"] == b"GAPPED" and meta[b"write_seq"] == b"5"   # later entries rise above the restored one
    assert [s for _, s in await md_redis.zrange(K.rollback(SERIES), 0, -1, withscores=True)] == [1, 5]
    other = bar(T0, "1.2399")
    clash = S.rollback_member(1, other.bucket_end_ms, encode_member(other, hash_of(other)))   # ws 1 is held
    conflict = S.boot_write_call(TOKEN, "lockA", [S.SeriesBoot(SERIES, entries=((1, clash),))], rollback_n=8)
    before = await _snapshot(md_redis)
    assert await runner.run_one(conflict) == ScriptRefused("STALE_SEQ")
    assert await _snapshot(md_redis) == before
    new = K.BootSentinel(RUN_ID, "1234567890", "node-a", MdGeneration(7, 2))
    stale = K.WriterToken("1234567890", 6, "node-b", "old")
    assert await runner.run_one(S.finish_call(stale, "lockA", new, "store_lost")) == ScriptRefused("NO_WRITER")
    assert await md_redis.get(K.BOOT) is None
    assert await runner.run_one(S.finish_call(TOKEN, "lockA", new, "store_lost")) == ScriptOk(b"OK")
    assert await md_redis.get(K.BOOT) == new.text.encode() and await md_redis.exists(K.BOOT_LOCK) == 0
    (_, ev), = [e for e in await md_redis.xrange(K.EVENTS) if e[1][b"k"] == b"md_reset"]
    assert ev == {b"k": b"md_reset", b"g": b"7.2", b"c": b"store_lost"}
    flush = _flush([_closed(bar(T0 + 1_800_000))], boot=new)
    (reply,) = await runner.run([flush])
    assert isinstance(reply, ScriptOk) and S.parse_flush_reply(reply.value).bars == ((S.CLOSED, 6),)


# ---------------------------------------------------------------------------------------------------- quarantine
async def test_quarantine_moves_a_member_out_of_md_bars(md_redis: Any, runner: ScriptRunner) -> None:
    b = bar(T0)
    await runner.run([_flush([_closed(b)])])
    member = encode_member(b, hash_of(b))
    call = S.quarantine_call(TOKEN, SENTINEL, SERIES, T0 + 999_000, [S.QuarantineEntry(b, "hash", member)])
    assert await runner.run_one(call) == ScriptOk(1)
    assert await md_redis.zcard(K.bars(SERIES)) == 0
    (q, score), = await md_redis.zrange(K.quarantine(SERIES), 0, -1, withscores=True)
    assert score == T0 and q.decode().split("|")[1:3] == ["hash", f"{T0:013d}"]
    assert await md_redis.hget(K.meta(SERIES), "quarantined") == b"1"
    await md_redis.delete(K.BOOT)
    assert await runner.run_one(call) == ScriptRefused("NO_BOOT")


# ---------------------------------------------------------------------------------------------------- runner
class _SpyRedis(fakeredis.FakeAsyncRedis):  # type: ignore[misc]
    pipelines: list[bool] = []

    def pipeline(self, transaction: bool = True, shard_hint: Any = None) -> Any:
        type(self).pipelines.append(transaction)
        return super().pipeline(transaction=transaction, shard_hint=shard_hint)

    async def eval(self, *args: Any, **kwargs: Any) -> Any:   # never used: EVALSHA only
        raise AssertionError("EVAL sent")


async def test_evalsha_through_a_non_transactional_pipeline_and_noscript_reload() -> None:
    r = _SpyRedis()
    run_ids = RunIds()
    await r.set(K.WRITER, TOKEN.text)
    await r.set(K.BOOT, SENTINEL.text)
    runner = ScriptRunner(r, run_id_reader=run_ids)
    try:
        replies = await runner.run([_flush([_closed(bar(T0))]), S.lease_call(TOKEN, "arm", "s1", 1000, "ARMED")])
        assert [type(x) for x in replies] == [ScriptOk, ScriptOk]
        assert _SpyRedis.pipelines and not any(_SpyRedis.pipelines)
        assert runner.hooks_run == 1 and run_ids.reads == 1
        await r.script_flush()   # what a restarted Redis looks like to EVALSHA
        (reply,) = await runner.run([_flush([_closed(bar(T0 + 900_000))])])
        assert isinstance(reply, ScriptOk) and S.parse_flush_reply(reply.value).bars == ((S.CLOSED, 2),)
        assert runner.hooks_run == 2 and run_ids.reads == 2   # SCRIPT LOAD and run_id in the same hook
    finally:
        await r.aclose()


async def test_a_connection_error_raises_store_unavailable_and_reruns_the_hook() -> None:
    server = fakeredis.FakeServer()
    r = fakeredis.FakeAsyncRedis(server=server)
    await r.set(K.WRITER, TOKEN.text)
    await r.set(K.BOOT, SENTINEL.text)
    runner = ScriptRunner(r, run_id_reader=RunIds())
    try:
        await runner.run([_flush([_closed(bar(T0))])])
        server.connected = False
        with pytest.raises(StoreUnavailable) as err:
            await runner.run([_flush([_closed(bar(T0 + 900_000))])])
        assert err.value.kind == "connection" and not runner.ready
        server.connected = True
        (reply,) = await runner.run([_flush([_closed(bar(T0 + 900_000))])])
        assert isinstance(reply, ScriptOk) and runner.hooks_run == 2
    finally:
        await r.aclose()


def test_store_errors_never_carry_message_text() -> None:
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import ResponseError
    from redis.exceptions import TimeoutError as RedisTimeoutError
    e = unavailable(RedisConnectionError("Error connecting to redis://md_writer:s3cret@10.0.0.9:6381"))
    assert e.kind == "connection" and "s3cret" not in str(e) and "10.0.0.9" not in str(e)
    assert unavailable(RedisTimeoutError("Timeout reading from md:bars:x")).kind == "timeout"
    refusal = ResponseError("BAD_ARGS closed")
    assert redis_error_code(refusal) == "BAD_ARGS" and safe_redis_error(refusal) == "ResponseError(BAD_ARGS)"
    assert redis_error_code(ValueError("NO_BOOT")) is None


def test_scripts_are_constants_and_never_scan_or_name_keys_dynamically() -> None:
    for script in S.SCRIPTS:
        assert isinstance(script.source, str) and len(script.sha) == 40
        for banned in ("'KEYS'", "'SCAN'", "'FLUSHDB'", "'FLUSHALL'", "'EVAL'", "'CONFIG'", "'INFO'"):
            assert banned not in script.source, (script.name, banned)
        # every key a script writes comes from KEYS (no key text is built inside Lua)
        for m in re.finditer(r"redis\.call\('([A-Z]+)', ([^,)]+)", script.source):
            assert m.group(2).startswith(("KEYS[", "key", "'STREAM'", "bars_key", "rev_key", "forming_key", "meta_key", "rb_key")), \
                (script.name, m.group(0))
    assert {"ZREMRANGEBYSCORE", "HINCRBY", "ZREMRANGEBYRANK", "XADD"} <= set(re.findall(r"'([A-Z]+)'", S.FLUSH))
    assert S.SCRIPT_CODES <= {"NO_BOOT", "NO_WRITER", "NO_LOCK", "BAD_ARGS", "STALE_SEQ"}


# ---------------------------------------------------------------------------------------------------- refused before any write
@pytest.mark.parametrize("field_", ["first_bucket", "last_bucket", "write_seq"])
async def test_flush_refuses_a_corrupted_meta_field_and_changes_nothing(md_redis: Any, runner: ScriptRunner,
                                                                       field_: str) -> None:
    await md_redis.hset(K.meta(SERIES), field_, "x")
    before = await _snapshot(md_redis)
    (reply,) = await runner.run([_flush([_closed(bar(T0))], _ticks(T0 + 5))])
    assert reply == ScriptRefused("BAD_ARGS")
    assert await _snapshot(md_redis) == before   # no tick, no bar: a retry cannot see an UNCHANGED half-write


async def test_boot_write_refuses_non_numeric_meta_numbers(md_redis: Any, runner: ScriptRunner) -> None:
    assert await runner.run_one(S.boot_lock_call(TOKEN, "lockA")) == ScriptOk(1)
    good = S.boot_write_call(TOKEN, "lockA", [S.SeriesBoot(SERIES, meta=(("first_bucket", str(T0)),))], rollback_n=8)
    for name in ("first_bucket", "last_bucket", "last_write_ms", "quarantined"):
        with pytest.raises(ValueError):
            S.boot_write_call(TOKEN, "lockA", [S.SeriesBoot(SERIES, meta=((name, "x"),))], rollback_n=8)
        args = list(good.args)
        args[args.index("first_bucket")] = name
        args[args.index(str(T0))] = "x"
        before = await _snapshot(md_redis)
        assert await runner.run_one(S.ScriptCall(good.script, good.keys, tuple(args))) == ScriptRefused("BAD_ARGS")
        assert await _snapshot(md_redis) == before
    assert await runner.run_one(good) == ScriptOk(b"OK")


async def test_header_numbers_are_bounded(md_redis: Any, runner: ScriptRunner) -> None:
    for n in (0, 65):
        with pytest.raises(ValueError):
            _flush([_closed(bar(T0))], rollback_n=n)
        with pytest.raises(ValueError):
            S.boot_write_call(TOKEN, "lockA", [], rollback_n=n)
    good = _flush([_closed(bar(T0))], _ticks(T0 + 5))
    before = await _snapshot(md_redis)
    for index, value in ((5, "1000"), (4, "1" * 14), (6, "1" * 14), (3, "1" * 14)):   # rb_n, maxlen, rev_keep, now
        args = list(good.args)
        args[index] = value
        assert await runner.run_one(S.ScriptCall(good.script, good.keys, tuple(args))) == ScriptRefused("BAD_ARGS")
    assert await _snapshot(md_redis) == before


async def test_quarantined_is_reserved_and_a_corrupted_count_refuses_quarantine(md_redis: Any,
                                                                               runner: ScriptRunner) -> None:
    with pytest.raises(ValueError):
        _flush([S.SeriesFlush(SERIES, meta=(("quarantined", "n/a"),))])
    good = _flush([S.SeriesFlush(SERIES, meta=(("sm12", "OK"),))])
    args = list(good.args)
    args[args.index("sm12")] = "quarantined"
    assert await runner.run_one(S.ScriptCall(good.script, good.keys, tuple(args))) == ScriptRefused("BAD_ARGS")
    b = bar(T0)
    await runner.run([_flush([_closed(b)])])
    await md_redis.hset(K.meta(SERIES), "quarantined", "n/a")   # corrupted by something outside the scripts
    before = await _snapshot(md_redis)
    call = S.quarantine_call(TOKEN, SENTINEL, SERIES, T0 + 999_000, [S.QuarantineEntry(b, "hash", encode_member(b, hash_of(b)))])
    assert await runner.run_one(call) == ScriptRefused("BAD_ARGS")
    assert await _snapshot(md_redis) == before   # the bar is still in md:bars, nothing quarantined


async def test_finish_refuses_a_sentinel_built_before_a_redis_restart(md_redis: Any, runner: ScriptRunner,
                                                                      run_ids: RunIds) -> None:
    assert await runner.run_one(S.boot_lock_call(TOKEN, "lockA")) == ScriptOk(1)
    stale = K.BootSentinel(RUN_ID, "1234567890", "node-a", MdGeneration(7, 2))   # built with the old run_id
    run_ids.value = "f00dfeed99"     # Redis restarted between BOOT_LOCK and FINISH
    runner.mark_reconnected()
    await md_redis.set(K.BOOT_LOCK, "lockA")   # (a real restart reloads it from the AOF)
    before = await _snapshot(md_redis)
    assert await runner.run_one(S.finish_call(TOKEN, "lockA", stale, "store_lost")) == ScriptRefused("NO_BOOT")
    assert await _snapshot(md_redis) == before   # md:boot unchanged, no md_reset, the lock still held
    fresh = K.BootSentinel("f00dfeed99", "1234567890", "node-a", MdGeneration(7, 2))
    assert await runner.run_one(S.finish_call(TOKEN, "lockA", fresh, "store_lost")) == ScriptOk(b"OK")
    assert await md_redis.get(K.BOOT) == fresh.text.encode()


async def test_boot_write_refuses_two_entries_with_one_write_seq_and_a_malformed_tail(md_redis: Any,
                                                                                     runner: ScriptRunner) -> None:
    assert await runner.run_one(S.boot_lock_call(TOKEN, "lockA")) == ScriptOk(1)
    b1, b2 = bar(T0), bar(T0 + 900_000)
    m1 = S.rollback_member(3, b1.bucket_end_ms, encode_member(b1, hash_of(b1)))
    m2 = S.rollback_member(3, b2.bucket_end_ms, encode_member(b2, hash_of(b2)))
    before = await _snapshot(md_redis)
    twice = S.boot_write_call(TOKEN, "lockA", [S.SeriesBoot(SERIES, entries=((3, m1), (3, m2)))], rollback_n=8)
    assert await runner.run_one(twice) == ScriptRefused("STALE_SEQ")
    broken = m1[:40] + "Z" + m1[41:]   # right length and prefix, not a bar member
    bad_tail = S.boot_write_call(TOKEN, "lockA", [S.SeriesBoot(SERIES, entries=((3, broken),))], rollback_n=8)
    assert await runner.run_one(bad_tail) == ScriptRefused("BAD_ARGS")
    assert await _snapshot(md_redis) == before


@pytest.mark.parametrize("price", ["-1.2345", "1e5", "", "1.2.3", " 1.2"])
def test_a_malformed_tick_price_is_refused_at_build_time(price: str) -> None:
    with pytest.raises(ValueError) as err:
        S.TickWrite(T0, price, "1.2347", T0, "BROKER", "TRADEABLE")
    assert price not in str(err.value) or not price
    with pytest.raises(ValueError):
        S.TickWrite(T0, "1.2345", "1.2347", T0, "BRO KER", "TRADEABLE")


async def test_run_sequential_stops_at_the_first_refusal(md_redis: Any, runner: ScriptRunner) -> None:
    b = bar(T0)
    await runner.run([_flush([_closed(b)])])
    assert await runner.run_one(S.boot_lock_call(TOKEN, "lockA")) == ScriptOk(1)
    other = bar(T0, "1.2399")
    clash = S.rollback_member(1, other.bucket_end_ms, encode_member(other, hash_of(other)))   # ws 1 is held
    new = K.BootSentinel(RUN_ID, "1234567890", "node-a", MdGeneration(7, 2))
    replies = await runner.run_sequential([
        S.boot_write_call(TOKEN, "lockA", [S.SeriesBoot(SERIES, entries=((1, clash),))], rollback_n=8),
        S.finish_call(TOKEN, "lockA", new, "store_lost")])
    assert replies == [ScriptRefused("STALE_SEQ")]   # FINISH was never sent
    assert await md_redis.get(K.BOOT) == SENTINEL.text.encode() and await md_redis.exists(K.BOOT_LOCK) == 1
