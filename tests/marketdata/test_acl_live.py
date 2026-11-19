"""The `redis-md` ACL users on a real Redis (design §A2; plan S13), modelled on
`tests/cache/test_dashboard_acl_live.py`. Skipped unless ORDER_SERVER_TEST_REDIS_MD names the DISPOSABLE dev
`redis-md` of `deploy/docker-compose.dev.yml` (profile `md`, e.g. `redis://127.0.0.1:56383/0`, no user in the URL):
the users and their throwaway dev-only passwords are read from that compose file.

It proves the rules work as written: redis-py's real connect callback marks the runner stale on every (re)connect;
`md_writer` runs every script path under the ACL (SCRIPT LOAD, EVALSHA through a non-transactional pipeline: a flush
followed by `WAITAOF 1 0 1500`, a second flush into the existing tick stream (`XINFO STREAM`), `LEASE` (SET PX),
`BOOT_LOCK` / `RENEW` (SET NX PX, PEXPIRE), `BOOT_WRITE`, `FINISH`, `QUARANTINE`, `RELEASE`), and cannot run ACL, CONFIG, FLUSHDB, FLUSHALL, KEYS,
SCAN, DEBUG, SHUTDOWN, REPLICAOF, MIGRATE, FUNCTION, SCRIPT FLUSH or EVAL nor touch a key outside `md:*`; `md_reader`
reads and cannot write; the default user is off; `md_health` can only PING."""
from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest
from redis.asyncio import Redis
from redis.exceptions import NoPermissionError, RedisError, ResponseError

from src.marketdata.codec import encode_member
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.client import MdWriteClient, build_reader
from src.marketdata.store.runner import ScriptOk, ScriptReply
from src.marketdata.types import MdGeneration

from .conftest import SERIES, T0, bar, hash_of

URL = os.environ.get("ORDER_SERVER_TEST_REDIS_MD")
pytestmark = [pytest.mark.md, pytest.mark.skipif(not URL, reason="ORDER_SERVER_TEST_REDIS_MD not set")]
COMPOSE = Path(__file__).resolve().parents[2] / "deploy" / "docker-compose.dev.yml"


def _password(user: str) -> str:
    text = COMPOSE.read_text(encoding="utf-8")
    m = re.search(rf'-\s*{user}\s*\n\s*-\s*"on"\s*\n\s*-\s*">([^"]+)"', text)
    assert m, f"no dev-only password for {user} in the dev compose file"
    return m.group(1)


def _ok(reply: ScriptReply) -> object:
    assert isinstance(reply, ScriptOk), reply
    return reply.value


def _url(user: str | None) -> str:
    assert URL is not None
    parts = urlsplit(URL)
    host = parts.hostname or "127.0.0.1"
    netloc = (f"{user}:{_password(user)}@" if user else "") + host + (f":{parts.port}" if parts.port else "")
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, ""))


async def test_md_writer_flushes_with_waitaof_and_is_refused_everything_else() -> None:
    writer = MdWriteClient.connect(_url("md_writer"))
    token = K.WriterToken("1", 1, "acltest", "nonce")
    b, b2 = bar(T0), bar(T0 + 900_000)
    try:
        assert writer.runner.connects == 0
        await writer.install_token(token)
        assert writer.runner.connects >= 1, "redis-py's connect callback did not fire"
        await writer.runner.on_connect()
        assert writer.runner.ready
        await writer.redis.connection_pool.disconnect()   # the next checkout reconnects: the callback fires again
        await writer.read_boot()
        assert not writer.runner.ready, "a reconnect did not mark the runner stale"
        await writer.runner.on_connect()
        run_id = writer.runner.run_id
        assert run_id
        sentinel = K.BootSentinel(run_id, "1", "acltest", MdGeneration(1, 1))
        await writer.redis.set(K.BOOT, sentinel.text)
        call = S.flush_call(token, sentinel, SERIES.adapter_id, SERIES.symbol, T0 + 960_000,
                            [S.TickWrite(T0 + 5, "1.2345", "1.2347", T0, "BROKER", "TRADEABLE")],
                            [S.SeriesFlush(SERIES, closed=(S.ClosedBarWrite(b, hash_of(b)),))], rollback_n=8)
        result = await writer.write_and_wait([call])
        assert isinstance(result.replies[0], ScriptOk)
        assert result.aof_local is not None and result.aof_local >= 1, "WAITAOF did not confirm the flush"
        second = S.flush_call(token, sentinel, SERIES.adapter_id, SERIES.symbol, T0 + 1_860_000,
                              [S.TickWrite(T0 + 900_005, "1.2346", "1.2348", T0 + 900_000, "BROKER", "TRADEABLE")],
                              [S.SeriesFlush(SERIES, closed=(S.ClosedBarWrite(b2, hash_of(b2)),))], rollback_n=8)
        parsed = S.parse_flush_reply(_ok((await writer.run([second]))[0]))   # the tick stream exists: XINFO STREAM
        assert parsed.ticks_added == 1 and parsed.bars == ((S.CLOSED, 2),)
        assert _ok(await writer.runner.run_one(S.lease_call(token, "acltest", "spec1", 30_000, "ARMED",
                                                            registry_field="spec1",
                                                            registry_value=SERIES.canonical))) == 1
        assert _ok(await writer.runner.run_one(S.boot_lock_call(token, "lockA"))) == 1
        assert _ok(await writer.runner.run_one(S.renew_call(token, "lockA"))) == 1
        lost = S.rollback_member(5, b2.bucket_end_ms + 900_000, encode_member(bar(T0 + 1_800_000),
                                                                             hash_of(bar(T0 + 1_800_000))))
        new = K.BootSentinel(run_id, "1", "acltest", MdGeneration(1, 2))
        replies = await writer.run_sequential([
            S.boot_write_call(token, "lockA", [S.SeriesBoot(SERIES, meta=(("first_bucket", str(T0)),),
                                                            entries=((5, lost),))],
                              rollback_n=8, registry=((SERIES.canonical, "acltest"),)),
            S.finish_call(token, "lockA", new, "store_lost")])
        assert [type(x) for x in replies] == [ScriptOk, ScriptOk]
        quarantine = S.quarantine_call(token, new, SERIES, T0 + 2_000_000,
                                       [S.QuarantineEntry(b, "acltest", encode_member(b, hash_of(b)))])
        assert _ok(await writer.runner.run_one(quarantine)) == 1
        assert _ok(await writer.runner.run_one(S.boot_lock_call(token, "lockB"))) == 1
        assert _ok(await writer.runner.run_one(S.release_call("lockB"))) == 1
        r = writer.redis
        for refused in (lambda: r.execute_command("ACL", "LIST"), lambda: r.execute_command("CONFIG", "GET", "*"),
                        lambda: r.flushdb(), lambda: r.flushall(), lambda: r.keys("md:*"), lambda: r.scan(0),
                        lambda: r.execute_command("DEBUG", "SLEEP", "0"),
                        lambda: r.execute_command("SHUTDOWN", "NOSAVE"),
                        lambda: r.execute_command("REPLICAOF", "NO", "ONE"),
                        lambda: r.execute_command("MIGRATE", "127.0.0.1", "1", "md:x", "0", "10"),
                        lambda: r.execute_command("FUNCTION", "LIST"), lambda: r.execute_command("SCRIPT", "FLUSH"),
                        lambda: r.eval("return 1", 0), lambda: r.set("other:key", "x"),
                        lambda: r.get("halt:state"), lambda: r.execute_command("MULTI")):
            with pytest.raises((NoPermissionError, ResponseError)):
                await refused()
    finally:
        try:
            await writer.redis.delete(K.WRITER, K.BOOT, K.BOOT_LOCK, K.EVENTS, K.REGISTRY_SERIES,
                                      K.ticks(SERIES.adapter_id, SERIES.symbol), K.demand("acltest", "spec1"),
                                      K.quarantine(SERIES), *K.series_keys(SERIES))
        finally:
            await writer.aclose(2.0)


async def test_md_reader_reads_and_cannot_write_and_the_default_user_is_off() -> None:
    reader: Redis = build_reader(_url("md_reader"))
    anonymous: Redis = build_reader(_url(None))
    health: Redis = build_reader(_url(None).replace("://", "://md_health:unused@", 1))
    try:
        assert await reader.ping()
        assert await reader.zrange(K.bars(SERIES), 0, -1) == []
        for refused in (lambda: reader.set("md:x", "1"), lambda: reader.hset("md:meta:x", "a", "1"),
                        lambda: reader.zadd("md:bars:x", {"m": 1}), lambda: reader.xadd("md:events", {"k": "v"}),
                        lambda: reader.delete("md:x"), lambda: reader.keys("*"), lambda: reader.scan(0),
                        lambda: reader.eval("return 1", 0), lambda: reader.get("halt:state")):
            with pytest.raises((NoPermissionError, ResponseError)):
                await refused()
        with pytest.raises(RedisError):
            await anonymous.ping()
        assert await health.ping()
        with pytest.raises((NoPermissionError, ResponseError)):
            await health.get("md:boot")
    finally:
        for c in (reader, anonymous, health):
            await c.aclose(close_connection_pool=True)
