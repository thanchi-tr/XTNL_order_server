"""`redis-md` keys, the writer token and sentinel, and the two clients (plan S13; design §A2, §A7, §A8, ST-10)."""
from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from redis.asyncio import BlockingConnectionPool, ConnectionPool

from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.client import (MdClients, MdReadClient, MdWriteClient, RedisAofWaiter, build_reader,
                                         build_writer, close_within)
from src.marketdata.store.runner import ScriptOk, ScriptRunner, StoreUnavailable
from src.marketdata.types import MdGeneration

from .conftest import RUN_ID, SENTINEL, SERIES, T0, TOKEN, FakeAof, RunIds, bar, hash_of

DUMMY_URL = "redis://md_writer:not-a-real-password@127.0.0.1:1/0"


# ---------------------------------------------------------------------------------------------------- keys
def test_every_key_is_under_md_and_parts_are_key_safe() -> None:
    built = [K.ticks("mock", "SYN1"), *K.series_keys(SERIES), K.quarantine(SERIES), K.demand("arm", "spec1"),
             K.skew("mock"), K.indicator("abcd", "atr"), K.indicator_checkpoints("abcd"), K.WRITER, K.BOOT,
             K.BOOT_LOCK, K.EVENTS, K.REGISTRY_SERIES, K.REGISTRY_IND]
    assert all(k.startswith(K.PREFIX) for k in built)
    assert K.bars(SERIES) == "md:bars:mock:SYN1:15m:BID:BROKER" and K.rollback(SERIES).startswith("md:rb:")
    for bad in ("a:b", "a b", "a*", "{x}", ""):
        with pytest.raises(ValueError):
            K.demand("arm", bad)


def test_writer_token_and_boot_sentinel() -> None:
    assert TOKEN.text == "1234567890:7:node-a:n0nce1"
    assert SENTINEL.text == f"{RUN_ID}:1234567890:node-a:7.1"
    assert K.BootSentinel.parse(SENTINEL.text.encode()) == SENTINEL
    for broken in (None, b"", "x", "r:c:n:7", "r:c:n:7.x", b"\xff\xfe", "r!:c:n:1.1"):
        assert K.BootSentinel.parse(broken) is None
    with pytest.raises(ValueError):
        K.WriterToken("c:1", 1, "n", "x")
    with pytest.raises(ValueError):
        K.BootSentinel("run-id", "c", "n", MdGeneration(1, 1))
    assert K.valid_run_id("abc123") and not K.valid_run_id("") and not K.valid_run_id("a:b")


# ---------------------------------------------------------------------------------------------------- client config
async def test_reader_and_writer_are_built_with_the_design_timeouts_and_no_silent_retries() -> None:
    reader = build_reader(DUMMY_URL)
    ref: list[ScriptRunner] = []
    writer = build_writer(DUMMY_URL, ref)
    try:
        rpool, wpool = reader.connection_pool, writer.connection_pool
        assert isinstance(rpool, ConnectionPool) and rpool.max_connections == 4
        assert rpool.connection_kwargs["socket_timeout"] == 0.5
        assert rpool.connection_kwargs["socket_connect_timeout"] == 0.5
        assert isinstance(wpool, BlockingConnectionPool) and wpool.max_connections == 1   # one writer connection
        assert wpool.connection_kwargs["socket_timeout"] == 2.0
        for pool in (rpool, wpool):
            assert pool.connection_kwargs["retry"].get_retries() == 0
            assert pool.connection_kwargs["decode_responses"] is False
        runner = ScriptRunner(writer, run_id_reader=RunIds())
        ref.append(runner)
        runner_ready_before = runner.ready

        class _Conn:
            connected = 0

            async def on_connect(self) -> None:
                self.connected += 1

        conn = _Conn()
        await wpool.connection_kwargs["redis_connect_func"](conn)   # what redis-py calls on every (re)connect
        assert conn.connected == 1 and not runner.ready and not runner_ready_before
    finally:
        assert await close_within([reader, writer])


async def test_clients_connect_without_io_and_close_in_budget() -> None:
    clients = MdClients.connect(DUMMY_URL)
    assert isinstance(clients.reader, MdReadClient) and isinstance(clients.writer, MdWriteClient)
    assert "not-a-real-password" not in repr(clients)
    assert await clients.aclose(0.5)


async def test_a_hung_close_is_dropped_inside_the_budget() -> None:
    class _Hung:
        async def aclose(self, close_connection_pool: bool = True) -> None:
            await asyncio.sleep(30)

    start = time.monotonic()
    assert await close_within([_Hung()], 0.05) is False  # type: ignore[list-item]
    assert time.monotonic() - start < 1.0


# ---------------------------------------------------------------------------------------------------- write path
def _call(start: int = T0) -> S.ScriptCall:
    b = bar(start)
    return S.flush_call(TOKEN, SENTINEL, SERIES.adapter_id, SERIES.symbol, start + 960_000, [],
                        [S.SeriesFlush(SERIES, closed=(S.ClosedBarWrite(b, hash_of(b)),))], rollback_n=8)


async def test_write_and_wait_reports_the_aof_confirmation(md_redis: Any, runner: ScriptRunner) -> None:
    aof = FakeAof(local=1)
    client = MdWriteClient(md_redis, runner, aof)
    ok = await client.write_and_wait([_call()])
    assert isinstance(ok.replies[0], ScriptOk) and ok.aof_local == 1 and ok.aof_confirmed and aof.calls == [1]
    aof.local = 0   # WAITAOF timed out: the bars stay queued and are re-written
    late = await client.write_and_wait([_call(T0 + 900_000)])
    assert late.aof_local == 0 and not late.aof_confirmed
    again = await client.write_and_wait([_call(T0 + 900_000)])   # the idempotent re-write
    assert isinstance(again.replies[0], ScriptOk)
    assert S.parse_flush_reply(again.replies[0].value).bars == ((S.UNCHANGED, 0),)
    aof.error = True
    failed = await client.write_and_wait([_call(T0 + 1_800_000)])
    assert failed.aof_local == 0 and failed.aof_error == "TimeoutError" and not failed.aof_confirmed


async def test_a_reconnect_before_waitaof_is_not_a_confirmation(md_redis: Any, runner: ScriptRunner) -> None:
    aof = FakeAof(local=1)
    client = MdWriteClient(md_redis, runner, aof)
    aof.on_wait = runner.mark_reconnected   # redis-py reconnected silently when WAITAOF checked the connection out
    result = await client.write_and_wait([_call()])
    assert isinstance(result.replies[0], ScriptOk) and aof.calls == [1]
    assert result.aof_local == 0 and result.aof_error == "reconnected" and not result.aof_confirmed
    aof.on_wait = None   # the re-write runs the hook again on the new connection and is confirmed there
    again = await client.write_and_wait([_call()])
    assert again.aof_confirmed and again.aof_local == 1 and runner.hooks_run == 2


async def test_aof_off_never_sends_waitaof(md_redis: Any, runner: ScriptRunner) -> None:
    client = MdWriteClient(md_redis, runner, None)
    result = await client.write_and_wait([_call()])
    assert result.aof_local is None and result.aof_confirmed


async def test_token_install_and_boot_read(md_redis: Any, runner: ScriptRunner) -> None:
    client = MdWriteClient(md_redis, runner, None)
    token = K.WriterToken("1234567890", 9, "node-b", "fresh")
    await client.install_token(token)
    assert await md_redis.get(K.WRITER) == token.text.encode()
    assert await client.read_boot() == SENTINEL
    assert await MdReadClient(md_redis).read_boot() == SENTINEL
    await md_redis.set(K.BOOT, "garbage")
    assert await client.read_boot() is None


async def test_redis_aof_waiter_sends_waitaof_1_0_1500_and_maps_errors() -> None:
    from redis.exceptions import TimeoutError as RedisTimeoutError

    class _R:
        sent: list[tuple[Any, ...]] = []
        reply: Any = [1, 0]
        error: BaseException | None = None

        async def execute_command(self, *args: Any) -> Any:
            self.sent.append(args)
            if self.error is not None:
                raise self.error
            return self.reply

    r = _R()
    waiter = RedisAofWaiter(r)  # type: ignore[arg-type]
    assert await waiter.wait() == 1 and r.sent == [("WAITAOF", 1, 0, 1500)]
    r.reply = [0, 0]
    assert await waiter.wait() == 0
    r.error = RedisTimeoutError("Timeout reading from 127.0.0.1:6381")
    with pytest.raises(StoreUnavailable) as err:
        await waiter.wait()
    assert err.value.kind == "timeout" and "127.0.0.1" not in str(err.value)
