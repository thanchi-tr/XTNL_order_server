"""Shared rig for the S16 tests: fakeredis + lupa, the real `MdWriteClient`, `MdWriter`, `MdBootstrap`,
`MdWriterService`, `MdPort` and `MdRehash` (synthetic instrument `mock:SYN1`, synthetic prices; no price appears in an
assertion message)."""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from src.marketdata.bootstrap import HeldLease, MdBootstrap, MdReset, MdWriterService, SeriesStateBook
from src.marketdata.events import RefetchQueue
from src.marketdata.hashing import Keyring
from src.marketdata.port_impl import MdPort
from src.marketdata.ports import NodeMdRecord
from src.marketdata.rehash import MdRehash
from src.marketdata.store import keys as K
from src.marketdata.store.client import MdWriteClient
from src.marketdata.store.reads import StoreReader
from src.marketdata.store.runner import ScriptRunner
from src.marketdata.ticks import MdHook
from src.marketdata.writer import MdWriter

from .conftest import KEYRING, TOKEN, FakeAof, RunIds


class Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


@dataclass
class Records:
    """`NodeRecordPort` fake (this node's `arm_status` record)."""

    record: NodeMdRecord | None = None
    saves: int = 0
    fail: bool = False

    async def load_record(self) -> NodeMdRecord | None:
        if self.fail:
            raise OSError("down")
        return self.record

    async def save_record(self, record: NodeMdRecord) -> None:
        if self.fail:
            raise OSError("down")
        self.saves += 1
        self.record = record


@dataclass
class Leader:
    ok: bool = True
    checks: int = 0

    async def confirm_leader(self) -> bool:
        self.checks += 1
        return self.ok


@dataclass
class Leases:
    held: list[HeldLease] = field(default_factory=list)

    def held_leases(self) -> Sequence[HeldLease]:
        return list(self.held)


@dataclass
class Rig:
    redis: Any
    client: MdWriteClient
    writer: MdWriter
    hook: MdHook
    boot: MdBootstrap
    svc: MdWriterService
    port: MdPort
    rehash: MdRehash
    clock: Clock
    run_ids: RunIds
    aof: FakeAof
    records: Records
    leader: Leader
    leases: Leases
    resets: list[MdReset]
    faults: list[str]
    completed: list[tuple[int, str, str]]
    book: SeriesStateBook

    async def at(self, now: int) -> None:
        self.clock.now = now
        await self.svc.step()

    def restart_redis(self, run_id: str) -> None:
        """A Redis restart with the AOF reloaded: a new `run_id`, the connection re-made (redis-py's callback)."""
        self.run_ids.value = run_id
        self.client.runner.mark_reconnected()


async def make_rig(md_redis: Any, run_ids: RunIds, *, start: int, token: K.WriterToken = TOKEN,
                   keyring: Keyring = KEYRING, records: Records | None = None, aof_off: bool = False,
                   rollback_n: int = 8, previous_term_epoch: int | None = None,
                   leases: Leases | None = None) -> Rig:
    aof = FakeAof()
    client = MdWriteClient(md_redis, ScriptRunner(md_redis, run_id_reader=run_ids), None if aof_off else aof)
    clock = Clock(start)
    hook = MdHook(clock_ms=clock)
    refetch = RefetchQueue()
    writer = MdWriter(hook, client, token=token, keyring=keyring, boot=None, rollback_n=rollback_n,
                      refetch=refetch, clock_ms=clock, mono_ms=clock)
    records = records or Records()
    leader, leases = Leader(), leases or Leases()
    faults: list[str] = []
    book = SeriesStateBook()
    reader = StoreReader(md_redis)
    boot = MdBootstrap(writer, client, reader, keyring=keyring, node=token.node, leader=leader, book=book,
                       records=records, leases=leases, on_fault=faults.append,
                       previous_term_epoch=previous_term_epoch, clock_ms=clock, mono_ms=clock)
    resets: list[MdReset] = []
    boot.on_reset(resets.append)
    rehash = MdRehash(writer, client, reader, keyring=keyring, clock_ms=clock)
    completed: list[tuple[int, str, str]] = []
    svc = MdWriterService(writer, boot, rehash=rehash, complete=lambda r, o, why: completed.append((r, o, why)),
                          mono_ms=clock)
    port = MdPort(StoreReader(md_redis), keyring=keyring, book=book, leader_epoch=lambda: writer.token.epoch_rev,
                  hook=hook, quarantine_sink=writer.quarantine_stored, refetch_queue=refetch, clock_ms=clock)
    writer.on_released(port.wake)
    return Rig(md_redis, client, writer, hook, boot, svc, port, rehash, clock, run_ids, aof, records, leader, leases,
               resets, faults, completed, book)


async def snapshot(r: Any) -> dict[str, tuple[str, Any]]:
    """Every `md:*` key with its type and content (an AOF image, for the "older AOF" drill)."""
    out: dict[str, tuple[str, Any]] = {}
    for key in sorted(await r.keys("md:*")):
        kind = (await r.type(key)).decode()
        name = key.decode()
        if kind == "string":
            out[name] = (kind, await r.get(key))
        elif kind == "hash":
            out[name] = (kind, await r.hgetall(key))
        elif kind == "zset":
            out[name] = (kind, await r.zrange(key, 0, -1, withscores=True))
        elif kind == "stream":
            out[name] = (kind, await r.xrange(key))
    return out


async def restore(r: Any, image: dict[str, tuple[str, Any]]) -> None:
    """Replace the whole store with `image` (FLUSHALL, then every key rebuilt)."""
    await r.flushall()
    for name, (kind, value) in image.items():
        if kind == "string":
            await r.set(name, value)
        elif kind == "hash":
            await r.hset(name, mapping=value)
        elif kind == "zset":
            await r.zadd(name, {m: s for m, s in value})
        elif kind == "stream":
            for entry_id, fields in value:
                await r.xadd(name, fields, id=entry_id)
