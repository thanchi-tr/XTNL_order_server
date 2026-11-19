"""This node's health record inputs (operations.md §4) and the STANDBY-entry Redis flush.

Hard checks: PostgreSQL primary writable (control pool), local Redis writable, chrony within 50 ms
(etcd reachability is added by the NodeController). Preferences: one TLS handshake per broker endpoint
(None while the market is known closed; for the leader, an adapter in STREAMING is healthy whatever a
probe says), chrony < 1 ms, hosting the Patroni primary. Plus the spill-file size.

Every probe has its own timeout and runs concurrently; a probe that fails or times out reports the
check as failed (an unknown health state fails the hard check). Nothing here logs a credential.
"""
from __future__ import annotations

import asyncio
import logging
import socket
import ssl
import subprocess
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import redis.asyncio as aioredis

from src.cache.scripts import read_boot_id
from src.db.pg import ConnectionSource
from src.runtime.health import HealthRecord
from src.runtime.pg_ports import primary_writable

log = logging.getLogger(__name__)

PROBE_KEY: Final = "xtnl:health:probe"
TLS_TIMEOUT_S: Final = 2.0


@dataclass(frozen=True)
class BrokerEndpoint:
    adapter_id: str
    host: str
    port: int = 443


TlsConnector = Callable[[str, int, float], Awaitable[None]]


async def tls_handshake(host: str, port: int, timeout_s: float) -> None:
    """One TLS handshake (certificate verified against the system / SSL_CERT_FILE trust), then close."""
    ctx = ssl.create_default_context()
    _, writer = await asyncio.wait_for(asyncio.open_connection(host, port, ssl=ctx, server_hostname=host),
                                       timeout_s)
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), 1.0)
    except (TimeoutError, OSError, ssl.SSLError):
        pass


class BrokerTlsMonitor:
    """Network-level reachability per adapter (DNS / TCP / TLS only; post-TLS errors never count).
    `failed_since_ms(adapter)` feeds broker-grounds abdication (BrokerLink.net_failed_since_ms)."""

    def __init__(self, endpoints: Sequence[BrokerEndpoint], *, connector: TlsConnector = tls_handshake,
                 timeout_s: float = TLS_TIMEOUT_S, min_interval_s: float = 5.0,
                 clock_ms: Callable[[], int] | None = None, monotonic: Callable[[], float] = time.monotonic) -> None:
        self.endpoints = {e.adapter_id: e for e in endpoints}
        self._connector = connector
        self._timeout = timeout_s
        self._interval = min_interval_s
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._mono = monotonic
        self._last: dict[str, bool] = {}
        self._failed_since: dict[str, int] = {}
        self._checked_at: float | None = None

    async def _one(self, ep: BrokerEndpoint) -> bool:
        try:
            await self._connector(ep.host, ep.port, self._timeout)
            return True
        except (OSError, TimeoutError, ssl.SSLError) as exc:
            log.info("TLS probe %s (%s) failed: %s", ep.adapter_id, ep.host, type(exc).__name__)
            return False

    async def check(self) -> Mapping[str, bool]:
        now = self._mono()
        if self._checked_at is not None and now - self._checked_at < self._interval:
            return dict(self._last)
        self._checked_at = now
        eps = list(self.endpoints.values())
        results = await asyncio.gather(*(self._one(e) for e in eps))
        wall = self._clock_ms()
        for ep, ok in zip(eps, results):
            self._last[ep.adapter_id] = ok
            if ok:
                self._failed_since.pop(ep.adapter_id, None)
            else:
                self._failed_since.setdefault(ep.adapter_id, wall)
        return dict(self._last)

    def failed_since_ms(self, adapter_id: str) -> int | None:
        return self._failed_since.get(adapter_id)


def parse_chrony_signed_s(csv_line: str) -> float | None:
    """`chronyc -c tracking`: field 5 is the signed system time offset in seconds (clock offset metric)."""
    parts = csv_line.strip().split(",")
    if len(parts) < 5:
        return None
    try:
        return float(parts[4])
    except ValueError:
        return None


def parse_chrony_tracking(csv_line: str) -> float | None:
    """`chronyc -c tracking`: field 5 is the system time offset in seconds. Returns |offset| in ms."""
    signed = parse_chrony_signed_s(csv_line)
    return None if signed is None else abs(signed) * 1000.0


class ChronyReader:
    """chrony offset via `chronyc -c tracking` in a worker thread (1 s timeout, cached 5 s).
    No chronyc (e.g. a Windows dev machine) -> None, which fails the hard check."""

    def __init__(self, *, command: Sequence[str] = ("chronyc", "-c", "tracking"), timeout_s: float = 1.0,
                 cache_s: float = 5.0, monotonic: Callable[[], float] = time.monotonic) -> None:
        self._cmd = list(command)
        self._timeout = timeout_s
        self._cache = cache_s
        self._mono = monotonic
        self._value: float | None = None
        self._at: float | None = None
        #: signed offset (s) of the last read, None when unreadable (xtnl_clock_offset_seconds)
        self.signed_offset_s: float | None = None

    def _run(self) -> float | None:
        try:
            out = subprocess.run(self._cmd, capture_output=True, text=True, timeout=self._timeout, check=False)
        except (OSError, subprocess.SubprocessError):
            self.signed_offset_s = None
            return None
        if out.returncode != 0:
            self.signed_offset_s = None
            return None
        line = out.stdout.splitlines()[0] if out.stdout else ""
        self.signed_offset_s = parse_chrony_signed_s(line)
        return parse_chrony_tracking(line)

    async def offset_ms(self) -> float | None:
        now = self._mono()
        if self._at is None or now - self._at >= self._cache:
            self._value = await asyncio.to_thread(self._run)
            self._at = now
        return self._value

    def synced(self) -> bool:
        return self._value is not None and self._value <= 50.0



def parse_w32tm_stripchart(output: str) -> float | None:
    """Signed offset in seconds from `w32tm /stripchart ... /dataonly`: the last line like
    "23:00:42, -00.9258516s". Error lines (e.g. "error: 0x800705B4") give None."""
    for line in reversed(output.strip().splitlines()):
        _, sep, value = line.partition(",")
        value = value.strip()
        if sep and value.endswith("s"):
            try:
                return float(value[:-1])
            except ValueError:
                return None
    return None


class W32tmReader(ChronyReader):
    """Windows development machines: the clock offset against an NTP server via `w32tm /stripchart`
    (one sample, in a worker thread). Same contract as ChronyReader: |offset| in ms, None when unreadable
    (which fails the hard check). The offset is measured, not assumed: an unsynced Windows clock fails."""

    def __init__(self, *, server: str = "time.windows.com", timeout_s: float = 5.0, cache_s: float = 60.0,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        super().__init__(command=("w32tm", "/stripchart", f"/computer:{server}", "/samples:1", "/dataonly"),
                         timeout_s=timeout_s, cache_s=cache_s, monotonic=monotonic)

    def _run(self) -> float | None:
        try:
            out = subprocess.run(self._cmd, capture_output=True, text=True, timeout=self._timeout, check=False)
        except (OSError, subprocess.SubprocessError):
            self.signed_offset_s = None
            return None
        signed = parse_w32tm_stripchart(out.stdout or "")
        self.signed_offset_s = signed
        return None if signed is None else abs(signed) * 1000.0


def clock_reader() -> ChronyReader:
    """chrony on the Linux engine nodes; w32tm on a Windows development machine."""
    import sys
    return W32tmReader() if sys.platform == "win32" else ChronyReader()

ADDRESSES_TTL_S: Final = 300.0   # refresh this node's address set every 5 min


def local_addresses() -> frozenset[str]:
    names: set[str] = {"127.0.0.1", "::1", "localhost"}
    host = socket.gethostname()
    names.add(host)
    try:
        for info in socket.getaddrinfo(host, None):
            names.add(str(info[4][0]))
    except OSError:
        pass
    return frozenset(names)


class NodeHealthProbe:
    def __init__(self, node_id: str, *, pg: ConnectionSource, redis: aioredis.Redis,
                 chrony: Callable[[], Awaitable[float | None]], tls: BrokerTlsMonitor | None = None,
                 patroni_primary_host: Callable[[], Awaitable[str | None]] | None = None,
                 spill_path: Path | None = None, timeout_s: float = 1.0,
                 addresses: Callable[[], frozenset[str]] = local_addresses,
                 addresses_ttl_s: float = ADDRESSES_TTL_S,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self.node_id = node_id
        self._pg = pg
        self._redis = redis
        self._chrony = chrony
        self._tls = tls
        self._primary_host = patroni_primary_host
        self._spill = spill_path
        self._timeout = timeout_s
        self._addresses = addresses
        self._addresses_ttl_s = addresses_ttl_s
        self._mono = monotonic
        self._addr_cache: frozenset[str] | None = None
        self._addr_at: float | None = None
        self._addr_job: asyncio.Future[frozenset[str]] | None = None
        self._spill_last = 0
        self._spill_job: asyncio.Future[int] | None = None
        # set by the wiring: the leader's own SM4 view (adapter -> STREAMING) and market-open view
        self.leader_streaming: Callable[[], Mapping[str, bool]] | None = None
        self.market_open: Callable[[str], bool | None] | None = None
        # set by the wiring once the optional services run (S32): this node's registry tokens (`KIND:VERSION`) for
        # the health record; None (the default, S10) publishes an empty registry
        self.registry_tokens: Callable[[], Iterable[str]] | None = None

    async def _guard(self, what: str, aw: Awaitable[bool]) -> bool:
        try:
            return bool(await asyncio.wait_for(aw, self._timeout))
        except Exception as exc:
            log.info("health probe %s failed: %s", what, type(exc).__name__)
            return False

    async def _redis_writable(self, now_ms: int) -> bool:
        return bool(await self._redis.set(PROBE_KEY, str(now_ms), px=5_000))

    async def _chrony_ms(self) -> float | None:
        try:
            return await asyncio.wait_for(self._chrony(), self._timeout + 0.5)
        except Exception:
            return None

    async def _hosts_primary(self) -> bool:
        if self._primary_host is None:
            return False
        try:
            host = await asyncio.wait_for(self._primary_host(), self._timeout + 2.0)
        except Exception:
            return False
        return host is not None and host in await self._local_addresses()

    async def _local_addresses(self) -> frozenset[str]:
        """This node's names / addresses, cached. gethostname + getaddrinfo are blocking DNS calls: they
        run in a worker thread under a timeout (operations.md §3), at most one lookup in flight, and a
        slow or failed lookup keeps the last known set (empty before the first success)."""
        now = self._mono()
        if self._addr_cache is not None and self._addr_at is not None and now - self._addr_at < self._addresses_ttl_s:
            return self._addr_cache
        if self._addr_job is None or self._addr_job.done():
            self._addr_job = asyncio.ensure_future(asyncio.to_thread(self._addresses))
        try:
            value = await asyncio.wait_for(asyncio.shield(self._addr_job), self._timeout)
        except Exception as exc:
            if self._addr_job.done():
                self._addr_job = None        # failed: the next probe starts a fresh lookup
            log.info("local address lookup failed: %s", type(exc).__name__)
            return self._addr_cache if self._addr_cache is not None else frozenset()
        self._addr_cache, self._addr_at = value, now
        return value

    async def _broker_tls(self) -> dict[str, bool | None]:
        if self._tls is None:
            return {}
        try:
            seen = await asyncio.wait_for(self._tls.check(), TLS_TIMEOUT_S + 1.0)
        except Exception:
            seen = {}
        streaming = self.leader_streaming() if self.leader_streaming is not None else {}
        out: dict[str, bool | None] = {}
        for adapter in self._tls.endpoints:
            if streaming.get(adapter):
                out[adapter] = True
                continue
            open_ = self.market_open(adapter) if self.market_open is not None else None
            out[adapter] = None if open_ is False else seen.get(adapter)
        return out

    def _stat_spill(self) -> int:
        if self._spill is None:
            return 0
        try:
            return self._spill.stat().st_size
        except FileNotFoundError:
            return 0

    async def _spill_bytes(self) -> int:
        """Spill-file size for the health record. The stat runs in a worker thread under the probe timeout
        (a stalled disk never blocks the event loop, operations.md §3), at most one in flight; a slow or
        failed stat reports the last known size."""
        if self._spill is None:
            return 0
        job = self._spill_job
        if job is not None and job.done():
            if not job.cancelled() and job.exception() is None:
                self._spill_last = job.result()      # a slow stat that finished since the last probe
            job = None
        if job is None:
            job = self._spill_job = asyncio.ensure_future(asyncio.to_thread(self._stat_spill))
        try:
            self._spill_last = await asyncio.wait_for(asyncio.shield(job), self._timeout)
        except Exception as exc:
            log.info("spill-file stat failed: %s", type(exc).__name__)
        return self._spill_last

    def _registry(self) -> tuple[str, ...]:
        """The sorted, de-duplicated registry tokens (pure, in memory). A raising source publishes none (logged with the
        error type only), so a registry fault never fails a hard check."""
        source = self.registry_tokens
        if source is None:
            return ()
        try:
            return tuple(sorted({str(t) for t in source() if t}))
        except Exception as exc:
            log.info("health registry tokens failed: %s", type(exc).__name__)
            return ()

    async def probe(self, now_ms: int) -> HealthRecord:
        pg_ok, redis_ok, chrony, primary, tls, spill = await asyncio.gather(
            self._guard("postgres", primary_writable(self._pg)),
            self._guard("redis", self._redis_writable(now_ms)),
            self._chrony_ms(), self._hosts_primary(), self._broker_tls(), self._spill_bytes())
        return HealthRecord(self.node_id, now_ms, "", pg_writable=pg_ok, etcd_reachable=True,
                            redis_writable=redis_ok, chrony_offset_ms=chrony, broker_tls=tls,
                            hosts_patroni_primary=primary, spill_bytes=spill, registry=self._registry())


class StandbyRedisFlush:
    """NodeController LocalRedis: STANDBY entry FLUSHDB of the engine's own Redis database (the one REDIS_URL
    selects), never FLUSHALL: another database on the same server is not the engine's to wipe (IMPLEMENTATION_PLAN
    4.9). An ex-leader's Redis never shows a frozen book, and redis:boot_id must be gone afterwards."""

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    async def flushdb(self) -> None:
        await self._redis.flushdb()
        if await read_boot_id(self._redis) is not None:
            raise RuntimeError("redis:boot_id survived FLUSHDB")
