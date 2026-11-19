"""Demand leases: what the market-data layer builds, bounded and prioritised (design §A6 Demand, §A5, PR11; plan S18,
R11-M20).

* A consumer asserts a lease on a spec id (a series or an indicator) with a TTL (60 s) and renews it; a spec is live
  while any lease on it is live. An indicator lease also keeps its input series live. `define(spec)` gives the id:
  `s` + `sha256(series canonical)[:16]` for a series, `indicator_id` (`sha256(spec)[:16]`) for an indicator, so every
  id is a key-safe word.
* **Classes** `ARMED > PREWARM`. Decision-grade demand (a BROKER series or an indicator over one) counts per symbol
  `(adapter, symbol)`; v1 accepts only the 15m bar spec for it. TICK series (the arm-tracker's cross-check leases)
  never count. To admit an ARMED lease for a new symbol at the ceiling the book evicts PREWARM-only symbols, newest
  first; ARMED is refused only at the **hard ceiling**: per adapter `min(decision symbols (default 8), the BARS
  budget's symbols)` and, across adapters, the §A5 memory formula (`maxmemory × target − fixed`) / bytes per symbol.
  The BARS budget is the adapter's per-minute `BARS` cap less the PR9 stop-read cost of its armed handles
  (`stop_reads_per_min`, R11-M20), divided by the reads one decision-grade symbol costs per minute. A PREWARM lease
  that does not fit is refused (it never evicts).
* `demand()` of an indicator lease registers the spec with `md-indicators` (`IndicatorHost.register`), which computes
  the canonical value from stored bars in a worker thread before `warm` is reported (§A6 `register()`).
* The book is the demand source of the other tasks: `broker_demand()` (`md-bars`' `DemandSource`: each demanded
  BROKER series with the bars its backfill needs, its indicators' declared `backfill_bars`), `is_demanded()`
  (`md-writer` shedding step 4)
  and `held_leases()` (the S16 bootstrap's in-process lease set). `sync()` writes new and due leases and releases with
  `LEASE` (token only, never `md:boot`), so the registry and the lease index survive a takeover.

Ids, classes, counts and reason codes only: nothing here holds a price.
"""
from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Final, Protocol

from src.marketdata.bootstrap import HeldLease
from src.marketdata.errors import safe_redis_error
from src.marketdata.indicators.base import IndicatorSpec, SeriesRef
from src.marketdata.indicators.registry import REGISTRY, IndicatorRegistry
from src.marketdata.store import keys as K
from src.marketdata.store import scripts as S
from src.marketdata.store.runner import ScriptOk, ScriptReply, StoreUnavailable
from src.marketdata.types import M15, LeaseClass, SeriesId, Source

log = logging.getLogger(__name__)

LEASE_TTL_MS: Final = 60_000
RENEW_EVERY_MS: Final = 20_000
MIB: Final = 1 << 20
BYTES_PER_SYMBOL: Final = 60 * MIB
"""§A5: one symbol with v1 demand is about 60 MB (bars, revisions, rollback set, checkpoints, tick stream)."""
FIXED_BYTES: Final = 15 * MIB
"""§A5: `md:events` at its `MAXLEN`."""
DEFAULT_BACKFILL_BARS: Final = 182
"""The backfill of a bare BROKER series lease (no indicator on it): a series-level default, not an indicator rule
(round 12, S38c: the engine reads each indicator's own declared `backfill_bars`, never ATR's `length`). 182 bars of
15m is about two days; an indicator lease asks for its own figure on top (the max is taken)."""

Spec = SeriesId | IndicatorSpec

UNKNOWN_SPEC: Final = "unknown_spec"
NOT_DECISION_GRADE: Final = "spec_not_decision_grade"
CEILING: Final = "ceiling"
REGISTRATION: Final = "registration_refused"


def series_spec_id(series: SeriesId) -> str:
    return "s" + hashlib.sha256(series.canonical.encode("utf-8")).hexdigest()[:16]


def spec_id_of(spec: Spec) -> str:
    return spec.indicator_id if isinstance(spec, IndicatorSpec) else series_spec_id(spec)


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


class IndicatorHost(Protocol):
    """`md-indicators` (`MdIndicators`): registration of a leased indicator and its removal."""

    async def register(self, spec: IndicatorSpec) -> object | None:
        """Computes the canonical value before reporting `warm`; None when the registration check refuses it."""
        ...

    def remove(self, indicator_id: str) -> bool: ...


class LeaseWritePort(Protocol):
    async def run(self, calls: Sequence[S.ScriptCall]) -> list[ScriptReply]: ...


@dataclass(frozen=True, kw_only=True)
class DemandSettings:
    ttl_ms: int = LEASE_TTL_MS
    renew_every_ms: int = RENEW_EVERY_MS
    decision_symbols_per_adapter: int = 8
    maxmemory_bytes: int = 1 << 30
    memory_target: Decimal = Decimal("0.5")
    bytes_per_symbol: int = BYTES_PER_SYMBOL
    fixed_bytes: int = FIXED_BYTES
    reads_per_symbol_per_min: Decimal = Decimal(2)
    """BARS reads one decision-grade symbol (bid and ask BROKER 15m) costs per minute, averaged over the live
    schedule; set from the PR3 finality lag."""

    def __post_init__(self) -> None:
        if min(self.ttl_ms, self.renew_every_ms, self.decision_symbols_per_adapter, self.bytes_per_symbol) < 1:
            raise ValueError("demand settings must be positive")
        if self.renew_every_ms >= self.ttl_ms or not Decimal(0) < self.memory_target <= Decimal(1):
            raise ValueError("demand renewals must come before the TTL; the memory target is a ratio")
        if self.reads_per_symbol_per_min <= 0:
            raise ValueError("reads per symbol must be positive")


@dataclass(frozen=True)
class Admission:
    admitted: bool
    reason: str = ""
    evicted: tuple[tuple[str, str], ...] = ()
    """`(spec_id, consumer)` of the PREWARM leases evicted to admit this one."""


@dataclass
class _Lease:
    spec_id: str
    consumer: str
    lease_class: LeaseClass
    seq: int
    expires_ms: int
    written_ms: int | None = None


class DemandBook:
    """The demand registry of one term (in process; the store copy is soft state rebuilt at bootstrap)."""

    def __init__(self, *, registry: IndicatorRegistry = REGISTRY, settings: DemandSettings | None = None,
                 bars_per_min: Callable[[str], int | None] | None = None,
                 stop_reads_per_min: Callable[[str], int] | None = None, host: IndicatorHost | None = None,
                 memory_ratio: Callable[[], float | None] | None = None,
                 clock_ms: Callable[[], int] = _wall_ms) -> None:
        """`memory_ratio` (S38e): `md-writer`'s last measured `used_memory / maxmemory` (None: unknown); it cuts the
        memory ceiling to what the store can still take (`memory_symbols`)."""
        self._registry = registry
        self._cfg = settings or DemandSettings()
        self._bars_per_min = bars_per_min
        self._stop_reads = stop_reads_per_min
        self._host = host
        self._memory_ratio = memory_ratio
        self._clock_ms = clock_ms
        self._defs: dict[str, Spec] = {}
        self._leases: dict[tuple[str, str], _Lease] = {}
        self._releases: list[tuple[str, str, str]] = []
        self._evicted: list[Callable[[str, str], None]] = []
        self._seq = 0
        self.refused = 0
        self.evictions = 0

    # -------------------------------------------------------------------------------------------- specs
    def define(self, spec: Spec) -> str:
        """The spec id of a series or an indicator (registered for later `demand` calls)."""
        sid = spec_id_of(spec)
        self._defs[sid] = spec
        return sid

    def spec(self, spec_id: str) -> Spec | None:
        return self._defs.get(spec_id)

    @staticmethod
    def _primary(spec: Spec) -> SeriesId:
        return spec.series if isinstance(spec, IndicatorSpec) else spec

    def _series_of(self, spec: Spec) -> set[SeriesId]:
        if not isinstance(spec, IndicatorSpec):
            return {spec}
        out = {spec.series}
        ind = self._registry.get(spec.kind, spec.version)
        if ind is not None:
            out.update(r.series for r in ind.inputs(spec) if isinstance(r, SeriesRef))
        return out

    @classmethod
    def _decision_key(cls, spec: Spec) -> tuple[str, str] | None:
        series = cls._primary(spec)
        if series.source is not Source.BROKER:
            return None
        return (series.adapter_id, series.symbol)

    # -------------------------------------------------------------------------------------------- ceiling
    def memory_symbols(self, current: int | None = None) -> int:
        """§A5: decision-grade symbols the store holds at the memory target: `(maxmemory × target − fixed) / bytes
        per symbol` at full (1-year, enforced by `RETAIN_BROKER`, S38e) retention. With a measured `used_memory`
        ratio (S38e) the ceiling is also cut to the `current` symbols plus the room left below the target, so
        history that piled up (series no longer demanded keep theirs until retention trims it) is counted as it is,
        not as a constant. An unknown ratio leaves the formula alone (the writer's shedding and OOM paging still
        hold)."""
        usable = int(Decimal(self._cfg.maxmemory_bytes) * self._cfg.memory_target) - self._cfg.fixed_bytes
        formula = max(0, usable // self._cfg.bytes_per_symbol)
        ratio = self._measured()
        if ratio is None or current is None:
            return formula
        room = int((self._cfg.memory_target - Decimal(str(ratio))) * self._cfg.maxmemory_bytes)
        return min(formula, current + max(room, 0) // self._cfg.bytes_per_symbol)

    def _measured(self) -> float | None:
        if self._memory_ratio is None:
            return None
        try:
            ratio = self._memory_ratio()
        except Exception:
            return None
        if ratio is None or not 0 <= ratio <= 10:
            return None
        return float(ratio)

    def adapter_ceiling(self, adapter_id: str) -> int:
        """Decision-grade symbols of one adapter: the local setting, cut by the BARS budget less the stop reads."""
        ceiling = self._cfg.decision_symbols_per_adapter
        cap = self._bars_per_min(adapter_id) if self._bars_per_min is not None else None
        if cap is not None:
            stop = self._stop_reads(adapter_id) if self._stop_reads is not None else 0
            budget = Decimal(max(cap - max(stop, 0), 0))
            ceiling = min(ceiling, int(budget / self._cfg.reads_per_symbol_per_min))
        return max(ceiling, 0)

    def symbols(self) -> dict[tuple[str, str], LeaseClass]:
        """Live decision-grade symbols with their highest lease class."""
        out: dict[tuple[str, str], LeaseClass] = {}
        for lease in self._leases.values():
            spec = self._defs.get(lease.spec_id)
            key = None if spec is None else self._decision_key(spec)
            if key is None:
                continue
            if lease.lease_class is LeaseClass.ARMED:
                out[key] = LeaseClass.ARMED
            else:
                out.setdefault(key, LeaseClass.PREWARM)
        return out

    def _fits(self, symbols: Mapping[tuple[str, str], LeaseClass], key: tuple[str, str]) -> bool:
        same = sum(1 for k in symbols if k[0] == key[0])
        return same < self.adapter_ceiling(key[0]) and len(symbols) < self.memory_symbols(len(symbols))

    # -------------------------------------------------------------------------------------------- leases
    def admit(self, spec_id: str, consumer: str, lease_class: LeaseClass) -> Admission:
        """Assert or renew a lease (sync; no I/O). Refused codes: `unknown_spec`, `spec_not_decision_grade`,
        `ceiling`."""
        now = self._clock_ms()
        self.expire(now)
        spec = self._defs.get(spec_id)
        if spec is None:
            return self._refuse(UNKNOWN_SPEC)
        try:
            K.demand(consumer, spec_id)
        except ValueError:
            return self._refuse(UNKNOWN_SPEC)
        key = self._decision_key(spec)
        if key is not None and self._primary(spec).spec != M15:
            return self._refuse(NOT_DECISION_GRADE)
        held = self._leases.get((spec_id, consumer))
        symbols = self.symbols()
        evicted: list[tuple[str, str]] = []
        if held is None and key is not None and key not in symbols and not self._fits(symbols, key):
            if lease_class is not LeaseClass.ARMED:
                return self._refuse(CEILING)
            plan = self._eviction_plan(symbols, key)
            if plan is None:
                return self._refuse(CEILING)
            for victim in plan:
                evicted += self._evict_symbol(victim)
        self._seq += 1
        if held is None:
            self._leases[(spec_id, consumer)] = _Lease(spec_id, consumer, lease_class, self._seq,
                                                       now + self._cfg.ttl_ms)
        else:
            held.lease_class, held.expires_ms = lease_class, now + self._cfg.ttl_ms
        return Admission(True, "", tuple(evicted))

    def _refuse(self, reason: str) -> Admission:
        self.refused += 1
        return Admission(False, reason)

    def _eviction_plan(self, symbols: Mapping[tuple[str, str], LeaseClass],
                       key: tuple[str, str]) -> list[tuple[str, str]] | None:
        """PREWARM-only symbols to evict, newest first (this adapter's first when the adapter ceiling binds), or
        None when even every PREWARM symbol would not make room (the hard ceiling)."""
        newest: dict[tuple[str, str], int] = {}
        for lease in self._leases.values():
            spec = self._defs.get(lease.spec_id)
            k = None if spec is None else self._decision_key(spec)
            if k is not None:
                newest[k] = max(newest.get(k, 0), lease.seq)
        prewarm = [k for k, c in symbols.items() if c is LeaseClass.PREWARM]
        prewarm.sort(key=lambda k: (k[0] != key[0], -newest.get(k, 0)))
        remaining = dict(symbols)
        plan: list[tuple[str, str]] = []
        for victim in prewarm:
            if self._fits(remaining, key):
                break
            same = sum(1 for k in remaining if k[0] == key[0])
            if same >= self.adapter_ceiling(key[0]) and victim[0] != key[0]:
                continue                       # another adapter's symbol frees no room under this adapter's ceiling
            del remaining[victim]
            plan.append(victim)
        return plan if self._fits(remaining, key) else None

    def _evict_symbol(self, key: tuple[str, str]) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for (sid, consumer), lease in list(self._leases.items()):
            spec = self._defs.get(sid)
            if spec is None or self._decision_key(spec) != key:
                continue
            self._drop(sid, consumer)
            out.append((sid, consumer))
        self.evictions += len(out)
        for sid, consumer in out:
            for cb in self._evicted:
                try:
                    cb(sid, consumer)
                except Exception:   # a consumer's fault never stops admission
                    log.warning("md demand: eviction listener failed")
        return out

    def _drop(self, spec_id: str, consumer: str) -> bool:
        lease = self._leases.pop((spec_id, consumer), None)
        if lease is None:
            return False
        spec = self._defs.get(spec_id)
        self._releases.append((consumer, spec_id, K.REGISTRY_IND if isinstance(spec, IndicatorSpec)
                               else K.REGISTRY_SERIES))
        if isinstance(spec, IndicatorSpec) and self._host is not None \
                and not any(s == spec_id for s, _ in self._leases):
            try:
                self._host.remove(spec_id)
            except Exception:
                log.warning("md demand: indicator removal failed for %s", spec_id)
        return True

    def expire(self, now_ms: int | None = None) -> int:
        """Drop leases past their TTL; returns how many."""
        now = self._clock_ms() if now_ms is None else now_ms
        gone = [k for k, lease in self._leases.items() if lease.expires_ms <= now]
        for sid, consumer in gone:
            self._drop(sid, consumer)
        return len(gone)

    def on_evicted(self, listener: Callable[[str, str], None]) -> None:
        """Called with `(spec_id, consumer)` of each PREWARM lease evicted for an ARMED one."""
        self._evicted.append(listener)

    # -------------------------------------------------------------------------------------------- the port side
    async def demand(self, spec_id: str, consumer: str, lease_class: LeaseClass) -> bool:
        """`MarketDataPort.demand`: admit, then register a new indicator (canonical value computed in a thread
        before `warm` is reported). False when refused."""
        held = (spec_id, consumer) in self._leases
        admission = self.admit(spec_id, consumer, lease_class)
        if not admission.admitted:
            return False
        spec = self._defs.get(spec_id)
        if isinstance(spec, IndicatorSpec) and self._host is not None and not held:
            try:
                status = await self._host.register(spec)
            except Exception:
                status = None
            if status is None:
                self._drop(spec_id, consumer)
                self.refused += 1
                return False
        return True

    async def release(self, spec_id: str, consumer: str) -> None:
        self._drop(spec_id, consumer)

    # -------------------------------------------------------------------------------------------- demand sources
    def live(self) -> frozenset[str]:
        self.expire()
        return frozenset(sid for sid, _ in self._leases)

    def demanded_series(self) -> frozenset[SeriesId]:
        out: set[SeriesId] = set()
        for sid in self.live():
            spec = self._defs.get(sid)
            if spec is not None:
                out |= self._series_of(spec)
        return frozenset(out)

    def is_demanded(self, series: SeriesId) -> bool:
        """`md-writer` shedding step 4: a TICK spec nobody demands is not stored."""
        return series in self.demanded_series()

    def broker_demand(self) -> dict[SeriesId, int]:
        """`md-bars`' demand source: every demanded BROKER series with the bars its backfill needs: the widest
        `backfill_bars` its indicators declare (each indicator's own window plus warm-up, the `Indicator` protocol),
        and at least `DEFAULT_BACKFILL_BARS` for a bare series lease."""
        out: dict[SeriesId, int] = {}
        for sid in self.live():
            spec = self._defs.get(sid)
            if spec is None:
                continue
            need = DEFAULT_BACKFILL_BARS
            if isinstance(spec, IndicatorSpec):
                ind = self._registry.get(spec.kind, spec.version)
                if ind is not None:
                    need = max(need, ind.backfill_bars(spec))
            for series in self._series_of(spec):
                if series.source is Source.BROKER:
                    out[series] = max(out.get(series, 0), need)
        return out

    def held_leases(self) -> list[HeldLease]:
        """The in-process lease set for the S16 bootstrap (`LeaseSourcePort`)."""
        now = self._clock_ms()
        out: list[HeldLease] = []
        for lease in self._leases.values():
            spec = self._defs.get(lease.spec_id)
            if spec is None or lease.expires_ms <= now:
                continue
            out.append(HeldLease(lease.consumer, lease.spec_id, lease.lease_class.value, lease.expires_ms - now,
                                 spec.canonical, K.REGISTRY_IND if isinstance(spec, IndicatorSpec)
                                 else K.REGISTRY_SERIES))
        return out

    # -------------------------------------------------------------------------------------------- store
    def lease_calls(self, token: K.WriterToken) -> list[tuple[S.ScriptCall, tuple[str, str] | None]]:
        """`LEASE` calls due now: releases, then new leases and renewals older than `renew_every_ms`."""
        now = self._clock_ms()
        calls: list[tuple[S.ScriptCall, tuple[str, str] | None]] = []
        for consumer, sid, registry_key in self._releases:
            calls.append((S.lease_call(token, consumer, sid, 0, "", registry_key=registry_key), None))
        for key, lease in self._leases.items():
            if lease.written_ms is not None and now - lease.written_ms < self._cfg.renew_every_ms:
                continue
            spec = self._defs[lease.spec_id]
            ind = isinstance(spec, IndicatorSpec)
            calls.append((S.lease_call(token, lease.consumer, lease.spec_id, self._cfg.ttl_ms, lease.lease_class.value,
                                       registry_key=K.REGISTRY_IND if ind else K.REGISTRY_SERIES,
                                       registry_field=lease.spec_id, registry_value=spec.canonical), key))
        return calls

    async def sync(self, write: LeaseWritePort, token: K.WriterToken) -> int:
        """Write the due lease calls (token only). Returns how many were accepted; a store error keeps them due."""
        self.expire()
        releases = list(self._releases)
        calls = self.lease_calls(token)
        if not calls:
            return 0
        try:
            replies = await write.run([c for c, _ in calls])
        except StoreUnavailable as e:
            log.info("md demand: lease writes deferred (%s)", safe_redis_error(e))
            return 0
        now = self._clock_ms()
        padded: list[ScriptReply | None] = [*replies, *([None] * (len(calls) - len(replies)))]
        n_rel = len(releases)
        failed = [rel for rel, reply in zip(releases, padded[:n_rel]) if not isinstance(reply, ScriptOk)]
        self._releases = failed + self._releases[n_rel:]
        ok = n_rel - len(failed)
        for (_, key), reply in zip(calls[n_rel:], padded[n_rel:]):
            if not isinstance(reply, ScriptOk) or key is None:
                continue
            ok += 1
            lease = self._leases.get(key)
            if lease is not None:
                lease.written_ms = now
        return ok


__all__ = ["DemandBook", "DemandSettings", "Admission", "IndicatorHost", "series_spec_id", "spec_id_of",
           "LEASE_TTL_MS", "DEFAULT_BACKFILL_BARS", "CEILING", "UNKNOWN_SPEC", "NOT_DECISION_GRADE"]
