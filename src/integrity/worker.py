"""SM3 IntegrityWorker: Redis vs PostgreSQL verification every INTEGRITY_CHECK_INTERVAL_SEC (tier-persistence.md §3).

Each non-blocking tick:
* boot: redis:boot_id vs run_id:epoch; a difference signals SM8 and ends the tick (SM8 owns Redis until READY);
* halt: halt:state.halt_event_id vs MAX(halt_events.id) (Redis read first): behind -> CAS re-apply; ahead ->
  alert, DEL, re-apply the latest row;
* Step 1: rows committed in [last_check - MAX_TXN_MS, now - 500 ms] plus a rotating sweep of K keys plus last
  tick's suspects, compared (version + checksum) with one pipelined Redis read; a mismatch must persist for two
  ticks before it is healed (behind -> CAS, ahead -> alert + DEL + re-apply); the full XOR digest is compared
  only when quiet, and a digest mismatch on two quiet ticks runs a full heal;
* Step 2: checksums and version / seq_id monotonicity (dbcheck); damage opens the incident path (incident).
Never pauses a symbol or stops consumers. `integrity_incident` feeds Oversee's snapshot flag.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Final, Literal

import redis.asyncio as aioredis

from src.cache.digest import ZERO_DIGEST
from src.cache.scripts import RedisState, read_boot_id
from src.integrity.dbcheck import DbFinding, DbIntegrityChecker, check_row
from src.integrity.heal import Healer, HealOutcome, NoBootError
from src.integrity.incident import IncidentMachine
from src.integrity.ports import (
    INCIDENT_STATES,
    INTEGRITY_CHECK_INTERVAL_SEC,
    SETTLE_MS,
    BootSignal,
    CommittedRow,
    IntegrityAlert,
    IntegrityDb,
    IntegrityState,
    Pager,
    null_pager,
)
from src.integrity.redis_ops import (
    BootIdentity,
    Drift,
    classify,
    last_feed_entry_ms,
    read_digest,
    read_meta,
    set_digest_if_unchanged,
)

log = logging.getLogger(__name__)

S = IntegrityState
HaltVerdict = Literal["match", "behind", "ahead", "none"]
DigestVerdict = Literal["match", "mismatch", "repaired", "not_quiet"]
DEFAULT_MAX_TXN_MS: Final = 15_000   # >= statement_timeout (5 s) + idle_in_transaction_session_timeout


@dataclass(frozen=True)
class IntegrityConfig:
    interval_s: float = INTEGRITY_CHECK_INTERVAL_SEC
    settle_ms: int = SETTLE_MS
    max_txn_ms: int = DEFAULT_MAX_TXN_MS
    window_limit: int = 5_000
    sweep_keys: int = 200
    quiet_ms: int = 500


@dataclass
class TickReport:
    boot_ok: bool = False
    halt: HaltVerdict = "none"
    compared: int = 0
    suspects: int = 0
    healed: dict[str, HealOutcome] = field(default_factory=dict)
    digest: DigestVerdict = "not_quiet"
    findings: list[DbFinding] = field(default_factory=list)
    incident: IntegrityState | None = None


class IntegrityWorker:
    def __init__(
        self,
        redis: aioredis.Redis,
        cache: RedisState,
        db: IntegrityDb,
        identity: BootIdentity,
        boot: BootSignal,
        incident: IncidentMachine,
        *,
        clock_ms: Callable[[], int],
        pager: Pager = null_pager,
        config: IntegrityConfig = IntegrityConfig(),
        checker: DbIntegrityChecker | None = None,
    ) -> None:
        self._redis = redis
        self._cache = cache
        self._db = db
        self._identity = identity
        self._boot = boot
        self.incident = incident
        self._clock_ms = clock_ms
        self._pager = pager
        self.cfg = config
        self.checker = checker if checker is not None else DbIntegrityChecker()
        self._healer = Healer(redis, cache, db, clock_ms=clock_ms, pager=pager)
        self._step = S.IDLE
        self._last_upper: int | None = None
        self._cursor = ""
        self._suspects: dict[str, Drift] = {}
        self._digest_mismatch_ticks = 0
        self.history: deque[tuple[IntegrityState, IntegrityState, str]] = deque(maxlen=500)
        self.last_report = TickReport()
        self.liveness_ms: int | None = None

    # ---- state ------------------------------------------------------------------------------------
    @property
    def state(self) -> IntegrityState:
        return self.incident.state if self.incident.state is not None else self._step

    @property
    def integrity_incident(self) -> bool:
        """True in HALTED / SWITCHING_OVER / MANUAL_INTERVENTION / RECONCILING (oversee snapshot flag)."""
        return self.state in INCIDENT_STATES

    async def operator_restored(self, restore_target_ms: int | None) -> None:
        await self.incident.operator_restored(restore_target_ms)

    def _set(self, new: IntegrityState, why: str = "") -> None:
        if new is not self._step:
            self.history.append((self._step, new, why))
            self._step = new

    # ---- loop -------------------------------------------------------------------------------------
    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.tick()
            except Exception:
                log.exception("integrity tick failed")
                self._set(S.IDLE, "tick error")
            self.liveness_ms = self._clock_ms()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), self.cfg.interval_s)

    async def tick(self) -> TickReport:
        rep = TickReport()
        self.last_report = rep
        if self.incident.active:
            await self.incident.advance()
            rep.incident = self.incident.state
            return rep
        now = self._clock_ms()
        self._set(S.CHECK_REDIS_VS_DB, "tick")
        try:
            expected = await self._identity.refresh()
            if await read_boot_id(self._redis) != expected:
                self._boot.signal("integrity: redis:boot_id != run_id:epoch")
                self._set(S.IDLE, "boot_id mismatch -> SM8")
                return rep
            rep.boot_ok = True
            rep.halt = await self._check_halt()
            rows = await self._step1(now, rep)
            rep.digest = await self._check_digest()
            self._set(S.CHECK_DB_INTEGRITY, "step 2")
            rep.findings = await self._step2(rows)
        except NoBootError as exc:
            self._boot.signal(f"integrity: NO_BOOT while healing {exc}")
        finally:
            if self._step is not S.IDLE:
                self._set(S.IDLE, "tick done")
        if rep.findings:
            await self.incident.open(rep.findings)
            rep.incident = self.incident.state
        return rep

    # ---- halt:state ---------------------------------------------------------------------------------
    async def _check_halt(self) -> HaltVerdict:
        try:
            current = await self._cache.read_halt_state()
        except (KeyError, ValueError):
            current = ("?", -1)                      # malformed hash: replace it
        latest = await self._db.latest_halt_event()
        if latest is None:
            if current is None:
                return "none"
            self._page("high", "halt_state_ahead", f"halt:state {current} but halt_events is empty: deleted")
            await self._cache.delete_halt_state()
            return "ahead"
        if current is not None and current[1] == latest.id and current[0] == latest.effective_level:
            return "match"
        if current is None or 0 <= current[1] < latest.id:
            await self._cache.set_halt_state(latest.effective_level, latest.id)
            return "behind"
        self._page("high", "halt_state_ahead",
                   f"halt:state {current} ahead of / different from halt_events {latest.id} "
                   f"{latest.effective_level}: deleted and re-applied")
        await self._cache.delete_halt_state()
        await self._cache.set_halt_state(latest.effective_level, latest.id)
        return "ahead"

    # ---- Step 1 ------------------------------------------------------------------------------------
    async def _step1(self, now: int, rep: TickReport) -> list[CommittedRow]:
        cfg = self.cfg
        upper = now - cfg.settle_ms
        lower = (self._last_upper if self._last_upper is not None else upper - int(cfg.interval_s * 1000))
        lower -= cfg.max_txn_ms
        window = list(await self._db.committed_rows(lower, upper, cfg.window_limit))
        truncated = len(window) >= cfg.window_limit
        self._last_upper = window[-1].committed_at_ms if truncated and window else upper
        sweep = list(await self._db.rows_after_key(self._cursor, cfg.sweep_keys))
        self._cursor = sweep[-1].key if len(sweep) >= cfg.sweep_keys else ""
        candidates = _newest_by_key(window + sweep)
        missing = [k for k in self._suspects if k not in candidates]
        extra = await self._db.rows_by_keys(missing) if missing else {}
        candidates.update(extra)
        keys = sorted(set(candidates) | set(self._suspects))
        metas = await read_meta(self._redis, keys)
        first = [k for k in keys if classify(candidates.get(k), metas.get(k)) is not Drift.MATCH]
        # Re-read PostgreSQL AFTER Redis: a key still differing now is not a copy in flight being "ahead".
        fresh = await self._db.rows_by_keys(first) if first else {}
        mismatched = {k: d for k in first
                      if (d := classify(fresh.get(k), metas.get(k))) is not Drift.MATCH}
        rep.compared = len(keys)
        persistent = [k for k in mismatched if k in self._suspects]
        if persistent:
            self._set(S.HEALING_REDIS, f"{len(persistent)} keys mismatched on two ticks")
            rep.healed = await self._healer.heal_keys(persistent)
        self._suspects = {k: d for k, d in mismatched.items() if k not in rep.healed}
        rep.suspects = len(self._suspects)
        return list(candidates.values()) + [r for r in fresh.values()]

    async def _check_digest(self) -> DigestVerdict:
        """Full XOR digest, only when quiet: no PostgreSQL commit and no fed write for quiet_ms."""
        quiet = self.cfg.quiet_ms
        latest0 = await self._db.latest_commit_ms()
        if latest0 is not None and self._clock_ms() - latest0 < quiet:
            return "not_quiet"
        sec, usec = await self._redis.time()
        redis_now = int(sec) * 1000 + int(usec) // 1000
        feed_ms = await last_feed_entry_ms(self._redis)
        if feed_ms is not None and redis_now - feed_ms < quiet:
            return "not_quiet"
        r0 = await read_digest(self._redis)
        pg = await self._db.digest()
        r1 = await read_digest(self._redis)
        if r1 != r0 or await self._db.latest_commit_ms() != latest0:
            return "not_quiet"
        if (r0 or ZERO_DIGEST) == pg:
            self._digest_mismatch_ticks = 0
            return "match"
        self._digest_mismatch_ticks += 1
        if self._digest_mismatch_ticks < 2:
            return "mismatch"
        self._digest_mismatch_ticks = 0
        self._set(S.HEALING_REDIS, "digest mismatch on two quiet ticks")
        report = await self._healer.heal_all()
        r2 = await read_digest(self._redis)
        pg2 = await self._db.digest()
        if (r2 or ZERO_DIGEST) == pg2:
            return "repaired"
        if await self._db.latest_commit_ms() == latest0 and await set_digest_if_unchanged(self._redis, r2, pg2):
            self._page("high", "digest_repaired",
                       f"every key matches PostgreSQL but state:digest drifted; reset ({report.rows} rows checked)")
            return "repaired"
        return "mismatch"

    # ---- Step 2 ------------------------------------------------------------------------------------
    async def _step2(self, rows: list[CommittedRow]) -> list[DbFinding]:
        findings = self.checker.check_rows(_newest_by_key(rows).values())
        content = [f for f in findings if f.kind in ("checksum", "digest_hash", "version_label")]
        if content:
            # Confirm row-content damage on a fresh read before halting ingestion.
            again = await self._db.rows_by_keys({f.key for f in content})
            confirmed = {(f.kind, f.key) for r in again.values() for f in check_row(r)}
            findings = [f for f in findings if f not in content or (f.kind, f.key) in confirmed]
        findings.extend(self.checker.check_high_water(await self._db.high_water()))
        return findings

    def _page(self, severity: Literal["info", "high", "critical"], code: str, detail: str) -> None:
        log.warning("integrity %s: %s", code, detail)
        self._pager(IntegrityAlert(severity, code, detail, self._clock_ms()))


def _newest_by_key(rows: Iterable[CommittedRow]) -> dict[str, CommittedRow]:
    """One row per key: the highest (version, seq_id) seen this tick."""
    out: dict[str, CommittedRow] = {}
    for r in rows:
        cur = out.get(r.key)
        if cur is None or (r.version, r.seq_id) > (cur.version, cur.seq_id):
            out[r.key] = r
    return out
