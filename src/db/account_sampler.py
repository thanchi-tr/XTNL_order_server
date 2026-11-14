"""Live account sampler: `AccountLive` -> `account_snapshots` rows with `live_ts_ms` (IMPLEMENTATION_PLAN 4.8;
tier-persistence.md §4 "live fields are sampled into it at a fixed interval, not on every tick").

SM8 step 3 seeds `account_live:{id}` from the newest such row (marked `sampled`), so a new leader or a
Redis rebuild is never blind on margin and equity.

* `AccountLiveSampler.offer(live)` is synchronous and does no I/O (the account consumer calls it after the
  Redis write); it keeps only the newest value per account. Seeded (`sampled`) values are never re-sampled.
* `flush()` writes, per account, the newest value once `interval_ms` has passed since that account's last
  written sample (and only if it is newer than it), in one fenced transaction. A write failure keeps the
  values for the next flush; a lost fence (`EpochMismatch`) propagates (the runtime hard-exits).
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Final, Protocol

from src.db.epoch import EngineDb
from src.db.fence import FENCE, EpochMismatch
from src.dto import AccountLive

log = logging.getLogger(__name__)

DEFAULT_SAMPLE_INTERVAL_MS: Final = 60_000

SQL_LIVE_SAMPLE: Final = (
    "INSERT INTO account_snapshots (adapter_id, account_id, seq_id, currency, equity, margin_used, free_margin, "
    "unrealised_pnl, live_ts_ms, sampled) SELECT $2::text, $3::text, $4::bigint, $5::text, $6::numeric, "
    f"$7::numeric, $8::numeric, $9::numeric, $10::bigint, true WHERE {FENCE} ON CONFLICT DO NOTHING"
)


@dataclass(frozen=True)
class LiveSample:
    live: AccountLive
    seq_id: int


class LiveSampleStore(Protocol):
    async def write_samples(self, samples: Sequence[LiveSample]) -> None: ...


class PgLiveSampleStore:
    """LiveSampleStore: one fenced transaction per batch."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db

    async def write_samples(self, samples: Sequence[LiveSample]) -> None:
        if not samples:
            return
        async with self._db.transaction() as tx:
            for s in samples:
                v = s.live
                # ON CONFLICT DO NOTHING may legitimately insert 0 rows: must=False re-checks the fence.
                await tx.write(SQL_LIVE_SAMPLE, v.adapter_id, v.account_id, s.seq_id, v.currency, v.equity,
                               v.margin_used, v.free_margin, v.unrealised_pnl, v.timestamp_ms)


class AccountLiveSampler:
    """Rate-limited sampler of live account fields (see the module docstring)."""

    def __init__(self, store: LiveSampleStore, seq: Callable[[], int], *, clock_ms: Callable[[], int],
                 interval_ms: int = DEFAULT_SAMPLE_INTERVAL_MS) -> None:
        self._store = store
        self._seq = seq
        self._clock = clock_ms
        self.interval_ms = interval_ms
        self._latest: dict[str, AccountLive] = {}
        self._written_at: dict[str, int] = {}       # account -> wall time of its last written sample
        self._written_ts: dict[str, int] = {}       # account -> live_ts_ms of its last written sample
        self.written = 0

    def offer(self, live: AccountLive) -> bool:
        """Keep the newest live value of the account (no I/O). False when ignored."""
        if live.sampled:
            return False
        prev = self._latest.get(live.account_id)
        if prev is not None and prev.timestamp_ms > live.timestamp_ms:
            return False
        self._latest[live.account_id] = live
        return True

    def due(self) -> list[AccountLive]:
        now = self._clock()
        out: list[AccountLive] = []
        for acct, live in sorted(self._latest.items()):
            last = self._written_at.get(acct)
            if last is not None and now - last < self.interval_ms:
                continue
            if live.timestamp_ms <= self._written_ts.get(acct, -1):
                continue
            out.append(live)
        return out

    async def flush(self) -> int:
        """Write every due sample; returns how many were written."""
        due = self.due()
        if not due:
            return 0
        samples = [LiveSample(live, self._seq()) for live in due]
        try:
            await self._store.write_samples(samples)
        except EpochMismatch:
            raise
        except Exception as exc:
            log.warning("account live sample write failed (%d accounts): %r", len(samples), exc)
            return 0
        now = self._clock()
        for live in due:
            self._written_at[live.account_id] = now
            self._written_ts[live.account_id] = live.timestamp_ms
        self.written += len(due)
        return len(due)


__all__ = ["AccountLiveSampler", "LiveSample", "LiveSampleStore", "PgLiveSampleStore", "SQL_LIVE_SAMPLE",
           "DEFAULT_SAMPLE_INTERVAL_MS"]
