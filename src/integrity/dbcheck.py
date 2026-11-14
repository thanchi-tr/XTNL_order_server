"""SM3 Step 2: PostgreSQL integrity checks (tier-persistence.md §3 step 2, operations.md §7).

Pure and synchronous: callers feed it rows and high-water marks read through IntegrityDb.
* checksum: the stored checksum equals payload_checksum() of the row rebuilt as a DTO, and the stored
  digest_hash equals sha256(key || version || checksum); a DTO row's version column equals version_str().
* version monotonicity: a key's (version, seq_id) never goes down between observations.
* seq_id monotonicity: MAX(seq_id) never goes down. The HLC has gaps by design, so only a decrease counts.
  Timeline-aware: a decrease within one Patroni timeline is damage; a decrease across a timeline switch is
  data lost by a failover (`failover_regression`), which needs a broker reconcile rather than a switchover.
  Epoch-aware: an epoch change (new leader) never resets the baseline, because the new leader re-seeds its
  HLC from MAX(seq_id) and so can only move it up.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final, Literal

from src.cache.digest import entity_hash
from src.cache.scripts import SEQ_WIDTH, pad
from src.dto import InstrumentSpec, payload_checksum, version_str
from src.integrity.ports import CommittedRow, DbHighWater

FindingKind = Literal["checksum", "digest_hash", "version_label", "version_regression", "seq_regression",
                      "failover_regression"]
DAMAGE_KINDS: Final = frozenset({"checksum", "digest_hash", "version_label", "version_regression",
                                 "seq_regression"})


@dataclass(frozen=True)
class DbFinding:
    kind: FindingKind
    key: str
    detail: str

    @property
    def is_damage(self) -> bool:
        return self.kind in DAMAGE_KINDS


def check_row(row: CommittedRow) -> list[DbFinding]:
    """Checksum / digest-hash / version-label consistency of one stored row."""
    out: list[DbFinding] = []
    if row.entity is not None:
        recomputed = payload_checksum(row.entity)
        if recomputed != row.checksum:
            out.append(DbFinding("checksum", row.key, f"stored {row.checksum[:12]} != computed {recomputed[:12]}"))
        if not isinstance(row.entity, InstrumentSpec):
            label = version_str(row.entity)
            if label != row.version:
                out.append(DbFinding("version_label", row.key, f"version column {row.version} != {label}"))
    expected_hash = entity_hash(row.key, row.version, row.checksum)
    if expected_hash != row.digest_hash:
        out.append(DbFinding("digest_hash", row.key, "digest_hash does not match key || version || checksum"))
    return out


class DbIntegrityChecker:
    """Keeps the monotonicity baselines (bounded) between ticks."""

    def __init__(self, max_tracked: int = 200_000) -> None:
        self._max = max_tracked
        self._seen: OrderedDict[str, tuple[str, str]] = OrderedDict()
        self.baseline: DbHighWater | None = None

    def reset(self) -> None:
        """After a switchover or an operator restore: the new primary's history is the baseline."""
        self._seen.clear()
        self.baseline = None

    def check_rows(self, rows: Iterable[CommittedRow]) -> list[DbFinding]:
        out: list[DbFinding] = []
        for row in rows:
            out.extend(check_row(row))
            cur = (row.version, pad(row.seq_id, SEQ_WIDTH))
            prev = self._seen.get(row.key)
            if prev is not None and cur < prev:
                out.append(DbFinding("version_regression", row.key,
                                     f"version went back from {prev[0]}/{prev[1]} to {cur[0]}/{cur[1]}"))
                continue    # keep the higher baseline: the regression stays visible until resolved
            self._seen[row.key] = cur
            self._seen.move_to_end(row.key)
            while len(self._seen) > self._max:
                self._seen.popitem(last=False)
        return out

    def check_high_water(self, hw: DbHighWater) -> list[DbFinding]:
        base = self.baseline
        if base is None or hw.max_seq_id >= base.max_seq_id:
            self.baseline = hw
            return []
        detail = (f"MAX(seq_id) {base.max_seq_id} -> {hw.max_seq_id} (timeline {base.timeline} -> {hw.timeline}, "
                  f"epoch {base.epoch} -> {hw.epoch})")
        if hw.timeline != base.timeline:
            return [DbFinding("failover_regression", "seq_id", detail)]
        return [DbFinding("seq_regression", "seq_id", detail)]
