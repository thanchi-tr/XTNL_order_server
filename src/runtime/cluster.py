"""Cluster-wide state for the NodeController (operations.md §3, §4).

`ClusterControl` implements three runtime protocols over PostgreSQL `cluster_state` / `leader_terms`
(src/db/cluster_state.py) plus the etcd mirror:

* `ClusterStateReader.read()` (once per controller step): PostgreSQL is the truth. When PostgreSQL
  cannot be read, the etcd mirror `/xtnl/safe_hold` is used, and when neither answers, the last known
  value. It never raises: an unreadable cluster state must not crash SM9.
* `AbdicationLedger` (synchronous, src/runtime/roles.py): answered from the last read plus this
  process's not-yet-written changes; every change is written in a fenced transaction (leader only)
  scheduled on the running loop. `flush()` awaits the pending writes (DRAINING calls it).
* `TermStore.own_terms(node)`: the STARTING back-off input.

`mirror_safe_hold()` (the leader, every few seconds, and the node that sets SAFE_HOLD at LEADER_INIT)
writes PostgreSQL's SAFE_HOLD to etcd so `may_campaign()` sees it even while PostgreSQL is unreachable.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Sequence

from src.db.cluster_state import (
    ClusterStateRow,
    clear_broker_abdication,
    clear_risksweep_abdication,
    own_term_rows,
    read_cluster_state,
    record_broker_abdication,
    record_risksweep_abdication,
)
from src.db.epoch import Epoch, epoch_transaction
from src.db.fence import EpochMismatch, FencedTx
from src.db.pg import ConnectionSource
from src.runtime.etcd import SAFE_HOLD_KEY, EtcdClient
from src.runtime.roles import ClusterState, TermRow

log = logging.getLogger(__name__)

_END_REASONS = frozenset({"switchover", "abdicate", "lost", "init_failed"})


def mirror_json(cs: ClusterState) -> str:
    return json.dumps({"safe_hold": cs.safe_hold, "since": cs.safe_hold_since, "reason": cs.safe_hold_reason,
                       "last_resume_at": cs.last_resume_at}, sort_keys=True, separators=(",", ":"))


def parse_mirror(text: str) -> ClusterState:
    d = json.loads(text)
    if not isinstance(d, dict):
        raise ValueError("safe_hold mirror is not a JSON object")
    since, resume = d.get("since"), d.get("last_resume_at")
    return ClusterState(safe_hold=bool(d.get("safe_hold", False)),
                        safe_hold_since=None if since is None else int(since),
                        safe_hold_reason=None if d.get("reason") is None else str(d["reason"]),
                        last_resume_at=None if resume is None else int(resume))


def _cluster_state(row: ClusterStateRow) -> ClusterState:
    return ClusterState(row.safe_hold, row.safe_hold_since, row.safe_hold_reason, row.last_resume_at)


def term_from_row(r: Sequence[object]) -> TermRow:
    reason = None if r[6] is None else str(r[6])
    if reason is not None and reason not in _END_REASONS:
        raise ValueError(f"unknown leader_terms.end_reason {reason!r}")

    def opt(v: object) -> int | None:
        return None if v is None else int(str(v))

    return TermRow(str(r[0]), int(str(r[1])), str(r[2]), int(str(r[3])), opt(r[4]), opt(r[5]),
                   reason)  # type: ignore[arg-type]


class ClusterControl:
    def __init__(self, source: ConnectionSource, *, epoch: Callable[[], Epoch | None],
                 etcd: EtcdClient | None = None, etcd_timeout_s: float = 1.0,
                 clock_ms: Callable[[], int] | None = None) -> None:
        self._source = source
        self._epoch = epoch
        self._etcd = etcd
        self._etcd_timeout = etcd_timeout_s
        self._clock_ms = clock_ms
        self.last: ClusterState = ClusterState()
        self.last_row: ClusterStateRow | None = None
        self.source_of_last: str = "default"
        self._blocked: dict[str, int] = {}
        self._risksweep_done = False
        # changes made here but not yet written: adapter -> abdicated_at (int) or None (cleared)
        self._pending_broker: dict[str, int | None] = {}
        self._pending_risksweep: bool | None = None
        self._writes: set[asyncio.Task[None]] = set()
        self._mirrored: str | None = None
        self.write_failures = 0

    # --- ClusterStateReader -----------------------------------------------------------------------------
    async def read(self) -> ClusterState:
        try:
            async with self._source.acquire() as conn:
                row = await read_cluster_state(conn)
        except Exception as exc:
            log.warning("cluster_state unreadable (%s); trying the etcd mirror", type(exc).__name__)
            return await self._read_mirror()
        self._apply_row(row)
        self.last = _cluster_state(row)
        self.source_of_last = "postgres"
        return self.last

    async def _read_mirror(self) -> ClusterState:
        if self._etcd is not None:
            etcd = self._etcd
            try:
                kv = await asyncio.to_thread(etcd.get, SAFE_HOLD_KEY, timeout_s=self._etcd_timeout)
                if kv is not None:
                    self.last = parse_mirror(kv.value)
                    self.source_of_last = "etcd"
                    return self.last
            except Exception as exc:
                log.warning("etcd safe_hold mirror unreadable (%s); using the last known state",
                            type(exc).__name__)
        self.source_of_last = "last_known"
        return self.last

    def _apply_row(self, row: ClusterStateRow) -> None:
        self.last_row = row
        blocked = dict(row.broker_abdications)
        for adapter, at in self._pending_broker.items():
            if at is None:
                blocked.pop(adapter, None)
            else:
                blocked[adapter] = at
        self._blocked = blocked
        self._risksweep_done = (row.risksweep_abdicated_at is not None if self._pending_risksweep is None
                                else self._pending_risksweep)

    # --- TermStore --------------------------------------------------------------------------------------
    async def own_terms(self, node_id: str) -> Sequence[TermRow]:
        async with self._source.acquire() as conn:
            rows = await own_term_rows(conn, node_id)
        return [term_from_row(r) for r in rows]

    # --- AbdicationLedger -------------------------------------------------------------------------------
    def risksweep_abdicated(self) -> bool:
        return self._risksweep_done

    def record_risksweep_abdication(self, now_ms: int) -> None:
        self._risksweep_done = True
        self._pending_risksweep = True
        incident = f"risksweep:{now_ms}"
        self._schedule("risksweep_abdicated", lambda tx: record_risksweep_abdication(tx, incident, now_ms),
                       lambda: self._settle_risksweep(True))

    def risksweep_recovered(self) -> None:
        if not self._risksweep_done:
            return
        self._risksweep_done = False
        self._pending_risksweep = False
        self._schedule("risksweep_recovered", clear_risksweep_abdication, lambda: self._settle_risksweep(False))

    def broker_blocked(self, adapter_id: str) -> bool:
        return adapter_id in self._blocked

    def record_broker_abdication(self, adapter_id: str, now_ms: int) -> None:
        self._blocked[adapter_id] = now_ms
        self._pending_broker[adapter_id] = now_ms
        self._schedule(f"broker_abdicated:{adapter_id}",
                       lambda tx: record_broker_abdication(tx, adapter_id, now_ms),
                       lambda: self._settle_broker(adapter_id, now_ms))

    def adapter_streaming(self, adapter_id: str) -> None:
        if adapter_id not in self._blocked:
            return
        del self._blocked[adapter_id]
        self._pending_broker[adapter_id] = None
        self._schedule(f"broker_streaming:{adapter_id}", lambda tx: clear_broker_abdication(tx, adapter_id),
                       lambda: self._settle_broker(adapter_id, None))

    def _settle_broker(self, adapter_id: str, value: int | None) -> None:
        if adapter_id in self._pending_broker and self._pending_broker[adapter_id] == value:
            del self._pending_broker[adapter_id]

    def _settle_risksweep(self, value: bool) -> None:
        if self._pending_risksweep is value:
            self._pending_risksweep = None

    def _schedule(self, what: str, write: Callable[[FencedTx], Awaitable[None]],
                  settled: Callable[[], None]) -> None:
        epoch = self._epoch()
        if epoch is None:
            log.warning("cluster_state %s not written: this node holds no epoch", what)
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            log.warning("cluster_state %s not written: no running event loop", what)
            return
        task = loop.create_task(self._write(what, epoch, write, settled), name=f"cluster_state:{what}")
        self._writes.add(task)
        task.add_done_callback(self._writes.discard)

    async def _write(self, what: str, epoch: Epoch, write: Callable[[FencedTx], Awaitable[None]],
                     settled: Callable[[], None]) -> None:
        try:
            async with self._source.acquire() as conn:
                async with epoch_transaction(conn, epoch) as tx:
                    await write(tx)
        except EpochMismatch:
            self.write_failures += 1
            log.critical("cluster_state %s refused: the fence is lost (epoch %s)", what, epoch.label)
            return
        except Exception as exc:
            self.write_failures += 1
            log.error("cluster_state %s not written: %r", what, exc)
            return
        settled()

    async def flush(self, timeout_s: float = 2.0) -> None:
        """Await the pending fenced writes (DRAINING, before the lease is released)."""
        if self._writes:
            await asyncio.wait(set(self._writes), timeout=timeout_s)

    # --- etcd mirror ------------------------------------------------------------------------------------
    async def mirror_safe_hold(self, *, force: bool = False) -> bool:
        """Write PostgreSQL's SAFE_HOLD to /xtnl/safe_hold (no lease: the key outlives the leader).
        Returns True when written; a no-op when unchanged since this process's last write."""
        if self._etcd is None:
            return False
        async with self._source.acquire() as conn:
            row = await read_cluster_state(conn)
        self._apply_row(row)
        self.last = _cluster_state(row)
        text = mirror_json(self.last)
        if text == self._mirrored and not force:
            return False
        etcd = self._etcd
        await asyncio.to_thread(etcd.put, SAFE_HOLD_KEY, text, timeout_s=self._etcd_timeout)
        self._mirrored = text
        log.info("SAFE_HOLD mirrored to etcd: %s", "set" if self.last.safe_hold else "clear")
        return True

