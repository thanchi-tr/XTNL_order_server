"""The tailer: exactly once, by database-issued ids (docs/design/live-view.md s4.3).

Commit order is not id order (up to MAX_TXN_MS between `nextval` and commit) and no clock column is a usable
cursor, so each append-only view is tailed by its bigserial key with:
- `H` the largest id seen, `C` the contiguous floor (every id <= C is done);
- `pending`: ids in (C, H] not seen yet, each with the database time it was first found missing;
- `seen`: ids in (C, H] already emitted;
- `expired`: ids that left `pending` unseen (at most 10 000, kept 24 h), rechecked every 60 s.
No clock value affects correctness; time columns are only wide partition-pruning bounds. Rows changed in place are
followed in watched sets, and small tables are re-read whole and diffed on values. The first reading after start
is silent.
"""
from __future__ import annotations

import copy
import logging
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Protocol

from src.liveview import sql as Q

log = logging.getLogger("xtnl.liveview.tailer")

MAX_TXN_MS: Final = 15_000
DAY_MS: Final = 86_400_000
EXPIRED_CAP: Final = 10_000
EXPIRED_KEEP_MS: Final = DAY_MS
RECHECK_EVERY_MS: Final = 60_000
START_WINDOW: Final = 1000
MAX_GAP_IDS: Final = 200_000
DLQ_FINAL_AFTER_MS: Final = 600_000

Row = dict[str, Any]


class Conn(Protocol):
    async def fetch(self, sql: str, *args: object) -> Sequence[Any]: ...

    async def fetchval(self, sql: str, *args: object) -> Any: ...


def as_row(record: Any) -> Row:
    return dict(record.items()) if hasattr(record, "items") else dict(record)


def hold_ms(poll_ms: int) -> int:
    """How long a missing id is held before it expires: MAX_TXN_MS + 2 polls + 30 s (47 s at defaults)."""
    return MAX_TXN_MS + 2 * poll_ms + 30_000


class IdCursor:
    """The per-source cursor state (pure logic, no I/O)."""

    def __init__(self) -> None:
        self.h = 0
        self.c = 0
        self.pending: dict[int, int] = {}
        self.seen: set[int] = set()
        self.expired: OrderedDict[int, int] = OrderedDict()
        self.evicted = 0

    def start(self, h: int, present: set[int], db_now: int) -> None:
        self.h = self.c = max(0, h)
        for k in range(max(0, self.h - START_WINDOW) + 1, self.h + 1):
            if k not in present:
                self.pending[k] = db_now
        self._floor()

    def accept(self, ids: Sequence[int], db_now: int) -> list[int]:
        """Ids of a poll's rows (ascending); returns the ids to emit (not seen before), and records the gaps."""
        out: list[int] = []
        returned = set(ids)
        for k in ids:
            if k in self.seen or (k <= self.c and k not in self.pending and k not in self.expired):
                continue
            out.append(k)
            self.pending.pop(k, None)
            if k > self.c:
                self.seen.add(k)
        new_h = max([self.h, *ids]) if ids else self.h
        lo = max(self.h + 1, new_h - MAX_GAP_IDS + 1)
        for k in range(lo, new_h + 1):
            if k not in returned and k not in self.seen:
                self.pending[k] = db_now
        self.h = new_h
        return out

    def expire(self, db_now: int, hold: int) -> int:
        gone = [k for k, t in self.pending.items() if db_now - t > hold]
        for k in gone:
            del self.pending[k]
            self.expired[k] = db_now
        while len(self.expired) > EXPIRED_CAP:
            self.expired.popitem(last=False)
            self.evicted += 1
        self._floor()
        return len(gone)

    def evict_old(self, db_now: int) -> int:
        old = [k for k, t in self.expired.items() if db_now - t > EXPIRED_KEEP_MS]
        for k in old:
            del self.expired[k]
        return len(old)

    def late_found(self, k: int) -> bool:
        return self.expired.pop(k, None) is not None

    def _floor(self) -> None:
        self.c = (min(self.pending) - 1) if self.pending else self.h
        self.seen = {k for k in self.seen if k > self.c}


@dataclass
class Watched:
    """Rows changed in place, followed by id until final (s4.3 watched sets)."""

    rows: dict[Any, Row] = field(default_factory=dict)
    final_since: dict[Any, int] = field(default_factory=dict)


@dataclass
class PollRows:
    db_now: int
    runs_new: list[Row] = field(default_factory=list)
    runs_changed: list[tuple[Row, Row]] = field(default_factory=list)
    tail: dict[str, list[Row]] = field(default_factory=dict)
    late: dict[str, list[Row]] = field(default_factory=dict)
    terms_changed: list[tuple[Row, Row]] = field(default_factory=list)
    alerts_changed: list[tuple[Row, Row]] = field(default_factory=list)
    dlq_changed: list[tuple[Row, Row]] = field(default_factory=list)
    streams: list[Row] = field(default_factory=list)
    streams_changed: list[tuple[Row | None, Row]] = field(default_factory=list)
    cluster_changed: tuple[Row, Row] | None = None
    cluster: Row | None = None
    epoch_changed: tuple[Row, Row] | None = None
    epoch: Row | None = None
    quarantine: list[Row] = field(default_factory=list)
    quarantine_changed: list[tuple[Row | None, Row]] = field(default_factory=list)
    auth: list[Row] = field(default_factory=list)
    auth_changed: list[tuple[Row | None, Row]] = field(default_factory=list)
    backlog: int = 0
    expired: int = 0


@dataclass
class Backfill:
    db_now: int
    rows: dict[str, list[Row]] = field(default_factory=dict)        # oldest first
    partial_from: dict[str, int] = field(default_factory=dict)      # source -> oldest loaded time (ms) if capped
    terms_open: list[Row] = field(default_factory=list)
    runs: list[Row] = field(default_factory=list)
    streams: list[Row] = field(default_factory=list)
    cluster: Row | None = None
    epoch: Row | None = None
    quarantine: list[Row] = field(default_factory=list)
    auth: list[Row] = field(default_factory=list)


def _alert_key(r: Row) -> tuple[Any, ...]:
    return (r.get("delivery"), r.get("escalated"), (r.get("attempts") or 0) > 0)


def _dlq_key(r: Row) -> tuple[Any, ...]:
    return (r.get("status"), r.get("retry_count"))


def _term_key(r: Row) -> tuple[Any, ...]:
    return (r.get("reached_leader_at") is not None, r.get("end_reason"), r.get("ended_at"))


def _run_key(r: Row) -> tuple[Any, ...]:
    return (r.get("stopped_at_ms"), r.get("end_code"))


def _prune(src: Q.TailSource, ms: int) -> int:
    return ms << src.shift if src.shift else ms


class Tailer:
    def __init__(self, *, poll_ms: int = 1000, history: int = 2000, backfill_h: int = 24) -> None:
        self.poll_ms = poll_ms
        self.history = history
        self.backfill_h = backfill_h
        self.cursors: dict[str, IdCursor] = {s.name: IdCursor() for s in Q.TAIL_SOURCES}
        self.alerts = Watched()
        self.dlq = Watched()
        self.dlq_adopt: set[int] = set()       # origin rows named by a lineage, watched from their next read
        self.terms = Watched()
        self.runs: dict[str, Row] = {}
        self.streams: dict[tuple[str, str], Row] = {}
        self.quarantine: dict[tuple[str, str], Row] = {}
        self.auth: dict[str, Row] = {}
        self.cluster: Row | None = None
        self.epoch: Row | None = None
        self.last_recheck = 0
        self.started = False

    # ---- all-or-nothing polls ----------------------------------------------------------------------------------
    def checkpoint(self) -> dict[str, Any]:
        """A copy of every piece of tailer state, taken before a poll; `restore` puts it back when the poll's
        transaction fails, so the next poll reads the same ids again and no row is lost."""
        return copy.deepcopy(self.__dict__)

    def restore(self, saved: dict[str, Any]) -> None:
        self.__dict__.clear()
        self.__dict__.update(saved)

    # ---- start ------------------------------------------------------------------------------------------------
    async def start(self, conn: Conn, db_now: int, day_start_ms: int) -> Backfill:
        """Cursors at the newest row of each source, pending for in-flight ids, the display-only backfill, the
        silent seeds of the watched sets and the diff sources."""
        bf = Backfill(db_now=db_now)
        since = min(db_now - self.backfill_h * 3_600_000, day_start_ms)
        for src in Q.TAIL_SOURCES:
            h = None
            if src.prune:
                h = await conn.fetchval(src.sql_max_pruned, _prune(src, db_now - 7 * DAY_MS))
            if h is None:
                h = await conn.fetchval(src.sql_max)
            h = int(h or 0)
            args: list[object] = [max(0, h - START_WINDOW), h]
            if src.prune:
                args.append(_prune(src, db_now - 7 * DAY_MS))
            present = {int(as_row(r)[src.key]) for r in await conn.fetch(src.sql_present, *args)}
            self.cursors[src.name].start(h, present, db_now)
            rows = [as_row(r) for r in await conn.fetch(src.sql_backfill, h, _prune(src, since), self.history)]
            rows.reverse()
            bf.rows[src.name] = rows
            if len(rows) >= self.history and rows:
                t = rows[0].get(src.backfill_time)
                bf.partial_from[src.name] = (int(t) >> src.shift) if isinstance(t, int) else since
        for r in (as_row(x) for x in await conn.fetch(Q.SQL_ALERTS_SEED, db_now - 7 * DAY_MS)):
            self.alerts.rows[r["outbox_id"]] = r
        for r in (as_row(x) for x in await conn.fetch(Q.SQL_DLQ_SEED)):
            self.dlq.rows[r["dlq_id"]] = r
        for r in (as_row(x) for x in await conn.fetch(Q.SQL_TERMS_SEED)):
            self.terms.rows[r["term_id"]] = r
            bf.terms_open.append(r)
        for r in bf.rows.get("terms", []):
            if r.get("ended_at") is None:
                self.terms.rows[r["term_id"]] = r
        for r in bf.rows.get("alerts", []):
            if r.get("kind") != "arm" and r.get("sent_at_ms") is None:
                self.alerts.rows[r["outbox_id"]] = r
        for r in bf.rows.get("dlq", []):
            if r.get("status") != "RESOLVED":
                self.dlq.rows.setdefault(r["dlq_id"], r)
        bf.runs = [as_row(x) for x in await conn.fetch(Q.SQL_RUNS_DISCOVER)]
        self.runs = {r["run_id"]: r for r in bf.runs}
        bf.streams = [as_row(x) for x in await conn.fetch(Q.SQL_STREAMS)]
        self.streams = {(r["adapter_id"], r["stream"]): r for r in bf.streams}
        bf.cluster = self.cluster = _first(await conn.fetch(Q.SQL_CLUSTER))
        bf.epoch = self.epoch = _first(await conn.fetch(Q.SQL_EPOCH))
        bf.quarantine = [as_row(x) for x in await conn.fetch(Q.SQL_QUARANTINE)]
        self.quarantine = {(r["adapter_id"], r["account_id"]): r for r in bf.quarantine}
        bf.auth = [as_row(x) for x in await conn.fetch(Q.SQL_AUTH)]
        self.auth = {r["adapter_id"]: r for r in bf.auth}
        self.last_recheck = db_now
        self.started = True
        return bf

    # ---- each poll --------------------------------------------------------------------------------------------
    async def poll(self, conn: Conn, db_now: int) -> PollRows:
        out = PollRows(db_now=db_now)
        hold = hold_ms(self.poll_ms)
        # engine runs: discovery by the newest 50, diffed by run_id
        runs = [as_row(x) for x in await conn.fetch(Q.SQL_RUNS_DISCOVER)]
        for r in sorted(runs, key=lambda x: (x.get("started_at_ms") or 0)):
            old = self.runs.get(r["run_id"])
            if old is None:
                out.runs_new.append(r)
            elif _run_key(old) != _run_key(r):
                out.runs_changed.append((old, r))
        self.runs = {r["run_id"]: r for r in runs}
        # append-only sources by id cursor, in narration order
        for src in Q.TAIL_SOURCES:
            rows, capped = await self._tail(conn, src, db_now)
            out.tail[src.name] = rows
            if capped:
                top = await conn.fetchval(src.sql_max)
                out.backlog += max(0, int(top or 0) - self.cursors[src.name].h)
            out.expired += self.cursors[src.name].expire(db_now, hold)
        # late recheck of expired ids
        if db_now - self.last_recheck >= RECHECK_EVERY_MS:
            self.last_recheck = db_now
            for src in Q.TAIL_SOURCES:
                cur = self.cursors[src.name]
                cur.evict_old(db_now)
                if not cur.expired:
                    continue
                args: list[object] = [list(cur.expired)]
                if src.prune:
                    args.append(_prune(src, db_now - 7 * DAY_MS))
                found = [as_row(x) for x in await conn.fetch(src.sql_recheck, *args)]
                late = [r for r in found if cur.late_found(int(r[src.key]))]
                if late:
                    out.late[src.name] = late
        if out.expired:
            log.info("ids expired from pending", extra={"event": "expired", "count": out.expired})
        evicted = sum(c.evicted for c in self.cursors.values())
        if evicted:
            log.info("expired ids evicted", extra={"event": "expired_evicted", "count": evicted})
            for c in self.cursors.values():
                c.evicted = 0
        # watched sets: new rows enter, changed keys are reported, final rows leave
        self._enter_watched(out)
        got = await _watched_read(conn, Q.SQL_TERMS_WATCHED, list(self.terms.rows))
        out.terms_changed = self._diff_watched(self.terms, got, "term_id", _term_key,
                                               lambda r: r.get("ended_at") is not None, db_now, 0)
        lo = min((int(r.get("created_at_ms") or db_now) for r in self.alerts.rows.values()), default=db_now)
        got = await _watched_read(conn, Q.SQL_ALERTS_WATCHED, list(self.alerts.rows), lo)
        out.alerts_changed = self._diff_watched(self.alerts, got, "outbox_id", _alert_key,
                                                lambda r: r.get("sent_at_ms") is not None, db_now, 0)
        # an origin named by a watched or new row is followed too (a lineage merge re-arms it in place); it joins
        # the set silently at its first read, so only its later changes are reported
        for r in [*self.dlq.rows.values(), *out.tail.get("dlq", []), *out.late.get("dlq", [])]:
            origin = r.get("origin_dlq_id")
            if origin is not None and origin not in self.dlq.rows:
                self.dlq_adopt.add(int(origin))
        adopt = sorted(self.dlq_adopt - set(self.dlq.rows))
        got = await _watched_read(conn, Q.SQL_DLQ_WATCHED, list(self.dlq.rows) + adopt)
        adopted = {r["dlq_id"]: r for r in got if r["dlq_id"] in set(adopt)}
        got = [r for r in got if r["dlq_id"] not in adopted]
        self.dlq_adopt.clear()
        out.dlq_changed = self._diff_watched(self.dlq, got, "dlq_id", _dlq_key,
                                             lambda r: r.get("status") == "RESOLVED", db_now, DLQ_FINAL_AFTER_MS)
        for k, r in adopted.items():
            self.dlq.rows[k] = r            # a RESOLVED origin still leaves after DLQ_FINAL_AFTER_MS, as any row
        # diff sources
        out.streams = [as_row(x) for x in await conn.fetch(Q.SQL_STREAMS)]
        out.streams_changed = _diff_keyed(self.streams, out.streams, lambda r: (r["adapter_id"], r["stream"]),
                                          ("conn_lost_at", "reconciled_at", "reconciled_epoch"))
        out.cluster = _first(await conn.fetch(Q.SQL_CLUSTER))
        if out.cluster and self.cluster and _vals(out.cluster, ("safe_hold", "safe_hold_since", "last_resume_at")) \
                != _vals(self.cluster, ("safe_hold", "safe_hold_since", "last_resume_at")):
            out.cluster_changed = (self.cluster, out.cluster)
        self.cluster = out.cluster or self.cluster
        out.epoch = _first(await conn.fetch(Q.SQL_EPOCH))
        if out.epoch and self.epoch and out.epoch.get("epoch") != self.epoch.get("epoch"):
            out.epoch_changed = (self.epoch, out.epoch)
        self.epoch = out.epoch or self.epoch
        out.quarantine = [as_row(x) for x in await conn.fetch(Q.SQL_QUARANTINE)]
        out.quarantine_changed = _diff_keyed(self.quarantine, out.quarantine,
                                             lambda r: (r["adapter_id"], r["account_id"]),
                                             ("quarantined_at_ms", "released_at_ms"))
        out.auth = [as_row(x) for x in await conn.fetch(Q.SQL_AUTH)]
        out.auth_changed = _diff_keyed(self.auth, out.auth, lambda r: r["adapter_id"],
                                       ("state", "failed_at_ms", "rearmed_at_ms"))
        return out

    async def _tail(self, conn: Conn, src: Q.TailSource, db_now: int) -> tuple[list[Row], bool]:
        cur = self.cursors[src.name]
        pending = sorted(cur.pending)
        last = -1
        rows: list[Row] = []
        capped = False
        for page in range(Q.MAX_PAGES):
            args: list[object] = [cur.h, [p for p in pending if p > last], last]
            if src.prune:
                args.append(_prune(src, db_now - DAY_MS))
            got = [as_row(x) for x in await conn.fetch(src.sql_tail, *args)]
            rows.extend(got)
            if len(got) < Q.PAGE_ROWS:
                break
            last = int(got[-1][src.key])
            if page == Q.MAX_PAGES - 1:
                capped = True
        ids = [int(r[src.key]) for r in rows]
        keep = set(cur.accept(ids, db_now))
        return [r for r in rows if int(r[src.key]) in keep], capped

    def _enter_watched(self, out: PollRows) -> None:
        """New rows enter the watched sets, whether the cursor or the late recheck found them."""
        for found in (out.tail, out.late):
            for r in found.get("terms", []):
                if r.get("ended_at") is None:
                    self.terms.rows[r["term_id"]] = r
            for r in found.get("alerts", []):
                if r.get("kind") != "arm" and r.get("sent_at_ms") is None:
                    self.alerts.rows[r["outbox_id"]] = r
            for r in found.get("dlq", []):
                self.dlq.rows[r["dlq_id"]] = r

    @staticmethod
    def _diff_watched(w: Watched, got: list[Row], key: str, track: Any, final: Any, db_now: int,
                      final_after: int) -> list[tuple[Row, Row]]:
        changed: list[tuple[Row, Row]] = []
        returned = {r[key]: r for r in got}
        for k in list(w.rows):
            new = returned.get(k)
            if new is None:          # moved to history or detached: leave silently, the last state stands
                w.rows.pop(k, None)
                w.final_since.pop(k, None)
                continue
            old = w.rows[k]
            if track(old) != track(new):
                changed.append((old, new))
            w.rows[k] = new
            if final(new):
                since = w.final_since.setdefault(k, db_now)
                if db_now - since >= final_after:
                    w.rows.pop(k, None)
                    w.final_since.pop(k, None)
            else:
                w.final_since.pop(k, None)
        return changed


WATCHED_CHUNK: Final = 2000


async def _watched_read(conn: Conn, sql: str, ids: list[Any], *extra: object) -> list[Row]:
    """A watched set read in chunks well under the statement's LIMIT, so no answer is ever truncated and an id the
    answer leaves out really is gone."""
    out: list[Row] = []
    for i in range(0, len(ids), WATCHED_CHUNK):
        out += [as_row(x) for x in await conn.fetch(sql, ids[i:i + WATCHED_CHUNK], *extra)]
    return out


def _first(rows: Sequence[Any]) -> Row | None:
    return as_row(rows[0]) if rows else None


def _vals(r: Mapping[str, Any], names: Sequence[str]) -> tuple[Any, ...]:
    return tuple(r.get(n) for n in names)


def _diff_keyed(state: dict[Any, Row], rows: list[Row], key: Any, names: Sequence[str]) -> list[tuple[Row | None, Row]]:
    """Value diff of a re-read source; updates `state` in place. Keys absent from the read are forgotten."""
    changed: list[tuple[Row | None, Row]] = []
    seen = set()
    for r in rows:
        k = key(r)
        seen.add(k)
        old = state.get(k)
        if old is None or _vals(old, names) != _vals(r, names):
            changed.append((old, r))
        state[k] = r
    for k in [k for k in state if k not in seen]:
        del state[k]
    return changed


__all__ = ["Backfill", "IdCursor", "MAX_TXN_MS", "PollRows", "Tailer", "Watched", "as_row", "hold_ms"]
