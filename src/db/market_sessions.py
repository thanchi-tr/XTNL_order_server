"""SM10 calendar persistence: versioned `market_sessions` rows (operations.md §10, tier-persistence.md §4;
IMPLEMENTATION_PLAN 5.5 for 5.1).

Sessions and holidays are loaded per (adapter, symbol) from the brokers (cTrader symbol schedule and
holidays; StoneX market information or a reviewed config) and **versioned** in PostgreSQL. The runtime
reads the latest version (`src/runtime/pg_ports.py PgScheduleLoader`); this module is the write side.

Row shape (`MarketSessionRow`), the contract between 5.1's read-only adapter method and this store. The
adapter (or the 5.6 wiring around it) maps whatever it fetched into these rows:

    adapter_id  "pepperstone" | "stonex" | ...
    symbol      the canonical symbol the adapter streams
    timezone    IANA zone the session times are written in (e.g. "America/New_York")
    schedule    {"sessions": [{"open": {"day": "SUN", "time": "17:00"}, "close": {"day": "FRI", "time": "17:00"}}],
                 "breaks":   [{"start": "17:00", "end": "17:05", "days": ["MON", "TUE", "WED", "THU"]}]}
    holidays    [{"date": "2026-12-25"} | {"start": "2026-12-24T13:00", "end": "2026-12-24T17:00", "reason": "..."}]
    source      "ctrader" | "stonex" | "mock" | "reviewed_config"
    is_default / holidays_known / note   the review flags of 5.1's `MarketSessions` (0010)

5.1's read-only `BrokerAdapter.fetch_market_sessions()` returns `src.adapters.base.MarketSessions`, whose
`timezone` / `schedule` / `holidays` are already these columns: `MarketSessionRow.from_sessions(ms)` converts
one without loss (the 5.6 wiring calls it on each fetched value).

(the JSON layout of `src/calendar/schedule.py`; every row is parsed with `symbol_schedule_from_json`
before anything is written, so the loader never has to skip a row this store wrote).

`PgMarketSessionStore.save(rows, adapters=...)` writes a **new calendar_version** only when the content
changed: the rows replace every row of the named adapters, the other adapters' rows are carried over from
the latest version unchanged (the loader reads exactly one version), and an identical set writes nothing.
The write is the leader's (fenced on the epoch in the statement) and serialised by an advisory lock, so
two writers never mint the same version.

The engine's saver builds its rows from a read taken before the lock (`latest()`), and the operator's
`market-sessions accept` (`src/ops/market_sessions.py`) writes its own version under the same lock
(operations.md §10 Accept). So an accept is never lost to a save based on an older read, `save` re-merges under
the lock: a `reviewed_config` row of a named adapter that the current version holds and the saver's read version
did not (committed after that read) is kept over the saver's row for its symbol. The saver never saw it, so its
row carries no decision about it; the next save, with a read that includes it, applies the refresher's usual merge
(`src/runtime/schedule_refresh.py`: a reviewed row is kept where the broker gave nothing or only a flagged
default). The read version is the one passed as `base_version`, else the one this store's last `latest()`
returned (fix 2026-11-25, live review round 3).
"""
from __future__ import annotations

import json
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Literal

from src.adapters.base import MarketSessions
from src.calendar.schedule import ScheduleError, symbol_schedule_from_json
from src.db.epoch import EngineDb
from src.db.fence import FENCE

SessionSource = Literal["ctrader", "stonex", "mock", "reviewed_config"]
#: 'mock' is MockBrokerAdapter's source (sandbox / replay), so a mock-fed calendar persists like a real one.
SOURCES: Final = frozenset({"ctrader", "stonex", "mock", "reviewed_config"})
MARKET_SESSIONS_LOCK: Final = 0x58544E4C43414C45   # "XTNLCALE"

SQL_LOCK: Final = "SELECT pg_advisory_xact_lock($1::bigint)"
SQL_LATEST_ROWS: Final = (
    "SELECT calendar_version, adapter_id, symbol, timezone, schedule, holidays, source, is_default, holidays_known, "
    "note FROM market_sessions "
    "WHERE calendar_version = (SELECT max(calendar_version) FROM market_sessions) ORDER BY adapter_id, symbol"
)
SQL_VERSION_ROWS: Final = (
    "SELECT calendar_version, adapter_id, symbol, timezone, schedule, holidays, source, is_default, holidays_known, "
    "note FROM market_sessions WHERE calendar_version = $1::bigint ORDER BY adapter_id, symbol"
)
SQL_INSERT_VERSION: Final = (
    "INSERT INTO market_sessions (calendar_version, adapter_id, symbol, timezone, schedule, holidays, source, "
    "is_default, holidays_known, note) "
    "SELECT $2::bigint, u.a, u.s, u.tz, u.sch::jsonb, u.hol::jsonb, u.src, u.dflt::boolean, u.hk::boolean, u.note "
    "FROM unnest($3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::text[], "
    "$11::text[]) AS u(a, s, tz, sch, hol, src, dflt, hk, note) "
    f"WHERE {FENCE}"
)


def _canon(v: Any) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), default=str)


def _json_value(v: object) -> Any:
    return json.loads(v) if isinstance(v, (str, bytes)) else v


@dataclass(frozen=True)
class MarketSessionRow:
    """One (adapter, symbol) schedule of a calendar version (see the module docstring for the layout)."""

    adapter_id: str
    symbol: str
    timezone: str
    schedule: Mapping[str, Any]
    holidays: Sequence[Mapping[str, Any]] = field(default=())
    source: SessionSource = "reviewed_config"
    is_default: bool = False
    holidays_known: bool = True
    note: str = ""

    @classmethod
    def from_sessions(cls, ms: MarketSessions) -> MarketSessionRow:
        """One value of 5.1's `fetch_market_sessions()` as a row (validated by `save`, not here)."""
        return cls(ms.adapter_id, ms.symbol, ms.timezone, dict(ms.schedule), tuple(dict(h) for h in ms.holidays),
                   ms.source,  # type: ignore[arg-type]   # checked against SOURCES by validate()
                   bool(ms.is_default), bool(ms.holidays_known), ms.note)

    @property
    def key(self) -> tuple[str, str]:
        return (self.adapter_id, self.symbol)

    def validate(self) -> None:
        """ScheduleError unless the runtime loader would accept this row as is."""
        if not self.adapter_id or not self.symbol:
            raise ScheduleError("adapter_id and symbol are required")
        if self.source not in SOURCES:
            raise ScheduleError(f"{self.adapter_id}:{self.symbol}: unknown source {self.source!r}")
        if not isinstance(self.schedule, Mapping):
            raise ScheduleError(f"{self.adapter_id}:{self.symbol}: schedule must be an object")
        try:
            symbol_schedule_from_json(self.adapter_id, self.symbol, self.timezone, self.schedule, list(self.holidays))
        except ScheduleError:
            raise
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise ScheduleError(f"{self.adapter_id}:{self.symbol}: {type(exc).__name__}: {exc}") from exc

    def content(self) -> tuple[str, str, str, str, str, str, str, str, str]:
        """Canonical text of every column (the change test and the insert values)."""
        return (self.adapter_id, self.symbol, self.timezone, _canon(self.schedule), _canon(list(self.holidays)),
                self.source, "true" if self.is_default else "false", "true" if self.holidays_known else "false",
                self.note)


def row_from_db(r: Sequence[Any]) -> MarketSessionRow:
    """(calendar_version, adapter_id, symbol, timezone, schedule, holidays, source[, is_default, holidays_known,
    note]) -> row."""
    src = str(r[6])
    extra = len(r) >= 10
    return MarketSessionRow(str(r[1]), str(r[2]), str(r[3]), _json_value(r[4]) or {},
                            tuple(_json_value(r[5]) or ()), src if src in SOURCES else "reviewed_config",  # type: ignore[arg-type]
                            bool(r[7]) if extra else False, bool(r[8]) if extra else True,
                            str(r[9] or "") if extra else "")


def merge_rows(latest: Iterable[MarketSessionRow], fresh: Iterable[MarketSessionRow],
               adapters: Collection[str]) -> list[MarketSessionRow]:
    """The next version: `fresh` replaces every row of `adapters`; other adapters' rows are carried over."""
    out = {r.key: r for r in latest if r.adapter_id not in adapters}
    for r in fresh:
        out[r.key] = r
    return [out[k] for k in sorted(out)]


def keep_accepted_since(merged: Iterable[MarketSessionRow], latest: Iterable[MarketSessionRow],
                        base: Iterable[MarketSessionRow], adapters: Collection[str]) -> list[MarketSessionRow]:
    """`merged` with every `reviewed_config` row of `adapters` that `latest` holds and `base` (the saver's read
    version) did not hold as is: an operator's accept committed after that read is kept, not overwritten."""
    seen = {r.key: r.content() for r in base}
    out = {r.key: r for r in merged}
    for r in latest:
        if r.adapter_id in adapters and r.source == "reviewed_config" and seen.get(r.key) != r.content():
            out[r.key] = r
    return [out[k] for k in sorted(out)]


def same_content(a: Iterable[MarketSessionRow], b: Iterable[MarketSessionRow]) -> bool:
    return sorted(r.content() for r in a) == sorted(r.content() for r in b)


@dataclass(frozen=True)
class SaveResult:
    version: int        # the calendar_version now current (0: nothing stored at all)
    changed: bool       # True when this call wrote that version


class PgMarketSessionStore:
    """Write side of `market_sessions` (the leader, fenced). Read side: `PgScheduleLoader`."""

    def __init__(self, db: EngineDb) -> None:
        self._db = db
        self._read_version: int | None = None     # the version the last latest() returned (the saver's read)

    async def latest(self) -> tuple[int, list[MarketSessionRow]]:
        async with self._db.source.acquire() as conn:
            rows = await conn.fetch(SQL_LATEST_ROWS)
        version = int(rows[0][0]) if rows else 0
        self._read_version = version
        return version, [row_from_db(r) for r in rows]

    async def save(self, rows: Sequence[MarketSessionRow], *, adapters: Collection[str],
                   base_version: int | None = None) -> SaveResult:
        """Store `rows` as the full set of the named `adapters` in a new calendar_version, when that changes
        anything. Every row must belong to one of `adapters` and parse (ScheduleError otherwise, nothing is
        written). Pass only adapters whose fetch succeeded: an adapter with no rows here loses its rows.
        `base_version` is the version `rows` were built from (default: this store's last `latest()`); a
        `reviewed_config` row of a named adapter committed after it is kept (an operator's accept is never lost,
        operations.md §10). A lost fence raises EpochMismatch (the caller hard-exits)."""
        wanted = set(adapters)
        if not wanted:
            raise ValueError("adapters is empty")
        seen: set[tuple[str, str]] = set()
        for r in rows:
            if r.adapter_id not in wanted:
                raise ScheduleError(f"{r.adapter_id}:{r.symbol}: adapter not in {sorted(wanted)}")
            if r.key in seen:
                raise ScheduleError(f"{r.adapter_id}:{r.symbol}: duplicate row")
            seen.add(r.key)
            r.validate()
        base = self._read_version if base_version is None else base_version
        self._read_version = None       # one read serves one save
        async with self._db.transaction() as tx:
            await tx.fetchrow(SQL_LOCK, MARKET_SESSIONS_LOCK)
            current = await tx.fetch(SQL_LATEST_ROWS)
            version = int(current[0][0]) if current else 0
            latest = [row_from_db(r) for r in current]
            merged = merge_rows(latest, rows, wanted)
            if base is not None and version > base:
                # a version committed after the saver's read (an operator accept): keep its reviewed rows
                read = [row_from_db(r) for r in await tx.fetch(SQL_VERSION_ROWS, base)] if base > 0 else []
                merged = keep_accepted_since(merged, latest, read, wanted)
            if not merged or same_content(merged, latest):
                return SaveResult(version, False)
            cols = list(zip(*(r.content() for r in merged)))
            await tx.write(SQL_INSERT_VERSION, version + 1, *[list(c) for c in cols], must=True)
        return SaveResult(version + 1, True)


__all__ = ["MARKET_SESSIONS_LOCK", "MarketSessionRow", "PgMarketSessionStore", "SOURCES", "SaveResult",
           "SessionSource", "keep_accepted_since", "merge_rows", "row_from_db", "same_content"]
