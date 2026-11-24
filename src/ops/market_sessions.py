"""`market-sessions show | accept`: the operator's review of the SM10 calendar rows (operations.md s10, s16).

The ScheduleRefresher (src/runtime/schedule_refresh.py) pages "market sessions need review" while a stored
schedule is a documented default, has no holiday source or carries a broker note. StoneX has no trading hours for
FX and no holiday source at all, so before this command that page could never be cleared. `accept` records the
operator's review as `source='reviewed_config'` rows:

* It copies the schedule (timezone, sessions, breaks) of the stored row of each named (adapter, symbol) in the latest
  calendar version, with the operator's holiday list when `--holidays` names one (then `holidays_known=true`), else
  the stored holidays. `is_default=false`, an empty note. Every row is checked with the engine's own parser
  (`MarketSessionRow.validate`), so the engine can always load it.
* It writes a new `calendar_version` (the latest rows with these replaced; other adapters and symbols unchanged),
  serialised with the engine's writer by the same advisory lock (`MARKET_SESSIONS_LOCK`) and fenced on the stored
  epoch like every other engine-state write of the CLI, with its `operator_audit` row in the same transaction. An
  unchanged set writes nothing (`noop`, audited).
* The engine keeps a reviewed row while the broker gives nothing or only a flagged default for that symbol, and the
  review page leaves such a symbol out (a missing symbol is still reported). The running leader loads the new
  version at its next schedule save (6 h refresh or the 17:00 New York roll) or its next LEADER_INIT.

`show` lists the latest version's rows: ids, timezone, source and review flags (no price, no account value).
Nothing here calls a broker or touches an order.
"""
from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final

from src.calendar.schedule import ScheduleError
from src.db.epoch import SQL_LOCK_TIMEOUT_2S
from src.db.market_sessions import (
    MARKET_SESSIONS_LOCK,
    SQL_INSERT_VERSION,
    SQL_LATEST_ROWS,
    SQL_LOCK,
    MarketSessionRow,
    row_from_db,
    same_content,
)

from src.ops.model import Actor, OpsRefused, OpsUsageError, Report

if TYPE_CHECKING:
    from src.ops.service import OpsService

ADAPTER_RE: Final = re.compile(r"^[a-z0-9_-]{1,32}$")
SYMBOL_RE: Final = re.compile(r"^[A-Za-z0-9._/:#-]{1,32}$")
MAX_SYMBOLS: Final = 50
MAX_HOLIDAYS: Final = 2000


def parse_symbols(text: str) -> list[str]:
    """'EURUSD,AUDUSD' -> ['AUDUSD', 'EURUSD'] (sorted, unique). OpsUsageError on an empty or odd name."""
    names = sorted({s.strip() for s in (text or "").split(",") if s.strip()})
    if not names:
        raise OpsUsageError("--symbols needs at least one symbol (comma separated)")
    if len(names) > MAX_SYMBOLS:
        raise OpsUsageError(f"--symbols takes at most {MAX_SYMBOLS} symbols")
    bad = [s for s in names if not SYMBOL_RE.match(s)]
    if bad:
        raise OpsUsageError(f"--symbols: not a symbol name: {', '.join(bad[:5])}")
    return names


def check_holidays(doc: object) -> list[dict[str, Any]]:
    """The operator's holiday file: a JSON list of objects (`{"date": "2026-12-25"}` or a `start` / `end` window,
    the `market_sessions.holidays` layout). The engine's parser checks the values with each row."""
    if not isinstance(doc, list) or not all(isinstance(h, Mapping) for h in doc):
        raise OpsUsageError("--holidays must be a JSON list of objects ({\"date\": \"YYYY-MM-DD\"} or start / end)")
    if len(doc) > MAX_HOLIDAYS:
        raise OpsUsageError(f"--holidays has more than {MAX_HOLIDAYS} entries")
    return [dict(h) for h in doc]


def _row_view(r: MarketSessionRow) -> dict[str, Any]:
    return {"adapter": r.adapter_id, "symbol": r.symbol, "timezone": r.timezone, "source": r.source,
            "is_default": r.is_default, "holidays_known": r.holidays_known, "holidays": len(r.holidays),
            "note": r.note or None}


async def sessions_show(svc: OpsService, adapter: str | None) -> Report:
    """The latest calendar version's rows (read-only, not audited)."""
    rep = Report("market_sessions.show")
    rows = await svc.conn.fetch(SQL_LATEST_ROWS)
    rep.fields["calendar_version"] = int(rows[0][0]) if rows else 0
    for r in (row_from_db(x) for x in rows):
        if adapter is None or r.adapter_id == adapter:
            rep.rows.append(_row_view(r))
    flagged = [x for x in rep.rows if x["source"] != "reviewed_config"
               and (x["is_default"] or not x["holidays_known"] or x["note"])]
    if flagged:
        rep.warnings.append(f"{len(flagged)} row(s) flagged for review; after checking the hours, accept them with "
                            "`market-sessions accept --adapter A --symbols S1,S2`")
    return rep


async def sessions_accept(svc: OpsService, adapter: str, symbols: Sequence[str],
                          holidays: Sequence[Mapping[str, Any]] | None, actor: Actor, *,
                          holidays_source: str | None = None) -> Report:
    """Store the operator-reviewed rows of `adapter` / `symbols` as a new calendar version (see the module doc)."""
    actor.validate(two_person=False)
    if not ADAPTER_RE.match(adapter or ""):
        raise OpsUsageError("--adapter must be an adapter id such as stonex or pepperstone")
    rep = Report("market_sessions.accept")
    target = f"{adapter}:{','.join(symbols)}"
    detail: dict[str, Any] = {"adapter": adapter, "symbols": list(symbols),
                              "holidays": None if holidays is None else len(holidays),
                              "holidays_source": holidays_source}
    try:
        async with svc.fenced() as tx:
            await tx.execute(SQL_LOCK_TIMEOUT_2S)
            await tx.fetchrow(SQL_LOCK, MARKET_SESSIONS_LOCK)
            current = await tx.fetch(SQL_LATEST_ROWS)
            version = int(current[0][0]) if current else 0
            latest = [row_from_db(r) for r in current]
            by_key = {r.key: r for r in latest}
            missing = [s for s in symbols if (adapter, s) not in by_key]
            if missing:
                raise OpsRefused("not_found", f"calendar v{version} has no {adapter} row for {', '.join(missing)}; "
                                              "the engine stores a symbol once it has fetched its schedule")
            accepted: list[MarketSessionRow] = []
            for s in symbols:
                cur = by_key[(adapter, s)]
                kept_known = cur.source == "reviewed_config" and cur.holidays_known
                row = MarketSessionRow(adapter, s, cur.timezone, cur.schedule,
                                       tuple(cur.holidays) if holidays is None else tuple(holidays),
                                       "reviewed_config", is_default=False,
                                       holidays_known=holidays is not None or kept_known, note="")
                try:
                    row.validate()
                except ScheduleError as exc:
                    raise OpsRefused("invalid_schedule", str(exc)[:500]) from exc
                accepted.append(row)
                rep.rows.append({**_row_view(row), "was_source": cur.source, "was_default": cur.is_default})
            merged = dict(by_key)
            for row in accepted:
                merged[row.key] = row
            rows = [merged[k] for k in sorted(merged)]
            detail["calendar_version"] = version
            if same_content(rows, latest):
                rep.outcome = "noop"
                rep.fields.update({"calendar_version": version, "changed": False})
                rep.audit_id = await svc.audit(tx, rep.command, target, actor, "noop", detail, tx.epoch)
                return rep
            cols = list(zip(*(r.content() for r in rows)))
            await tx.write(SQL_INSERT_VERSION, version + 1, *[list(c) for c in cols], must=True)
            detail["new_calendar_version"] = version + 1
            rep.audit_id = await svc.audit(tx, rep.command, target, actor, "applied", detail, tx.epoch)
            rep.fields.update({"calendar_version": version + 1, "previous_version": version, "changed": True,
                               "epoch": tx.epoch})
    except OpsRefused as r:
        return await svc.refused(rep, r, target, actor, detail)
    rep.outcome = "applied"
    rep.warnings.append("the running leader loads this version at its next schedule save (6 h refresh or the "
                        "17:00 New York roll) or its next start; the review page then leaves these symbols out")
    return rep


__all__ = ["check_holidays", "parse_symbols", "sessions_accept", "sessions_show"]
