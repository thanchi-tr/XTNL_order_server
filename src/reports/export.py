"""`python -m src.ops pinescript`: read the window (read-only), build trades, render, write one file per symbol.

Files are named `<SYMBOL>_<YYYY-MM-DD>.pine` (normalised symbol, UTC date of the export) in the output folder.
The summary carries file names and counts only (orders, positions, trades, pending orders, drawings used, left
out): never an amount.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from src.db.pg import PgConnection
from src.reports.lifecycle import (LogRead, Window, build_lifecycles, build_timelines, build_trades,
                                   normalise_symbol, read_versions)
from src.reports.pinescript import RenderInfo, RenderLimits, RenderResult, render_symbol


@dataclass(frozen=True)
class SymbolExport:
    symbol: str
    file_name: str
    result: RenderResult


@dataclass(frozen=True)
class ExportPlan:
    """What the read produced, rendered but not yet written (pure: testable without files)."""

    read: LogRead
    exports: tuple[SymbolExport, ...]


def file_name(symbol: str, now_ms: int) -> str:
    day = datetime.fromtimestamp(now_ms / 1000, tz=UTC).strftime("%Y-%m-%d")
    return f"{normalise_symbol(symbol) or 'UNKNOWN'}_{day}.pine"


def plan_from_read(read: LogRead, *, now_ms: int, symbol: str | None = None, adapter: str | None = None,
                   limits: RenderLimits = RenderLimits()) -> ExportPlan:
    """Trades and pending orders of the read, grouped per normalised symbol (optionally only `symbol`), one script
    each."""
    timelines = build_timelines(read.versions)
    trades, pending = build_trades(build_lifecycles(timelines, now_ms), now_ms)
    want = normalise_symbol(symbol) if symbol else None
    by_symbol: dict[str, list[str]] = {}
    for ref, tl in timelines.items():
        by_symbol.setdefault(normalise_symbol(tl.last.symbol), []).append(ref[0])
    notes: list[str] = []
    if read.truncated:
        notes.append("the window held more log rows than one export reads; the oldest were not read")
    if read.skipped_rows:
        notes.append(f"rows that could not be placed (no id, side or status): {read.skipped_rows}")
    out: list[SymbolExport] = []
    for sym in sorted(by_symbol):
        if want is not None and sym != want:
            continue
        kinds = by_symbol[sym]
        mine = [t for t in trades if normalise_symbol(t.symbol) == sym]
        mine_p = [p for p in pending if normalise_symbol(p.symbol) == sym]
        display = next(iter([t.symbol for t in mine] + [p.symbol for p in mine_p]), sym)
        info = RenderInfo(generated_ms=now_ms, since_ms=read.window.since_ms, until_ms=read.window.until_ms,
                          adapter=adapter, orders=kinds.count("ORDER"), positions=kinds.count("POSITION"),
                          notes=tuple(notes))
        out.append(SymbolExport(sym, file_name(sym, now_ms), render_symbol(display, mine, mine_p, info, limits)))
    return ExportPlan(read, tuple(out))


def write_plan(plan: ExportPlan, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for e in plan.exports:
        path = out_dir / e.file_name
        path.write_text(e.result.text, encoding="utf-8", newline="\n")
        paths.append(path)
    return paths


async def export_pinescript(conn: PgConnection, *, days: int, now_ms: int, out_dir: Path,
                            symbol: str | None = None, adapter: str | None = None) -> tuple[ExportPlan, list[Path]]:
    read = await read_versions(conn, Window.last_days(days, now_ms), adapter=adapter)
    plan = plan_from_read(read, now_ms=now_ms, symbol=symbol, adapter=adapter)
    return plan, write_plan(plan, out_dir)


def summary_lines(plan: ExportPlan, paths: Sequence[Path]) -> list[str]:
    """File names and counts only."""
    lines: list[str] = []
    for e, p in zip(plan.exports, paths):
        r = e.result
        lines.append(f"{p} orders={r.orders} positions={r.positions} trades={r.trades} pending={r.pending} "
                     f"drawn={len(r.drawn)}+{len(r.drawn_pending)} lines={r.lines} labels={r.labels} "
                     f"boxes={r.boxes} left_out={r.left_out + r.left_out_pending}")
    if not plan.exports:
        lines.append("no order or position versions in the window")
    return lines
