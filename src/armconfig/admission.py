"""The admission check of an ARM document (design §B4 arm-config, §A5, §A6 Demand, PR9; plan S33, R11-M20). Pure.

`arm config accept <hash>` (S34) runs `admission(doc, loads, limits, at_ms)` and refuses the accept, with the
numbers, when a limit is exceeded. Nothing here reads a price: it counts series, bytes and broker reads.

* **Expansion.** Every enabled assignment in force at `at_ms` or future-dated (a tombstone and a superseded row add
  nothing) covers, per bar-capable adapter it names (NULL = every adapter in `loads`): its explicit symbol, or for a
  wildcard symbol the adapter's **pre-warm set** (held symbols and `XTNL_TICK_SYMBOLS`, at most `PREWARM_CAP` per
  adapter; never "all instruments", §B4). Per covered `(adapter, symbol)` the pinned strategy's needs (trigger bar
  spec and side for a long and a short, history and indicator needs of the strategy and its guards) give the BROKER
  series and indicators, plus the TICK 1m and 15m bid and ask series the broker bars are cross-checked against.
* **Symbols:** decision-grade symbols per adapter at most `decision_symbols_per_adapter` (default 8, the demand
  ceiling of `src/marketdata/demand.py`).
* **Bytes** (§A5 formula): `Σ series (bars retained × MEMBER_BYTES) + Σ indicators (values × INDICATOR_BYTES)` plus
  the tick stream and the revisions / rollback set / checkpoints per symbol and the fixed `md:events`, against
  `maxmemory × memory_target` (`redis-md` `maxmemory 1gb`, target 50 %). Retention: BROKER 1 year, TICK 1m 7 days,
  TICK 15m 90 days; an indicator keeps as many values as its input series.
* **BARS reads per bar end** per adapter, against the adapter's per-minute `BARS` cap over the read window
  (`post_grace`): every BROKER series read `reads_per_series_per_bar` times (first read and the verification re-read)
  plus, with PR9 stop reads on and armed handles, one list read per trading account per anchor (PRE, POST and the
  POST refresh: `STOP_ANCHOR_READS`), never one per handle. **REOPEN cadence:** at a market open every series re-reads
  the pre-close bar on top of the first post-open bar's reads, plus the stop list reads, inside `reopen_window_s`.
  An adapter without a `BARS` cap (unmetered) is reported, never refused on reads.

`AdmissionReport.refusals` are the human-readable reasons (codes and numbers: counts, bytes, reads; never a price or
an account value). An empty tuple admits.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import ROUND_FLOOR, Decimal
from typing import Final

from src.armconfig.validate import Assignment, StrategyEntry, ValidDocument, latest_per_key
from src.riskmgr.registry import REGISTRY, ArmRegistry

MIB: Final = 1 << 20
DAY_S: Final = 86_400
MEMBER_BYTES: Final = 270
"""§A5: one skiplist-encoded bar member (record and content hash, skiplist node, dict entry, allocator rounding)."""
INDICATOR_BYTES: Final = 300
"""§A5: one indicator value with its lineage."""
TICK_STREAM_BYTES: Final = 10 * MIB
REVISION_BYTES: Final = 2 * MIB
"""Per symbol: `md:bars_rev`, `md:quarantine`, the rollback set and checkpoints."""
EVENTS_BYTES: Final = 15 * MIB
BROKER_RETENTION_S: Final = 365 * DAY_S
TICK_RETENTION_S: Final[Mapping[int, int]] = {60: 7 * DAY_S, 900: 90 * DAY_S}
"""The TICK cross-check series (1m and 15m, bid and ask) and their retention."""
TICK_SIDES: Final = ("BID", "ASK")
POSITION_SIDES: Final = ("BUY", "SELL")
PREWARM_CAP: Final = 8
STOP_ANCHOR_READS: Final = 3
"""PR9: one list read at PRE, one at POST and the POST refresh, per trading account with armed handles."""


@dataclass(frozen=True, kw_only=True)
class AdmissionLimits:
    maxmemory_bytes: int = 1 << 30
    memory_target: Decimal = Decimal("0.5")
    decision_symbols_per_adapter: int = 8
    reads_per_series_per_bar: int = 2
    read_window_s: int = 60
    reopen_window_s: int = 120

    @property
    def byte_limit(self) -> int:
        return int((Decimal(self.maxmemory_bytes) * self.memory_target).to_integral_value(rounding=ROUND_FLOOR))


@dataclass(frozen=True, kw_only=True)
class AdapterLoad:
    """What one adapter can serve and already carries (built by the caller from local state, never Supabase)."""

    adapter_id: str
    symbols: frozenset[str] = frozenset()
    """The adapter's instruments (an explicit symbol it does not list adds nothing)."""
    prewarm: tuple[str, ...] = ()
    """Held symbols and `XTNL_TICK_SYMBOLS` of this adapter (the wildcard expansion, cut to `PREWARM_CAP`)."""
    bar_capable: bool = True
    bars_per_min: int | None = None
    """The adapter's `BARS` cap per minute; None = unmetered."""
    stop_reads: bool = False
    """PR9 (`XTNL_ARM_STOP_READS=1`)."""
    armed_handles: int = 0
    stop_accounts: int = 0
    """Trading accounts with armed handles: the list reads per anchor."""


@dataclass(frozen=True, kw_only=True)
class AdapterAdmission:
    adapter_id: str
    symbols: int
    broker_series: int
    tick_series: int
    indicators: int
    bytes: int
    reads_per_bar_end: int
    stop_reads_per_bar_end: int
    reopen_reads: int
    armed_handles: int
    read_budget: int | None
    reopen_budget: int | None


@dataclass(frozen=True)
class AdmissionReport:
    adapters: tuple[AdapterAdmission, ...]
    total_bytes: int
    byte_limit: int
    refusals: tuple[str, ...] = ()
    notes: tuple[str, ...] = field(default=())

    @property
    def admitted(self) -> bool:
        return not self.refusals

    @property
    def series(self) -> int:
        return sum(a.broker_series + a.tick_series for a in self.adapters)


@dataclass
class _SymbolNeeds:
    broker: set[tuple[int, str]] = field(default_factory=set)
    indicators: set[tuple[str, int, int, str, tuple[tuple[str, str], ...]]] = field(default_factory=set)


def live_assignments(doc: ValidDocument, at_ms: int) -> tuple[Assignment, ...]:
    """Enabled assignments in force at `at_ms` plus enabled future-dated rows, by row id."""
    in_force = latest_per_key(doc.assignments, lambda a: a.key, at_ms).values()
    rows = {a.row_id: a for a in in_force if a.enabled}
    rows.update({a.row_id: a for a in doc.assignments if a.enabled and a.effective_from_ms > at_ms})
    return tuple(rows[k] for k in sorted(rows))


def _needs(entry: StrategyEntry, registry: ArmRegistry, out: _SymbolNeeds) -> str | None:
    """Add the strategy's BROKER series and indicators to `out`; a code when they cannot be read."""
    params = entry.params
    strategy = registry.strategy(params.kind, params.impl_version)
    if strategy is None:
        return "strategy_unknown"
    try:
        specs = {ev.bar_spec for ev in strategy.triggers(params)}
        history = list(strategy.required_history(params))
        indicators = list(strategy.required_indicators(params))
        for g in params.guards:
            guard = registry.guard(g.kind, g.version)
            if guard is None:
                return "guard_unknown"
            history += guard.required_history(g)
            indicators += guard.required_indicators(g)
        for spec in specs:
            for side in POSITION_SIDES:
                out.broker.add((spec.timeframe_s, strategy.trigger_side(params, side).value))
    except Exception:   # a class that raises on its own params is refused at validation; never crash the check
        return "needs_unreadable"
    out.broker.update((h.bar_spec.timeframe_s, h.side.value) for h in history)
    for n in indicators:
        out.broker.add((n.bar_spec.timeframe_s, n.side.value))
        out.indicators.add((n.kind, n.version, n.bar_spec.timeframe_s, n.side.value, n.params))
    return None


def _symbols(a: Assignment, load: AdapterLoad) -> tuple[str, ...]:
    if a.symbol is not None:
        return (a.symbol,) if a.symbol in load.symbols else ()
    return tuple(load.prewarm[:PREWARM_CAP])


def _per_symbol_bytes(needs: _SymbolNeeds) -> int:
    total = TICK_STREAM_BYTES + REVISION_BYTES
    for tf, retention in TICK_RETENTION_S.items():
        total += len(TICK_SIDES) * (retention // tf) * MEMBER_BYTES
    for tf, _side in needs.broker:
        total += (BROKER_RETENTION_S // tf) * MEMBER_BYTES
    for _kind, _version, tf, _side, _params in needs.indicators:
        total += (BROKER_RETENTION_S // tf) * INDICATOR_BYTES
    return total


def _budget(per_min: int | None, window_s: int) -> int | None:
    return None if per_min is None else per_min * window_s // 60


def admission(doc: ValidDocument, loads: Sequence[AdapterLoad], limits: AdmissionLimits | None = None, *,
              at_ms: int, registry: ArmRegistry = REGISTRY) -> AdmissionReport:
    """The admission report of `doc` over `loads` (module docstring)."""
    lim = limits or AdmissionLimits()
    by_adapter = {ld.adapter_id: ld for ld in loads}
    needs: dict[tuple[str, str], _SymbolNeeds] = {}
    notes: list[str] = []
    refusals: list[str] = []
    for a in live_assignments(doc, at_ms):
        entry = doc.strategy(a.strategy_code, a.strategy_version)
        if entry is None:
            continue
        targets = [by_adapter[a.adapter]] if a.adapter in by_adapter else [] if a.adapter is not None else loads
        for load in targets:
            if not load.bar_capable:
                notes.append(f"assignments:{a.row_id}: adapter {load.adapter_id} serves no broker bars (unsupported)")
                continue
            for symbol in _symbols(a, load):
                problem = _needs(entry, registry, needs.setdefault((load.adapter_id, symbol), _SymbolNeeds()))
                if problem is not None:
                    refusals.append(f"assignments:{a.row_id}: strategy needs not readable ({problem})")
    report: list[AdapterAdmission] = []
    total = EVENTS_BYTES
    for load in loads:
        mine = {s: n for (aid, s), n in needs.items() if aid == load.adapter_id}
        broker = sum(len(n.broker) for n in mine.values())
        ticks = len(mine) * len(TICK_SIDES) * len(TICK_RETENTION_S)
        indicators = sum(len(n.indicators) for n in mine.values())
        size = sum(_per_symbol_bytes(n) for n in mine.values())
        total += size
        stop = (load.stop_accounts * STOP_ANCHOR_READS
                if load.stop_reads and load.armed_handles > 0 and mine else 0)
        reads = broker * lim.reads_per_series_per_bar + stop
        reopen = broker * (1 + lim.reads_per_series_per_bar) + (2 * stop if stop else 0)
        read_budget = _budget(load.bars_per_min, lim.read_window_s)
        reopen_budget = _budget(load.bars_per_min, lim.reopen_window_s)
        report.append(AdapterAdmission(
            adapter_id=load.adapter_id, symbols=len(mine), broker_series=broker, tick_series=ticks,
            indicators=indicators, bytes=size, reads_per_bar_end=reads, stop_reads_per_bar_end=stop,
            reopen_reads=reopen, armed_handles=load.armed_handles, read_budget=read_budget,
            reopen_budget=reopen_budget))
        if len(mine) > lim.decision_symbols_per_adapter:
            refusals.append(f"{load.adapter_id}: {len(mine)} decision symbols > ceiling "
                            f"{lim.decision_symbols_per_adapter}")
        if read_budget is not None and reads > read_budget:
            refusals.append(f"{load.adapter_id}: {reads} BARS reads per bar end ({broker} series x "
                            f"{lim.reads_per_series_per_bar} + {stop} stop reads) > budget {read_budget} "
                            f"({load.bars_per_min}/min over {lim.read_window_s} s)")
        if reopen_budget is not None and reopen > reopen_budget:
            refusals.append(f"{load.adapter_id}: {reopen} BARS reads at a reopen > budget {reopen_budget} "
                            f"({load.bars_per_min}/min over {lim.reopen_window_s} s)")
        if load.bars_per_min is None and mine:
            notes.append(f"{load.adapter_id}: no BARS cap (unmetered): reads not checked")
    limit = lim.byte_limit
    if total > limit:
        refusals.append(f"redis-md: {total} bytes at full retention > {limit} "
                        f"({lim.maxmemory_bytes} x {lim.memory_target})")
    return AdmissionReport(adapters=tuple(report), total_bytes=total, byte_limit=limit, refusals=tuple(refusals),
                           notes=tuple(dict.fromkeys(notes)))


__all__ = [
    "AdapterAdmission", "AdapterLoad", "AdmissionLimits", "AdmissionReport", "INDICATOR_BYTES", "MEMBER_BYTES",
    "PREWARM_CAP", "STOP_ANCHOR_READS", "admission", "live_assignments",
]
