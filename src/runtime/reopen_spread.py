"""The REOPEN clean-spread retry (design `automated-risk-manager.md` §B6.3 REOPEN; live review round 5).

§B6.3 REOPEN: "From the first OPEN instant + settle it is retried every 5 s ... post-guards on the live quote).
`PRICE_THROUGH_LEVEL` and a spread that is not clean (the existing clean-spread rule) are **retryable**, because
spreads are widest at the open; only `ADJUST`, `COMPLETE`, a closed position, or the first post-open bar's POST
decision (at the latest its `decision_cap`) ends it."

`ReopenSpreadGate` is the BarClock's evaluation port: it wraps the S21 join (`PipelineEvaluator`) and changes only a
REOPEN attempt. When that attempt's result would be a final `ADJUST` and the live exit quote's spread is not clean
by the existing rule, the attempt does not end the re-check: the job returns a non-final `PENDING` result (code
`exit_spread_not_clean`), which the BarClock retries on its 5 s REOPEN cadence with no row
(`BarClock._not_ready(..., REOPEN_RETRY)`). Such an attempt never replaces the held last HOLD, so on expiry (the
first post-open bar's POST or its `decision_cap`) "the last HOLD is sent with its reason" still sends one of the
§B6.2 outcome codes, or, with none held, only the re-check is cleared (the existing expiry behaviour). POST, LATE,
PRE and CORRECTED attempts, and every REOPEN result other than `ADJUST` (a HOLD, `COMPLETE`), pass unchanged: §B6.3
names the spread rule as a REOPEN retry condition only. No new outcome code is added to §B6.2.

The existing clean-spread rule (`adapter-isolation.md` §5, the Redis `clean_mid` write, the orchestrator's clean-mid
watch): a quote is clean when its spread in pips, `(ask - bid) / point_size` (the adapters' `spread_pips`), is at
most the installed ruleset's `marks.clean_spread_pips(adapter_id, symbol)` (`ActiveRuleset.clean_spread`, the same
hook the session hands to the `TieredStateCommitter` and the orchestrator: `EngineSession.clean_spread`). The
threshold is read at each check; the instrument's `point_size` at each check from `instruments`
(`src.db.instrument_reads.PgPointSizes`). A threshold, quote or point size that cannot be read leaves cleanliness
unknown, which holds the `ADJUST` back like an unclean spread (fail safe: the re-check still ends at its expiry).

The threshold and the point size stay here, in `src/runtime`: `src/riskmgr` never imports `src.oversee.config` or
`src.runtime` (the read-only lockout, `.claude/rules/riskmgr.md` §2). Nothing here logs, pages, stores or renders the
spread, the threshold, the pip / point size or a price (`riskmgr.md` §5): logs and counters carry codes, ids and
counts only.
"""
from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Final, Protocol

from src.db.arm_ports import is_lost_fence
from src.marketdata.ports import QuoteRead
from src.marketdata.types import DataUnavailable
from src.riskmgr.barclock import EvalRequest, EvaluationPort, Evaluated, NotReady, Prepared, Verdict
from src.riskmgr.outcomes import Outcome
from src.riskmgr.types import ArmEvent

log = logging.getLogger(__name__)

EXIT_SPREAD_NOT_CLEAN: Final = "exit_spread_not_clean"
"""The code of a REOPEN attempt held back by the clean-spread rule (retried, never written)."""

CleanSpread = Callable[[str, str], Decimal | None]
"""`(adapter_id, symbol)` -> the installed clean-spread threshold in pips (`ActiveRuleset.clean_spread`)."""

PointSizes = Callable[[Sequence[tuple[str, str]]], Awaitable[Mapping[tuple[str, str], object]]]
"""`(adapter_id, symbol)` pairs -> each instrument's `point_size` (`src.db.instrument_reads.PgPointSizes`)."""


class QuoteSource(Protocol):
    async def quote(self, adapter_id: str, symbol: str) -> QuoteRead | DataUnavailable: ...


@dataclass
class ReopenSpreadCounters:
    """Counts only (never a spread, a threshold or a point size)."""

    checks: int = 0
    clean: int = 0
    unclean_spread: int = 0
    unknown: int = 0
    """The threshold, the quote or the point size could not be read: the `ADJUST` is held back (fail safe)."""


def held_back(result: Evaluated) -> bool:
    """A final REOPEN `ADJUST`: the only result the clean-spread rule holds back (any other passes unchanged)."""
    return (result.verdict is Verdict.DECISION and result.final and result.decision is not None
            and result.decision.outcome == Outcome.ADJUST.value)


class ReopenSpreadGate:
    """`EvaluationPort` of the BarClock: `inner` (the S21 `PipelineEvaluator`) with the §B6.3 REOPEN clean-spread
    retry (see the module docstring)."""

    def __init__(self, inner: EvaluationPort, *, md: QuoteSource, clean_spread: CleanSpread,
                 point_sizes: PointSizes, fatal: Callable[[BaseException], bool] = is_lost_fence) -> None:
        self._inner = inner
        self._md = md
        self._clean_spread = clean_spread
        self._point_sizes = point_sizes
        self._fatal = fatal
        self.counters = ReopenSpreadCounters()

    async def prepare(self, request: EvalRequest) -> Prepared | NotReady:
        got = await self._inner.prepare(request)
        if request.event is not ArmEvent.REOPEN or isinstance(got, NotReady):
            return got
        row = request.row
        clean = await self.spread_clean(row.key.adapter_id, row.symbol)
        if clean is True:
            return got
        inner_job = got.job

        def job() -> Evaluated:          # pure, like the job it wraps
            result = inner_job()
            if held_back(result):
                return Evaluated(verdict=Verdict.PENDING, reason=EXIT_SPREAD_NOT_CLEAN)
            return result

        return Prepared(got.spec, job)

    async def spread_clean(self, adapter_id: str, symbol: str) -> bool | None:
        """True when the live quote is clean by the existing rule, False when it is not, None when unknown."""
        self.counters.checks += 1
        verdict = await self._judge(adapter_id, symbol)
        if verdict is None:
            self.counters.unknown += 1
        elif verdict:
            self.counters.clean += 1
        else:
            self.counters.unclean_spread += 1
        return verdict

    async def _judge(self, adapter_id: str, symbol: str) -> bool | None:
        try:
            limit = self._clean_spread(adapter_id, symbol)
        except Exception as e:
            log.warning("arm reopen %s/%s: clean-spread threshold not read (%s)", adapter_id, symbol,
                        type(e).__name__)
            return None
        if limit is None:
            return None
        try:
            got = await self._md.quote(adapter_id, symbol)
        except Exception as e:
            if self._fatal(e):
                raise
            log.info("arm reopen %s/%s: exit quote not read (%s)", adapter_id, symbol, type(e).__name__)
            return None
        if not isinstance(got, QuoteRead) or got.quote is None:
            return None
        try:
            sizes = await self._point_sizes([(adapter_id, symbol)])
        except Exception as e:
            if self._fatal(e):
                raise
            log.info("arm reopen %s/%s: point size not read (%s)", adapter_id, symbol, type(e).__name__)
            return None
        raw = sizes.get((adapter_id, symbol))
        if raw is None:
            return None
        try:
            point = Decimal(str(raw))
            if not point.is_finite() or point <= 0:
                return None
            return (got.quote.ask - got.quote.bid) / point <= limit
        except (ArithmeticError, InvalidOperation, ValueError):
            return None


__all__ = ["EXIT_SPREAD_NOT_CLEAN", "CleanSpread", "PointSizes", "QuoteSource", "ReopenSpreadCounters",
           "ReopenSpreadGate", "held_back"]
