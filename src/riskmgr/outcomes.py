"""Engine outcomes, their categories and the reason codes (design §B6.2, §B6.6; plan S19).

Every decision ends in one `OutcomeCode`: an engine `Outcome` with the category the engine table gives it, or a
**KIND-scoped** reason code of a strategy (step 7) or a guard (step 6), which takes the category that strategy or
guard maps it to in its `REASONS`. A KIND-scoped reason may map only to `INFO` or `HOLD`: `ACTIONABLE` belongs to the
engine's `ADJUST` alone (step 13), and FAULTs are engine outcomes (`DATA_UNAVAILABLE`, `STRATEGY_ERROR`, ...).

`NO_BROKER_BAR` is INFO: an OPEN bucket the broker did not trade (S38c).

`UNRESOLVED_RANGE(reason)` is INFO for `armed_late`, `downtime`, `paused`, `failed` (never paged, never counted
toward D7) and FAULT for any other reason (the reason the bar was last pending with while the ARM ran), §B6.6.

`PRE_NOT_CONFIRMED(reason)` is INFO and carries the reason the bar end disagreed with this bar's PRE (§B6.3 (7a)):
a guard HOLD or a strategy reason with `trigger_met` false (a KIND-scoped code with its owner KIND, for example
`FIRST_BAR_VOLATILE` of `first_bar`, `BELOW_TRIGGER` of kind (a)), or the engine's `AT_TARGET_UNCONFIRMED` (no owner).

A KIND-scoped reason may carry a `detail`, an engine-form code (`atr_degenerate`, `timeout`, `stale`) naming the
sub-reason, e.g. `FIRST_BAR_UNKNOWN(atr_degenerate)` (§B6.5, §B6.6).

Reason codes are codes only: never a price, a value or an error text.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Final


class Category(str, Enum):
    ACTIONABLE = "ACTIONABLE"
    INFO = "INFO"
    HOLD = "HOLD"
    FAULT = "FAULT"


class Outcome(str, Enum):
    """The engine outcomes of §B6.2. `STRATEGY_REASON` / `GUARD_REASON` carry a KIND-scoped reason code."""

    ADJUST = "ADJUST"
    COMPLETE = "COMPLETE"
    PRE_NOT_CONFIRMED = "PRE_NOT_CONFIRMED"
    AT_TARGET_UNCONFIRMED = "AT_TARGET_UNCONFIRMED"
    LOOSENED_AFTER_COMPLETE = "LOOSENED_AFTER_COMPLETE"
    BROKER_AHEAD = "BROKER_AHEAD"
    STRATEGY_REASON = "STRATEGY_REASON"
    GUARD_REASON = "GUARD_REASON"
    MARKET_CLOSED = "MARKET_CLOSED"
    PRICE_THROUGH_LEVEL = "PRICE_THROUGH_LEVEL"
    NO_STOP = "NO_STOP"
    DATA_UNAVAILABLE = "DATA_UNAVAILABLE"
    STRATEGY_ERROR = "STRATEGY_ERROR"
    WRITE_ERROR = "WRITE_ERROR"
    UNRESOLVED_RANGE = "UNRESOLVED_RANGE"
    NO_BROKER_BAR = "NO_BROKER_BAR"
    """A scheduled-OPEN bucket `md-bars` resolved `no_trade` (a complete broker answer with no bar: an unscheduled
    holiday, a thin bucket): INFO, decides the bar (advances the cursor), no trigger, no `commit_state`, never counted
    toward `fail_unresolved_n` (round 12, plan S38c)."""


ENGINE_CATEGORY: Final[Mapping[Outcome, Category]] = {
    Outcome.ADJUST: Category.ACTIONABLE,
    Outcome.COMPLETE: Category.INFO,
    Outcome.PRE_NOT_CONFIRMED: Category.INFO,
    Outcome.AT_TARGET_UNCONFIRMED: Category.HOLD,
    Outcome.LOOSENED_AFTER_COMPLETE: Category.INFO,
    Outcome.BROKER_AHEAD: Category.INFO,
    Outcome.MARKET_CLOSED: Category.HOLD,
    Outcome.PRICE_THROUGH_LEVEL: Category.HOLD,
    Outcome.NO_STOP: Category.FAULT,
    Outcome.DATA_UNAVAILABLE: Category.FAULT,
    Outcome.STRATEGY_ERROR: Category.FAULT,
    Outcome.WRITE_ERROR: Category.FAULT,
    Outcome.NO_BROKER_BAR: Category.INFO,
}
"""Fixed categories; `STRATEGY_REASON`, `GUARD_REASON` and `UNRESOLVED_RANGE` take theirs from the reason."""

KIND_REASON_CATEGORIES: Final = frozenset({Category.INFO, Category.HOLD})
"""The categories a strategy or guard may map its reason codes to."""

REASON_RE: Final = re.compile(r"[A-Z][A-Z0-9_]{0,47}")
"""Form of a KIND-scoped reason code (`BELOW_TRIGGER`, `FIRST_BAR_VOLATILE`)."""
CODE_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,47}")
"""Form of an engine reason code (`stop_stale`, `armed_late`)."""

# DATA_UNAVAILABLE reasons named by the design (later stages add theirs; codes only).
STOP_STALE: Final = "stop_stale"
INCREMENT_UNKNOWN: Final = "increment_unknown"
STOP_AT_BAR_END: Final = "stop_at_bar_end"
GUARD_PENDING: Final = "guard_pending"
FIRST_BAR_PENDING: Final = "first_bar_pending"
EXIT_QUOTE_NOT_LIVE: Final = "exit_quote_not_live"

# UNRESOLVED_RANGE / UNRESOLVED reasons (§B5.2, §B6.6).
ARMED_LATE: Final = "armed_late"
DOWNTIME: Final = "downtime"
PAUSED: Final = "paused"
FAILED: Final = "failed"
BROKER_BAR_NOT_FINAL: Final = "broker_bar_not_final"
RELEASE_UNVERIFIED: Final = "release_awaiting_verification"
"""A bar-end `COMPLETE` (a release) waits for its trigger bar's verification re-read to confirm the same hash
(round 13, plan S38d; an `ADJUST` is never held): no row until then (the bar stays pending)."""
UNRESOLVED_INFO_REASONS: Final = frozenset({ARMED_LATE, DOWNTIME, PAUSED, FAILED})


_ENGINE_NAMES: Final = frozenset(o.value for o in Outcome)


def _owner_ok(owner: str) -> bool:
    """The owner of a KIND-scoped reason is a strategy or guard KIND (lower-case code form)."""
    return bool(CODE_RE.fullmatch(owner)) and len(owner) <= 32


def unresolved_category(reason: str) -> Category:
    """INFO for the four no-fault reasons, FAULT for any other (§B6.2, §B6.6)."""
    return Category.INFO if reason in UNRESOLVED_INFO_REASONS else Category.FAULT


def reason_map_problems(owner: str, reasons: Mapping[str, Category]) -> list[str]:
    """Problems of a strategy's or guard's `REASONS` (form, category, no clash with an engine outcome name)."""
    problems: list[str] = []
    if not reasons:
        problems.append(f"{owner}: REASONS is empty")
    for code, category in reasons.items():
        if not isinstance(code, str) or not REASON_RE.fullmatch(code):
            problems.append(f"{owner}: reason code out of form (expected {REASON_RE.pattern})")
            continue
        if code in _ENGINE_NAMES:
            problems.append(f"{owner}: reason {code} clashes with an engine outcome")
        if not isinstance(category, Category) or category not in KIND_REASON_CATEGORIES:
            problems.append(f"{owner}: reason {code} must map to INFO or HOLD")
    return problems


@dataclass(frozen=True)
class OutcomeCode:
    """One decision's outcome: the engine outcome, its category, an optional reason code and, for a KIND-scoped
    reason, the KIND of the strategy or guard that owns it (a guard reason is stored with its guard KIND, §B6.1) and
    an optional `detail` code (the sub-reason, e.g. `atr_degenerate`)."""

    outcome: Outcome
    category: Category
    reason: str = ""
    owner: str = ""
    detail: str = ""

    def __post_init__(self) -> None:
        fixed = ENGINE_CATEGORY.get(self.outcome)
        if self.detail and (not self.owner or not CODE_RE.fullmatch(self.detail)):
            raise ValueError("a detail is an engine-form code on a KIND-scoped reason only")
        if self.outcome in (Outcome.STRATEGY_REASON, Outcome.GUARD_REASON):
            if not _owner_ok(self.owner) or not REASON_RE.fullmatch(self.reason) \
                    or self.category not in KIND_REASON_CATEGORIES:
                raise ValueError("a KIND-scoped reason needs its owner, a reason code and INFO or HOLD")
        elif self.outcome is Outcome.PRE_NOT_CONFIRMED:
            if self.category is not Category.INFO:
                raise ValueError("PRE_NOT_CONFIRMED is INFO")
            if self.owner:
                if not _owner_ok(self.owner) or not REASON_RE.fullmatch(self.reason) \
                        or self.reason in _ENGINE_NAMES:
                    raise ValueError("PRE_NOT_CONFIRMED carries a KIND-scoped reason code with its owner")
            elif self.reason != Outcome.AT_TARGET_UNCONFIRMED.value:
                raise ValueError("PRE_NOT_CONFIRMED carries a KIND-scoped reason or AT_TARGET_UNCONFIRMED")
        elif self.outcome is Outcome.UNRESOLVED_RANGE:
            if not CODE_RE.fullmatch(self.reason) or self.category is not unresolved_category(self.reason) \
                    or self.owner:
                raise ValueError("UNRESOLVED_RANGE takes its category from its reason")
        elif self.category is not fixed or self.owner or (self.reason and not CODE_RE.fullmatch(self.reason)):
            raise ValueError(f"{self.outcome.value} has the fixed category {fixed} and an engine reason code")

    @classmethod
    def engine(cls, outcome: Outcome, reason: str = "") -> OutcomeCode:
        category = ENGINE_CATEGORY.get(outcome)
        if category is None:
            raise ValueError(f"{outcome.value} is not a fixed engine outcome")
        return cls(outcome, category, reason)

    @classmethod
    def kind_reason(cls, outcome: Outcome, owner: str, reason: str, reasons: Mapping[str, Category], *,
                    detail: str = "") -> OutcomeCode:
        """A strategy (`STRATEGY_REASON`) or guard (`GUARD_REASON`) reason, categorised by its owner's `REASONS`."""
        category = reasons.get(reason)
        if category is None:
            raise ValueError(f"{owner}: reason {reason} is not in its REASONS")
        return cls(outcome, category, reason, owner, detail)

    @classmethod
    def pre_not_confirmed(cls, owner: str, reason: str, *, detail: str = "") -> OutcomeCode:
        """`PRE_NOT_CONFIRMED(reason)` (INFO, §B6.3 (7a)): `owner` is the KIND of the guard or strategy whose reason
        it carries, or empty with the engine's `AT_TARGET_UNCONFIRMED`."""
        return cls(Outcome.PRE_NOT_CONFIRMED, Category.INFO, reason, owner, detail)

    @classmethod
    def unresolved(cls, reason: str) -> OutcomeCode:
        return cls(Outcome.UNRESOLVED_RANGE, unresolved_category(reason), reason)

    @property
    def actionable(self) -> bool:
        return self.category is Category.ACTIONABLE
