"""Shared Oversee value types: rule levels, results, rule states and transitions (oversee-rules.md §2, §6).

`BreachState` mirrors one PostgreSQL `halt_breaches` row (tier-persistence.md §4).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, IntEnum
from typing import Literal

SubjectType = Literal["account", "account_day", "order", "position"]
TransitionKind = Literal["activated", "raised", "lowered", "resolved", "capped", "uncapped"]
ResolvedBy = Literal["sweep", "day_end", "entity_end", "reload", "operator"]
SymbolKey = tuple[str, str]          # (adapter_id, symbol)
StateKey = tuple[str, str, str]      # (rule_id, subject_type, subject_id)

EVALUATION_RULE_ID = "evaluation"    # synthetic capped SOFT `evaluation_failed` state (§2)


class Level(IntEnum):
    """Rule result level, ordered by severity (§2)."""

    PASS = 0
    FLAG = 1
    SOFT_HALT = 2
    HARD_HALT = 3


class Cap(str, Enum):
    """SOFT when an input is stale or estimated; HARD always needs NONE (§2)."""

    NONE = "NONE"
    SOFT = "SOFT"


class HaltLevel(IntEnum):
    """Effective halt level (`halt_events.effective_level`, §8)."""

    NONE = 0
    SOFT = 1
    HARD = 2


def halt_level_of(level: Level) -> HaltLevel:
    if level >= Level.HARD_HALT:
        return HaltLevel.HARD
    if level >= Level.SOFT_HALT:
        return HaltLevel.SOFT
    return HaltLevel.NONE


@dataclass(frozen=True)
class Subject:
    """What a rule state is about: account, (account, trading_day), order or position (§2)."""

    subject_type: SubjectType
    subject_id: str
    account_id: str


@dataclass(frozen=True)
class Result:
    """A known rule result (§6). `release_level` is the level against band edges lowered by the
    release margin (§2 hysteresis); it defaults to `level`."""

    level: Level
    reason: str
    cap: Cap = Cap.NONE
    risk_pct: Decimal | None = None
    release_level: Level | None = None
    stale_symbols: frozenset[SymbolKey] = frozenset()
    wants_cash_flows: bool = False

    @property
    def effective_level(self) -> Level:
        """The level within the cap: a capped result reaches at most SOFT_HALT."""
        if self.cap is Cap.SOFT:
            return min(self.level, Level.SOFT_HALT)
        return self.level

    @property
    def effective_release_level(self) -> Level:
        rl = self.level if self.release_level is None else max(self.release_level, self.level)
        if self.cap is Cap.SOFT:
            return min(rl, Level.SOFT_HALT)
        return rl

    def capped(self) -> Result:
        return Result(self.level, self.reason, Cap.SOFT, self.risk_pct, self.release_level,
                      self.stale_symbols, self.wants_cash_flows)


@dataclass(frozen=True)
class Unknown:
    """A missing or unknown input (§2): risk_unknown, mark_unknown, live_unknown, evaluation_error."""

    reason: str
    detail: str = ""


RuleOutcome = Result | Unknown


@dataclass(frozen=True)
class BreachState:
    """One `halt_breaches` row. `level` is the active level (PASS = not active)."""

    rule_id: str
    subject_type: SubjectType
    subject_id: str
    account_id: str
    level: Level = Level.PASS
    cap: Cap = Cap.NONE
    reason: str = ""
    pending_level: Level | None = None
    pending_since: int | None = None
    active_since: int | None = None
    activation_id: int | None = None
    release_pending_since: int | None = None
    unknown_since: int | None = None
    unknown_origin: bool = False
    uncapped_ms: int = 0
    last_pass_ms: int | None = None
    below_soft_since: int | None = None
    resolved_at: int | None = None
    resolved_by: ResolvedBy | None = None
    last_sweep_id: int = 0
    clear_generation: int = 0

    @property
    def key(self) -> StateKey:
        return (self.rule_id, self.subject_type, self.subject_id)

    @property
    def active(self) -> bool:
        return self.level >= Level.FLAG and self.resolved_at is None

    @property
    def capped_or_unknown(self) -> bool:
        return self.cap is Cap.SOFT or self.unknown_since is not None

    @staticmethod
    def blank(rule_id: str, subject: Subject) -> BreachState:
        return BreachState(rule_id, subject.subject_type, subject.subject_id, subject.account_id)


@dataclass(frozen=True)
class Transition:
    """One `oversee_events` row, unique on (rule_id, subject_type, subject_id, transition, sweep_id) (§7)."""

    rule_id: str
    subject_type: SubjectType
    subject_id: str
    account_id: str
    transition: TransitionKind
    sweep_id: int
    at_ms: int
    level: Level
    cap: Cap
    reason: str
    activation_id: int | None = None
    #: The UNKNOWN result's own detail when the transition carries that result's reason (e.g. the inconsistent
    #: child ids of `stop_kind_inconsistent`, oversee-rules.md §6). Alert text only: never part of `reason`, of the
    #: `oversee_events` key or of the stored rule state, so every exact-equality use of `reason` is unchanged.
    detail: str = ""

    @property
    def event_key(self) -> tuple[str, str, str, str, int]:
        return (self.rule_id, self.subject_type, self.subject_id, self.transition, self.sweep_id)


@dataclass(frozen=True)
class Page:
    """An operational page raised by Oversee itself (evaluation_failed, quarantine, slow pass ...)."""

    severity: Literal["info", "high", "critical"]
    code: str
    account_id: str | None = None
    detail: str = ""
    at_ms: int = 0
    extra: dict[str, str] = field(default_factory=dict)
