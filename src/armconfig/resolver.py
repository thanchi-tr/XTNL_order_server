"""The one resolver of an accepted ARM document (design §B9 Versions and rows in force; plan S25). Pure.

Shared by validation, the installer (S33), the arm-tracker (S26) and `arm config resolve` (S35):

* **Rows in force at `t`:** per assignment key (scope plus the RESOURCE of the named strategy, `Assignment.key`) and
  per route, the row with the greatest `effective_from_ms <= t` (validation refuses two rows with one key and one
  `effective_from_ms`; a tie that slips through anyway goes to the higher row id). An assignment with `enabled =
  false` is a tombstone: nothing is in force for its key from then on. A future-dated row is accepted ahead and
  applies only from its `effective_from_ms`.
* **Precedence** among the rows in force that match a position: `edge` (later) > `symbol` > `account` > `adapter`, a
  set column beating NULL (`Assignment.specificity`, compared as a tuple). Since rows in force are one per key, the
  resolver gives at most one row per (scope, RESOURCE).
* Assignments are resolved at the position's arming time and route policy at each event's bar end (callers pass that
  time). A newer row or a tombstone never touches an armed position: its pin is read from `arm_positions`, never
  from here (§B12).
"""
from __future__ import annotations

from dataclasses import dataclass

from src.armconfig.validate import Assignment, RoutePolicy, StrategyEntry, ValidDocument, latest_per_key


@dataclass(frozen=True)
class PositionScope:
    """What a position matches assignments on. `account_id` is the account id (aliases were mapped at validation);
    `edge_code` is the position's edge (Phase 1: every assignment has a NULL edge, so it matches any)."""

    adapter_id: str
    account_id: str
    symbol: str
    edge_code: str | None = None


@dataclass(frozen=True)
class Resolved:
    """The assignment in force for a position and the strategy row it names (the pin source at arming)."""

    assignment: Assignment
    strategy: StrategyEntry


def assignments_in_force(doc: ValidDocument, at_ms: int) -> tuple[Assignment, ...]:
    """Per key, the row with the greatest `effective_from_ms <= at_ms`, tombstones dropped; ordered by row id."""
    best = latest_per_key(doc.assignments, lambda a: a.key, at_ms)
    return tuple(sorted((a for a in best.values() if a.enabled), key=lambda a: a.row_id))


def _matches(a: Assignment, scope: PositionScope, resource: str) -> bool:
    return (a.resource == resource
            and (a.edge_code is None or a.edge_code == scope.edge_code)
            and (a.symbol is None or a.symbol == scope.symbol)
            and (a.account is None or a.account == scope.account_id)
            and (a.adapter is None or a.adapter == scope.adapter_id))


def resolve(doc: ValidDocument, scope: PositionScope, resource: str, at_ms: int) -> Resolved | None:
    """The assignment in force at `at_ms` for this position and RESOURCE, by precedence; None when none applies."""
    candidates = [a for a in assignments_in_force(doc, at_ms) if _matches(a, scope, resource)]
    if not candidates:
        return None
    chosen = max(candidates, key=lambda a: a.specificity)
    strategy = doc.strategy(chosen.strategy_code, chosen.strategy_version)
    if strategy is None:   # validation guarantees it; a hand-built document without it resolves to nothing
        return None
    return Resolved(chosen, strategy)


def route_policy_at(doc: ValidDocument, route: str, at_ms: int) -> RoutePolicy:
    """The route's policy in force at `at_ms` (a bar end), else the default policy (§B8 defaults)."""
    best = latest_per_key((p for p in doc.route_policies if p.route == route), lambda p: p.route, at_ms).get(route)
    return best if best is not None else RoutePolicy(route=route)


__all__ = ["PositionScope", "Resolved", "assignments_in_force", "resolve", "route_policy_at"]
