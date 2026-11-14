"""Operator CLI value types: who acts (Actor), what came back (Report), and why a command stopped.

Every mutating command names an operator and a written reason; two-person commands also name an approver
who is not the operator (operations.md s16 and the runbooks' two-person rule). The same rules are CHECK
constraints on `operator_audit` (migration 0007), so a row that bypassed the CLI is held to them too.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final, Literal

NAME_RE: Final = re.compile(r"^[A-Za-z0-9._@-]{2,64}$")
MIN_REASON: Final = 8
MAX_REASON: Final = 1000
RESERVED: Final = frozenset({"system"})   # halt_events."by" = 'system' means the RiskSweep

Outcome = Literal["ok", "applied", "noop", "refused"]


class OpsUsageError(ValueError):
    """The command line is incomplete or invalid (exit 2; nothing is written or audited)."""


class OpsRefused(Exception):
    """The command was refused by a precondition; nothing changed (exit 3, audited as 'refused')."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class Actor:
    operator: str
    reason: str
    approver: str | None = None
    os_user: str | None = None

    def validate(self, *, two_person: bool) -> None:
        if not NAME_RE.match(self.operator or ""):
            raise OpsUsageError("--operator must be 2-64 characters of [A-Za-z0-9._@-]")
        if self.operator.lower() in RESERVED:
            raise OpsUsageError(f"--operator {self.operator!r} is reserved")
        reason = (self.reason or "").strip()
        if len(reason) < MIN_REASON or len(reason) > MAX_REASON:
            raise OpsUsageError(f"--reason must be a written reason of {MIN_REASON}-{MAX_REASON} characters")
        if self.approver is not None:
            if not NAME_RE.match(self.approver) or self.approver.lower() in RESERVED:
                raise OpsUsageError("--approver must be 2-64 characters of [A-Za-z0-9._@-]")
            if self.approver.lower() == self.operator.lower():
                raise OpsUsageError("--approver must be a second person, not the operator")
        elif two_person:
            raise OpsUsageError("this command needs two-person control: add --approver <second operator>")


@dataclass
class Report:
    """What a command returns: an outcome, ordered fields, optional rows, warnings."""

    command: str
    outcome: Outcome = "ok"
    fields: dict[str, Any] = field(default_factory=dict)
    rows: list[dict[str, Any]] = field(default_factory=list)
    sections: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    audit_id: int | None = None
    degraded: bool = False   # applied, but a best-effort follow-up (the etcd mirror) failed: exit 4


def iso_ms(ms: object) -> str | None:
    """Epoch milliseconds -> ISO-8601 UTC ('2026-11-13T09:21:13.000Z'); None stays None."""
    if ms is None:
        return None
    try:
        value = int(str(ms))
    except ValueError:
        return str(ms)
    return datetime.fromtimestamp(value / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{value % 1000:03d}Z"
