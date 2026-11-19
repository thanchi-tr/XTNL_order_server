"""Synthetic ARM configuration documents for the S25 tests (instruments, adapters and accounts are made up)."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.armconfig.document import Document, parse_document
from src.armconfig.validate import KnownIds, ValidationResult, ValidDocument, ValidationStatus, validate_document

KNOWN = KnownIds(
    symbols=frozenset({"SYN.A", "SYN.B"}),
    adapters=frozenset({"mock", "mock2"}),
    accounts={"acct-1": "acct-1", "main": "acct-1", "acct-2": "acct-2"},
    routes=frozenset({"main", "second"}),
)
META: dict[str, object] = {"set_at_ms": 1_700_000_000_000, "session_user": "postgres", "current_user": "xtnl_arm_writer"}
KIND_A = "break_even_on_risk_close"
FIRST_BAR = [{"kind": "first_bar", "version": 1, "params": {}}]


def strategy_row(row_id: int = 1, *, code: str = "be_a", version: int = 1, kind: str = KIND_A, impl_version: int = 1,
                 params: Mapping[str, object] | None = None, **extra: object) -> dict[str, object]:
    return {"id": row_id, "code": code, "version": version, "kind": kind, "impl_version": impl_version,
            "params": dict(params) if params is not None else {"guards": FIRST_BAR}, **META, **extra}


def assignment_row(row_id: int = 1, *, symbol: str | None = None, account: str | None = None,
                   adapter: str | None = None, code: str = "be_a", version: int = 1, enabled: bool = True,
                   effective_from_ms: int = 1_000, edge_code: str | None = None, **extra: object) -> dict[str, object]:
    return {"id": row_id, "edge_code": edge_code, "symbol": symbol, "account": account, "adapter": adapter,
            "strategy_code": code, "strategy_version": version, "enabled": enabled,
            "effective_from_ms": effective_from_ms, **META, **extra}


def route_row(row_id: int = 1, *, route: str = "main", mode: str = "every_bar",
              pre: list[str] | None = None, post: list[str] | None = None, effective_from_ms: int = 1_000,
              **extra: object) -> dict[str, object]:
    return {"id": row_id, "route": route, "mode": mode, "pre_channels": pre if pre is not None else ["arm_email"],
            "post_channels": post if post is not None else ["arm_email", "arm_push"],
            "effective_from_ms": effective_from_ms, **META, **extra}


def raw_doc(strategies: list[dict[str, object]] | None = None, assignments: list[dict[str, object]] | None = None,
            route_policy: list[dict[str, object]] | None = None, schema_version: int = 1,
            **extra: object) -> dict[str, Any]:
    return {"schema_version": schema_version,
            "strategies": strategies if strategies is not None else [strategy_row()],
            "assignments": assignments if assignments is not None else [assignment_row()],
            "route_policy": route_policy if route_policy is not None else [route_row()], **extra}


def parsed(raw: Mapping[str, object]) -> Document:
    doc = parse_document(raw)
    assert isinstance(doc, Document), doc
    return doc


def validated(raw: Mapping[str, object], known: KnownIds = KNOWN) -> ValidationResult:
    return validate_document(parse_document(raw), known)


def valid(raw: Mapping[str, object], known: KnownIds = KNOWN) -> ValidDocument:
    result = validated(raw, known)
    assert result.status is ValidationStatus.VALID, [p.page_text for p in result.problems]
    assert result.document is not None
    return result.document
