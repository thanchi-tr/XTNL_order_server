"""`operator_requests` kinds and payloads (migration 0007, widened by 0009; IMPLEMENTATION_PLAN 5.5 / 5.6).

The operator CLI (`python -m src.ops`) writes a request in the same transaction as its `operator_audit`
row, fenced on the epoch that was current; the leader of that epoch consumes it in-process and marks it
consumed (fenced). A request of an older epoch is moot (a new leader starts fresh).

| kind                 | target       | payload (jsonb)                                             | consumer (5.6)                                    |
|----------------------|--------------|-------------------------------------------------------------|---------------------------------------------------|
| `reset_task`         | task name    | `{}`                                                        | `TaskSupervisor.reset_task(name, operator)`       |
| `release_account`    | account_id   | `{"v": 1, "account_id", "adapter_id"}`                      | `RiskSweep.release_account(operator, account_id)` |
| `refresh_cash_flows` | account_id   | `{"v": 1, "account_id", "adapter_id", "since_ms": int|null}`| orchestrator `fetch_cash_flows(since)` for it     |

`since_ms = null` means "since the current trading day's 17:00 America/New_York roll" (oversee-rules.md
§4 refresh_cash_flows); the consumer resolves it with its own roll clock. The orchestrator keeps its
at-most-once-a-minute-per-account limit (operations.md §15); the request is consumed either way and its
`result` says what happened (`requested`, `rate_limited`, `unknown_account`, ...).
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

from src.db.fence import FENCE

RequestKind = Literal["reset_task", "release_account", "refresh_cash_flows"]
REQUEST_KINDS: Final[frozenset[str]] = frozenset({"reset_task", "release_account", "refresh_cash_flows"})
PAYLOAD_VERSION: Final = 1

#: The pending requests of an epoch, with their payload (5.6 replaces pg_ports' payload-less query with this).
SQL_PENDING_WITH_PAYLOAD: Final = (
    "SELECT r.request_id, r.kind, r.target, a.operator, r.payload FROM operator_requests r "
    "JOIN operator_audit a ON a.audit_id = r.audit_id "
    "WHERE r.requested_epoch = $1::bigint AND r.consumed_at_ms IS NULL ORDER BY r.request_id LIMIT 50"
)
#: The insert the CLI uses for a request with a payload ($1 = epoch, fenced in the statement).
SQL_INSERT_WITH_PAYLOAD: Final = (
    "INSERT INTO operator_requests (kind, target, audit_id, requested_epoch, payload) "
    f"SELECT $2::text, $3::text, $4::bigint, $1::bigint, $5::text::jsonb WHERE {FENCE} RETURNING request_id"
)


class RequestPayloadError(ValueError):
    """A request payload does not match its kind (the consumer records it as `bad_payload`)."""


@dataclass(frozen=True)
class AccountRelease:
    """`release_account`: release the RiskSweep's in-memory quarantine of `account_id`."""

    account_id: str
    adapter_id: str | None

    def to_json(self) -> str:
        return _dumps({"v": PAYLOAD_VERSION, "account_id": self.account_id, "adapter_id": self.adapter_id})


@dataclass(frozen=True)
class CashFlowRefresh:
    """`refresh_cash_flows`: fetch_cash_flows for `account_id` since `since_ms` (None = the current roll)."""

    account_id: str
    adapter_id: str | None
    since_ms: int | None = None

    def to_json(self) -> str:
        return _dumps({"v": PAYLOAD_VERSION, "account_id": self.account_id, "adapter_id": self.adapter_id,
                       "since_ms": self.since_ms})


def _dumps(d: Mapping[str, Any]) -> str:
    return json.dumps(dict(d), sort_keys=True, separators=(",", ":"))


def _obj(payload: object) -> Mapping[str, Any]:
    if isinstance(payload, (str, bytes)):
        try:
            payload = json.loads(payload)
        except ValueError as exc:
            raise RequestPayloadError("payload is not JSON") from exc
    if payload is None:
        return {}
    if not isinstance(payload, Mapping):
        raise RequestPayloadError("payload is not an object")
    return payload


def _account(target: str, p: Mapping[str, Any]) -> tuple[str, str | None]:
    account = p.get("account_id", target)
    if not isinstance(account, str) or not account or account != target:
        raise RequestPayloadError("account_id must equal the request target")
    adapter = p.get("adapter_id")
    if adapter is not None and not isinstance(adapter, str):
        raise RequestPayloadError("adapter_id must be a string or null")
    return account, adapter


def parse_release(target: str, payload: object) -> AccountRelease:
    account, adapter = _account(target, _obj(payload))
    return AccountRelease(account, adapter)


def parse_cash_flow_refresh(target: str, payload: object) -> CashFlowRefresh:
    p = _obj(payload)
    account, adapter = _account(target, p)
    since = p.get("since_ms")
    if since is not None and (isinstance(since, bool) or not isinstance(since, int) or since < 0):
        raise RequestPayloadError("since_ms must be a non-negative integer or null")
    return CashFlowRefresh(account, adapter, since)


__all__ = ["AccountRelease", "CashFlowRefresh", "PAYLOAD_VERSION", "REQUEST_KINDS", "RequestKind",
           "RequestPayloadError", "SQL_INSERT_WITH_PAYLOAD", "SQL_PENDING_WITH_PAYLOAD", "parse_cash_flow_refresh",
           "parse_release"]
