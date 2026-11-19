"""`operator_requests` kinds and payloads (migration 0007, widened by 0009 and 0017; IMPLEMENTATION_PLAN 5.5 / 5.6).

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

ARM and market-data kinds (0017, design §B13; each is read only once `optional-bootstrap` registers its handler, S31 /
S32, and filed by the ops CLI, S34; a handler only enqueues the work to the owning task, which answers later):

| kind                 | target                       | owning task (answers applied / refused / moot)                      |
|----------------------|------------------------------|---------------------------------------------------------------------|
| `arm_disarm`         | `adapter_id:position_id`     | `arm-tracker`: the position -> DISARMED                             |
| `arm_repin`          | `adapter_id:position_id`     | `arm-tracker`: `migrate_state` under the `row_version` CAS          |
| `arm_rearm`          | `adapter_id:position_id`     | `arm-tracker`: SUSPENDED -> ARMED, cursor as at arming (§B6.6)      |
| `arm_clear_failed`   | `SCOPE_KIND:scope_id`        | FAILED probe runner: probe the recorded cause, then clear (§B14)    |
| `md_backfill`        | series id                    | `md-bars`: re-fetch a bar range (also from `arm replay`)            |
| `md_rebuild`         | indicator id                 | `md-indicators`: rebuild one indicator                              |
| `md_writer_reset`    | `md-writer`                  | `md-writer`: reset its circuit and re-run the bootstrap             |
| `md_rehash`          | `md`                         | `md-writer`: rewrite members under the current HMAC key id (§A13)   |

Payloads (S32, `src/runtime/optional_plan.py`): `md_backfill` takes `{"v": 1, "from_bucket_ms": int,
"to_bucket_ms": int}` (bucket starts on the series' grid); `md_rebuild`, `md_writer_reset`, `md_rehash` and
`arm_clear_failed` take `{}`; `arm_disarm` and `arm_rearm` take `{}`, `arm_repin` `{}` (the assignment in force) or
`{"v": 1, "strategy_code": str, "strategy_version": int}` (the ops CLI, S34; the `arm-tracker` kinds wait for their
handler). None ever carries a price or value. The ops CLI (`src/ops/arm.py`) files every one of these kinds only while
`arm_status` shows a leader heartbeat younger than 60 s and fewer than 20 of them are pending in the epoch. While a
scope is FAILED, `md_rebuild`, `md_backfill`, `md_writer_reset`, `md_rehash` and `reset_task` are still applied.

Kind filter (design §B13, plan S10, RT-M10): the leader reads only the kinds it can act on, the core kinds above
plus those with a registered handler (`EngineSession.register_request_handler`), so a request of another kind stays
pending (never `unsupported_kind`) and is applied once its handler registers in the same epoch; pending requests of
an unhandled kind never hide core ones behind the `LIMIT 50` (the `(requested_epoch, kind)` index serves the filter).
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

from src.db.fence import FENCE

RequestKind = Literal["reset_task", "release_account", "refresh_cash_flows", "arm_disarm", "arm_repin", "arm_rearm",
                      "arm_clear_failed", "md_backfill", "md_rebuild", "md_writer_reset", "md_rehash"]
#: The kinds the session itself consumes (always in the pending query's kind list).
CORE_REQUEST_KINDS: Final[frozenset[str]] = frozenset({"reset_task", "release_account", "refresh_cash_flows"})
#: The ARM and market-data kinds (0017): read only while a handler for them is registered (design §B13).
ARM_REQUEST_KINDS: Final[frozenset[str]] = frozenset({
    "arm_disarm", "arm_repin", "arm_rearm", "arm_clear_failed", "md_backfill", "md_rebuild", "md_writer_reset",
    "md_rehash"})
#: Repair kinds still applied while a scope is FAILED (they repair, never advise; design §B13, §B14).
REPAIR_REQUEST_KINDS: Final[frozenset[str]] = frozenset({
    "md_rebuild", "md_backfill", "md_writer_reset", "md_rehash", "reset_task"})
#: Every kind the `operator_requests_kind_check` constraint allows (0017 widens both together, design §B13).
REQUEST_KINDS: Final[frozenset[str]] = CORE_REQUEST_KINDS | ARM_REQUEST_KINDS
PAYLOAD_VERSION: Final = 1

#: The pending requests of an epoch whose kind is in `$2` (the core kinds plus the registered ones), with their
#: payload (5.6 replaces pg_ports' payload-less query with this; S10 adds the kind filter). `$3` lists the ids the
#: session has accepted and not answered yet: they are left out before the `LIMIT 50`, so requests waiting on a
#: hung owner task never hide a core one (design §B13).
SQL_PENDING_WITH_PAYLOAD: Final = (
    "SELECT r.request_id, r.kind, r.target, a.operator, r.payload FROM operator_requests r "
    "JOIN operator_audit a ON a.audit_id = r.audit_id "
    "WHERE r.requested_epoch = $1::bigint AND r.consumed_at_ms IS NULL AND r.kind = ANY($2::text[]) "
    "AND NOT (r.request_id = ANY($3::bigint[])) "
    "ORDER BY r.request_id LIMIT 50"
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


__all__ = ["ARM_REQUEST_KINDS", "AccountRelease", "CORE_REQUEST_KINDS", "CashFlowRefresh", "PAYLOAD_VERSION",
           "REPAIR_REQUEST_KINDS", "REQUEST_KINDS", "RequestKind", "RequestPayloadError", "SQL_INSERT_WITH_PAYLOAD", "SQL_PENDING_WITH_PAYLOAD",
           "parse_cash_flow_refresh", "parse_release"]
