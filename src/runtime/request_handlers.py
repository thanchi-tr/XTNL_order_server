"""Registered operator-request handlers: what a handler may answer (design §B13, plan S10, RT-M10, R11-m).

The session consumes the core kinds itself (`src/db/operator_requests.py` CORE_REQUEST_KINDS). Any other kind is
dispatched to the handler an optional service registered for it (`EngineSession.register_request_handler`, from
`optional-bootstrap` in S31); while no handler is registered the kind is not even read, so its requests stay
pending (never `unsupported_kind`) and are applied once the handler registers in the same epoch.

**Handlers are non-blocking.** A handler is a plain (synchronous) callable: it decides at once, or it only enqueues
the work to the task that owns it (`arm-tracker`, `md-bars`, `md-indicators`, `md-writer`, the FAILED probe runner)
and returns `Accepted(owner_task)`. An accepted request is left out of later polls (it takes no slot of the pending
query's LIMIT); the owning task answers it later with `EngineSession.complete_request(request_id, result)`, which
only records the answer (no I/O), and the operator loop marks the request consumed on its next pass. A slow probe
therefore never delays a core request such as `reset_task`. An owner task that is not registered when its request
is accepted is waited for (the handler is not called again); an owner that was registered and has since gone gets
the request dispatched again.

Results are stored in `operator_requests.result` as `applied`, `refused:<reason>` or `moot:<reason>`. A reason is
a short code (lower-case letters and `_` only, starting with a letter), never a value: no price, level, amount,
number or credential.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from src.runtime.ports import OperatorRequest

#: A registrable request kind (the `operator_requests.kind` text; the CHECK constraint lists them, 0017).
KIND_PATTERN: Final = re.compile(r"^[a-z][a-z0-9_]{1,62}$")
#: A reason code in a stored result: never free text, so no value can reach `operator_requests.result`.
#: Letters and `_` only: no digit, `.` or `-`, so no number (a price, level or amount) can be written as a reason.
REASON_PATTERN: Final = re.compile(r"^[a-z][a-z_]{0,63}$")
#: A supervisor task name (`TaskSpec.name`).
TASK_PATTERN: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")


def _check_reason(reason: str) -> None:
    if not REASON_PATTERN.match(reason):
        raise ValueError("a request result reason must be a short lower-case code")


@dataclass(frozen=True)
class Applied:
    """The request was carried out."""

    def result(self) -> str:
        return "applied"


@dataclass(frozen=True)
class Refused:
    """The request was refused (it may be filed again later), with a reason code."""

    reason: str

    def __post_init__(self) -> None:
        _check_reason(self.reason)

    def result(self) -> str:
        return f"refused:{self.reason}"


@dataclass(frozen=True)
class Moot:
    """Nothing to do any more (the target has gone, the state already matches), with a reason code."""

    reason: str

    def __post_init__(self) -> None:
        _check_reason(self.reason)

    def result(self) -> str:
        return f"moot:{self.reason}"


@dataclass(frozen=True)
class Accepted:
    """The handler enqueued the work to `owner_task`, which answers through `EngineSession.complete_request`."""

    owner_task: str

    def __post_init__(self) -> None:
        if not TASK_PATTERN.match(self.owner_task):
            raise ValueError("Accepted.owner_task must be a supervisor task name")


#: A final answer (what `complete_request` takes and what is stored).
RequestResult = Applied | Refused | Moot
#: What a handler returns.
HandlerResult = Applied | Refused | Moot | Accepted
#: A registered handler: synchronous and non-blocking (it never awaits; slow work goes to the owning task).
RequestHandler = Callable[[OperatorRequest], HandlerResult]

#: Stored when a handler raises or returns something that is not a HandlerResult (a bug in the handler; logged
#: with the error type only).
HANDLER_ERROR: Final = Refused("handler_error")
#: Stored when a handler returns an awaitable (a handler must not await; a coroutine is closed unrun, a Task or
#: Future is cancelled).
HANDLER_NOT_SYNC: Final = Refused("handler_not_sync")


def check_kind(kind: str) -> None:
    if not KIND_PATTERN.match(kind):
        raise ValueError("a request kind must be lower-case letters, digits and '_'")


__all__ = ["Accepted", "Applied", "HANDLER_ERROR", "HANDLER_NOT_SYNC", "HandlerResult", "KIND_PATTERN", "Moot",
           "REASON_PATTERN", "Refused", "RequestHandler", "RequestResult", "TASK_PATTERN", "check_kind"]
