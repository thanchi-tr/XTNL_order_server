"""Structured JSON logs (operations.md §9 Logs; IMPLEMENTATION_PLAN 5.6).

One JSON object per line:

    {"ts": "2026-11-13T09:15:02.123Z", "level": "WARNING", "logger": "src.pubsub.orchestrator",
     "msg": "...", "epoch": "4711", "node": "node-a", "client_order_id": "...", "seq_id": 123}

* `ts` (UTC, ms), `level`, `logger`, `msg` (the formatted message, URL / DSN redacted by the handler's
  `UrlRedactingFilter`), `epoch` and `node` are always present (`null` when unknown: a STANDBY has no epoch).
* `client_order_id`, `position_id`, `seq_id` and `account_id` are present when known, taken from (in order)
  the record itself (`logger.info(..., extra={...})` or an `EntityLogger`), then the task's log context
  (`log_context(...)`, a contextvar, so a coroutine and every synchronous call under it share it).
* `exc` / `stack` carry a formatted exception / stack (redacted like the message).

`node` is set once per process (`set_process_fields(node=...)`) and `epoch` by each leader term (cleared at
the end of the term). JSON is the default on Linux (the engine nodes, shipped off-node by the journald /
CloudWatch agent) and plain text elsewhere; `XTNL_LOG_FORMAT=json|text` overrides. Standard library only.
"""
from __future__ import annotations

import contextlib
import contextvars
import json
import logging
import os
import sys
import time
from collections.abc import Iterator, Mapping, MutableMapping
from typing import Any, Final, Literal

from src.observability.redact import QUIET_HTTP_LOGGERS, UrlRedactingFilter

LOG_FORMAT_VAR: Final = "XTNL_LOG_FORMAT"
LogFormat = Literal["json", "text"]
CONTEXT_FIELDS: Final = ("client_order_id", "position_id", "seq_id", "account_id")
TEXT_FORMAT: Final = "%(asctime)s %(levelname)s %(name)s %(message)s"

_CONTEXT: contextvars.ContextVar[Mapping[str, object]] = contextvars.ContextVar("xtnl_log_context", default={})
_PROCESS: dict[str, str | None] = {"node": None, "epoch": None}


# ============================================================================================ fields
def set_process_fields(*, node: str | None = None, epoch: str | None = None, clear_epoch: bool = False) -> None:
    """Process-wide fields on every line: `node` (once at start-up) and `epoch` (per leader term)."""
    if node is not None:
        _PROCESS["node"] = node
    if epoch is not None:
        _PROCESS["epoch"] = epoch
    elif clear_epoch:
        _PROCESS["epoch"] = None


def clear_epoch_if(epoch: str) -> None:
    """End of a term: drop `epoch` unless a newer term already replaced it."""
    if _PROCESS.get("epoch") == epoch:
        _PROCESS["epoch"] = None


def process_fields() -> dict[str, str | None]:
    return dict(_PROCESS)


def _clean(fields: Mapping[str, object]) -> dict[str, object]:
    return {k: v for k, v in fields.items() if k in CONTEXT_FIELDS and v is not None and v != ""}


@contextlib.contextmanager
def log_context(**fields: object) -> Iterator[None]:
    """Bind entity fields (client_order_id, position_id, seq_id, account_id) to every line logged inside the
    block, by this coroutine and the synchronous code it calls. None values are ignored."""
    merged = {**_CONTEXT.get(), **_clean(fields)}
    token = _CONTEXT.set(merged)
    try:
        yield
    finally:
        _CONTEXT.reset(token)


def current_context() -> Mapping[str, object]:
    return _CONTEXT.get()


def entity_fields(item: object) -> dict[str, object]:
    """The log-context fields of a DTO (orders, positions, accounts, cash flows, ticks): whatever it carries.
    A seq_id of 0 (not stamped yet) is left out."""
    out: dict[str, object] = {}
    for name in ("client_order_id", "position_id", "account_id"):
        v = getattr(item, name, None)
        if isinstance(v, str) and v:
            out[name] = v
    if "position_id" not in out:
        linked = getattr(item, "linked_position_id", None)
        if isinstance(linked, str) and linked:
            out["position_id"] = linked
    seq = getattr(item, "seq_id", None)
    if isinstance(seq, int) and not isinstance(seq, bool) and seq > 0:
        out["seq_id"] = seq
    return out


class EntityLogger(logging.LoggerAdapter):  # type: ignore[type-arg]
    """LoggerAdapter carrying entity fields: `EntityLogger(log, client_order_id=..., seq_id=...).warning(...)`.
    A call's own `extra=` wins over the adapter's fields."""

    def __init__(self, logger: logging.Logger, **fields: object) -> None:
        super().__init__(logger, _clean(fields))

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple[Any, MutableMapping[str, Any]]:
        extra = dict(self.extra or {})
        extra.update(kwargs.get("extra") or {})
        kwargs["extra"] = extra
        return msg, kwargs


# ============================================================================================ formatter
def _iso_ms(created: float) -> str:
    whole = int(created)
    ms = int((created - whole) * 1000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(whole)) + f".{ms:03d}Z"


class JsonFormatter(logging.Formatter):
    """One JSON object per record (see the module docstring). Never raises: an unserialisable value is
    written as its str()."""

    def format(self, record: logging.LogRecord) -> str:
        try:
            msg = record.getMessage()
        except Exception as exc:                     # a bad %-format must not lose the line
            msg = f"{record.msg!r} (format error: {type(exc).__name__})"
        out: dict[str, object] = {
            "ts": _iso_ms(record.created), "level": record.levelname, "logger": record.name, "msg": msg,
            "epoch": _PROCESS.get("epoch"), "node": _PROCESS.get("node")}
        ctx = _CONTEXT.get()
        for name in CONTEXT_FIELDS:
            v = getattr(record, name, None)
            if v is None:
                v = ctx.get(name)
            if v is not None and v != "":
                out[name] = v
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            out["exc"] = record.exc_text
        if record.stack_info:
            out["stack"] = self.formatStack(record.stack_info)
        return json.dumps(out, default=str, ensure_ascii=False, separators=(",", ":"))


# ============================================================================================ set-up
def default_format(environ: Mapping[str, str] | None = None, platform: str | None = None) -> LogFormat:
    env = os.environ if environ is None else environ
    raw = (env.get(LOG_FORMAT_VAR) or "").strip().lower()
    if raw in ("json", "text"):
        return raw  # type: ignore[return-value]
    return "json" if (platform or sys.platform).startswith("linux") else "text"


def configure_logging(level: str, root: logging.Logger | None = None, *, fmt: LogFormat | None = None,
                      node: str | None = None, environ: Mapping[str, str] | None = None) -> LogFormat:
    """Root logging: one stderr handler (when none exists), JSON or text, every handler redacting URLs, the
    HTTP client loggers at WARNING. Returns the format in use."""
    chosen = fmt or default_format(environ)
    if root is None:
        logging.basicConfig(level=level.upper(), format=TEXT_FORMAT)
    target = root or logging.getLogger()
    target.setLevel(level.upper())
    if node is not None:
        set_process_fields(node=node)
    for name in QUIET_HTTP_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
    for handler in target.handlers:
        if not any(isinstance(f, UrlRedactingFilter) for f in handler.filters):
            handler.addFilter(UrlRedactingFilter())
        if chosen == "json":
            handler.setFormatter(JsonFormatter())
    return chosen


__all__ = ["CONTEXT_FIELDS", "EntityLogger", "JsonFormatter", "LOG_FORMAT_VAR", "LogFormat", "clear_epoch_if",
           "configure_logging", "current_context", "default_format", "entity_fields", "log_context",
           "process_fields", "set_process_fields"]
