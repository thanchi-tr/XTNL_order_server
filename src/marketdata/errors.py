"""Loggable descriptions of `redis-md` errors (design §B3, §B9 wording; plan S12, R11-m).

`safe_redis_error(exc)` returns the exception's class name and, for a Redis `ResponseError` (or a subclass), the
error's code (`NOSCRIPT`, `NOPERM`, `NO_BOOT`, ...) when that code is on `REDIS_ERROR_CODES`. It never returns the
message: a Redis error message can echo a key, a script argument, a URL with credentials or a value, and none of those
may reach a log, a page or PostgreSQL. A code that is not on the allow-list is reported as `other`.

This module cannot import `redis` (the lockout allows it only under `src/marketdata/store`), so a `ResponseError` is
recognised by the class path of any class in the exception's method resolution order.
"""
from __future__ import annotations

import re
from typing import Final

REDIS_ERROR_CODES: Final = frozenset({
    "ERR", "NOSCRIPT", "NOPERM", "NOAUTH", "WRONGPASS", "BUSY", "LOADING", "OOM", "READONLY", "MASTERDOWN",
    "WRONGTYPE", "EXECABORT", "CROSSSLOT", "TRYAGAIN", "CLUSTERDOWN", "MOVED", "ASK", "NOREPLICAS", "UNBLOCKED",
    # codes the md write scripts return (`src/marketdata/store/scripts.py` SCRIPT_CODES; one place to widen)
    "NO_BOOT", "NO_WRITER", "NO_LOCK", "BAD_ARGS", "STALE_SEQ",
})
RESPONSE_ERROR_CLASS: Final = "redis.exceptions.ResponseError"
OTHER_CODE: Final = "other"

_CLASS_TEXT: Final = re.compile(r"<class '([A-Za-z_][A-Za-z0-9_.]*)'>")
_TOKEN: Final = re.compile(r"[A-Z][A-Z0-9_]*")


def _class_path(cls: type) -> str:
    try:
        text = str(cls)
    except Exception:   # a metaclass whose __str__ raises
        return "unknown"
    m = _CLASS_TEXT.fullmatch(text)
    return m.group(1) if m else "unknown"


def _is_response_error(exc: BaseException) -> bool:
    return any(_class_path(cls) == RESPONSE_ERROR_CLASS for cls in type(exc).mro())


def _code(exc: BaseException) -> str:
    try:
        status = getattr(exc, "status_code", None)
    except Exception:   # a property that raises: this helper runs on error paths and must never raise
        status = None
    candidates = [status] if isinstance(status, str) and status else []
    try:
        text = str(exc).strip()
    except Exception:   # an exception whose __str__ raises
        text = ""
    if text:
        candidates.append(text.split()[0].lstrip("-"))
    for candidate in candidates:
        if _TOKEN.fullmatch(candidate) and candidate in REDIS_ERROR_CODES:
            return candidate
    return OTHER_CODE


def redis_error_code(exc: BaseException) -> str | None:
    """The allow-listed code of a Redis `ResponseError` (`other` when not listed), or None for any other exception
    (the store maps script refusals such as `NO_BOOT` with it; never message text)."""
    return _code(exc) if _is_response_error(exc) else None


def safe_redis_error(exc: BaseException) -> str:
    """`ClassName`, or `ClassName(CODE)` for a Redis `ResponseError`; never message text."""
    name = _class_path(type(exc)).rpartition(".")[2]
    if _is_response_error(exc):
        return f"{name}({_code(exc)})"
    return name
