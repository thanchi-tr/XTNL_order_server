"""Loggable descriptions of PostgreSQL errors for the ARM and market-data paths (design §B3, §B9; plan S24, R11-m).

`safe_db_error(exc)` returns the exception's class name and, when the driver attached one, its SQLSTATE
(`ClassName(23505)`). It never returns the message, the detail or the query: a PostgreSQL message can echo a row
value, a parameter, a constraint's column values or a DSN, and none of those may reach a log, a page, an outbox body
or PostgreSQL itself. A SQLSTATE that is not five upper-case letters or digits is dropped; a class name that is not a
plain identifier is reported as `Error`. The counterpart for `redis-md` is `src/marketdata/errors.py`
`safe_redis_error`.
"""
from __future__ import annotations

import re
from typing import Final

SQLSTATE_RE: Final = re.compile(r"[0-9A-Z]{5}")
_NAME_RE: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,79}")
UNKNOWN_NAME: Final = "Error"


def _sqlstate(exc: BaseException) -> str | None:
    try:
        code = getattr(exc, "sqlstate", None)
    except Exception:   # a property that raises: this helper runs on error paths and must never raise
        return None
    return code if isinstance(code, str) and SQLSTATE_RE.fullmatch(code) else None


def _name(exc: BaseException) -> str:
    try:
        name = type(exc).__name__
    except Exception:   # a metaclass that misbehaves
        return UNKNOWN_NAME
    return name if isinstance(name, str) and _NAME_RE.fullmatch(name) else UNKNOWN_NAME


def safe_db_error(exc: BaseException) -> str:
    """`ClassName` or `ClassName(SQLSTATE)`; never message text."""
    code = _sqlstate(exc)
    name = _name(exc)
    return name if code is None else f"{name}({code})"


__all__ = ["SQLSTATE_RE", "UNKNOWN_NAME", "safe_db_error"]
