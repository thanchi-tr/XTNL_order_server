"""URL / DSN redaction for log output (IMPLEMENTATION_PLAN 4.9; moved here from src/main.py in 5.6 so the JSON
formatter, the dashboard and the engine share one implementation; src.main re-exports the names).

Every http(s) URL and redis:// / postgres:// DSN is reduced to scheme://host[:port]: userinfo (password),
path, query (e.g. `?password=`) and fragment are removed. The path and query of a URL can carry capabilities
(the ntfy topic, the dead-man check id, a session token).
"""
from __future__ import annotations

import logging
import re
from typing import Final

# Libraries that log full request URLs at INFO (httpx: "HTTP Request: POST https://host/path?query").
QUIET_HTTP_LOGGERS: Final = ("httpx", "httpcore")
REDACTED_SCHEMES: Final = ("https", "http", "rediss", "redis", "postgresql", "postgres")
_SCHEME: Final = "(?P<scheme>" + "|".join(REDACTED_SCHEMES) + ")"
# Userinfo first: everything from "scheme://" up to the LAST "@" of the token (a password may contain "/",
# "?", "#" or "@"), so no part of it survives even when the authority below would end early.
_USERINFO_RE: Final = re.compile(_SCHEME + r"://[^\s'\"<>]*@", re.IGNORECASE)
# The authority may be a bracketed IPv6 literal or a multi-host list (h1:5432,h2:5432); the rest runs to
# whitespace or a quote, so a "(" or "[" inside a path or query never ends the match early and leaves the
# query behind.
_URL_RE: Final = re.compile(
    _SCHEME + r"://(?P<authority>(?:\[[0-9A-Fa-f:.%a-zA-Z]*\]|[^\s/?#'\"<>()\[\]@]+)"
    r"(?::\d+)?)(?P<rest>[^\s'\"<>]*)",
    re.IGNORECASE)


def redact_urls(text: str) -> str:
    """Reduce every http(s) URL and redis:// / postgres:// DSN to scheme://host[:port]."""
    text = _USERINFO_RE.sub(lambda m: f"{m.group('scheme')}://", text)

    def sub(m: re.Match[str]) -> str:
        return f"{m.group('scheme')}://{m.group('authority')}" + ("/..." if m.group("rest") else "")
    return _URL_RE.sub(sub, text)


class UrlRedactingFilter(logging.Filter):
    """Handler filter: any record whose text mentions an http(s) URL or a redis:// / postgres:// DSN is
    rewritten with it reduced to its scheme and host (the message, a formatted exception and the stack).
    Never drops a record."""

    _exc_formatter = logging.Formatter()

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        if "://" in msg:
            red = redact_urls(msg)
            if red != msg:
                record.msg, record.args = red, None
        if record.exc_info and not record.exc_text:
            record.exc_text = self._exc_formatter.formatException(record.exc_info)
        if record.exc_text and "://" in record.exc_text:
            record.exc_text = redact_urls(record.exc_text)
        if record.stack_info and "://" in record.stack_info:
            record.stack_info = redact_urls(record.stack_info)
        return True


__all__ = ["QUIET_HTTP_LOGGERS", "REDACTED_SCHEMES", "UrlRedactingFilter", "redact_urls"]
