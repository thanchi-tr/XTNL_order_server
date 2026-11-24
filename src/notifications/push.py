"""Mobile push transports over HTTPS (httpx): ntfy-style and Pushover-style.

* ntfy: `POST <topic URL>` with the body as the message and `Title` / `Priority` / `Tags` headers;
  optional `Authorization: Bearer <token>`.
* Pushover: `POST /1/messages.json` form with `token`, `user`, `title`, `message`, `priority`
  (critical = emergency priority 2, re-alerting every 60 s until acknowledged in the app, for 1 h).

A body over the backend limit (Pushover 1024 characters, ntfy `NTFY_MAX_BODY` UTF-8 bytes) is cut on a line
boundary by `fit_lines` and ends with "... (truncated: N more lines, see email)", never mid-line.

Errors are `ChannelError`s naming the status only, never the URL (a ntfy topic is a capability) or
the credentials.
"""
from __future__ import annotations

import base64
from typing import Final

import httpx

from src.notifications.errors import ChannelError
from src.notifications.settings import PushSettings
from src.oversee.notify import AlertMessage

NTFY_MAX_BODY: Final = 4000
"""ntfy's message limit is 4096 **bytes**; the body is fitted in UTF-8 bytes against this, with headroom."""
PUSHOVER_MAX_TITLE: Final = 250
PUSHOVER_MAX_MESSAGE: Final = 1024
_NTFY_PRIORITY: Final = {"info": "3", "high": "4", "critical": "5"}
_NTFY_TAGS: Final = {"info": "information_source", "high": "warning", "critical": "rotating_light"}
_PUSHOVER_PRIORITY: Final = {"info": "0", "high": "1", "critical": "2"}


TRUNCATION_MARKER: Final = "\n... (truncated: {n} more lines, see email)"
"""Appended to a push body cut to fit its backend limit; N counts the dropped non-empty lines."""


def _size(text: str, in_bytes: bool) -> int:
    return len(text.encode("utf-8")) if in_bytes else len(text)


def fit_lines(body: str, limit: int, *, in_bytes: bool = False) -> str:
    """`body` cut to fit `limit` (characters, or UTF-8 bytes with `in_bytes`) on a line boundary.

    Unchanged when it fits. Otherwise whole lines are kept up to the last newline that leaves room for
    `TRUNCATION_MARKER`, so a push never ends mid-line and always says it was cut. A first line too long on its own
    is hard-cut (on a character boundary) and still marked. The result is never longer than `limit`.
    """
    if _size(body, in_bytes) <= limit:
        return body
    lines = body.split("\n")
    dropped = 0
    for keep in range(len(lines) - 1, 0, -1):
        dropped += 1 if lines[keep].strip() else 0
        marker = TRUNCATION_MARKER.format(n=dropped)
        head = "\n".join(lines[:keep])
        if _size(head, in_bytes) + _size(marker, in_bytes) <= limit:
            return head + marker
    # no newline fits: hard-cut the first line and still mark the cut
    marker = TRUNCATION_MARKER.format(n=sum(1 for ln in lines[1:] if ln.strip()))
    room = limit - _size(marker, in_bytes)
    if room <= 0:
        return body.encode("utf-8")[:limit].decode("utf-8", "ignore") if in_bytes else body[:limit]
    head = lines[0]
    while head and _size(head, in_bytes) > room:
        over = _size(head, in_bytes) - room
        head = head[:-max(1, over // 4 if in_bytes else over)]
    return head + marker


def _header_text(s: str) -> str:
    """HTTP header values must be latin-1: non-ASCII titles use an RFC 2047 encoded word (ntfy decodes it)."""
    s = " ".join(s.split())[:250]
    if s.isascii():
        return s
    return "=?UTF-8?B?" + base64.b64encode(s.encode("utf-8")).decode("ascii") + "?="


def _raise_for(resp: httpx.Response, backend: str) -> None:
    code = resp.status_code
    if 200 <= code < 300:
        return
    if code in (401, 403):
        raise ChannelError(f"{backend} auth rejected ({code})", auth=True)
    raise ChannelError(f"{backend} rejected ({code})", retryable=code == 429 or code >= 500)


class NtfyTransport:
    backend: Final = "ntfy"

    def __init__(self, settings: PushSettings, client: httpx.AsyncClient) -> None:
        self._s = settings
        self._client = client

    async def deliver(self, msg: AlertMessage) -> None:
        headers = {"Title": _header_text(msg.subject), "Priority": _NTFY_PRIORITY[msg.severity],
                   "Tags": _NTFY_TAGS[msg.severity], "content-type": "text/plain; charset=utf-8"}
        if self._s.token:
            headers["Authorization"] = f"Bearer {self._s.token}"
        try:
            body = fit_lines(msg.body, NTFY_MAX_BODY, in_bytes=True).encode("utf-8")
            resp = await self._client.post(self._s.url, content=body,
                                           headers=headers, timeout=self._s.timeout_s)
        except httpx.HTTPError as exc:
            raise ChannelError(f"ntfy request failed: {type(exc).__name__}") from None
        _raise_for(resp, "ntfy")


class PushoverTransport:
    backend: Final = "pushover"

    def __init__(self, settings: PushSettings, client: httpx.AsyncClient) -> None:
        if not settings.token or not settings.user:
            raise ValueError("pushover needs an application token and a user key")
        self._s = settings
        self._client = client

    async def deliver(self, msg: AlertMessage) -> None:
        priority = _PUSHOVER_PRIORITY[msg.severity]
        form = {"token": self._s.token or "", "user": self._s.user or "",
                "title": " ".join(msg.subject.split())[:PUSHOVER_MAX_TITLE],
                "message": fit_lines(msg.body, PUSHOVER_MAX_MESSAGE), "priority": priority}
        if priority == "2":
            form["retry"] = "60"
            form["expire"] = "3600"
        try:
            resp = await self._client.post(self._s.url, data=form, timeout=self._s.timeout_s)
        except httpx.HTTPError as exc:
            raise ChannelError(f"pushover request failed: {type(exc).__name__}") from None
        if resp.status_code == 400:
            raise ChannelError("pushover rejected (400)", retryable=False)
        _raise_for(resp, "pushover")
        try:
            ok = resp.json().get("status") == 1
        except (ValueError, AttributeError):
            ok = False
        if not ok:
            raise ChannelError("pushover response without status 1")
