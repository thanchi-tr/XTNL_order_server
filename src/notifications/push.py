"""Mobile push transports over HTTPS (httpx): ntfy-style and Pushover-style.

* ntfy: `POST <topic URL>` with the body as the message and `Title` / `Priority` / `Tags` headers;
  optional `Authorization: Bearer <token>`.
* Pushover: `POST /1/messages.json` form with `token`, `user`, `title`, `message`, `priority`
  (critical = emergency priority 2, re-alerting every 60 s until acknowledged in the app, for 1 h).

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
PUSHOVER_MAX_TITLE: Final = 250
PUSHOVER_MAX_MESSAGE: Final = 1024
_NTFY_PRIORITY: Final = {"info": "3", "high": "4", "critical": "5"}
_NTFY_TAGS: Final = {"info": "information_source", "high": "warning", "critical": "rotating_light"}
_PUSHOVER_PRIORITY: Final = {"info": "0", "high": "1", "critical": "2"}


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
            resp = await self._client.post(self._s.url, content=msg.body[:NTFY_MAX_BODY].encode("utf-8"),
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
                "message": msg.body[:PUSHOVER_MAX_MESSAGE], "priority": priority}
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
