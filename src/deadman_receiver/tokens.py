"""The receiver's bearer tokens: they follow the secrets source, with an overlap window (operations.md §13
"rotation without restart"; IMPLEMENTATION_PLAN 8.2).

The accepted set is re-read from the secrets source (`src.secrets.source`: the environment, or one AWS Secrets
Manager secret in a region outside eu-west-2, cached and refreshed by its own thread) on every receiver tick,
without I/O on the event loop (`resolve_cached`):

* XTNL_RECEIVER_TOKEN        the current token (required, >= 16 characters);
* XTNL_RECEIVER_TOKEN_NEXT   an optional next token, accepted beside the current one (the rotation step that
                             needs no timing: put the new token here, switch the engine, then promote it);
* a token that leaves the set (a rotation replaced it) stays accepted for XTNL_RECEIVER_TOKEN_OVERLAP_S
  (default 1800 s) after the receiver first saw it gone, so the engine, which re-reads its own secret every
  5 min (and at once after a 401), is never refused while it catches up.

A secret that becomes unreadable or invalid (no token, a short token) keeps the last valid set (never an empty
or weaker one) and is reported as `issue`, which the service pages. A request with an unknown token asks the
source to re-read the secret, at most once per `INVALIDATE_EVERY_MS`, so a receiver whose cached secret is
behind catches up within seconds. Constant-time compares against every accepted token; no value is ever
logged, repr'd or put into a message (only variable names and the slot that matched).
"""
from __future__ import annotations

import hmac
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Final, Literal

log = logging.getLogger(__name__)

TOKEN_VAR: Final = "XTNL_RECEIVER_TOKEN"
TOKEN_NEXT_VAR: Final = "XTNL_RECEIVER_TOKEN_NEXT"
TOKEN_OVERLAP_VAR: Final = "XTNL_RECEIVER_TOKEN_OVERLAP_S"
MIN_TOKEN_CHARS: Final = 16
DEFAULT_OVERLAP_S: Final = 1_800
MIN_OVERLAP_S: Final = 600            # > 2 x the engine's 300 s secret refresh
MAX_OVERLAP_S: Final = 86_400
INVALIDATE_EVERY_MS: Final = 60_000
Slot = Literal["current", "next", "previous"]


class TokenError(ValueError):
    """The token variables are invalid (the message names variables only)."""


@dataclass(frozen=True)
class TokenSet:
    current: str = field(repr=False)
    next: str | None = field(default=None, repr=False)

    def values(self) -> tuple[str, ...]:
        return (self.current,) if self.next is None else (self.current, self.next)


def _get(env: Mapping[str, str], name: str) -> str | None:
    v = env.get(name)
    return v.strip() if v is not None and v.strip() else None


def token_set(env: Mapping[str, str]) -> TokenSet:
    """The current / next tokens from one resolved mapping. Raises TokenError (variable names only)."""
    current = _get(env, TOKEN_VAR)
    if current is None or len(current) < MIN_TOKEN_CHARS:
        raise TokenError(f"{TOKEN_VAR} must be set (at least {MIN_TOKEN_CHARS} characters)")
    nxt = _get(env, TOKEN_NEXT_VAR)
    if nxt is not None and len(nxt) < MIN_TOKEN_CHARS:
        raise TokenError(f"{TOKEN_NEXT_VAR} must have at least {MIN_TOKEN_CHARS} characters when set")
    return TokenSet(current, None if nxt == current else nxt)


def overlap_s(env: Mapping[str, str]) -> int:
    raw = _get(env, TOKEN_OVERLAP_VAR)
    if raw is None:
        return DEFAULT_OVERLAP_S
    try:
        v = int(raw)
    except ValueError:
        v = -1
    if not MIN_OVERLAP_S <= v <= MAX_OVERLAP_S:
        raise TokenError(f"{TOKEN_OVERLAP_VAR} must be an integer in [{MIN_OVERLAP_S}, {MAX_OVERLAP_S}]")
    return v


class TokenRing:
    """The accepted tokens. Owned by the receiver's event loop (not thread-safe); `load` must not block."""

    def __init__(self, initial: TokenSet, *, load: Callable[[], Mapping[str, str]] | None = None,
                 invalidate: Callable[[], None] | None = None, overlap_ms: int = DEFAULT_OVERLAP_S * 1000,
                 parse: Callable[[Mapping[str, str]], TokenSet] = token_set, label: str = "dead-man receiver") -> None:
        self._set = initial
        self._load = load
        self._parse = parse          # K10: the dashboard's heartbeat listener reads its own keys (heartbeat.json)
        self._label = label
        self._invalidate = invalidate
        self._overlap_ms = overlap_ms
        self._retired: dict[str, int] = {}            # token -> accepted until (receiver ms)
        self._last_invalidate_ms: int | None = None
        self._last_slot: Slot | None = None
        self.issue: str | None = None
        self.rotations = 0

    @property
    def current(self) -> TokenSet:
        return self._set

    @property
    def overlap_ms(self) -> int:
        """How long a replaced token is still accepted. Settable (K10: the dashboard re-reads `token_overlap_s`);
        a new value applies to tokens replaced from then on, never to one already in its overlap."""
        return self._overlap_ms

    @overlap_ms.setter
    def overlap_ms(self, value: int) -> None:
        self._overlap_ms = int(value)

    def retired_count(self, now_ms: int) -> int:
        return sum(1 for until in self._retired.values() if until > now_ms)

    def refresh(self, now_ms: int) -> None:
        """Re-read the source (no I/O) and apply a rotation; prune expired previous tokens."""
        self._retired = {t: until for t, until in self._retired.items() if until > now_ms}
        if self._load is None:
            return
        try:
            new = self._parse(self._load())
        except TokenError as exc:
            if self.issue != str(exc):
                log.error("%s token source invalid (keeping the last valid tokens): %s", self._label, exc)
            self.issue = str(exc)
            return
        except Exception as exc:   # the source itself (e.g. the secret not read yet): keep the last valid set
            msg = f"token source unreadable ({type(exc).__name__})"
            if self.issue != msg:
                log.error("%s %s; keeping the last valid tokens", self._label, msg)
            self.issue = msg
            return
        if self.issue is not None:
            log.warning("%s token source valid again", self._label)
        self.issue = None
        if new == self._set:
            return
        kept = set(new.values())
        for old in self._set.values():
            if old not in kept:
                self._retired[old] = now_ms + self._overlap_ms
        for t in kept:
            self._retired.pop(t, None)
        self._set = new
        self.rotations += 1
        log.warning("%s tokens rotated: %s; the replaced token is accepted for %d s more", self._label,
                    "current + next" if new.next is not None else "current only", self._overlap_ms // 1000)

    def match(self, presented: str, now_ms: int) -> Slot | None:
        """The slot whose token equals `presented` (every candidate compared, constant time), else None."""
        p = presented.encode("utf-8")
        hits: list[Slot] = []
        cands: list[tuple[Slot, str]] = [("current", self._set.current)]
        if self._set.next is not None:
            cands.append(("next", self._set.next))
        cands.extend(("previous", t) for t, until in self._retired.items() if until > now_ms)
        for slot, token in cands:
            if hmac.compare_digest(p, token.encode("utf-8")):
                hits.append(slot)
        return hits[0] if hits else None

    def authorized(self, header: str | None, now_ms: int) -> bool:
        presented = header.strip() if header is not None else ""
        slot = self.match(presented[len("Bearer "):].strip(), now_ms) if presented.startswith("Bearer ") else None
        if slot is None:
            self._ask_reread(now_ms)
            return False
        if slot != self._last_slot:
            # Lets the operator see when the engine switched (rotation step 3: promote the next token).
            log.info("%s: heartbeat authenticated with the %s token", self._label, slot)
            self._last_slot = slot
        return True

    def _ask_reread(self, now_ms: int) -> None:
        if self._invalidate is None:
            return
        if self._last_invalidate_ms is not None and now_ms - self._last_invalidate_ms < INVALIDATE_EVERY_MS:
            return
        self._last_invalidate_ms = now_ms
        try:
            self._invalidate()
        except Exception as exc:
            log.warning("%s: secret re-read request failed (%s)", self._label, type(exc).__name__)


__all__ = ["DEFAULT_OVERLAP_S", "TOKEN_NEXT_VAR", "TOKEN_OVERLAP_VAR", "TOKEN_VAR", "TokenError", "TokenRing",
           "TokenSet", "overlap_s", "token_set"]
