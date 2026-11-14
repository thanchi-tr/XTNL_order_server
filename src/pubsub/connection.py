"""SM4: broker adapter stream connection state machine (adapter-isolation.md §5).

Pure and event-driven: callers report what happened, the machine answers with the next state.
Backoff is min(60, 2**n + U(0, 0.5)) s; `reset_backoff()` (PRE_OPEN readiness) clears the count.
Credential errors are not network errors: the 3rd one, or `on_known_bad()` (a credential generation
already recorded bad), ends in AUTH_FAILED, left only by re-arming.
"""
from __future__ import annotations

import random
from collections import deque
from collections.abc import Callable
from enum import Enum

from src.config import ExecutionLockedError, broker_env

MAX_CREDENTIAL_ATTEMPTS = 3
MAX_BACKOFF_S = 60.0


class ConnState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    AUTHENTICATING = "AUTHENTICATING"
    SNAPSHOTTING = "SNAPSHOTTING"
    STREAMING = "STREAMING"
    BACKOFF = "BACKOFF"
    RESYNCING = "RESYNCING"
    AUTH_FAILED = "AUTH_FAILED"
    REFUSED = "REFUSED"


class IllegalConnTransition(RuntimeError):
    """An event arrived in a state that has no transition for it (a driver bug)."""


S = ConnState
_SESSION_STATES = frozenset({S.AUTHENTICATING, S.SNAPSHOTTING, S.STREAMING, S.RESYNCING})


def backoff_delay(attempt: int, jitter: Callable[[], float] = random.random) -> float:
    """min(60, 2**attempt + U(0, 0.5)) seconds."""
    return min(MAX_BACKOFF_S, 2.0 ** min(attempt, 16) + 0.5 * jitter())


class ConnectionStateMachine:
    """SM4 for one adapter. Every transition is appended to `history`."""

    def __init__(self, adapter_id: str, jitter: Callable[[], float] = random.random,
                 env_check: Callable[[], object] = broker_env,
                 on_change: Callable[[ConnState, ConnState, str], None] | None = None) -> None:
        self.adapter_id = adapter_id
        self.state = S.DISCONNECTED
        self.attempt = 0                 # consecutive transient failures (backoff exponent)
        self.credential_errors = 0
        self.backoff_s = 0.0
        self.resync_pending = False      # a gap waits for the ring to fall below 50 %
        # Bounded: a 24/7 engine reconnects for months; keep only the recent transitions for diagnostics.
        self.history: deque[tuple[ConnState, ConnState, str]] = deque(maxlen=500)
        self._jitter = jitter
        self._env_check = env_check
        self._on_change = on_change

    # --- helpers ------------------------------------------------------------------------------
    def _go(self, new: ConnState, why: str) -> ConnState:
        old = self.state
        self.state = new
        self.history.append((old, new, why))
        if self._on_change is not None:
            self._on_change(old, new, why)
        return new

    def _require(self, event: str, *allowed: ConnState) -> None:
        if self.state not in allowed:
            raise IllegalConnTransition(f"{self.adapter_id}: {event} not allowed in {self.state.value}")

    # --- events -------------------------------------------------------------------------------
    def start(self, known_bad: bool = False) -> ConnState:
        """DISCONNECTED -> AUTHENTICATING, or REFUSED (raises) when BROKER_ENV is not allowed,
        or AUTH_FAILED when adapter_auth_state already records the credential generation as bad."""
        self._require("start", S.DISCONNECTED)
        try:
            self._env_check()
        except ExecutionLockedError:
            self._go(S.REFUSED, "env refused")
            raise
        if known_bad:
            return self._go(S.AUTH_FAILED, "credential generation known bad")
        return self._go(S.AUTHENTICATING, "start")

    def on_authenticated(self) -> ConnState:
        self._require("authenticated", S.AUTHENTICATING)
        self.credential_errors = 0
        return self._go(S.SNAPSHOTTING, "authenticated")

    def on_credential_error(self, detail: str = "") -> ConnState:
        """Bad password, PasswordChangeRequired, invalid_grant: retry auth, AUTH_FAILED on the 3rd."""
        self._require("credential_error", S.AUTHENTICATING)
        self.credential_errors += 1
        if self.credential_errors >= MAX_CREDENTIAL_ATTEMPTS:
            return self._go(S.AUTH_FAILED, f"credential error x{self.credential_errors}: {detail}")
        return self._go(S.AUTHENTICATING, f"credential error {self.credential_errors}: {detail}")

    def on_known_bad(self, detail: str = "") -> ConnState:
        """adapter_auth_state already records this credential generation AUTH_FAILED (a cluster-wide fact,
        operations.md §4): AUTHENTICATING -> AUTH_FAILED at once, without spending connect attempts."""
        self._require("known_bad", S.AUTHENTICATING)
        self.credential_errors = MAX_CREDENTIAL_ATTEMPTS
        return self._go(S.AUTH_FAILED, f"credential generation known bad: {detail}" if detail
                        else "credential generation known bad")

    def reset_backoff(self) -> bool:
        """PRE_OPEN readiness (operations.md §10): forget the transient-failure count so the next retry is
        immediate. Returns True when SM4 is in BACKOFF (the driver should end the wait and reconnect now).
        Never leaves AUTH_FAILED: only a changed credential generation or an operator re-arms."""
        self.attempt = 0
        if self.state is S.BACKOFF:
            self.backoff_s = 0.0
            return True
        return False

    def on_transient_error(self, detail: str = "") -> ConnState:
        """Network / timeout / REST / socket / subscribe error, heartbeat timeout -> BACKOFF."""
        self._require("transient_error", *_SESSION_STATES)
        self.backoff_s = backoff_delay(self.attempt, self._jitter)
        self.attempt += 1
        return self._go(S.BACKOFF, f"transient: {detail}")

    def on_backoff_elapsed(self) -> ConnState:
        self._require("backoff_elapsed", S.BACKOFF)
        return self._go(S.AUTHENTICATING, f"retry attempt {self.attempt}")

    def on_snapshot_published(self) -> ConnState:
        self._require("snapshot_published", S.SNAPSHOTTING)
        self.attempt = 0
        return self._go(S.STREAMING, "snapshot published")

    def on_auth_lost(self, detail: str = "") -> ConnState:
        self._require("auth_lost", S.SNAPSHOTTING, S.STREAMING, S.RESYNCING)
        return self._go(S.AUTHENTICATING, f"auth lost: {detail}")

    def on_gap(self, ring_below_half: bool, detail: str = "") -> ConnState:
        """A gap or lost subscription while STREAMING: RESYNCING once the DLQ ring is below 50 %."""
        if self.state != S.STREAMING:
            self.resync_pending = self.state in (S.SNAPSHOTTING, S.RESYNCING) or self.resync_pending
            return self.state
        if not ring_below_half:
            self.resync_pending = True
            return self.state
        self.resync_pending = False
        return self._go(S.RESYNCING, f"gap: {detail}")

    def on_market_open(self) -> ConnState:
        """SM10 open: a STREAMING adapter runs a REST-only RESYNCING."""
        self._require("market_open", S.STREAMING)
        return self._go(S.RESYNCING, "market open")

    def on_reconciled(self, all_subscribed: bool, dropped_after_watermark: bool,
                      ring_below_half: bool, deadline_passed: bool) -> ConnState:
        """End of a RESYNCING pass: STREAMING, retry (new watermark) or BACKOFF."""
        self._require("reconciled", S.RESYNCING)
        if all_subscribed and not dropped_after_watermark:
            self.resync_pending = False
            self.attempt = 0
            return self._go(S.STREAMING, "reconciled")
        if ring_below_half and not deadline_passed:
            return self._go(S.RESYNCING, "retry with a new watermark")
        return self.on_transient_error("resync deadline or ring above 50 %")

    def on_resubscribe_failed(self, failures: int, max_failures: int) -> ConnState:
        self._require("resubscribe_failed", S.RESYNCING)
        if failures >= max_failures:
            return self.on_transient_error(f"{failures} failed re-subscribes")
        return self.state

    def on_rearmed(self) -> ConnState:
        """Credential generation changed or an operator re-armed the adapter."""
        self._require("rearmed", S.AUTH_FAILED)
        self.credential_errors = 0
        self.attempt = 0
        return self._go(S.AUTHENTICATING, "re-armed")

    def stop(self) -> ConnState:
        if self.state in (S.REFUSED, S.DISCONNECTED):
            return self.state
        return self._go(S.DISCONNECTED, "stop")
