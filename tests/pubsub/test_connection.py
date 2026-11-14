from __future__ import annotations

import pytest

from src.config import ExecutionLockedError, broker_env
from src.pubsub.connection import ConnectionStateMachine, ConnState, IllegalConnTransition, backoff_delay

S = ConnState


def sm(jitter: float = 0.0) -> ConnectionStateMachine:
    return ConnectionStateMachine("mock", jitter=lambda: jitter, env_check=lambda: "sandbox")


def streaming() -> ConnectionStateMachine:
    m = sm()
    m.start()
    m.on_authenticated()
    m.on_snapshot_published()
    return m


def test_happy_path() -> None:
    m = sm()
    assert m.state is S.DISCONNECTED
    assert m.start() is S.AUTHENTICATING
    assert m.on_authenticated() is S.SNAPSHOTTING
    assert m.on_snapshot_published() is S.STREAMING
    assert [h[1] for h in m.history] == [S.AUTHENTICATING, S.SNAPSHOTTING, S.STREAMING]


def test_refused_on_unknown_env() -> None:
    m = ConnectionStateMachine("mock", env_check=lambda: broker_env({"BROKER_ENV": "live"}))
    with pytest.raises(ExecutionLockedError):
        m.start()
    assert m.state is S.REFUSED
    with pytest.raises(IllegalConnTransition):
        m.on_authenticated()


def test_auth_failed_after_three_credential_errors_no_loop() -> None:
    m = sm()
    m.start()
    assert m.on_credential_error("bad pw") is S.AUTHENTICATING
    assert m.on_credential_error("bad pw") is S.AUTHENTICATING
    assert m.on_credential_error("bad pw") is S.AUTH_FAILED
    with pytest.raises(IllegalConnTransition):
        m.on_backoff_elapsed()
    assert m.on_rearmed() is S.AUTHENTICATING
    assert m.credential_errors == 0


def test_known_bad_generation_goes_straight_to_auth_failed() -> None:
    m = sm()
    assert m.start(known_bad=True) is S.AUTH_FAILED


def test_success_resets_credential_counter() -> None:
    m = sm()
    m.start()
    m.on_credential_error()
    m.on_credential_error()
    m.on_authenticated()
    assert m.credential_errors == 0


def test_backoff_formula_and_cap() -> None:
    assert backoff_delay(0, lambda: 0.0) == 1.0
    assert backoff_delay(3, lambda: 1.0) == 8.5
    assert backoff_delay(10, lambda: 0.5) == 60.0
    assert backoff_delay(10_000, lambda: 0.0) == 60.0


def test_transient_errors_back_off_exponentially_and_reset_on_streaming() -> None:
    m = sm(jitter=1.0)
    m.start()
    delays = []
    for _ in range(4):
        assert m.on_transient_error("net") is S.BACKOFF
        delays.append(m.backoff_s)
        assert m.on_backoff_elapsed() is S.AUTHENTICATING
    assert delays == [1.5, 2.5, 4.5, 8.5]
    m.on_authenticated()
    m.on_snapshot_published()
    assert m.attempt == 0


def test_snapshot_error_and_auth_lost() -> None:
    m = sm()
    m.start()
    m.on_authenticated()
    assert m.on_auth_lost() is S.AUTHENTICATING
    m.on_authenticated()
    assert m.on_transient_error("REST 500") is S.BACKOFF


def test_gap_waits_for_ring_below_half_then_resyncs() -> None:
    m = streaming()
    assert m.on_gap(ring_below_half=False) is S.STREAMING
    assert m.resync_pending
    assert m.on_gap(ring_below_half=True) is S.RESYNCING
    assert not m.resync_pending


def test_resync_outcomes() -> None:
    m = streaming()
    m.on_gap(True)
    assert m.on_reconciled(all_subscribed=True, dropped_after_watermark=True, ring_below_half=True,
                           deadline_passed=False) is S.RESYNCING      # retry with a new watermark
    assert m.on_reconciled(True, False, True, False) is S.STREAMING
    m.on_gap(True)
    assert m.on_reconciled(False, False, True, True) is S.BACKOFF      # deadline
    m.on_backoff_elapsed()
    m.on_authenticated()
    m.on_snapshot_published()
    m.on_gap(True)
    assert m.on_reconciled(True, True, False, False) is S.BACKOFF      # ring above 50 %


def test_resubscribe_failures_lead_to_backoff() -> None:
    m = streaming()
    m.on_gap(True)
    assert m.on_resubscribe_failed(1, 3) is S.RESYNCING
    assert m.on_resubscribe_failed(3, 3) is S.BACKOFF


def test_auth_lost_from_streaming_and_resyncing_and_market_open() -> None:
    m = streaming()
    assert m.on_auth_lost() is S.AUTHENTICATING
    m = streaming()
    assert m.on_market_open() is S.RESYNCING
    assert m.on_auth_lost() is S.AUTHENTICATING


def test_illegal_events_raise() -> None:
    m = sm()
    with pytest.raises(IllegalConnTransition):
        m.on_snapshot_published()
    with pytest.raises(IllegalConnTransition):
        m.on_transient_error()
    m.start()
    with pytest.raises(IllegalConnTransition):
        m.on_rearmed()


def test_on_change_callback_and_stop() -> None:
    seen: list[tuple[ConnState, ConnState, str]] = []
    m = ConnectionStateMachine("mock", env_check=lambda: None, on_change=lambda a, b, w: seen.append((a, b, w)))
    m.start()
    assert m.stop() is S.DISCONNECTED
    assert [(a, b) for a, b, _ in seen] == [(S.DISCONNECTED, S.AUTHENTICATING), (S.AUTHENTICATING, S.DISCONNECTED)]
