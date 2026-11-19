"""Dead-man heartbeat HTTPS transport (operations.md §9): httpx.MockTransport only, no network."""
from __future__ import annotations

import asyncio
import json
import logging

import httpx
import pytest

from src.runtime.deadman import DeadmanSender, HttpsDeadmanTransport, check_deadman_url

NOW = 1_000_000


class Source:
    """Minimal DeadmanSource: everything unknown (sent as JSON null), one node."""

    def node_id(self) -> str:
        return "n1"

    def epoch_label(self) -> str | None:
        return "7"

    def gate_state(self) -> str | None:
        return None

    def halt_level(self) -> str | None:
        return "NONE"

    def adapter_ids(self) -> list[str]:
        return []

    def sm4_state(self, adapter_id: str) -> str | None:
        return None

    def market_open(self, adapter_id: str) -> bool | None:
        return None

    def maintenance_until_ms(self, adapter_id: str) -> int | None:
        return None

    def calendar_version(self) -> int | None:
        return None

    def last_committed_broker_event_ms(self) -> int | None:
        return None

    def oldest_unacked_feed_ms(self) -> int | None:
        return None

    def risksweep_pass_ms(self) -> dict[str, int]:
        return {}

    def oldest_undelivered_notification_ms(self) -> int | None:
        return None

    def sm8_state(self) -> str | None:
        return None

    def sm5_state(self) -> str | None:
        return None

    def spill_bytes(self) -> int | None:
        return None

    def degraded(self) -> list[str]:
        return []

    def arm_oldest_undelivered_notification_ms(self) -> int | None:
        return None

    def arm_overdue_positions(self) -> int | None:
        return None

    def arm_failed(self) -> None:
        return None


TOKEN ="deadman-token-never-logged"
URL = "https://deadman.example/ping/check-1234"


def mock(handler: object) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]


def test_url_rules() -> None:
    assert check_deadman_url(URL) == "deadman.example"
    assert check_deadman_url("http://127.0.0.1:9000/beat") == "127.0.0.1"
    for bad in ("http://deadman.example/beat", "ftp://x/y", "https://", "not a url"):
        with pytest.raises(ValueError):
            check_deadman_url(bad)
    assert "check-1234" not in repr(HttpsDeadmanTransport(URL, token=TOKEN))
    assert TOKEN not in repr(HttpsDeadmanTransport(URL, token=TOKEN))


async def test_post_with_bearer_token_and_full_payload() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(202)

    s = DeadmanSender(URL, Source(), lease_valid=lambda: True, is_leader=lambda: True, client=mock(handler),
                      clock_ms=lambda: NOW, token=TOKEN)
    assert await s.send_once() and s.last_ok_ms == NOW
    req = seen[0]
    assert req.method == "POST" and str(req.url) == URL
    assert req.headers["authorization"] == f"Bearer {TOKEN}" and req.headers["content-type"] == "application/json"
    body = json.loads(req.content)
    assert body["oldest_undelivered_notification_age_ms"] is None and body["node"] == "n1"


async def test_failures_logged_without_url_path_or_token(caplog: pytest.LogCaptureFixture) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=req)

    s = DeadmanSender(URL, Source(), lease_valid=lambda: True, is_leader=lambda: True, client=mock(handler),
                      clock_ms=lambda: NOW, token=TOKEN)
    with caplog.at_level(logging.WARNING):
        for _ in range(4):
            assert not await s.send_once()
    assert s.failures == 4 and s.consecutive_failures == 4
    assert "check-1234" not in caplog.text and TOKEN not in caplog.text and "deadman.example" in caplog.text
    assert caplog.records[-1].levelno == logging.ERROR          # a minute of silence: error, not warning


async def test_recovery_resets_consecutive_failures() -> None:
    codes = [500, 204]
    s = DeadmanSender(URL, Source(), lease_valid=lambda: True, is_leader=lambda: True,
                      client=mock(lambda r: httpx.Response(codes.pop(0))), clock_ms=lambda: NOW)
    assert not await s.send_once() and s.consecutive_failures == 1
    assert await s.send_once() and s.consecutive_failures == 0 and s.sent == 1


async def test_owned_client_is_closed_when_run_stops() -> None:
    t = HttpsDeadmanTransport(URL)
    owned = t._http()
    s = DeadmanSender(None, Source(), lease_valid=lambda: True, is_leader=lambda: False, transport=t,
                      interval_s=0.001)
    stop = asyncio.Event()
    stop.set()
    await s.run(stop)
    assert owned.is_closed


def test_sender_needs_url_or_transport() -> None:
    with pytest.raises(ValueError):
        DeadmanSender(None, Source(), lease_valid=lambda: True, is_leader=lambda: True)
