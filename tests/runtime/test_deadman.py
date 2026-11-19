"""Dead-man push heartbeat payload and sender (operations.md §9)."""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import httpx
import pytest

from src.runtime.deadman import DeadmanSender, build_payload

NOW = 1_000_000


@dataclass
class Source:
    passes: dict[str, int] = field(default_factory=lambda: {"acct-1": NOW - 400, "acct-2": NOW - 2500})
    feed: int | None = NOW - 1200

    def node_id(self) -> str:
        return "n1"

    def epoch_label(self) -> str | None:
        return "101"

    def gate_state(self) -> str | None:
        return "CLOSED"

    def halt_level(self) -> str | None:
        return "NONE"

    def adapter_ids(self) -> Sequence[str]:
        return ["pepper", "stonex"]

    def sm4_state(self, adapter_id: str) -> str | None:
        return {"stonex": "STREAMING"}.get(adapter_id)

    def market_open(self, adapter_id: str) -> bool | None:
        return adapter_id == "stonex"

    def maintenance_until_ms(self, adapter_id: str) -> int | None:
        return NOW + 60_000 if adapter_id == "pepper" else None

    def calendar_version(self) -> int | None:
        return 7

    def last_committed_broker_event_ms(self) -> int | None:
        return NOW - 300

    def oldest_unacked_feed_ms(self) -> int | None:
        return self.feed

    def risksweep_pass_ms(self) -> Mapping[str, int]:
        return self.passes

    def oldest_undelivered_notification_ms(self) -> int | None:
        return None

    def sm8_state(self) -> str | None:
        return "READY"

    def sm5_state(self) -> str | None:
        return "OK"

    def spill_bytes(self) -> int | None:
        return 0

    def degraded(self) -> Sequence[str]:
        return ["feed-observer"]

    def arm_oldest_undelivered_notification_ms(self) -> int | None:
        return None

    def arm_overdue_positions(self) -> int | None:
        return None

    def arm_failed(self) -> None:
        return None


def test_payload_fields_and_ages() -> None:
    p = build_payload(Source(), NOW)
    d = json.loads(p.to_json())
    assert d["node"] == "n1" and d["epoch"] == "101" and d["sent_at_ms"] == NOW
    assert d["adapters"]["stonex"] == {"sm4_state": "STREAMING", "market_open": True, "maintenance_until_ms": None}
    assert d["adapters"]["pepper"] == {"sm4_state": None, "market_open": False, "maintenance_until_ms": NOW + 60_000}
    assert d["oldest_risksweep_pass_age_ms"] == 2500           # the oldest per-account pass
    assert d["oldest_unacked_feed_age_ms"] == 1200 and d["last_committed_broker_event_age_ms"] == 300
    assert d["oldest_undelivered_notification_age_ms"] is None   # present as null, never omitted
    assert d["calendar_version"] == 7 and d["degraded"] == ["feed-observer"]
    expected = {"node", "epoch", "sent_at_ms", "gate_state", "halt_level", "adapters",
                "last_committed_broker_event_age_ms", "oldest_unacked_feed_age_ms", "oldest_risksweep_pass_age_ms",
                "calendar_version", "oldest_undelivered_notification_age_ms", "sm8_state", "sm5_state",
                "spill_bytes", "degraded"}
    assert set(d) == expected


def test_no_accounts_means_null_pass_age() -> None:
    assert build_payload(Source(passes={}), NOW).oldest_risksweep_pass_age_ms is None


def sender(handler: httpx.MockTransport, *, leader: bool = True, lease: bool = True) -> DeadmanSender:
    return DeadmanSender("https://deadman.example/beat", Source(), lease_valid=lambda: lease,
                         is_leader=lambda: leader, client=httpx.AsyncClient(transport=handler),
                         clock_ms=lambda: NOW)


async def test_sender_posts_json() -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(204)

    s = sender(httpx.MockTransport(handler))
    assert await s.send_once()
    assert s.sent == 1 and json.loads(seen[0].content)["node"] == "n1"
    assert seen[0].headers["content-type"] == "application/json"


async def test_sender_skips_when_not_leader_or_lease_invalid_and_counts_failures() -> None:
    calls: list[int] = []

    def ok(req: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(200)

    assert not await sender(httpx.MockTransport(ok), leader=False).send_once()
    assert not await sender(httpx.MockTransport(ok), lease=False).send_once()
    assert calls == []
    bad = sender(httpx.MockTransport(lambda req: httpx.Response(503)))
    assert not await bad.send_once() and bad.failures == 1


def test_url_must_be_http() -> None:
    with pytest.raises(ValueError):
        DeadmanSender("ftp://x", Source(), lease_valid=lambda: True, is_leader=lambda: True)
