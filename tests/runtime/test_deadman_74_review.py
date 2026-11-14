"""IMPLEMENTATION_PLAN 7.4 review: the dashboard's heartbeat copy is best effort and bounded, so a hung dashboard
listener never delays the dead-man beat (the receiver pages after 60 s of silence). No network."""
from __future__ import annotations

import asyncio
import time
from typing import Any

from src.runtime.deadman import DeadmanPayload, DeadmanSender


class _Src:
    def __getattr__(self, name: str) -> Any:
        values: dict[str, Any] = {"node_id": "node-a", "epoch_label": "74", "adapter_ids": (),
                                  "risksweep_pass_ms": {}, "degraded": ()}
        return lambda *a: values.get(name)


class _Receiver:
    host = "receiver"

    def __init__(self) -> None:
        self.posts = 0

    async def post(self, p: DeadmanPayload) -> None:
        self.posts += 1

    async def aclose(self) -> None:
        return None


class _HungCopy:
    host = "dashboard"

    async def post(self, p: DeadmanPayload) -> None:
        await asyncio.Event().wait()           # never answers (a dashboard behind a black-holed VPN route)

    async def aclose(self) -> None:
        return None


async def test_a_hung_dashboard_copy_never_holds_up_the_dead_man_beat() -> None:
    main = _Receiver()
    sender = DeadmanSender(None, _Src(), lease_valid=lambda: True, is_leader=lambda: True, clock_ms=lambda: 1,
                           transport=main, copies=[_HungCopy()], timeout_s=0.2)
    started = time.monotonic()
    ok = await asyncio.wait_for(sender.send_once(), 5.0)
    assert ok is True and main.posts == 1 and sender.sent == 1
    assert sender.failures == 0 and sender.copies_sent == 0      # the copy failed; it is no dead-man failure
    assert time.monotonic() - started < 2.0


def test_a_heartbeat_copy_url_without_a_token_is_a_start_up_warning() -> None:
    from src.runtime.wiring import HEARTBEAT_COPY_TOKEN_VAR, HEARTBEAT_COPY_URL_VAR, load_settings
    from tests.runtime.test_wiring import MOCK, cfg
    url = "https://dashboard.example.invalid:8444/xtnl/heartbeat"
    no_token = load_settings(cfg(MOCK), {**MOCK, HEARTBEAT_COPY_URL_VAR: url})
    assert [w for w in no_token.warnings if HEARTBEAT_COPY_TOKEN_VAR in w and "refuses" in w]
    with_token = load_settings(cfg(MOCK), {**MOCK, HEARTBEAT_COPY_URL_VAR: url,
                                           HEARTBEAT_COPY_TOKEN_VAR: "copy-token-0123456789"})
    assert not [w for w in with_token.warnings if HEARTBEAT_COPY_TOKEN_VAR in w]
    assert "copy-token-0123456789" not in " ".join(with_token.warnings)
