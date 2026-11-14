"""PRE_OPEN readiness check (operations.md §10)."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from src.calendar.readiness import ReadinessDispatcher, readiness_check


@dataclass
class FakeActions:
    streaming: bool = True
    token_s: float | None = 48 * 3600.0
    chrony: bool = True
    fail_snapshot: bool = False
    calls: list[str] = field(default_factory=list)
    pages: list[tuple[str, str, str]] = field(default_factory=list)
    gate: asyncio.Event | None = None

    async def reset_backoff_and_reconnect(self, adapter_id: str) -> None:
        self.calls.append(f"reconnect:{adapter_id}")
        if self.gate is not None:
            await self.gate.wait()

    async def refresh_account_snapshot(self, adapter_id: str) -> None:
        self.calls.append(f"snapshot:{adapter_id}")
        if self.fail_snapshot:
            raise ConnectionError("REST down")

    async def fetch_cash_flows(self, adapter_id: str) -> None:
        self.calls.append(f"cash_flows:{adapter_id}")

    def token_valid_for_s(self, adapter_id: str) -> float | None:
        return self.token_s

    def chrony_synced(self) -> bool:
        return self.chrony

    def adapter_streaming(self, adapter_id: str) -> bool:
        return self.streaming

    def page(self, severity: str, title: str, body: str) -> None:
        self.pages.append((severity, title, body))


async def test_all_steps_run_and_ok() -> None:
    a = FakeActions()
    r = await readiness_check(a, "stonex", "pre_open")
    assert r.ok and a.calls == ["reconnect:stonex", "snapshot:stonex", "cash_flows:stonex"] and a.pages == []


async def test_problems_are_collected_and_paged() -> None:
    a = FakeActions(streaming=False, token_s=3600.0, chrony=False, fail_snapshot=True)
    r = await readiness_check(a, "pepper", "roll")
    assert not r.ok
    assert len(r.problems) == 4 and "cash_flows:pepper" in a.calls   # later steps still ran
    assert a.pages and a.pages[0][0] == "critical" and "pepper" in a.pages[0][1]


async def test_no_token_means_no_token_problem() -> None:
    a = FakeActions(token_s=None)
    assert (await readiness_check(a, "stonex", "pre_open")).ok


async def test_dispatcher_dedupes_in_flight_requests() -> None:
    a = FakeActions(gate=asyncio.Event())
    d = ReadinessDispatcher(a)
    stop = asyncio.Event()
    runner = asyncio.create_task(d.run(stop))
    d.request("stonex", "pre_open")
    await asyncio.sleep(0.01)
    d.request("stonex", "roll")          # in flight: ignored
    assert a.gate is not None
    a.gate.set()
    await asyncio.sleep(0.01)
    d.request("stonex", "roll")          # done: accepted again
    await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(runner, 1)
    assert [r.reason for r in d.reports] == ["pre_open", "roll"]
