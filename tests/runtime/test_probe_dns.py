"""NodeHealthProbe: the local-address lookup (gethostname + getaddrinfo, blocking DNS) never runs on the
event loop (operations.md §3). Review findings blocking-dns-on-event-loop-in-health-probe /
blocking-dns-in-health-probe. No network: the lookup is an injected function."""
from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

from src.runtime.probes import NodeHealthProbe
from tests.runtime.fakes import FakeClock


async def _primary() -> str:
    return "10.0.0.5"


async def _chrony() -> float:
    return 0.1


def _probe(addresses: Any, clock: FakeClock, timeout_s: float = 0.2) -> NodeHealthProbe:
    return NodeHealthProbe("n1", pg=None, redis=None, chrony=_chrony,  # type: ignore[arg-type]
                           patroni_primary_host=_primary, addresses=addresses, timeout_s=timeout_s,
                           monotonic=clock)


async def test_slow_resolver_never_blocks_the_loop() -> None:
    threads: list[str] = []
    release = threading.Event()

    def slow() -> frozenset[str]:
        threads.append(threading.current_thread().name)
        release.wait(5)                         # a degraded VPC resolver
        return frozenset({"10.0.0.5"})

    probe = _probe(slow, FakeClock(100.0))
    beats = 0

    async def heartbeat() -> None:             # stands in for the LoopWatchdog
        nonlocal beats
        while True:
            beats += 1
            await asyncio.sleep(0.01)

    hb = asyncio.create_task(heartbeat())
    started = time.monotonic()
    try:
        assert await probe._hosts_primary() is False        # unknown address set: not the primary's node
        elapsed = time.monotonic() - started
    finally:
        release.set()
        hb.cancel()
    assert elapsed < 1.0 and beats >= 3                     # the loop kept running during the lookup
    assert threads and threading.main_thread().name not in threads


async def test_address_set_is_cached_and_refreshed() -> None:
    calls: list[int] = []

    def lookup() -> frozenset[str]:
        calls.append(1)
        return frozenset({"10.0.0.5"})

    clock = FakeClock(0.0)
    probe = _probe(lookup, clock)
    assert await probe._hosts_primary() is True
    assert await probe._hosts_primary() is True
    assert len(calls) == 1                                  # cached: no DNS per 1 s probe
    clock.advance(301.0)
    assert await probe._hosts_primary() is True
    assert len(calls) == 2


async def test_failed_refresh_keeps_the_last_known_set() -> None:
    state = {"fail": False}

    def lookup() -> frozenset[str]:
        if state["fail"]:
            raise OSError("resolver down")
        return frozenset({"10.0.0.5"})

    clock = FakeClock(0.0)
    probe = _probe(lookup, clock)
    assert await probe._hosts_primary() is True
    state["fail"] = True
    clock.advance(301.0)
    assert await probe._hosts_primary() is True


def test_w32tm_stripchart_offset_parsing() -> None:
    from src.runtime.probes import parse_w32tm_stripchart
    out = ("Tracking time.windows.com [20.101.57.9:123].\nCollecting 1 samples.\n"
           "The current time is 13/11/2026 11:00:40 PM.\n23:00:40, -00.9288817s\n")
    assert parse_w32tm_stripchart(out) == -0.9288817
    assert parse_w32tm_stripchart("23:00:40, +00.0123000s") == 0.0123
    assert parse_w32tm_stripchart("23:00:40, error: 0x800705B4") is None
    assert parse_w32tm_stripchart("") is None


async def test_w32tm_reader_reports_the_measured_offset(monkeypatch: object) -> None:
    import subprocess
    from src.runtime.probes import W32tmReader

    class Done:
        returncode, stdout = 0, "23:00:40, -00.9288817s\n"

    def fake_run(*a: object, **k: object) -> Done:
        return Done()

    import pytest
    mp = pytest.MonkeyPatch()
    mp.setattr(subprocess, "run", fake_run)
    try:
        r = W32tmReader()
        ms = await r.offset_ms()
        assert ms is not None and abs(ms - 928.8817) < 1e-6   # an unsynced Windows clock fails the 50 ms check
        assert r.synced() is False
    finally:
        mp.undo()
