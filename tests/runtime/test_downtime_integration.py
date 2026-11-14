"""Integration of the two downtime catch-up parts (adapters' `fetch_history`, the engine's uptime + catch-up): the
StoneX history switches reach the adapter, the mock broker survives reconnects and scripts historic rows, a
CLOSED history position keeps the entry this term knows, and run-local.ps1 restarts the engine after a hard exit.
No network. The end-to-end scenarios are tests/e2e/test_downtime_memory.py and tests/e2e/test_downtime_pg.py."""
from __future__ import annotations

import asyncio
import re
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from src.adapters.mock import MockBrokerAdapter, build_fx_fixture
from src.adapters.stonex.adapter import StoneXAdapter
from src.config import StoneXSettings, env_flag
from src.pubsub.orchestrator import _keep_known_entry
from src.runtime.exits import EXIT_CODES, exit_code
from src.runtime.wiring import AdapterPlan, make_adapter

ROOT = Path(__file__).resolve().parents[2]
CREDS = {"STONE_X_USERNAME": "u", "STONE_X_PASS": "p", "STONE_X_APP_KEY": "k"}


# ===================================================================== StoneX history switches
def test_stonex_history_switches_default_on_and_reach_the_adapter() -> None:
    """Both confirmed on the live account (2026-11-14): on unless explicitly set to 0."""
    on = StoneXSettings.from_env(CREDS)
    assert (on.stop_limit_history, on.history_paging_confirmed) == (True, True)
    off = StoneXSettings.from_env(CREDS | {"STONE_X_STOP_LIMIT_HISTORY": "0",
                                           "STONE_X_HISTORY_PAGING_CONFIRMED": "false"})
    assert (off.stop_limit_history, off.history_paging_confirmed) == (False, False)
    for settings in (off, on):
        adapter = make_adapter(AdapterPlan("stonex", stonex=settings), lambda: None)
        assert isinstance(adapter, StoneXAdapter)
        assert adapter._stop_limit_history is settings.stop_limit_history          # noqa: SLF001
        assert adapter._history_paging_confirmed is settings.history_paging_confirmed  # noqa: SLF001
    assert "password" not in repr(on) and CREDS["STONE_X_APP_KEY"] not in repr(on).split("username=")[0]


@pytest.mark.parametrize("raw", ["2", "maybe", "enable"])
def test_a_bad_stonex_history_switch_is_refused_by_name(raw: str) -> None:
    with pytest.raises(ValueError, match="STONE_X_STOP_LIMIT_HISTORY") as err:
        StoneXSettings.from_env(CREDS | {"STONE_X_STOP_LIMIT_HISTORY": raw})
    assert raw not in str(err.value)


@pytest.mark.parametrize("name", ["engine.env.example", "local.env.example"])
def test_env_examples_assign_the_stonex_history_switches_on(name: str) -> None:
    assigned: dict[str, str] = {}
    for raw in (ROOT / "deploy" / "env" / name).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            assigned[k.strip()] = v.strip()
    for var in ("STONE_X_STOP_LIMIT_HISTORY", "STONE_X_HISTORY_PAGING_CONFIRMED"):
        assert var in assigned and env_flag(assigned, var) is True


# ===================================================================== the mock broker
async def test_the_mock_serves_its_streams_again_after_a_close_and_reconnect() -> None:
    mock = MockBrokerAdapter(finite_streams=False, announce_subscribed=False)
    await mock.connect()
    gen = mock.stream_order_updates()
    await mock.close()                                    # a socket loss / drain ends the live streams ...
    with pytest.raises(StopAsyncIteration):
        await gen.__anext__()
    pos = build_fx_fixture().positions[0]
    mock.push_order_update(pos)                           # ... a change while disconnected is kept
    await mock.connect()                                  # ... and a reconnect serves them again
    gen2 = mock.stream_order_updates()
    assert await asyncio.wait_for(gen2.__anext__(), 1) == pos
    nxt = asyncio.ensure_future(gen2.__anext__())
    await asyncio.sleep(0.01)
    assert not nxt.done()                                 # still live, waiting for the next item
    nxt.cancel()


async def test_the_mock_finite_streams_stay_ended_after_a_reconnect() -> None:
    mock = MockBrokerAdapter(finite_streams=True, announce_subscribed=False)
    await mock.close()
    await mock.connect()
    assert [i async for i in mock.stream_order_updates()] == []


async def test_list_in_history_lists_a_row_without_changing_the_current_book() -> None:
    fx = build_fx_fixture()
    mock = MockBrokerAdapter.from_fixture(fx)
    eu = next(p for p in fx.positions if p.position_id == "P-EU-1")
    old = replace(eu, units=Decimal(5000), raw_qty=Decimal(5000), broker_updated_at_ms=eu.broker_updated_at_ms - 1_000)
    mock.list_in_history(old)
    h = await mock.fetch_history(old.broker_updated_at_ms - 10, eu.broker_updated_at_ms + 10)
    assert h.complete and [p.units for p in h.positions] == [Decimal(5000)]
    assert (await mock.fetch_position_status("P-EU-1")) == eu


# ===================================================================== the known entry of a closed position
def test_a_closed_history_position_without_an_entry_keeps_the_known_side_and_entry() -> None:
    known = build_fx_fixture().positions[1]                               # PF-1 BUY @ 0.6550, OPEN
    closed = replace(known, status="CLOSED", units=Decimal(0), raw_qty=Decimal(0), entry_price=None, side="SELL",
                     broker_updated_at_ms=known.broker_updated_at_ms + 9)
    kept = _keep_known_entry(closed, known)
    assert (kept.entry_price, kept.side, kept.status, kept.broker_updated_at_ms) == \
        (known.entry_price, known.side, "CLOSED", closed.broker_updated_at_ms)
    assert kept.checksum != closed.checksum                               # sealed again over the filled row
    assert _keep_known_entry(closed, None) is closed                      # nothing known: as the broker gave it
    with_entry = replace(closed, entry_price=Decimal("0.6551"))
    assert _keep_known_entry(with_entry, known) is with_entry             # a reported entry is never changed
    still_open = replace(closed, status="OPEN")
    assert _keep_known_entry(still_open, known) is still_open


# ===================================================================== run-local.ps1
def test_run_local_restarts_only_after_a_hard_exit() -> None:
    script = (ROOT / "deploy" / "local" / "run-local.ps1").read_text(encoding="utf-8")
    assert "[switch]$NoRestart" in script
    m = re.search(r"\$code -lt (\d+) -or \$code -gt (\d+)", script)
    assert m is not None
    lo, hi = int(m.group(1)), int(m.group(2))
    # every hard exit (and the unknown-reason default) restarts; a clean stop (0) or a refusal does not
    assert all(lo <= c <= hi for c in EXIT_CODES.values()) and lo <= exit_code("unknown") <= hi
    assert not lo <= 0 <= hi and not lo <= 1 <= hi and not lo <= 2 <= hi
    assert "Start-Sleep" in script and "[Math]::Min($delay * 2, 120)" in script
