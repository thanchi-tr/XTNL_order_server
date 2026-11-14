"""IngestionOrchestrator optional ports: AdapterAuthStore (known-bad credential generations), InstrumentSink
(SNAPSHOTTING instrument refresh) and resubmit_cash_flow() for the DLQ worker."""
from __future__ import annotations

import asyncio
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

import pytest

from src.adapters.base import AuthFailedError
from src.dto import CashFlowDTO, InstrumentSpec
from src.pubsub import connection as connmod
from src.pubsub.connection import ConnState
from src.pubsub.dlq_rows import DETERMINISTIC_REASONS, MISSING_CLIENT_ORDER_ID, is_deterministic
from tests.pubsub.conftest import ManualClock, cash_flow
from tests.pubsub.test_orchestrator import make, wait_for


class EpochMismatch(RuntimeError):
    """Stands in for src.db.fence.EpochMismatch (matched by name: src.pubsub never imports src.db)."""


class FakeAuthStore:
    def __init__(self) -> None:
        self.bad: set[tuple[str, str]] = set()
        self.recorded: list[tuple[str, str, str]] = []
        self.checked: list[tuple[str, str]] = []
        self.error: BaseException | None = None

    async def known_bad(self, adapter_id: str, credential_generation: str) -> bool:
        self.checked.append((adapter_id, credential_generation))
        if self.error is not None:
            raise self.error
        return (adapter_id, credential_generation) in self.bad

    async def record_auth_failed(self, adapter_id: str, credential_generation: str, detail: str) -> None:
        self.recorded.append((adapter_id, credential_generation, detail))
        self.bad.add((adapter_id, credential_generation))

    async def rearm(self, adapter_id: str, credential_generation: str, operator: str) -> bool:
        return False


class FakeInstrumentSink:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], int]] = []
        self.fail: list[BaseException] = []
        self.order: list[str] = []

    async def upsert_instruments(self, specs: Sequence[InstrumentSpec], seq_id: int) -> int:
        self.order.append("upsert")
        if self.fail:
            raise self.fail.pop(0)
        self.calls.append(([s.symbol for s in specs], seq_id))
        return len(specs)


def spec(symbol: str = "EURUSD") -> InstrumentSpec:
    return InstrumentSpec(adapter_id="mock", symbol=symbol, base_ccy=symbol[:3], quote_ccy=symbol[3:],
                          pnl_ccy=symbol[3:], contract_size=Decimal(1), unit_kind="UNITS",
                          point_size=Decimal("0.0001"), min_qty=Decimal(1), swap_booked_to_balance=True)


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")
    monkeypatch.setattr(connmod, "backoff_delay", lambda attempt, jitter: 0.0)


# ===================================================================== AdapterAuthStore
async def test_known_bad_generation_is_never_connected_until_it_changes(tmp_path: Path, clock: ManualClock) -> None:
    auth = FakeAuthStore()
    auth.bad.add(("mock", "v1"))
    gen = {"mock": "v1"}
    env = make(tmp_path, clock, auth_store=auth, credential_generation=lambda a: gen[a], auth_poll_s=0.01)
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.AUTH_FAILED)
    await asyncio.sleep(0.05)
    assert env.adapter.called("connect") == [] and auth.recorded == []
    gen["mock"] = "v2"                                      # Secrets Manager rotated the credential
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
    assert len(env.adapter.called("connect")) == 1 and auth.checked[-1] == ("mock", "v2")
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_exhausted_credential_errors_are_recorded_once(tmp_path: Path, clock: ManualClock) -> None:
    auth = FakeAuthStore()
    env = make(tmp_path, clock, auth_store=auth, credential_generation=lambda a: "v7")
    env.adapter.connect_errors = [AuthFailedError("bad password")] * 3
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.AUTH_FAILED)
    assert len(env.adapter.called("connect")) == 3
    assert auth.recorded == [("mock", "v7", "bad password")]
    env.orch.rearm("mock")                                   # local re-arm: the store still says bad
    await asyncio.sleep(0.05)
    assert rt.conn.state is ConnState.AUTH_FAILED and len(env.adapter.called("connect")) == 3
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_default_generation_comes_from_the_adapter(tmp_path: Path, clock: ManualClock) -> None:
    auth = FakeAuthStore()
    env = make(tmp_path, clock, auth_store=auth)
    env.adapter.credential_generation = "secret-version-3"  # type: ignore[attr-defined]
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
    assert auth.checked == [("mock", "secret-version-3")]
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_unreadable_auth_state_backs_off_without_connecting(tmp_path: Path, clock: ManualClock) -> None:
    auth = FakeAuthStore()
    auth.error = ConnectionError("postgres down")
    env = make(tmp_path, clock, auth_store=auth)
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: ConnState.BACKOFF in [n for _, n, _ in rt.conn.history])
    assert env.adapter.called("connect") == []
    auth.error = None
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_lost_fence_from_the_auth_store_propagates(tmp_path: Path, clock: ManualClock) -> None:
    auth = FakeAuthStore()
    auth.error = EpochMismatch("fenced")
    env = make(tmp_path, clock, auth_store=auth)
    with pytest.raises(EpochMismatch):
        await asyncio.wait_for(env.orch.run_adapter("mock", asyncio.Event()), 2)
    assert env.adapter.called("connect") == []


# ===================================================================== InstrumentSink
async def test_instrument_refresh_is_saved_before_streams_start(tmp_path: Path, clock: ManualClock) -> None:
    sink = FakeInstrumentSink()
    env = make(tmp_path, clock, instrument_sink=sink)
    env.adapter.instruments = [spec("EURUSD"), spec("USDJPY")]
    rt = env.orch.runtime("mock")
    orig = env.adapter.fetch_instruments

    async def fetch() -> list[InstrumentSpec]:
        sink.order.append("fetch")
        return await orig()
    env.adapter.fetch_instruments = fetch  # type: ignore[method-assign]
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
    names = [n for n, _ in env.adapter.calls]
    assert sink.order == ["fetch", "upsert"] and names.index("fetch_instruments") < names.index("subscribe")
    (symbols, seq_id), = sink.calls
    assert symbols == ["EURUSD", "USDJPY"] and seq_id > 0 and rt.unsaved_instruments == []
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_failed_instrument_save_does_not_block_and_is_retried(tmp_path: Path, clock: ManualClock) -> None:
    sink = FakeInstrumentSink()
    sink.fail = [ConnectionError("postgres down")]
    env = make(tmp_path, clock, instrument_sink=sink)
    env.adapter.instruments = [spec()]
    rt = env.orch.runtime("mock")
    stop = asyncio.Event()
    task = asyncio.create_task(env.orch.run_adapter("mock", stop))
    await wait_for(lambda: rt.conn.state is ConnState.STREAMING)
    assert sink.calls == [] and [s.symbol for s in rt.unsaved_instruments] == ["EURUSD"]
    clock.ms += 30_000
    rt.wake.set()
    await wait_for(lambda: sink.calls != [])
    assert rt.unsaved_instruments == []
    stop.set()
    await asyncio.wait_for(task, 2)


async def test_lost_fence_from_the_instrument_sink_propagates(tmp_path: Path, clock: ManualClock) -> None:
    sink = FakeInstrumentSink()
    sink.fail = [EpochMismatch("fenced")]
    env = make(tmp_path, clock, instrument_sink=sink)
    env.adapter.instruments = [spec()]
    with pytest.raises(EpochMismatch):
        await env.orch.run_snapshot("mock")


# ===================================================================== DLQ re-submission
async def test_resubmit_cash_flow_publishes_without_touching_the_heartbeat(tmp_path: Path,
                                                                          clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    rt = env.orch.runtime("mock")
    rt.last_inbound_ms = 123
    assert env.orch.resubmit_cash_flow(cash_flow("T9"))
    items = env.account_items()
    assert len(items) == 1 and isinstance(items[0], CashFlowDTO) and items[0].seq_id > 0
    assert rt.last_inbound_ms == 123


def test_missing_client_order_id_is_deterministic() -> None:
    assert MISSING_CLIENT_ORDER_ID in DETERMINISTIC_REASONS and is_deterministic("missing_client_order_id")
    assert not is_deterministic("QueueFull")
