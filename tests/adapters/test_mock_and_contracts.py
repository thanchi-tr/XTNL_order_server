"""MockBrokerAdapter behaviour and the adapter-package contracts (read-only, import isolation)."""
from __future__ import annotations

import ast
import asyncio
import pathlib
import re
from decimal import Decimal

import pytest

import src.adapters as adapters_pkg
from src.adapters.base import (
    BrokerAdapter,
    StreamEvent,
    StreamEventKind,
    TransientBrokerError,
    assert_read_only,
)
from src.adapters.mock import MockBrokerAdapter, MockFixture, build_fx_fixture
from src.adapters.stonex import StoneXAdapter
from src.dto import AccountDTO, AccountLive, OrderDTO, OrderStatus, PositionDTO, PriceDTO

ADAPTERS_DIR = pathlib.Path(adapters_pkg.__file__).parent
FORBIDDEN_PACKAGES = ("src.db", "src.cache", "src.pubsub", "src.integrity", "src.oversee")


# --- contracts ---------------------------------------------------------------------------------------
@pytest.mark.parametrize("cls", [StoneXAdapter, MockBrokerAdapter])
def test_adapters_are_read_only(cls: type) -> None:
    assert issubclass(cls, BrokerAdapter)
    assert_read_only(cls)


def test_assert_read_only_catches_a_mutation_method() -> None:
    bad = type("Bad", (), {"Place_Order": lambda self: None})
    with pytest.raises(TypeError):
        assert_read_only(bad)


def _imports(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_adapters_never_import_storage_bus_or_oversee() -> None:
    files = sorted(ADAPTERS_DIR.rglob("*.py"))
    assert any(f.name == "adapter.py" for f in files) and any(f.name == "mock.py" for f in files)
    for path in files:
        for name in _imports(path):
            assert not name.startswith(FORBIDDEN_PACKAGES), f"{path.name} imports {name}"


def test_no_order_changing_paths_outside_the_deny_list() -> None:
    pattern = re.compile(r"/(newtradeorder|newstoplimitorder|updatetradeorder|updatestoplimitorder|cancel)\b",
                         re.IGNORECASE)
    for path in ADAPTERS_DIR.rglob("*.py"):
        if path.name == "transport.py":
            continue
        assert not pattern.search(path.read_text(encoding="utf-8")), path.name


# --- MockBrokerAdapter -------------------------------------------------------------------------------
def test_fx_fixture_shape() -> None:
    fx = build_fx_fixture()
    assert {i.symbol for i in fx.instruments} == {"EURUSD", "AUDUSD"}
    (account,) = fx.accounts
    assert account.currency == "AUD"
    parent = next(o for o in fx.orders if o.broker_order_id == "PF-1")
    child = next(o for o in fx.orders if o.broker_order_id == "PF-1-SL")
    pos = next(p for p in fx.positions if p.position_id == "PF-1")
    assert parent.status == OrderStatus.PARTIALLY_FILLED and parent.remaining_units == Decimal(40000)
    assert pos.units == Decimal(60000) and pos.stop_loss is None
    assert child.is_reduce_only and child.linked_position_id == "PF-1" and child.side == "SELL"
    assert all(o.seq_id == 0 and o.observed_seq == 0 for o in fx.orders)


async def test_mock_rest_reads() -> None:
    mock = MockBrokerAdapter.from_fixture(build_fx_fixture())
    await mock.connect()
    assert mock.connected and mock.adapter_id == "mock"
    assert len(await mock.fetch_instruments()) == 2
    assert {o.broker_order_id for o in await mock.fetch_open_orders()} == {"P-EU-1-SL", "PF-1", "PF-1-SL"}
    assert {p.position_id for p in await mock.fetch_positions()} == {"P-EU-1", "PF-1"}
    accounts, lives = await mock.fetch_account_snapshot()
    assert accounts[0].balance == 100000 and lives[0].equity == Decimal("100150.00")
    assert len(await mock.fetch_cash_flows(0)) == 1
    assert await mock.fetch_cash_flows(2**62) == []
    assert (await mock.fetch_order_status("PF-1")) is not None
    assert await mock.fetch_position_status("nope") is None
    filled = OrderDTO(client_order_id="X", broker_order_id="X", account_id="A", adapter_id="mock", symbol="EURUSD",
                      side="BUY", status=OrderStatus.FILLED, entry_price=None, ordered_units=Decimal(1),
                      remaining_units=Decimal(0))
    mock.set_order(filled)
    assert "X" not in {o.broker_order_id for o in await mock.fetch_open_orders()}
    assert await mock.fetch_order_status("X") == filled
    assert mock.calls[0] == "connect"


async def test_mock_failure_injection() -> None:
    mock = MockBrokerAdapter()
    mock.fail_next("fetch_positions", TransientBrokerError("boom"))
    with pytest.raises(TransientBrokerError):
        await mock.fetch_positions()
    assert await mock.fetch_positions() == []


async def test_mock_finite_streams_replay_the_script() -> None:
    fx = build_fx_fixture()
    mock = MockBrokerAdapter.from_fixture(fx)
    items = [i async for i in mock.stream_order_updates()]
    assert isinstance(items[0], StreamEvent) and items[0].kind == StreamEventKind.SUBSCRIBED
    assert [type(i) for i in items[1:]] == [OrderDTO, PositionDTO]
    assert {p.position_id: p.units for p in await mock.fetch_positions()}["P-EU-1"] == Decimal(10000)
    account = [i async for i in mock.stream_account()]
    assert isinstance(account[1], AccountLive)
    prices = [p async for p in mock.stream_ticks(["EURUSD"]) if isinstance(p, PriceDTO)]
    assert [p.symbol for p in prices] == ["EURUSD"]     # AUDUSD not subscribed


async def test_mock_live_streams_take_pushed_items() -> None:
    mock = MockBrokerAdapter(finite_streams=False, announce_subscribed=False)
    gen = mock.stream_account()
    nxt = asyncio.ensure_future(gen.__anext__())
    await asyncio.sleep(0.01)
    assert not nxt.done()
    account = AccountDTO(account_id="A", adapter_id="mock", currency="AUD", balance=Decimal(5))
    mock.push_account_update(account)
    assert await asyncio.wait_for(nxt, 1) == account
    mock.end_streams()
    with pytest.raises(StopAsyncIteration):
        await gen.__anext__()


async def test_mock_ensure_ticks_extends_the_price_filter() -> None:
    fx = build_fx_fixture()
    mock = MockBrokerAdapter.from_fixture(fx, finite_streams=False, announce_subscribed=False)
    gen = mock.stream_ticks([])
    await mock.ensure_ticks(["AUDUSD"])
    first = await asyncio.wait_for(gen.__anext__(), 1)
    assert isinstance(first, PriceDTO) and first.symbol == "AUDUSD"
    await gen.aclose()


def test_mock_from_fixture_rejects_unknown_arguments() -> None:
    with pytest.raises(TypeError):
        MockBrokerAdapter.from_fixture(MockFixture(), nope=1)


def test_mock_versioning_is_configurable() -> None:
    from src.adapters.base import VersionDecl
    mock = MockBrokerAdapter(versioning={"ORDER": VersionDecl(True, True), "POSITION": VersionDecl(True, True),
                                         "ACCOUNT": VersionDecl(True, False)})
    assert mock.VERSIONING["ACCOUNT"].has_broker_seq
    assert not MockBrokerAdapter().VERSIONING["ACCOUNT"].has_broker_time


# --- ARM prerequisites: scripted order_kind and instrument metadata (design PR1, PR7) ------------------
async def test_mock_scripts_order_kind_and_instrument_metadata() -> None:
    fx = build_fx_fixture()
    mock = MockBrokerAdapter.from_fixture(fx)
    # the default fixture leaves every new field None, so its checksums are unchanged
    assert all(o.order_kind is None for o in await mock.fetch_open_orders())
    assert all(i.price_increment is None and i.asset_class is None for i in await mock.fetch_instruments())

    before = {o.broker_order_id: o for o in await mock.fetch_open_orders()}["PF-1-SL"]
    stop = mock.set_order_kind("PF-1-SL", "STOP")
    assert stop.order_kind == "STOP" and stop.checksum != before.checksum
    served = await mock.fetch_order_status("PF-1-SL")
    assert served is not None and served.order_kind == "STOP"
    assert mock.set_order_kind("PF-1-SL", None).checksum == before.checksum

    spec = mock.set_instrument_metadata("EURUSD", price_increment=Decimal("0.00001"), asset_class="FX")
    assert spec.min_stop_distance is None
    eur = {i.symbol: i for i in await mock.fetch_instruments()}["EURUSD"]
    assert (eur.price_increment, eur.asset_class) == (Decimal("0.00001"), "FX")
    with pytest.raises(KeyError):
        mock.set_order_kind("NO-SUCH-ORDER", "LIMIT")
