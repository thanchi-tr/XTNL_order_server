"""Plan S03 / S39: the optional ``fetch_price_bars`` capability (PR3, R11-M15 range form), the mock's scripted bars,
and the StoneX bar read (GET only, through the adapter's ReadOnlyTransport). No network."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest

from src.adapters.base import (
    BrokerAdapter,
    PriceBarsDecodeError,
    TransientBrokerError,
    assert_read_only,
)
from src.adapters.mock import MockBrokerAdapter
from src.adapters.ratelimit import RestPriority, rest_priority
from src.adapters.stonex.bars import decode_bar_history, interval_for
from src.adapters.stonex.rest import StoneXRestClient, rest_get_only
from src.adapters.transport import is_denied_path
from src.config import ExecutionLockedError
from src.dto import PRICE_BARS_NOT_SUPPORTED, BrokerBar

from .conftest import EURUSD_ID, SESSION, FakeStoneX, make_adapter, make_settings, wcf

T0 = 1_767_225_600_000          # on the 15m grid
TF = 900
TF_MS = TF * 1000


def bar(i: int, *, partial: bool = False, side: str | None = None) -> BrokerBar:
    base = Decimal("1.1") + Decimal(i) / 10_000
    return BrokerBar(bucket_start_ms=T0 + i * TF_MS, open=base, high=base + Decimal("0.0005"),
                     low=base - Decimal("0.0005"), close=base + Decimal("0.0001"), side=side, partial=partial)


# --- the base default -------------------------------------------------------------------------------------
class _Bare(MockBrokerAdapter):
    """An adapter that keeps the base default."""

    fetch_price_bars = BrokerAdapter.fetch_price_bars
    price_bars_supported = BrokerAdapter.price_bars_supported


async def test_base_default_is_not_supported() -> None:
    from src.adapters.stonex.adapter import StoneXAdapter
    from src.adapters.pepperstone import PepperstoneAdapter
    assert PepperstoneAdapter.fetch_price_bars is BrokerAdapter.fetch_price_bars       # not bar-capable in v1
    assert PepperstoneAdapter.price_bars_supported is BrokerAdapter.price_bars_supported
    assert StoneXAdapter.fetch_price_bars is not BrokerAdapter.fetch_price_bars        # S39: after the PR3 probe
    for cls in (StoneXAdapter, PepperstoneAdapter):
        assert_read_only(cls)
    adapter = _Bare()
    assert adapter.price_bars_supported is False
    resp = await adapter.fetch_price_bars("EURUSD", "BID", TF, count=3)
    assert not resp.supported and resp.bars == () and resp.note == PRICE_BARS_NOT_SUPPORTED
    with pytest.raises(ValueError, match="not both"):
        await adapter.fetch_price_bars("EURUSD", "BID", TF, count=3, from_ms=T0, to_ms=T0)
    sx = make_adapter(FakeStoneX())
    assert sx.price_bars_supported is True
    with pytest.raises(ValueError, match="not both"):
        await sx.fetch_price_bars("EURUSD", "BID", TF, count=3, from_ms=T0, to_ms=T0)


# --- MockBrokerAdapter scripted bars -----------------------------------------------------------------------
def scripted() -> MockBrokerAdapter:
    mock = MockBrokerAdapter(wall_ms=lambda: T0 + 10 * TF_MS)
    mock.set_price_bars("EURUSD", "BID", TF, [bar(i) for i in range(8)] + [bar(8, partial=True)])
    return mock


async def test_mock_count_form_returns_newest_with_the_forming_bar() -> None:
    mock = scripted()
    resp = await mock.fetch_price_bars("EURUSD", "BID", TF, count=3)
    assert resp.supported and not resp.truncated and resp.received_ms == T0 + 10 * TF_MS
    # count = complete bars; the forming bar comes on top (StoneX PriceBars + PartialPriceBar)
    assert [b.bucket_start_ms for b in resp.bars] == [T0 + k * TF_MS for k in (5, 6, 7, 8)]
    assert [b.partial for b in resp.bars] == [False, False, False, True]
    assert mock.price_bar_requests == [("EURUSD", "BID", TF, 3, None, None)]


async def test_mock_count_form_cut_by_the_cap_is_truncated() -> None:
    mock = MockBrokerAdapter(wall_ms=lambda: T0 + 10 * TF_MS)
    mock.set_price_bars("EURUSD", "BID", TF, [bar(i) for i in range(8)] + [bar(8, partial=True)], max_bars=2)
    capped = await mock.fetch_price_bars("EURUSD", "BID", TF, count=5)
    assert capped.truncated and [b.bucket_start_ms for b in capped.bars] == [T0 + k * TF_MS for k in (6, 7, 8)]
    assert not (await mock.fetch_price_bars("EURUSD", "BID", TF, count=2)).truncated


async def test_mock_range_form_and_cap() -> None:
    mock = scripted()
    resp = await mock.fetch_price_bars("EURUSD", "BID", TF, from_ms=T0 + 2 * TF_MS, to_ms=T0 + 4 * TF_MS)
    assert [b.bucket_start_ms for b in resp.bars] == [T0 + k * TF_MS for k in (2, 3, 4)] and not resp.truncated
    assert (resp.from_ms, resp.to_ms, resp.count) == (T0 + 2 * TF_MS, T0 + 4 * TF_MS, None)
    mock.set_price_bars("EURUSD", "BID", TF, [bar(i) for i in range(8)], max_bars=2)
    capped = await mock.fetch_price_bars("EURUSD", "BID", TF, from_ms=T0, to_ms=T0 + 7 * TF_MS)
    assert capped.truncated and [b.bucket_start_ms for b in capped.bars] == [T0, T0 + TF_MS]
    mock.set_price_bars("EURUSD", "BID", TF, [bar(0)], truncated=True)
    assert (await mock.fetch_price_bars("EURUSD", "BID", TF, count=5)).truncated


async def test_mock_failing_and_undecodable_reads_then_recovery() -> None:
    mock = scripted()
    mock.fail_price_bars("EURUSD", "BID", TF, TransientBrokerError("scripted outage"))
    mock.undecodable_price_bars("EURUSD", "BID", TF, times=2)
    with pytest.raises(TransientBrokerError):
        await mock.fetch_price_bars("EURUSD", "BID", TF, count=1)
    for _ in range(2):
        with pytest.raises(PriceBarsDecodeError):
            await mock.fetch_price_bars("EURUSD", "BID", TF, count=1)
    assert [b.partial for b in (await mock.fetch_price_bars("EURUSD", "BID", TF, count=1)).bars] == [False, True]
    other = await mock.fetch_price_bars("EURUSD", "ASK", TF, count=1)   # never scripted: empty, complete
    assert other.supported and other.bars == () and not other.truncated


async def test_mock_records_the_rest_priority_and_can_be_unsupported() -> None:
    mock = scripted()
    with rest_priority(RestPriority.POLL):
        await mock.fetch_price_bars("EURUSD", "BID", TF, count=1)
    await mock.fetch_price_bars("EURUSD", "BID", TF, count=1)
    assert mock.rest_calls[-2:] == [("fetch_price_bars", RestPriority.POLL),
                                    ("fetch_price_bars", RestPriority.RECONCILE)]
    mock.set_price_bars_supported(False)
    assert mock.price_bars_supported is False
    resp = await mock.fetch_price_bars("EURUSD", "BID", TF, count=1)
    assert not resp.supported and resp.bars == ()
    assert len(mock.rest_calls) == 2                       # an unsupported answer spends no REST read
    with pytest.raises(ValueError):
        await mock.fetch_price_bars("EURUSD", "BID", TF)


# --- StoneX bar read (PR3, S39): GET only, through ReadOnlyTransport --------------------------------------------
class BarsFake(FakeStoneX):
    """FakeStoneX plus the candidate CIAPI bar endpoints."""

    def __init__(self) -> None:
        super().__init__()
        self.bar_body: Any = {
            "PriceBars": [{"BarDate": wcf(T0 + k * TF_MS), "Open": 1.1, "High": 1.2, "Low": 1.0, "Close": 1.15}
                          for k in range(3)],
            "PartialPriceBar": {"BarDate": wcf(T0 + 3 * TF_MS), "Open": 1.15, "High": 1.16, "Low": 1.14,
                                "Close": 1.15},
        }

    def handler(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        if len(parts) == 5 and parts[2] == "market" and parts[4] in ("barhistory", "barhistorybetween"):
            self.requests.append(request)
            if request.headers.get("Session") != SESSION:
                return httpx.Response(401, json={"ErrorCode": 4011})
            return httpx.Response(200, json=self.bar_body)
        return super().handler(request)


def bar_requests(fake: FakeStoneX) -> list[httpx.Request]:
    return [r for r in fake.requests if "/market/" in r.url.path and "barhistory" in r.url.path]


async def test_stonex_bar_read_count_and_range_forms() -> None:
    fake = BarsFake()
    adapter = make_adapter(fake)
    await adapter.connect()
    resp = await adapter.fetch_price_bars("EURUSD", "BID", TF, count=3)
    assert resp.supported and len(resp.bars) == 4 and resp.bars[-1].partial and not resp.truncated
    short = await adapter.fetch_price_bars("EURUSD", "BID", TF, count=5)    # 3 complete of 5 asked
    assert short.truncated and len(short.bars) == 4
    assert resp.bars[0].bucket_start_ms == T0 and resp.bars[0].side is None
    assert "PriceBars" in resp.field_names and "bar.BarDate" in resp.field_names
    req, _ = bar_requests(fake)
    assert req.method == "GET" and req.url.path == f"/TradingAPI/market/{EURUSD_ID}/barhistory"
    assert dict(req.url.params) == {"interval": "MINUTE", "span": "15", "PriceBars": "3", "priceType": "BID"}

    ranged = await adapter.fetch_price_bars("EURUSD", "ASK", TF, from_ms=T0 + 1, to_ms=T0 + TF_MS + 1)
    assert not ranged.truncated and (ranged.from_ms, ranged.to_ms) == (T0 + 1, T0 + TF_MS + 1)
    req = bar_requests(fake)[-1]
    assert req.url.path.endswith("/barhistorybetween") and req.method == "GET"
    assert req.url.params["fromTimeStampUTC"] == str(T0 // 1000)
    assert req.url.params["toTimeStampUTC"] == str((T0 + TF_MS) // 1000 + 1)    # rounded up: covers to_ms
    assert all(not is_denied_path(r.url.path) for r in fake.requests)
    unsupported = await adapter.fetch_price_bars("EURUSD", "BID", 120, count=1)
    assert not unsupported.supported and "120" in unsupported.note
    await adapter.close()


async def test_stonex_bar_read_rejects_undecodable_answers() -> None:
    fake = BarsFake()
    adapter = make_adapter(fake)
    await adapter.connect()
    v = 1.23457                                            # distinctive: must never appear in the error
    for body in ({"PriceBars": ["x"]}, {"PriceBars": "x"},
                 {"PriceBars": [{"BarDate": wcf(T0), "Open": v, "High": v, "Low": v}]},
                 {"PriceBars": [{"Open": v, "High": v, "Low": v, "Close": v}]},
                 {"PriceBars": [{"BarDate": wcf(T0), "Open": v, "High": v, "Low": v, "Close": "x1.23457"}]}):
        fake.bar_body = body
        with pytest.raises(PriceBarsDecodeError) as info:
            await adapter.fetch_price_bars("EURUSD", "BID", TF, count=1)
        assert "1.23457" not in str(info.value)
    await adapter.close()


def test_decode_reads_side_tags_and_sorts_oldest_first() -> None:
    bars, names = decode_bar_history({"PriceType": "Bid", "PriceBars": [
        {"BarDate": wcf(T0 + TF_MS), "Open": 2, "High": 3, "Low": 1, "Close": 2},
        {"BarDate": wcf(T0), "Open": 2, "High": 3, "Low": 1, "Close": 2, "PriceType": "ASK"},
    ], "PartialPriceBar": None})
    assert [b.bucket_start_ms for b in bars] == [T0, T0 + TF_MS]
    assert [b.side for b in bars] == ["ASK", "BID"] and not any(b.partial for b in bars)
    assert names == ("PartialPriceBar", "PriceBars", "PriceType", "bar.BarDate", "bar.Close", "bar.High",
                     "bar.Low", "bar.Open", "bar.PriceType")
    assert interval_for(60) == ("MINUTE", 1) and interval_for(3600) == ("HOUR", 1) and interval_for(7) is None


async def test_get_only_guard_refuses_any_other_method_before_sending() -> None:
    fake = FakeStoneX()
    rest = StoneXRestClient.create(make_settings(), http=httpx.AsyncClient(transport=httpx.MockTransport(fake.handler)))
    await rest.log_on()                                    # LogOn is a POST: allowed outside the guard
    sent = len(fake.requests)
    with rest_get_only():
        with pytest.raises(ExecutionLockedError, match="only GET"):
            await rest.market_information("1001", ["1"])    # a read-only POST is still refused inside it
        with pytest.raises(ExecutionLockedError, match="only GET"):
            await rest.validate()
    assert len(fake.requests) == sent                      # nothing reached the server
    await rest.aclose()


async def test_bar_paths_cannot_be_bent_and_denied_paths_stay_denied() -> None:
    fake = FakeStoneX()
    rest = StoneXRestClient.create(make_settings(), http=httpx.AsyncClient(transport=httpx.MockTransport(fake.handler)))
    await rest.log_on()
    for bad in ("../order/1", "401484347/../../order", "12 34", ""):
        with pytest.raises(ValueError, match="market id"):
            await rest.price_bar_history(bad, "MINUTE", 15, 3, "BID")
    with rest_get_only(), pytest.raises(ExecutionLockedError):
        await rest._get(f"{make_settings().base_url_v1}/order/newtradeorder")    # denied by ReadOnlyTransport
    assert all("barhistory" not in r.url.path and "newtradeorder" not in r.url.path for r in fake.requests)
    await rest.aclose()
