"""Plan S39: StoneX broker bars after the PR3 probe (2026-11-17). ``StoneXAdapter.fetch_price_bars`` reads the
confirmed ``barhistory`` / ``barhistorybetween`` GETs through the adapter's ReadOnlyTransport; answers are decoded
from the probe's field list (synthetic values); bars name no side, so the side is the request's and the §A4 side
check accepts it; md-bars' port reads them at the BARS priority. The ops bar-capable set follows the go-live gates.
No network; no assertion message carries a value."""
from __future__ import annotations

import math
from decimal import Decimal
from typing import Any

import httpx
import pytest

from src.adapters.base import PriceBarsDecodeError, assert_read_only
from src.adapters.stonex import adapter as stonex_adapter
from src.adapters.ratelimit import RestPriority, rest_priority
from src.adapters.stonex.adapter import StoneXAdapter
from src.adapters.stonex.bars import decode_bar_history
from src.adapters.stonex.rest import StoneXRestClient
from src.adapters.transport import is_denied_path
from src.config import ExecutionLockedError
from src.dto import PriceBarsResponse
from src.marketdata.types import BarSpec, SeriesId, Side, Source, Unsupported
from src.marketdata.validate import Check, split_response
from src.ops import arm as ops_arm
from src.riskmgr import golive
from src.runtime.md_ports import BrokerBarsPortImpl

from .conftest import EURUSD_ID, SESSION, FakeStoneX, make_adapter, wcf

T0 = 1_767_225_600_000          # on the 15m grid
TF = 900
TF_MS = TF * 1000
RECEIVED = T0 + 3 * TF_MS + 60_000     # inside bucket 3: buckets 0-2 closed, bucket 3 forming
#: The field names the PR3 report listed for every series (names only).
PR3_FIELDS = ("PartialPriceBar", "PriceBars", "bar.BarDate", "bar.Close", "bar.High", "bar.Low", "bar.Open",
              "bar.Volume")


def _row(start: int, k: int) -> dict[str, Any]:
    base = 2.5 + k / 1000                   # synthetic instrument values
    return {"BarDate": wcf(start), "Open": base, "High": base + 0.004, "Low": base - 0.004, "Close": base + 0.001,
            "Volume": 7 + k}


class Pr3Fake(FakeStoneX):
    """FakeStoneX answering the bar GETs in the shape the PR3 probe recorded: ``PriceBars`` dated by the bucket
    start, a ``PartialPriceBar`` on the grid, ``Volume`` on every bar, no side tag anywhere."""

    def __init__(self) -> None:
        super().__init__()
        self.bar_requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        if len(parts) == 5 and parts[2] == "market" and parts[4] in ("barhistory", "barhistorybetween"):
            self.requests.append(request)
            self.bar_requests.append(request)
            if request.headers.get("Session") != SESSION:
                return httpx.Response(401, json={"ErrorCode": 4011})
            return httpx.Response(200, json={"PriceBars": [_row(T0 + k * TF_MS, k) for k in range(3)],
                                             "PartialPriceBar": _row(T0 + 3 * TF_MS, 3)})
        return super().handler(request)


def series(side: Side = Side.BID) -> SeriesId:
    return SeriesId("stonex", "EURUSD", BarSpec(TF), side, Source.BROKER)


async def connected(fake: Pr3Fake) -> StoneXAdapter:
    adapter = make_adapter(fake, wall_ms=lambda: RECEIVED)
    await adapter.connect()
    return adapter


def test_the_pr3_answer_decodes_start_dated_side_less_bars() -> None:
    body = {"PriceBars": [_row(T0 + k * TF_MS, k) for k in range(2)], "PartialPriceBar": _row(T0 + 2 * TF_MS, 2)}
    bars, names = decode_bar_history(body)
    assert names == PR3_FIELDS
    assert [b.bucket_start_ms for b in bars] == [T0, T0 + TF_MS, T0 + 2 * TF_MS]      # BarDate = bucket start
    assert [b.partial for b in bars] == [False, False, True]
    assert all(b.side is None for b in bars)                                          # no side tag on bars
    assert bars[0].open == Decimal("2.5") and bars[0].close == Decimal("2.501")
    assert "2.5" not in repr(bars[0])                                                 # OHLC never in repr


async def test_fetch_price_bars_is_the_capability_and_reads_get_only() -> None:
    fake = Pr3Fake()
    adapter = await connected(fake)
    assert adapter.price_bars_supported
    assert_read_only(StoneXAdapter)
    resp = await adapter.fetch_price_bars("EURUSD", "ASK", 60, count=3)
    assert resp.supported and resp.side == "ASK" and resp.timeframe_s == 60 and resp.received_ms == RECEIVED
    assert resp.field_names == PR3_FIELDS and not resp.truncated
    req = fake.bar_requests[-1]
    assert req.method == "GET" and req.url.path == f"/TradingAPI/market/{EURUSD_ID}/barhistory"
    assert dict(req.url.params) == {"interval": "MINUTE", "span": "1", "PriceBars": "3", "priceType": "ASK"}
    ranged = await adapter.fetch_price_bars("EURUSD", "BID", TF, from_ms=T0, to_ms=T0 + 2 * TF_MS)
    assert ranged.supported and (ranged.from_ms, ranged.to_ms) == (T0, T0 + 2 * TF_MS)
    assert fake.bar_requests[-1].url.path.endswith("/barhistorybetween")
    assert fake.bar_requests[-1].url.params["priceType"] == "BID"
    assert all(r.method == "GET" for r in fake.bar_requests)
    assert not any(is_denied_path(r.url.path) for r in fake.requests)
    unsupported = await adapter.fetch_price_bars("EURUSD", "BID", 120, count=1)
    assert not unsupported.supported and unsupported.bars == ()
    await adapter.close()


async def test_bar_reads_build_no_http_client_of_their_own(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = Pr3Fake()
    adapter = await connected(fake)

    def refuse(*_a: object, **_kw: object) -> None:
        raise AssertionError("a bar read built its own HTTP client")

    monkeypatch.setattr(httpx.AsyncClient, "__init__", refuse)
    monkeypatch.setattr(httpx.Client, "__init__", refuse)
    await adapter.fetch_price_bars("EURUSD", "BID", TF, count=3)
    await adapter.fetch_price_bars("EURUSD", "ASK", TF, from_ms=T0, to_ms=T0 + TF_MS)
    assert len(fake.bar_requests) == 2
    monkeypatch.undo()
    await adapter.close()


async def test_request_side_passes_the_side_check_and_another_side_fails_it() -> None:
    """§A4 check (3): StoneX bars state no side, so the response's (request-derived) side is what is checked."""
    fake = Pr3Fake()
    adapter = await connected(fake)
    resp = await adapter.fetch_price_bars("EURUSD", "BID", TF, count=3)
    split = split_response(resp, series(Side.BID))
    assert [b.bucket_start_ms for b in split.valid] == [T0, T0 + TF_MS, T0 + 2 * TF_MS]
    assert split.invalid == () and [b.bucket_start_ms for b in split.forming] == [T0 + 3 * TF_MS]
    wrong = split_response(resp, series(Side.ASK))
    assert wrong.valid == () and all(Check.SIDE in r.failed for _, r in wrong.invalid)
    await adapter.close()


async def test_md_bars_port_reads_stonex_at_the_bars_priority() -> None:
    fake = Pr3Fake()
    adapter = await connected(fake)
    port = BrokerBarsPortImpl({"stonex": adapter}.__getitem__)
    await adapter.fetch_price_bars("EURUSD", "BID", TF, count=1)     # the market id lookup, once per symbol
    before =adapter.rest_budget.stats().requests.get(RestPriority.BARS.label, 0)
    with rest_priority(RestPriority.POLL):          # the port sets its own class whatever the caller's
        got = await port.fetch_price_bars("stonex", "EURUSD", Side.BID, TF, count=3)
    assert isinstance(got, PriceBarsResponse) and len(got.bars) == 4
    assert adapter.rest_budget.stats().requests.get(RestPriority.BARS.label, 0) == before + 1
    assert await port.fetch_price_bars("stonex", "EURUSD", Side.BID, 120, count=1) == Unsupported("not_supported")
    await adapter.close()


# --- S39 review: range parameters and filter, truncation, decode errors, the GET-only guard ---------------------
async def test_range_bounds_are_whole_seconds_and_the_answer_stays_inside_them() -> None:
    fake = Pr3Fake()
    adapter = await connected(fake)
    from_ms, to_ms = T0 + 1_500, T0 + 2 * TF_MS - 500        # not whole seconds: the GET widens both ends
    resp = await adapter.fetch_price_bars("EURUSD", "BID", TF, from_ms=from_ms, to_ms=to_ms)
    params = fake.bar_requests[-1].url.params
    assert params["fromTimeStampUTC"] == str(from_ms // 1000)
    assert params["toTimeStampUTC"] == str(math.ceil(to_ms / 1000))
    # the fake answers T0 .. T0 + 3 buckets: only the bucket starting inside [from_ms, to_ms] is kept
    assert [b.bucket_start_ms for b in resp.bars] == [T0 + TF_MS]
    split = split_response(resp, series(Side.BID))
    assert split.invalid == () and [b.bucket_start_ms for b in split.valid] == [T0 + TF_MS]
    await adapter.close()


async def test_truncated_for_a_short_count_answer_and_a_capped_range_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = Pr3Fake()
    adapter = await connected(fake)
    assert (await adapter.fetch_price_bars("EURUSD", "BID", TF, count=5)).truncated       # 3 complete < 5
    assert not (await adapter.fetch_price_bars("EURUSD", "BID", TF, count=3)).truncated
    monkeypatch.setattr(stonex_adapter, "RANGE_CAP", 3)                                # the answer has 3 complete
    capped = await adapter.fetch_price_bars("EURUSD", "BID", TF, from_ms=T0, to_ms=T0 + 3 * TF_MS)
    assert capped.truncated
    monkeypatch.setattr(stonex_adapter, "RANGE_CAP", 4)
    assert not (await adapter.fetch_price_bars("EURUSD", "BID", TF, from_ms=T0, to_ms=T0 + 3 * TF_MS)).truncated
    await adapter.close()


@pytest.mark.parametrize("body", [
    {"PriceBars": [{"Open": 2.5, "High": 2.6, "Low": 2.4, "Close": 2.55}]},     # no BarDate
    {"PriceBars": {"BarDate": "x", "Open": 2.5}},                                 # PriceBars not a list
    [{"BarDate": "x", "Open": 2.5}],                                              # the answer not an object
])
def test_a_malformed_answer_is_a_decode_error_without_values(body: object) -> None:
    with pytest.raises(PriceBarsDecodeError) as caught:
        decode_bar_history(body)
    assert "2.5" not in str(caught.value) and "2.6" not in str(caught.value) and "2.4" not in str(caught.value)


async def test_a_non_get_inside_a_bar_read_is_refused_before_it_is_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    """`rest_get_only` is in force around the bar read itself, not only observed as GETs on the fake."""
    fake = Pr3Fake()
    adapter = await connected(fake)
    await adapter.fetch_price_bars("EURUSD", "BID", TF, count=1)        # the market id lookup, once per symbol
    sent_before = len(fake.requests)

    async def posting(self: StoneXRestClient, market_id: str, *_a: object) -> object:
        return await self._send("POST", f"https://example.invalid/market/{market_id}/barhistory", json={})

    monkeypatch.setattr(StoneXRestClient, "price_bar_history", posting)
    monkeypatch.setattr(StoneXRestClient, "price_bar_history_between", posting)
    with pytest.raises(ExecutionLockedError):
        await adapter.fetch_price_bars("EURUSD", "BID", TF, count=3)
    with pytest.raises(ExecutionLockedError):
        await adapter.fetch_price_bars("EURUSD", "ASK", TF, from_ms=T0, to_ms=T0 + TF_MS)
    assert len(fake.requests) == sent_before                             # nothing reached the transport
    monkeypatch.undo()
    await adapter.close()


# --- gates and the ops bar-capable set -----------------------------------------------------------------------
def test_s39_wires_pr3_finality_and_the_stop_and_roll_items_are_closed() -> None:
    """2026-11-26: pr3_stop_children measured by `probe_bars --check-stops` (WIRED), pr3_roll_prints accepted by the
    user (ACCEPTED); neither is closed by S39 itself, so neither is an S39 blocker any more."""
    by_id = golive.BY_ID
    assert by_id["pr3_finality"].state is golive.GateState.WIRED
    assert by_id["pr3_stop_children"].state is golive.GateState.WIRED
    assert by_id["pr3_roll_prints"].state is golive.GateState.ACCEPTED
    assert not by_id["pr3_stop_children"].closed_by_s39 and not by_id["pr3_roll_prints"].closed_by_s39
    assert golive.s39_blockers() == ()


def test_bar_capable_adapters_follow_the_gate_table() -> None:
    assert ops_arm.BROKER_BAR_ADAPTERS == frozenset({"stonex"})
    assert ops_arm.bar_capable_adapters([]) == frozenset({"stonex"})                  # every blocker closed
    assert ops_arm.bar_capable_adapters([object()]) == frozenset()
    assert ops_arm.BAR_CAPABLE_ADAPTERS == ops_arm.bar_capable_adapters()
    if golive.s39_blockers():                                                        # a gate S39 waits for is open
        assert ops_arm.BAR_CAPABLE_ADAPTERS == frozenset()
        assert ops_arm.ArmContext.from_env({}).bar_capable == frozenset()
    else:                                                                            # since 2026-11-26
        assert ops_arm.BAR_CAPABLE_ADAPTERS == frozenset({"stonex"})
        assert ops_arm.ArmContext.from_env({}).bar_capable == frozenset({"stonex"})
