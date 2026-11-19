"""Plan S03: the read-only PR3 bar probe (``src.adapters.stonex.probe_bars``) against the mock and the fake CIAPI.
No network, no real clock: the probe's clock and sleep are injected."""
from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from src.adapters.base import ActiveStopOrders, BrokerError, SessionExpiredError, TransientBrokerError
from src.adapters.mock import MockBrokerAdapter
from src.adapters.ratelimit import RestPriority
from src.adapters.stonex.probe_bars import (
    NOT_MEASURED,
    STOPS_NOT_MEASURED,
    default_report_path,
    percentile,
    price_fields_in,
    run,
)
from src.adapters.transport import is_denied_path
from src.dto import BarSide, BrokerBar, PriceBarsResponse, PriceDTO

from tests.oversee.helpers import order, position

from .conftest import EURUSD_ID, TRADING, make_adapter, wcf
from .test_price_bars_capability import BarsFake, bar_requests

T0 = 1_767_225_600_000          # on the 15m grid
PROVISIONAL_S = 20              # the fake broker revises a closed bar for this long after its end


class Clock:
    def __init__(self, now_ms: int) -> None:
        self.now = now_ms

    def ms(self) -> int:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += int(seconds * 1000)


class EvolvingMock(MockBrokerAdapter):
    """Bars computed from the clock: closed buckets (dated by their start, or by their end with ``date_by_end``), a
    forming bar (dated on the grid, or ``partial_at_tick_ms`` before the read: by its last tick, off the grid; none
    with ``with_partial=False``), and a just-closed bucket whose close is revised until PROVISIONAL_S after its end (a
    finality lag between +5 s and +30 s). ``fail_calls``: count-form call number (1-based) -> the error it raises."""

    def __init__(self, clock: Clock, *, date_by_end: bool = False, partial_at_tick_ms: int | None = None,
                 with_partial: bool = True, fail_calls: dict[int, BrokerError] | None = None,
                 **kwargs: object) -> None:
        super().__init__(wall_ms=clock.ms, **kwargs)   # type: ignore[arg-type]
        self.clock = clock
        self.date_by_end = date_by_end
        self.partial_at_tick_ms = partial_at_tick_ms
        self.with_partial = with_partial
        self.fail_calls = dict(fail_calls or {})
        self.count_calls = 0

    def _bar(self, start: int, tf_ms: int, *, partial: bool = False) -> BrokerBar:
        base = Decimal("1.0861") + Decimal(start // tf_ms % 7) / 10_000
        provisional = start + tf_ms <= self.clock.now < start + tf_ms + PROVISIONAL_S * 1000
        close = base + (Decimal("0.0003") if provisional or partial else Decimal("0.0001"))
        dated = start + tf_ms if self.date_by_end else start
        if partial and self.partial_at_tick_ms is not None:
            dated = self.clock.now - self.partial_at_tick_ms
        return BrokerBar(bucket_start_ms=dated, open=base, high=base + Decimal("0.0005"),
                         low=base - Decimal("0.0005"), close=close, side=None, partial=partial)

    async def fetch_price_bars(self, symbol: str, side: BarSide, timeframe_s: int, *, count: int | None = None,
                               from_ms: int | None = None, to_ms: int | None = None) -> PriceBarsResponse:
        await super().fetch_price_bars(symbol, side, timeframe_s, count=count, from_ms=from_ms, to_ms=to_ms)
        tf_ms = timeframe_s * 1000
        current = self.clock.now // tf_ms * tf_ms
        if count is not None:
            self.count_calls += 1
            error = self.fail_calls.pop(self.count_calls, None)
            if error is not None:
                raise error
            starts = [current - k * tf_ms for k in range(count, 0, -1)]      # count complete + the forming bar
            bars = [self._bar(s, tf_ms) for s in starts]
            if self.with_partial:
                bars.append(self._bar(current, tf_ms, partial=True))
        else:
            assert from_ms is not None and to_ms is not None
            bars = [self._bar(s, tf_ms) for s in range(from_ms, to_ms + 1, tf_ms) if s + tf_ms <= self.clock.now]
        return PriceBarsResponse(adapter_id=self.adapter_id, symbol=symbol, side=side, timeframe_s=timeframe_s,
                                 bars=tuple(bars), truncated=False, received_ms=self.clock.now, count=count,
                                 from_ms=from_ms, to_ms=to_ms, field_names=("PriceBars", "bar.BarDate"))


def ticks() -> list[PriceDTO]:
    out = []
    for i in range(4):
        broker = i < 3
        ts = T0 + 1000 * i
        out.append(PriceDTO(adapter_id="mock", symbol="EURUSD", bid=Decimal("1.08613"), ask=Decimal("1.08627"),
                            spread_pips=Decimal("1.4"), timestamp_ms=ts if broker else ts + 250,
                            ts_source="BROKER" if broker else "RECEIPT", received_ms=ts + 250 + 50 * i))
    return out


async def test_probe_against_the_mock_measures_and_writes_no_price(tmp_path: Path) -> None:
    clock = Clock(T0 + 100_000)
    mock = EvolvingMock(clock, price_stream=ticks())
    out = tmp_path / "report.json"
    code = await run(None, adapter=mock, symbols=["EURUSD"], timeframes_s=(60, 900), rounds=2, tick_seconds=1.0,
                     out=out, clock_ms=clock.ms, sleep=clock.sleep)
    assert code == 0 and mock.connect_count == 1 and not mock.connected
    text = out.read_text(encoding="utf-8")
    report = json.loads(text)
    assert price_fields_in(report) == []
    for value in ("1.086", "1.4", "0.0005"):
        assert value not in text
    assert report["flags"]["bar_reader"] == "fetch_price_bars" and report["not_measured"] == list(NOT_MEASURED)

    series = {(s["side"], s["timeframe_s"]): s for s in report["series"]}
    assert set(series) == {("BID", 60), ("BID", 900), ("ASK", 60), ("ASK", 900)}
    for s in series.values():
        assert s["supported"] and s["time_convention"] == "start"
        assert s["convention_votes"]["partial_start"] > 0 and s["convention_votes"]["complete_start"] == 2
        assert s["convention_votes"]["partial_end"] == s["convention_votes"]["complete_end"] == 0
        assert s["finality_lag_s"] == {"p50": 30.0, "p99": 30.0, "samples": 2, "never_final": 0, "unmeasured": 0}
        assert [r["status"] for r in s["rounds"]] == ["final", "final"]
        assert s["checks_failed"] == {"price_not_positive": 0, "high_low_bounds": 0, "off_grid": 0}
        assert s["side_tag"]["bars_tagged"] == 0 and s["field_names"] == ["PriceBars", "bar.BarDate"]
        first = s["rounds"][0]
        assert first["bucket_end_ms"] == T0 + 900_000
        assert [r["offset_s"] for r in first["reads"]] == [5, 30, 300]
        assert [r["same_as_previous"] for r in first["reads"]] == [None, False, True]
        assert all(r["bucket_present"] and r["next_bucket_present"] and r["partial_present"] for r in first["reads"])
        assert s["range_form"]["ok"] and s["range_form"]["outside_range"] == 0 and s["range_form"]["off_grid"] == 0
    assert series[("BID", 900)]["rounds"][1]["bucket_end_ms"] == T0 + 1_800_000

    t = report["ticks"]
    assert t["ticks"] == 4 and t["broker_timed"] == 3 and t["receipt_timed"] == 1
    assert t["tick_time_resolution_ms"] == 1000 and t["subscribed_events"] == 1
    assert t["receipt_minus_broker_ms"] == {"p50": 300.0, "p99": 350.0, "samples": 3}
    assert report["relogons"] == {"count": 0, "failed": 0}
    assert {pr for m, pr in mock.rest_calls if m == "fetch_price_bars"} == {RestPriority.POLL}   # lowest class


async def test_probe_reports_unsupported_and_failing_series(tmp_path: Path) -> None:
    clock = Clock(T0 + 100_000)
    mock = MockBrokerAdapter(wall_ms=clock.ms)
    mock.set_price_bars_supported(False)
    out = tmp_path / "r.json"
    assert await run(None, adapter=mock, sides=["BID"], timeframes_s=[900], tick_seconds=0, out=out,
                     clock_ms=clock.ms, sleep=clock.sleep) == 0
    (s,) = json.loads(out.read_text(encoding="utf-8"))["series"]
    # no convention and no successful read: unmeasured under both hypotheses, never a "never final" verdict
    assert not s["supported"] and s["time_convention"] == "unknown" and s["finality_lag_s"] is None
    assert s["finality_lag_s_if_start"]["unmeasured"] == s["finality_lag_s_if_end"]["unmeasured"] == 1
    assert s["rounds"][0]["status"] == "convention_unknown"
    assert not any(r["ok"] for r in s["rounds"][0]["if_start"]["reads"])
    assert "never_final" not in json.dumps(s)

    mock = MockBrokerAdapter(wall_ms=clock.ms)
    mock.undecodable_price_bars("EURUSD", "BID", 900, times=3)
    assert await run(None, adapter=mock, sides=["BID"], timeframes_s=[900], tick_seconds=0, out=out,
                     clock_ms=clock.ms, sleep=clock.sleep) == 0
    (s,) = json.loads(out.read_text(encoding="utf-8"))["series"]
    errors = [r["error"] for r in s["rounds"][0]["if_start"]["reads"]]
    assert all(e.startswith("PriceBarsDecodeError") for e in errors) and s["range_form"]["ok"]


async def test_probe_against_stonex_reads_bars_with_get_only(tmp_path: Path) -> None:
    clock = Clock(T0 + 100_000)
    fake = BarsFake()
    adapter = make_adapter(fake, wall_ms=clock.ms)
    out = tmp_path / "sx.json"
    assert await run(None, adapter=adapter, sides=["BID"], timeframes_s=[900], offsets_s=[0], tick_seconds=0,
                     out=out, clock_ms=clock.ms, sleep=clock.sleep) == 0
    report = json.loads(out.read_text(encoding="utf-8"))
    assert report["flags"]["bar_reader"] == "fetch_price_bars" and price_fields_in(report) == []
    bars = bar_requests(fake)
    assert len(bars) == 2 and all(r.method == "GET" for r in bars)          # count form + range form
    assert {r.url.path.rsplit("/", 1)[-1] for r in bars} == {"barhistory", "barhistorybetween"}
    assert not any(is_denied_path(r.url.path) for r in fake.requests)
    others = {r.url.path for r in fake.requests if r.method != "GET"}
    assert others <= {"/v2/Session", "/v2/market/information", "/TradingAPI/session/deleteSession"}


async def test_forming_bar_dated_by_its_last_tick_never_votes_start(tmp_path: Path) -> None:
    clock = Clock(T0 + 100_000)
    mock = EvolvingMock(clock, date_by_end=True, partial_at_tick_ms=1_234)     # the real convention is 'end'
    out = tmp_path / "offgrid.json"
    assert await run(None, adapter=mock, sides=["BID"], timeframes_s=[60, 900], rounds=2, tick_seconds=0, out=out,
                     clock_ms=clock.ms, sleep=clock.sleep) == 0
    for s in json.loads(out.read_text(encoding="utf-8"))["series"]:
        votes = s["convention_votes"]
        assert votes["partial_start"] == votes["partial_end"] == 0 and votes["partial_off_grid"] > 0
        assert votes["complete_end"] == 2 and votes["complete_start"] == 0
        assert s["time_convention"] == "end"
        assert s["finality_lag_s"]["samples"] == 2 and s["finality_lag_s"]["p50"] == 30.0
        assert s["checks_failed"]["off_grid"] == 0           # complete bars dated by their end are on the grid


async def test_unknown_convention_reports_both_hypotheses(tmp_path: Path) -> None:
    clock = Clock(T0 + 100_000)
    mock = EvolvingMock(clock, with_partial=False)          # no forming bar, and no read before the next bucket ends
    out = tmp_path / "unknown.json"
    assert await run(None, adapter=mock, sides=["BID"], timeframes_s=[60], offsets_s=[300], tick_seconds=0,
                     out=out, clock_ms=clock.ms, sleep=clock.sleep) == 0
    (s,) = json.loads(out.read_text(encoding="utf-8"))["series"]
    assert s["time_convention"] == "unknown" and s["finality_lag_s"] is None
    (rnd,) = s["rounds"]
    assert rnd["status"] == "convention_unknown" and rnd["finality_lag_s"] is None
    assert rnd["if_start"]["status"] == rnd["if_end"]["status"] == "final"
    assert s["finality_lag_s_if_start"] == {"p50": 300.0, "p99": 300.0, "samples": 1, "not_final": 0,
                                            "unmeasured": 0}
    assert "never_final" not in json.dumps(s)


async def test_failed_last_read_is_unmeasured_not_never_final(tmp_path: Path) -> None:
    clock = Clock(T0 + 100_000)
    mock = EvolvingMock(clock, fail_calls={3: TransientBrokerError("scripted outage at +5 min")})
    out = tmp_path / "failed.json"
    assert await run(None, adapter=mock, sides=["BID"], timeframes_s=[900], rounds=2, tick_seconds=0, out=out,
                     clock_ms=clock.ms, sleep=clock.sleep) == 0
    (s,) = json.loads(out.read_text(encoding="utf-8"))["series"]
    assert [r["status"] for r in s["rounds"]] == ["unmeasured", "final"]
    assert s["rounds"][0]["finality_lag_s"] is None and not s["rounds"][0]["reads"][-1]["ok"]
    assert s["finality_lag_s"] == {"p50": 30.0, "p99": 30.0, "samples": 1, "never_final": 0, "unmeasured": 1}


async def test_session_expiry_logs_on_again_and_retries_the_read(tmp_path: Path) -> None:
    clock = Clock(T0 + 100_000)
    mock = EvolvingMock(clock, fail_calls={2: SessionExpiredError("scripted expiry")})
    out = tmp_path / "relogon.json"
    assert await run(None, adapter=mock, sides=["BID"], timeframes_s=[900], rounds=2, tick_seconds=0, out=out,
                     clock_ms=clock.ms, sleep=clock.sleep) == 0
    report = json.loads(out.read_text(encoding="utf-8"))
    assert mock.connect_count == 2 and report["relogons"] == {"count": 1, "failed": 0}
    (s,) = report["series"]
    assert all(r["ok"] for rnd in s["rounds"] for r in rnd["reads"])
    assert s["finality_lag_s"]["samples"] == 2


async def test_side_tags_record_raw_values_and_count_only_known_forms(tmp_path: Path) -> None:
    clock = Clock(T0 + 100_000)
    mock = MockBrokerAdapter(wall_ms=clock.ms)
    tags = ["1", "B", "ASK", "OFFER", "2", "BID"]
    mock.set_price_bars("EURUSD", "BID", 900, [
        BrokerBar(bucket_start_ms=T0 - (6 - i) * 900_000, open=Decimal("1.08613"), high=Decimal("1.08623"),
                  low=Decimal("1.08603"), close=Decimal("1.08613"), side=tag) for i, tag in enumerate(tags)])
    out = tmp_path / "tags.json"
    assert await run(None, adapter=mock, sides=["BID"], timeframes_s=[900], offsets_s=[5], count=6, tick_seconds=0,
                     out=out, clock_ms=clock.ms, sleep=clock.sleep) == 0
    text = out.read_text(encoding="utf-8")
    (s,) = json.loads(text)["series"]
    tag = s["side_tag"]
    assert tag["values"] == ["1", "2", "ASK", "B", "BID", "OFFER"]
    assert tag["mismatches"] > 0 and tag["matches"] == tag["mismatches"] == tag["form_unknown"]   # 2 of each
    assert tag["matches"] + tag["mismatches"] + tag["form_unknown"] == tag["bars_tagged"]
    assert "1.086" not in text


def test_report_path_never_defaults_into_the_repository_or_onedrive(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[2]
    assert default_report_path(1, temp_dir=str(repo / "tmp")) is None
    assert default_report_path(1, temp_dir=str(tmp_path / "OneDrive - Personal")) is None
    assert default_report_path(1, temp_dir=str(tmp_path)) == tmp_path / "xtnl-pr3-bars-1.json"


async def test_probe_refuses_an_unsafe_default_path(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[2]
    mock = MockBrokerAdapter()
    assert await run(None, adapter=mock, tick_seconds=0, rounds=0, temp_dir=str(repo)) == 2
    assert mock.connect_count == 0 and not list(repo.glob("xtnl-pr3-bars-*.json"))


def test_price_fields_in_finds_keys_and_decimals() -> None:
    assert price_fields_in({"a": [{"Close": 1}], "b": Decimal(1), "c": {"n": 2}}) == ["$.a[0].Close", "$.b"]
    assert percentile([], 50) is None and percentile([5.0, 30.0, 300.0], 50) == 30.0
    assert percentile([5.0, 30.0, 300.0], 99) == 300.0


@pytest.mark.parametrize("argv", [["--help"]])
def test_cli_help_needs_no_credentials(argv: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    from src.adapters.stonex.probe_bars import main
    with pytest.raises(SystemExit) as info:
        main(argv)
    assert info.value.code == 0 and "--out" in capsys.readouterr().out


def test_probe_and_bar_modules_build_no_http_client_of_their_own() -> None:
    root = Path(__file__).resolve().parents[2] / "src" / "adapters" / "stonex"
    for name in ("probe_bars.py", "bars.py"):
        source = (root / name).read_text(encoding="utf-8")
        for banned in ("import httpx", "AsyncClient", "aiohttp", "urllib", "import requests", "import socket",
                       "ReadOnlyTransport("):
            assert banned not in source, f"{name} must read only through the adapter ({banned})"


# --- plan S39: --check-stops (PR9 stop children in activestoplimitorders, ids only) ----------------------------
class HidingMock(MockBrokerAdapter):
    """The mock's stop list read, minus the orders in ``hidden`` (a child the broker's list does not show)."""

    def __init__(self, hidden: set[str]) -> None:
        super().__init__()
        self.hidden = hidden

    async def fetch_active_stop_orders(self) -> ActiveStopOrders:
        answer = await super().fetch_active_stop_orders()
        return replace(answer, orders=tuple(o for o in answer.orders if o.broker_order_id not in self.hidden))

    async def fetch_attached_stop_ids(self) -> list[tuple[str, str]]:
        """The independent source: every linked working STOP stands in for a child on its position's own row."""
        return [(o.broker_order_id, o.linked_position_id) for o in await self.fetch_open_orders()
                if o.order_kind == "STOP" and o.linked_position_id is not None]


async def test_check_stops_lists_children_by_id_only_and_skips_bars(tmp_path: Path) -> None:
    mock = HidingMock({"s2"})
    mock.set_stop_orders_supported(True)
    for pid in ("p1", "p2", "p3"):
        mock.set_position(position(pid))
    mock.set_order(order("s1", side="SELL", stop="1.0950", reduce_only=True, linked="p1", kind="STOP"))
    mock.set_order(order("s2", side="SELL", stop="1.0950", reduce_only=True, linked="p2", kind="STOP"))
    mock.set_order(order("t1", side="SELL", entry="1.1100", reduce_only=True, linked="p1", kind="LIMIT"))
    mock.set_order(order("e9", kind="STOP"))                                  # an entry: no linked position
    out = tmp_path / "stops.json"
    assert await run(None, adapter=mock, check_stops=True, out=out) == 0
    text = out.read_text(encoding="utf-8")
    report = json.loads(text)
    assert price_fields_in(report) == [] and "1.09" not in text and "1.1" not in text
    stops = report["stop_children"]
    assert stops == {"supported": True, "conclusive": False, "not_conclusive": "positions_without_stop_child",
                     "open_positions": 3, "stop_children": 2, "listed": 1, "missing": 1, "listed_ids": ["s1"],
                     "missing_ids": ["s2"], "attached_source": True, "attached_children": 2, "attached_listed": 1,
                     "attached_missing": 1, "attached_listed_ids": ["s1"], "attached_missing_ids": ["s2"],
                     "positions_without_stop_child": 1, "error": ""}      # p3 has no stop in either read
    assert report["series"] == [] and report["ticks"] is None and report["flags"]["check_stops"]
    assert STOPS_NOT_MEASURED not in report["not_measured"] and STOPS_NOT_MEASURED in NOT_MEASURED
    assert not any(m == "fetch_price_bars" for m, _ in mock.rest_calls)     # no bar read in this mode
    mock.set_order(order("s3", side="SELL", stop="1.0950", reduce_only=True, linked="p3", kind="STOP"))
    assert await run(None, adapter=mock, check_stops=True, out=out) == 0
    stops = json.loads(out.read_text(encoding="utf-8"))["stop_children"]
    assert stops["conclusive"] and stops["not_conclusive"] == "" and stops["positions_without_stop_child"] == 0
    assert stops["attached_listed_ids"] == ["s1", "s3"] and stops["attached_missing_ids"] == ["s2"]


async def test_check_stops_without_children_or_list_read_is_inconclusive(tmp_path: Path) -> None:
    mock = MockBrokerAdapter()
    mock.set_position(position("p1"))
    out = tmp_path / "none.json"
    assert await run(None, adapter=mock, check_stops=True, out=out) == 0
    stops = json.loads(out.read_text(encoding="utf-8"))["stop_children"]
    assert stops["supported"] and not stops["conclusive"] and stops["open_positions"] == 1    # nothing to look for
    assert stops["not_conclusive"] == "no_attached_source" and not stops["attached_source"]  # the plain mock
    mock.set_order(order("s1", side="SELL", stop="1.0950", reduce_only=True, linked="p1", kind="STOP"))
    mock.set_stop_orders_supported(False)
    assert await run(None, adapter=mock, check_stops=True, out=out) == 0
    stops = json.loads(out.read_text(encoding="utf-8"))["stop_children"]
    assert not stops["supported"] and not stops["conclusive"] and stops["stop_children"] == 1
    mock.fail_next("fetch_positions", TransientBrokerError("down"))
    assert await run(None, adapter=mock, check_stops=True, out=out) == 0
    stops = json.loads(out.read_text(encoding="utf-8"))["stop_children"]
    assert stops["error"].startswith("TransientBrokerError") and not stops["conclusive"]


async def test_check_stops_against_stonex_compares_attached_children_with_the_list(tmp_path: Path) -> None:
    fake = BarsFake()

    def pos_row(order_id: int) -> dict[str, object]:
        return {"OrderId": order_id, "MarketId": int(EURUSD_ID), "Direction": "buy", "Quantity": 25000,
                "Price": 1.0855, "TradingAccountId": int(TRADING), "LastChangedDateTimeUTC": wcf(T0),
                "StopOrder": {"OrderId": order_id + 1, "TriggerPrice": 1.08, "Quantity": 25000}}

    fake.positions = [pos_row(555), pos_row(700)]
    fake.actives = [{"OrderId": 556, "ParentOrderId": 555, "MarketId": int(EURUSD_ID), "Direction": "Sell",
                     "Quantity": 25000, "TriggerPrice": 1.08, "TradingAccountId": int(TRADING), "Type": "Stop",
                     "Status": 2}]
    adapter = make_adapter(fake)
    out = tmp_path / "sx-stops.json"
    assert await run(None, adapter=adapter, check_stops=True, out=out) == 0
    stops = json.loads(out.read_text(encoding="utf-8"))["stop_children"]
    assert (stops["open_positions"], stops["stop_children"]) == (2, 2)
    assert stops["listed_ids"] == ["556"] and stops["missing_ids"] == ["701"] and stops["conclusive"]
    assert (stops["attached_children"], stops["attached_listed_ids"], stops["attached_missing_ids"]) ==         (2, ["556"], ["701"])
    assert not bar_requests(fake) and not any(is_denied_path(r.url.path) for r in fake.requests)
    others = {r.url.path for r in fake.requests if r.method != "GET"}
    assert others <= {"/v2/Session", "/v2/market/information", "/TradingAPI/session/deleteSession"}


async def test_check_stops_is_not_conclusive_from_the_list_it_checks(tmp_path: Path) -> None:
    """S39 review: no openpositions row carries a StopOrder; the only child is in activestoplimitorders, which
    fetch_open_orders merges in, so it is "listed" by construction and proves nothing."""
    fake = BarsFake()
    fake.positions = [{"OrderId": 555, "MarketId": int(EURUSD_ID), "Direction": "buy", "Quantity": 25000,
                       "Price": 1.0855, "TradingAccountId": int(TRADING), "LastChangedDateTimeUTC": wcf(T0)}]
    fake.actives = [{"OrderId": 556, "ParentOrderId": 555, "MarketId": int(EURUSD_ID), "Direction": "Sell",
                     "Quantity": 25000, "TriggerPrice": 1.08, "TradingAccountId": int(TRADING), "Type": "Stop",
                     "Status": 2}]
    adapter = make_adapter(fake)
    out = tmp_path / "sx-list-only.json"
    assert await run(None, adapter=adapter, check_stops=True, out=out) == 0
    stops = json.loads(out.read_text(encoding="utf-8"))["stop_children"]
    assert (stops["stop_children"], stops["listed_ids"]) == (1, ["556"])          # from the merged read
    assert stops["attached_source"] and stops["attached_children"] == 0
    assert not stops["conclusive"] and stops["not_conclusive"] == "no_attached_child"
    assert all(r.method == "GET" for r in fake.requests if "openpositions" in r.url.path)


def test_cli_offers_check_stops(capsys: pytest.CaptureFixture[str]) -> None:
    from src.adapters.stonex.probe_bars import main
    with pytest.raises(SystemExit):
        main(["--help"])
    assert "--check-stops" in capsys.readouterr().out
