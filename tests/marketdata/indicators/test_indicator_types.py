"""Indicator types, the registry and new classes with no store or port change (plan S18; design §A6, R11-m): quantum
per output (PRICE, RATIO, COUNT), spec ids, value and checkpoint codecs, the registration check (cycles, references,
retention, recompute order), and a multi-output, a multi-input, a lower-timeframe and an anchored test indicator run
through `md-indicators`. Synthetic instruments only."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from decimal import Decimal
from typing import ClassVar

import pytest

from src.marketdata.indicators.atr import ATR_V1
from src.marketdata.indicators.base import (
    Count,
    History,
    IndicatorRef,
    IndicatorSpec,
    IndicatorValue,
    InputRef,
    Lineage,
    Output,
    Point,
    SeriesRef,
    State,
    Unit,
    Window,
    decode_value,
    encode_value,
    quantum,
)
from src.marketdata.indicators.checkpoint import CHECKPOINT_EVERY, decode_checkpoint, due, encode_checkpoint
from src.marketdata.indicators.engine import IndicatorSettings, MdIndicators
from src.marketdata.indicators.registry import REGISTRY, IndicatorRegistry, SpecPlan, plan_specs
from src.marketdata.types import M1, M15, Bar, Quality, SeriesId, Side, Source

from .helpers import INC, Q, SYN, SYN2, SYN_1M, SYN_ASK, T0, Bands, ChildRange, FakeBars, HourHigh, MidClose, walk


# ---------------------------------------------------------------------------------------------------- quantum, spec
def test_quantum_per_output_unit() -> None:
    assert quantum(Unit.PRICE, Decimal("0.00001")) == Decimal("1E-9")
    assert quantum(Unit.PRICE, Decimal("0.01")) == Decimal("0.000001")
    assert quantum(Unit.RATIO, Decimal("0.01")) == Decimal("0.000001")
    assert quantum(Unit.COUNT, Decimal("0.01")) == Decimal(1)


def test_spec_id_is_sha256_of_the_canonical_spec_and_pins_the_increment() -> None:
    a = REGISTRY.spec("ATR", 1, SYN, {"length": "14"}, INC)
    assert len(a.indicator_id) == 16 and int(a.indicator_id, 16) >= 0
    assert a.indicator_id == REGISTRY.spec("ATR", 1, SYN, {}, INC).indicator_id        # normalised params
    assert a.indicator_id != REGISTRY.spec("ATR", 1, SYN, {"length": "5"}, INC).indicator_id
    assert a.indicator_id != REGISTRY.spec("ATR", 1, SYN, {}, Decimal("0.0001")).indicator_id
    assert a.indicator_id != REGISTRY.spec("ATR", 1, SYN2, {}, INC).indicator_id
    assert IndicatorSpec.parse(a.canonical) == a
    assert IndicatorSpec.parse("{bad") is None and IndicatorSpec.parse('{"k":"ATR"}') is None
    assert "price_increment" not in repr(a) and str(INC) not in repr(a)
    with pytest.raises(LookupError):
        REGISTRY.spec("NOPE", 1, SYN, {}, INC)


def test_value_member_round_trip_and_fault_marker() -> None:
    lin = Lineage("0123456789abcdef", T0 - 140 * Q, "a" * 16, 3)
    v = IndicatorValue(indicator_id="0123456789abcdef", output="value", bucket_start_ms=T0, value=Decimal("0.000123456"),
                       quality=Quality.COMPLETE, warm=True, final_at_ms=T0 + Q + 30_000, lineage=lin)
    m = encode_value(v)
    assert m.startswith(f"{T0:013d}|") and decode_value(v.indicator_id, "value", m) == v
    assert "0.000123456" not in repr(v)
    fault = replace(v, value=None, warm=False, final_at_ms=None, lineage=Lineage(v.indicator_id, None, "0" * 16, 0,
                                                                                    "job_hung"))
    back = decode_value(v.indicator_id, "value", encode_value(fault))
    assert back is not None and back.faulted and back.lineage.reason == "job_hung" and not back.warm
    for bad in ("", "x|y", m + "|extra", m.replace("|C|", "|Z|")):
        assert decode_value(v.indicator_id, "value", bad) is None
    with pytest.raises(ValueError):
        encode_value(replace(v, lineage=replace(lin, reason="Bad Reason")))


def test_checkpoints_every_96_buckets_and_codec() -> None:
    assert due(T0, M15) and not due(T0 + Q, M15) and due(T0 + CHECKPOINT_EVERY * Q, M15)
    state = State.of(atr=Decimal("0.00012345"), close=Decimal("1.1"), n=140)
    member = encode_checkpoint(T0, state)
    ck = decode_checkpoint(member)
    assert ck is not None and ck.bucket_start_ms == T0 and ck.state == state and ck.state.dec("atr") == Decimal("0.00012345")
    assert "0.00012345" not in repr(ck)
    assert decode_checkpoint("123|{") is None and decode_checkpoint(f"{T0:013d}|[1]") is None
    assert State.decode("{\"a\": 1}") is None


# ---------------------------------------------------------------------------------------------------- registry
class Smooth(Bands):
    """A dependant: the mean of the last 3 values of another indicator's output (an `IndicatorRef` input)."""

    KIND: ClassVar[str] = "TEST_SMOOTH"
    OUTPUTS: ClassVar[tuple[Output, ...]] = (Output("value", Unit.PRICE),)
    links: ClassVar[dict[str, str]] = {}

    def parse_params(self, raw: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
        return (("src", raw["src"]),)

    def inputs(self, spec: IndicatorSpec) -> tuple[InputRef, ...]:
        src = Smooth.links.get(spec.series.symbol, spec.param("src"))
        return (IndicatorRef(src, "value"),)

    def window(self, spec: IndicatorSpec) -> Mapping[InputRef, Window]:
        return {self.inputs(spec)[0]: Count(3)}

    def seed(self, spec: IndicatorSpec, history: History) -> State:
        pts = [p for p in history.get(self.inputs(spec)[0], ()) if isinstance(p, Point)]
        return State.of(value=sum((p.value for p in pts), Decimal(0)) / len(pts)) if pts else State.of(n=0)

    def outputs(self, spec: IndicatorSpec, state: State) -> Mapping[str, Decimal] | None:
        v = state.dec("value")
        return None if v is None else {"value": v}


class Wide(Bands):
    KIND: ClassVar[str] = "TEST_WIDE"

    def window(self, spec: IndicatorSpec) -> Mapping[InputRef, Window]:
        return {SeriesRef(spec.series): Count(20_000)}


def test_registry_keys_and_deprecation() -> None:
    reg = IndicatorRegistry((ATR_V1,))
    with pytest.raises(ValueError):
        reg.add(ATR_V1)
    assert reg.keys() == (("ATR", 1),) and reg.get("ATR", 1) is ATR_V1
    s = reg.spec("ATR", 1, SYN, {}, INC)
    reg.deprecate("ATR", 1)
    with pytest.raises(LookupError):
        reg.spec("ATR", 1, SYN, {}, INC)
    assert reg.of(s) is ATR_V1                         # a pinned spec keeps its class


def test_plan_orders_inputs_first_and_refuses_cycles_unknown_inputs_and_retention() -> None:
    reg = IndicatorRegistry((ATR_V1, Smooth(), Wide()))
    atr = reg.spec("ATR", 1, SYN, {}, INC)
    smooth = reg.spec("TEST_SMOOTH", 1, SYN, {"src": atr.indicator_id}, INC)
    plan = plan_specs(reg, {smooth.indicator_id: smooth, atr.indicator_id: atr})
    assert isinstance(plan, SpecPlan) and plan.order == (atr.indicator_id, smooth.indicator_id)
    missing = plan_specs(reg, {smooth.indicator_id: smooth})
    assert isinstance(missing, list) and "not registered" in missing[0]
    a = reg.spec("TEST_SMOOTH", 1, SYN, {"src": "0" * 16}, INC)
    b = reg.spec("TEST_SMOOTH", 1, SYN2, {"src": "1" * 16}, INC)
    Smooth.links = {"SYN1": b.indicator_id, "SYN2": a.indicator_id}
    try:
        cyc = plan_specs(reg, {a.indicator_id: a, b.indicator_id: b})
    finally:
        Smooth.links = {}
    assert isinstance(cyc, list) and all("cycle" in p for p in cyc)
    wide = reg.spec("TEST_WIDE", 1, SYN_1M, {"n": "4"}, INC)
    over = plan_specs(reg, {wide.indicator_id: wide})
    assert isinstance(over, list) and "retention" in over[0]
    assert isinstance(plan_specs(reg, {"f" * 16: atr}), list)          # id must match its spec


# ---------------------------------------------------------------------------------------------------- new classes
REG = IndicatorRegistry((ATR_V1, Bands(), MidClose(), ChildRange(), HourHigh()))


def engine(bars: FakeBars) -> MdIndicators:
    return MdIndicators(bars, registry=REG, settings=IndicatorSettings(retry_ms=0))


async def test_multi_output_indicator_needs_no_store_or_port_change() -> None:
    fake = FakeBars()
    bars = walk(20)
    fake.put(bars)
    eng = engine(fake)
    s = REG.spec("TEST_BANDS", 1, SYN, {"n": "4"}, INC)
    st = await eng.register(s)
    assert st is not None and st.last_bucket_ms == bars[-1].bucket_start_ms and st.warm
    last4 = bars[-4:]
    hi, lo = max(b.high for b in last4), min(b.low for b in last4)
    assert eng.latest(s.indicator_id, "upper") == hi and eng.latest(s.indicator_id, "lower") == lo
    assert eng.latest(s.indicator_id, "mid") == (hi + lo) / 2


async def test_multi_input_indicator_bid_and_ask() -> None:
    fake = FakeBars()
    bid = walk(6)
    ask = [replace(b, series=SYN_ASK, open=b.open + 2 * INC, high=b.high + 2 * INC, low=b.low + 2 * INC,
                   close=b.close + 2 * INC) for b in bid]
    fake.put(bid + ask)
    eng = engine(fake)
    s = REG.spec("TEST_MID", 1, SYN, {}, INC)
    assert await eng.register(s) is not None
    assert eng.latest(s.indicator_id, "value") == bid[-1].close + INC
    ratio = eng.latest(s.indicator_id, "ratio")
    assert ratio is not None and ratio > 1
    assert quantum(MidClose.OUTPUTS[1].unit, INC) == Decimal("0.000001")


async def test_lower_timeframe_indicator_uses_closed_children_only() -> None:
    fake = FakeBars()
    kids = [replace(b, quality=Quality.COMPLETE) for b in walk(33, series=SYN_1M, start=T0)]   # 2 buckets + 3 min
    fake.put(kids)
    eng = engine(fake)
    s = REG.spec("TEST_CHILDREN", 1, SYN_1M, {}, INC)
    st = await eng.register(s)
    assert st is not None and st.last_bucket_ms == T0 + Q          # the third bucket is still forming: no lookahead
    inside = kids[15:30]
    assert eng.latest(s.indicator_id, "count") == Decimal(15)
    assert eng.latest(s.indicator_id, "range") == max(b.high for b in inside) - min(b.low for b in inside)


async def test_anchored_indicator_restarts_at_the_hour() -> None:
    fake = FakeBars()
    hour = T0 - T0 % 3_600_000
    bars = walk(7, start=hour)                       # 10:00 .. 11:30 style: 4 bars in the first hour, 3 in the next
    fake.put(bars[:3])
    eng = engine(fake)
    s = REG.spec("TEST_ANCHOR", 1, SYN, {}, INC)
    await eng.register(s)
    assert eng.latest(s.indicator_id, "bars") == Decimal(3)
    assert eng.latest(s.indicator_id, "high") == max(b.high for b in bars[:3])
    fake.put(bars[3:5])
    from src.marketdata.types import BarEndEvent
    eng.on_released(BarEndEvent(SYN, bars[4].bucket_end_ms, "h", 5, 0))
    await eng.cycle()
    assert eng.latest(s.indicator_id, "bars") == Decimal(1)          # a new hour: the window restarts
    assert eng.latest(s.indicator_id, "high") == bars[4].high


def test_tick_series_ids_are_key_safe() -> None:
    s = SeriesId("mock", "SYN1", M1, Side.BID, Source.TICK)
    spec = REG.spec("TEST_CHILDREN", 1, s, {}, INC)
    assert spec.series.spec == M1 and ChildRange().clock(spec) == M15
    assert isinstance(walk(1)[0], Bar)
