"""Demand leases (plan S18; design §A6 Demand, §A5, PR11, R11-M20): ARMED evicts PREWARM newest first and is refused
only at the hard ceiling (per adapter: decision symbols and the BARS budget less the stop reads; across adapters: the
memory formula), TTL expiry, the indicator registration before `warm`, the demand sources of `md-bars`, `md-writer`
and the bootstrap, and the `LEASE` writes. Synthetic instruments only."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

from src.marketdata.bootstrap import SeriesStateBook
from src.marketdata.demand import (
    CEILING,
    DEFAULT_BACKFILL_BARS,
    NOT_DECISION_GRADE,
    UNKNOWN_SPEC,
    DemandBook,
    DemandSettings,
    series_spec_id,
)
from src.marketdata.indicators.base import IndicatorSpec
from src.marketdata.indicators.atr import ATR_V1
from src.marketdata.indicators.engine import IndicatorSettings, IndState, MdIndicators
from src.marketdata.indicators.registry import REGISTRY, IndicatorRegistry
from src.marketdata.port_impl import MdPort
from src.marketdata.store import keys as K
from src.marketdata.store.reads import StoreReader
from src.marketdata.store.runner import ScriptOk, ScriptRunner, StoreUnavailable
from src.marketdata.types import H1, M15, LeaseClass, SeriesId, Side, Source

from .conftest import KEYRING, SENTINEL, TOKEN, RunIds
from .indicators.helpers import FakeBars, Raising, walk

INC = Decimal("0.00001")


def broker(symbol: str, adapter: str = "mock") -> SeriesId:
    return SeriesId(adapter, symbol, M15, Side.BID, Source.BROKER)


class Clock:
    def __init__(self) -> None:
        self.now = 1_700_000_000_000

    def __call__(self) -> int:
        return self.now


def book(**kw: Any) -> tuple[DemandBook, Clock]:
    clock = Clock()
    settings = kw.pop("settings", DemandSettings(decision_symbols_per_adapter=2))
    return DemandBook(settings=settings, clock_ms=clock, **kw), clock


def test_spec_ids_are_key_safe_words() -> None:
    b, _ = book()
    sid = b.define(broker("SYN1"))
    assert sid == series_spec_id(broker("SYN1")) and sid.startswith("s") and len(sid) == 17 and sid.isalnum()
    atr = REGISTRY.spec("ATR", 1, broker("SYN1"), {}, INC)
    assert b.define(atr) == atr.indicator_id and b.spec(atr.indicator_id) == atr


def test_armed_evicts_prewarm_newest_first_and_is_refused_only_at_the_ceiling() -> None:
    b, _ = book()
    evicted: list[tuple[str, str]] = []
    b.on_evicted(lambda s, c: evicted.append((s, c)))
    a, bb, c, d, e = (b.define(broker(f"SYN{i}")) for i in range(1, 6))
    assert b.admit(a, "arm", LeaseClass.PREWARM).admitted
    assert b.admit(bb, "arm", LeaseClass.PREWARM).admitted
    assert b.admit(c, "arm", LeaseClass.PREWARM).reason == CEILING          # PREWARM never evicts
    got = b.admit(c, "arm", LeaseClass.ARMED)
    assert got.admitted and got.evicted == ((bb, "arm"),) and evicted == [(bb, "arm")]   # the newest PREWARM
    assert b.admit(d, "arm", LeaseClass.ARMED).evicted == ((a, "arm"),)
    refused = b.admit(e, "arm", LeaseClass.ARMED)
    assert not refused.admitted and refused.reason == CEILING                # every symbol ARMED: the hard ceiling
    assert set(b.symbols()) == {("mock", "SYN3"), ("mock", "SYN4")}
    # more leases on an admitted symbol take no room; a TICK series never counts
    atr = b.define(REGISTRY.spec("ATR", 1, broker("SYN3"), {}, INC))
    assert b.admit(atr, "arm", LeaseClass.ARMED).admitted
    tick = b.define(replace(broker("SYN9"), source=Source.TICK))
    assert b.admit(tick, "arm", LeaseClass.ARMED).admitted and len(b.symbols()) == 2


def test_refusals_unknown_and_not_decision_grade() -> None:
    b, _ = book()
    assert b.admit("s" + "0" * 16, "arm", LeaseClass.ARMED).reason == UNKNOWN_SPEC
    hourly = b.define(replace(broker("SYN1"), spec=H1))
    assert b.admit(hourly, "arm", LeaseClass.ARMED).reason == NOT_DECISION_GRADE
    ok = b.define(broker("SYN1"))
    assert b.admit(ok, "bad consumer", LeaseClass.ARMED).reason == UNKNOWN_SPEC


def test_ceiling_from_the_bars_budget_less_stop_reads_and_the_memory_formula() -> None:
    stops = {"mock": 6}
    b, _ = book(settings=DemandSettings(), bars_per_min=lambda a: 10, stop_reads_per_min=lambda a: stops[a])
    assert b.adapter_ceiling("mock") == 2                       # (10 − 6) / 2 reads per symbol
    stops["mock"] = 0
    assert b.adapter_ceiling("mock") == 5
    assert b.memory_symbols() == 8                              # (1 GiB × 0.5 − 15 MiB) / 60 MiB
    small, _ = book(settings=DemandSettings(maxmemory_bytes=200 << 20))
    assert small.memory_symbols() == 1
    x, y = small.define(broker("SYN1", "mock")), small.define(broker("SYN1", "other"))
    assert small.admit(x, "arm", LeaseClass.PREWARM).admitted
    got = small.admit(y, "arm", LeaseClass.ARMED)              # memory binds across adapters: evicts the other's
    assert got.admitted and got.evicted == ((x, "arm"),)


def test_ttl_expiry_frees_the_symbol() -> None:
    b, clock = book()
    a, bb, c = (b.define(broker(f"SYN{i}")) for i in range(1, 4))
    assert b.admit(a, "arm", LeaseClass.ARMED).admitted and b.admit(bb, "arm", LeaseClass.ARMED).admitted
    clock.now += 30_000
    assert b.admit(a, "arm", LeaseClass.ARMED).admitted        # renewed
    clock.now += 40_000                                         # bb expired (70 s), a still live (40 s)
    assert b.admit(c, "arm", LeaseClass.ARMED).admitted and b.live() == frozenset({a, c})


def test_demand_sources_for_bars_writer_and_bootstrap() -> None:
    b, _ = book()
    atr50 = REGISTRY.spec("ATR", 1, broker("SYN1"), {"length": "50"}, INC)
    i = b.define(atr50)
    s2 = b.define(broker("SYN2"))
    t = b.define(replace(broker("SYN1"), source=Source.TICK))
    for sid in (i, s2, t):
        assert b.admit(sid, "arm", LeaseClass.ARMED).admitted
    assert b.broker_demand() == {broker("SYN1"): 650, broker("SYN2"): DEFAULT_BACKFILL_BARS}
    assert b.is_demanded(broker("SYN1"))                        # the indicator lease keeps its input series live
    assert b.is_demanded(replace(broker("SYN1"), source=Source.TICK))
    assert not b.is_demanded(broker("SYN7"))
    held = {h.spec_id: h for h in b.held_leases()}
    assert held[i].registry_key == K.REGISTRY_IND and held[i].definition == atr50.canonical
    assert held[s2].registry_key == K.REGISTRY_SERIES and held[s2].definition == broker("SYN2").canonical
    assert all(0 < h.ttl_ms <= 60_000 and h.value == "ARMED" for h in held.values())


class Host:
    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.registered: list[str] = []
        self.removed: list[str] = []

    async def register(self, spec: IndicatorSpec) -> object | None:
        self.registered.append(spec.indicator_id)
        return object() if self.ok else None

    def remove(self, indicator_id: str) -> bool:
        self.removed.append(indicator_id)
        return True


async def test_indicator_demand_registers_then_release_removes() -> None:
    host = Host()
    b, _ = book(host=host)
    spec = REGISTRY.spec("ATR", 1, broker("SYN1"), {}, INC)
    sid = b.define(spec)
    assert await b.demand(sid, "arm", LeaseClass.ARMED) is True
    assert await b.demand(sid, "arm", LeaseClass.ARMED) is True      # a renewal does not register again
    assert await b.demand(sid, "dash", LeaseClass.PREWARM) is True
    assert host.registered == [sid, sid]
    await b.release(sid, "arm")
    assert host.removed == []                                         # still leased by another consumer
    await b.release(sid, "dash")
    assert host.removed == [sid]
    refusing, _ = book(host=Host(ok=False))
    sid2 = refusing.define(spec)
    assert await refusing.demand(sid2, "arm", LeaseClass.ARMED) is False and refusing.live() == frozenset()


async def test_register_computes_the_canonical_value_before_warm_is_reported() -> None:
    fake = FakeBars()
    fake.put(walk(200, series=broker("SYN1")))
    eng = MdIndicators(fake, settings=IndicatorSettings(retry_ms=0))
    b, _ = book(host=eng)
    spec = REGISTRY.spec("ATR", 1, broker("SYN1"), {}, INC)
    sid = b.define(spec)
    assert await b.demand(sid, "arm", LeaseClass.ARMED)
    assert eng.is_warm(sid) and eng.latest(sid, "value") is not None


async def test_a_faulted_indicator_stays_faulted_for_a_second_consumer_and_after_every_lease_lapses() -> None:
    fake = FakeBars()
    fake.put(walk(30, series=broker("SYN1")))
    reg = IndicatorRegistry((ATR_V1, Raising()))
    calls: list[Any] = []

    class Write:
        async def run(self, batch: Any) -> Any:
            calls.extend(batch)
            return [ScriptOk(1) for _ in batch]

    eng = MdIndicators(fake, registry=reg, write=Write(), token=lambda: TOKEN, boot=lambda: SENTINEL,
                       settings=IndicatorSettings(retry_ms=0))
    b, _ = book(host=eng, registry=reg)
    sid = b.define(reg.spec("TEST_RAISE", 1, broker("SYN1"), {}, INC))
    Raising.fail = True
    try:
        assert await b.demand(sid, "arm", LeaseClass.ARMED)
        await eng.cycle()
        await eng.cycle()
        assert eng.status(sid).state is IndState.FAULTED                 # type: ignore[union-attr]
        written = len(calls)
        Raising.fail = False
        assert await b.demand(sid, "dash", LeaseClass.PREWARM)          # a second consumer: no job, no IND write
        assert len(calls) == written and eng.status(sid).state is IndState.FAULTED     # type: ignore[union-attr]
        await b.release(sid, "arm")
        await b.release(sid, "dash")
        assert eng.status(sid) is None
        assert await b.demand(sid, "arm", LeaseClass.ARMED)             # demanded again after the lapse
        await eng.cycle()
        st = eng.status(sid)
        assert st is not None and st.state is IndState.FAULTED and len(calls) == written
    finally:
        Raising.fail = True


async def test_lease_writes_registry_and_release(md_redis: Any, run_ids: RunIds) -> None:
    await md_redis.set(K.WRITER, TOKEN.text)
    runner = ScriptRunner(md_redis, run_id_reader=run_ids)
    b, clock = book()
    spec = REGISTRY.spec("ATR", 1, broker("SYN1"), {}, INC)
    i, s = b.define(spec), b.define(broker("SYN2"))
    assert b.admit(i, "arm", LeaseClass.ARMED).admitted and b.admit(s, "arm", LeaseClass.PREWARM).admitted
    assert await b.sync(runner, TOKEN) == 2
    assert (await md_redis.get(K.demand("arm", i))).decode() == "ARMED"
    assert 0 < await md_redis.pttl(K.demand("arm", s)) <= 60_000
    assert (await md_redis.hget(K.REGISTRY_IND, i)).decode() == spec.canonical
    assert (await md_redis.hget(K.REGISTRY_SERIES, s)).decode() == broker("SYN2").canonical
    live = await StoreReader(md_redis).live_leases()
    assert {(c, sid) for c, sid, _ in live} == {("arm", i), ("arm", s)}
    assert await b.sync(runner, TOKEN) == 0                         # nothing due before the renewal interval
    clock.now += 20_000
    assert b.admit(i, "arm", LeaseClass.ARMED).admitted
    assert await b.sync(runner, TOKEN) == 2                         # renewals
    await b.release(s, "arm")

    class Down:
        async def run(self, calls: Any) -> Any:
            raise StoreUnavailable("timeout", "TimeoutError")

    assert await b.sync(Down(), TOKEN) == 0                         # kept due
    assert await b.sync(runner, TOKEN) == 1
    assert await md_redis.exists(K.demand("arm", s)) == 0


async def test_port_demand_goes_to_the_book(md_redis: Any) -> None:
    b, _ = book()
    port = MdPort(StoreReader(md_redis), keyring=KEYRING, book=SeriesStateBook(), leader_epoch=lambda: 7,
                  demand_book=b)
    sid = port.define(broker("SYN1"))
    assert await port.demand(sid, "arm", LeaseClass.ARMED) is True and b.live() == frozenset({sid})
    await port.release(sid, "arm")
    assert b.live() == frozenset()
    bare = MdPort(StoreReader(md_redis), keyring=KEYRING, book=SeriesStateBook(), leader_epoch=lambda: 7)
    assert bare.define(broker("SYN1")) == sid and await bare.demand(sid, "arm", LeaseClass.ARMED) is False


def test_backfill_depth_is_each_indicators_declared_figure_never_atrs_length() -> None:
    """Round 12 / S38c (generic indicators): the engine reads the window and warm-up each indicator declares
    (`warm_counts`, `warm_span`, `backfill_bars`), never an ATR `length`: a test indicator with its own rule asks for
    its own backfill depth with no engine change, and a bare series lease uses the series-level default."""
    import ast
    import inspect

    from src.marketdata import demand
    from .indicators.helpers import Bands

    class DeepBands(Bands):
        KIND = "TEST_DEEP_BANDS"

        def warm_span(self, spec: IndicatorSpec) -> int:
            return 7

        def backfill_bars(self, spec: IndicatorSpec) -> int:
            return int(spec.param("n")) + 7 + 300

    reg = IndicatorRegistry([DeepBands()])
    b, _ = book(registry=reg)
    spec = reg.spec("TEST_DEEP_BANDS", 1, broker("SYN1"), {"n": "20"}, INC)
    i = b.define(spec)
    s2 = b.define(broker("SYN2"))
    for sid in (i, s2):
        assert b.admit(sid, "arm", LeaseClass.ARMED).admitted
    assert b.broker_demand() == {broker("SYN1"): 327, broker("SYN2"): DEFAULT_BACKFILL_BARS}
    tree = ast.parse(inspect.getsource(demand))
    imported = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    assert "src.marketdata.indicators.atr" not in imported
