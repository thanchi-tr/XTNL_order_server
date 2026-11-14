"""4.5 / 4.8 in a whole leader term: ruleset load + hot reload, clean-spread thresholds, live account sampler,
data lifecycle, FLUSHDB scoped to the engine's database, readiness back-off reset, SM10 price evidence.
No network: fakeredis + lupa, in-memory ports (MockBrokerAdapter)."""
from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src.db.account_sampler import LiveSample
from src.db.epoch import Epoch
from src.db.lifecycle import LifecycleReport
from src.dto import PriceDTO
from src.oversee.ruleset import RulesetRow
from src.pubsub.connection import ConnState
from src.runtime.pg_ports import PgLeaderPorts
from src.runtime.ports import LeaderPorts
from src.runtime.session import EngineSession, SessionContext, SessionTimings, _EngineRedisFlush
from tests.e2e.memory_backend import MemoryBackend
from tests.e2e.scenario import ADAPTER, NODE, FakeLease, become_leader, eventually, fixture, mock_adapter, wall_ms

TIMINGS = dataclasses.replace(SessionTimings(sm8_deadline_s=30.0, sm8_price_wait_ms=500, drain_close_s=2.0,
                                             housekeeping_s=1.0, monitor_poll_s=0.2, consumer_poll_s=0.2),
                              ruleset_poll_s=0.05, live_sample_flush_s=0.05, lifecycle_poll_s=0.05)


async def run_id(_: Any) -> str:
    return "run-h"


async def _is(v: bool) -> bool:
    return v


class FakeRulesets:
    def __init__(self) -> None:
        self.rows: dict[int, RulesetRow] = {}
        self.active_v: int | None = None
        self.audits: list[tuple[int | None, str]] = []

    async def active_version(self) -> int | None:
        return self.active_v

    async def active(self) -> RulesetRow | None:
        return None if self.active_v is None else self.rows.get(self.active_v)

    async def earlier(self, below_version: int, limit: int) -> Sequence[RulesetRow]:
        return [self.rows[v] for v in sorted(self.rows, reverse=True) if v < below_version][:limit]

    async def audit(self, version: int | None, outcome: Any, detail: str) -> None:
        self.audits.append((version, outcome))


class FakeSamples:
    def __init__(self) -> None:
        self.samples: list[LiveSample] = []

    async def write_samples(self, samples: Sequence[LiveSample]) -> None:
        self.samples.extend(samples)


class FakeLifecycle:
    def __init__(self) -> None:
        self.runs: list[int] = []

    async def run_once(self, now_ms: int) -> LifecycleReport:
        self.runs.append(now_ms)
        return LifecycleReport(ensured=["event_log_p202611"], awaiting_archive=["event_log_p202607"])


@pytest.fixture
async def server() -> AsyncIterator[fakeredis.FakeServer]:
    yield fakeredis.FakeServer()


async def test_leader_init_flushes_only_the_engine_database(server: fakeredis.FakeServer) -> None:
    engine = fakeredis.FakeAsyncRedis(server=server, db=0)
    other = fakeredis.FakeAsyncRedis(server=server, db=1)
    await engine.set("redis:boot_id", "old:1")
    await engine.set("price:mock:EURUSD", "x")
    await other.set("someone-else", "keep")
    await _EngineRedisFlush(engine).flushdb()
    assert await engine.dbsize() == 0 and await other.get("someone-else") == b"keep"
    await engine.aclose()
    await other.aclose()


async def test_term_with_housekeeping_ports(server: fakeredis.FakeServer, tmp_path: Path) -> None:
    redis = fakeredis.FakeAsyncRedis(server=server)
    epoch = Epoch("c", 4)
    backend = MemoryBackend(redis, run_id, epoch.token)
    rulesets, samples, lifecycle = FakeRulesets(), FakeSamples(), FakeLifecycle()
    rulesets.rows[3] = RulesetRow(3, {"marks": {"symbol_clean_spread_pips": {ADAPTER: {"EURUSD": "1.5"}}},
                                      "margin_usage": {"enabled": False}}, "ops", "tighten EURUSD")
    rulesets.active_v = 3

    def ports(e: Epoch) -> LeaderPorts:
        base = backend.ports(e)
        fields = {f.name: getattr(base, f.name) for f in dataclasses.fields(LeaderPorts)}
        return PgLeaderPorts(**fields, rulesets=rulesets, live_samples=samples, lifecycle=lifecycle)

    fx = fixture(wall_ms())
    adapter = mock_adapter(fx)
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=ports,
                         adapters=lambda: [adapter], lease=FakeLease(), run_id_reader=run_id, exit_fn=lambda r: None,
                         timings=TIMINGS)
    session = EngineSession(ctx, epoch)
    ticks: list[tuple[str, bool]] = []
    session.calendar.version = 1
    session.calendar.on_tick = (lambda a, s, b, k, ts, now, snapshot=False:  # type: ignore[method-assign]
                                ticks.append((s, snapshot)))
    leader = await become_leader(session, epoch)
    try:
        # 4.5: LEADER_INIT installed v3 before the first pass; the price path uses its thresholds
        assert session.rules.version == 3 and "margin_usage" not in session.sweep._registry.rule_ids
        assert session.rules.clean_spread(ADAPTER, "EURUSD") == Decimal("1.5")
        assert session.rules.clean_spread(ADAPTER, "AUDUSD") == Decimal("5")
        assert session.committer._clean_spread is not None
        assert session.committer._clean_spread(ADAPTER, "EURUSD") == Decimal("1.5")
        assert rulesets.audits[0] == (3, "installed")
        await eventually(lambda: _is(session.orchestrator.runtime(ADAPTER).conn.state is ConnState.STREAMING),
                         "STREAMING")
        # 4.8: live account fields sampled into account_snapshots (fenced store), lifecycle job ran once
        await eventually(lambda: _is(bool(samples.samples)), "live sample")
        assert samples.samples[0].live.account_id == fx.accounts[0].account_id and samples.samples[0].seq_id > 0
        await eventually(lambda: _is(bool(lifecycle.runs)), "lifecycle run")
        await eventually(lambda: _is(bool(ticks)), "price evidence")
        assert ticks[0][1] is True                     # a symbol's first tick is a subscription snapshot
        # hot reload: a new active version installs at the next sweep pass
        rulesets.rows[4] = RulesetRow(4, {}, "ops", "back to spec")
        rulesets.active_v = 4
        await eventually(lambda: _is(session.rules.version == 4), "ruleset v4 installed")
        assert "margin_usage" in session.sweep._registry.rule_ids
        assert session.rules.clean_spread(ADAPTER, "EURUSD") == Decimal("5")
        # readiness: the back-off reset hook is SM4's
        called: list[str] = []
        session.orchestrator.reset_backoff = lambda a: called.append(a) or False  # type: ignore[method-assign]
        await session.readiness._actions.reset_backoff_and_reconnect(ADAPTER)
        assert called == [ADAPTER]
        assert not leader.exits
    finally:
        await leader.shutdown()
        await redis.aclose()


async def test_session_without_housekeeping_ports_runs_the_base_config(server: fakeredis.FakeServer,
                                                                       tmp_path: Path) -> None:
    redis = fakeredis.FakeAsyncRedis(server=server)
    epoch = Epoch("c", 5)
    backend = MemoryBackend(redis, run_id, epoch.token)
    ctx = SessionContext(node_id=NODE, spill_path=tmp_path / "spill.jsonl", redis=redis, ports=backend.ports,
                         adapters=lambda: [mock_adapter(fixture(wall_ms()))], lease=FakeLease(),
                         run_id_reader=run_id, exit_fn=lambda r: None, timings=TIMINGS)
    session = EngineSession(ctx, epoch)
    assert session.ruleset_reloader is None and session.live_sampler is None and session.lifecycle is None
    assert await session.load_ruleset(0) == 1 and session.rules.current is ctx.oversee
    session._on_price_tick(PriceDTO(adapter_id=ADAPTER, symbol="EURUSD", bid=Decimal(1), ask=Decimal(1),
                                    spread_pips=None, timestamp_ms=1), True)   # no calendar yet: ignored
    await session.aclose()
    await redis.aclose()
