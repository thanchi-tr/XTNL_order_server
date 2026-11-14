"""Rule configuration from the `rulesets` table (IMPLEMENTATION_PLAN 4.5; oversee-rules.md §6-§7)."""
from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import replace
from decimal import Decimal
from typing import Any

import pytest

from src.oversee.config import BandRuleConfig, OverseeConfig
from src.oversee.halt import HaltSwitch
from src.oversee.memory import InMemoryOverseeDb
from src.oversee.ruleset import (
    ActiveRuleset,
    LiveOverseeConfig,
    RulesetError,
    RulesetReloader,
    RulesetRow,
    parse_ruleset,
    ruleset_json,
)
from src.oversee.snapshot import AccountSnapshot
from src.oversee.sweep import HlcSweepIds, RiskSweep
from tests.oversee.helpers import AD, T0, live, position, quote, snap

D = Decimal
POS_KEY = ("max_position_risk", "position", f"{AD}:P")


class Clock:
    def __init__(self, t: int = T0) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t


# ============================================================================================ parsing
def test_empty_ruleset_is_the_spec_defaults() -> None:
    cfg = parse_ruleset({}, 4)
    assert cfg == replace(OverseeConfig(), ruleset_version=4)
    assert cfg.max_aggregate_open_risk.flag_above_pct == D("2.0")      # per-section defaults kept
    assert cfg.marks.clean_spread_pips("stonex", "EURUSD") == D("5")


def test_overrides_and_per_symbol_clean_spread() -> None:
    raw = json.dumps({
        "max_position_risk": {"flag_above_pct": "0.8", "halt_above_pct": 1.2},
        "margin_usage": {"enabled": False},
        "sweep": {"release_ms": 4000, "balance_skew_ms": {"ctrader": 3000}},
        "marks": {"max_clean_spread_pips": "4", "symbol_clean_spread_pips": {
            "stonex": {"EURUSD": "1.5", "*": "3"}, "ctrader": {"XAUUSD": 40}}},
    })
    cfg = parse_ruleset(raw, 7)       # JSON text; 1.2 is parsed exactly (Decimal), never as a float
    assert cfg.max_position_risk == BandRuleConfig(flag_above_pct=D("0.8"), halt_above_pct=D("1.2"))
    assert not cfg.margin_usage.enabled and cfg.sweep.release_ms == 4000
    assert cfg.sweep.skew_ms("ctrader") == 3000 and cfg.sweep.skew_ms("stonex") == 5000
    m = cfg.marks
    assert m.clean_spread_pips("stonex", "EURUSD") == D("1.5")   # symbol
    assert m.clean_spread_pips("stonex", "GBPUSD") == D("3")     # adapter wildcard
    assert m.clean_spread_pips("ctrader", "XAUUSD") == D("40")
    assert m.clean_spread_pips("ctrader", "EURUSD") == D("4")    # default
    assert parse_ruleset(ruleset_json(cfg), 7) == cfg            # what the CLI writes reads back


def test_every_problem_is_listed() -> None:
    with pytest.raises(RulesetError) as err:
        parse_ruleset({
            "bogus": {}, "sweep": {"interval_ms": 50, "release_ms": True, "nope": 1},
            "max_position_risk": {"flag_above_pct": "2", "halt_above_pct": "1.5"},
            "max_daily_loss": {"limit_pct_of_sod_equity": "abc"},
            "missing_stop_loss": {"grace_ms": 2.5},
            "marks": {"symbol_clean_spread_pips": {"stonex": {"EURUSD": "-1"}}},
        }, 9)
    text = str(err.value)
    for needle in ("bogus: unknown section", "sweep.nope: unknown key", "sweep.release_ms: expected an integer",
                   "sweep.interval_ms: 50 below 100", "flag_above_pct 2 > halt_above_pct 1.5",
                   "limit_pct_of_sod_equity: not a number", "grace_ms: expected an integer",
                   "symbol_clean_spread_pips.stonex.EURUSD: must be > 0"):
        assert needle in text, needle
    assert err.value.version == 9 and len(err.value.problems) >= 8


@pytest.mark.parametrize("raw", ["not json", "[1, 2]", {"max_daily_loss": {"limit_pct_of_sod_equity": 0}},
                                 {"max_daily_loss": {"confirm_ms": 120_000}}, {"marks": {"stale_mark_ms": 0}},
                                 {"sweep": {"release_margin": "1"}}, {"margin_usage": []}])
def test_invalid_rulesets_are_rejected(raw: object) -> None:
    with pytest.raises(RulesetError):
        parse_ruleset(raw, 1)


def test_live_config_reads_the_installed_ruleset() -> None:
    live_cfg = LiveOverseeConfig(OverseeConfig())
    assert isinstance(live_cfg, OverseeConfig) and live_cfg.ruleset_version == 1
    live_cfg.install(parse_ruleset({"sweep": {"cool_down_ms": 1234}}, 2))
    assert live_cfg.sweep.cool_down_ms == 1234 and live_cfg.ruleset_version == 2
    assert live_cfg.installed.ruleset_version == 2


# ============================================================================================ boundary
def test_staged_ruleset_installs_only_at_the_sweep_boundary() -> None:
    clock = Clock()
    active = ActiveRuleset(OverseeConfig(), clock_ms=clock, stale_install_ms=60_000)
    ids = active.wrap_sweep_ids(HlcSweepIds(clock))
    v2 = parse_ruleset({"max_position_risk": {"enabled": False}}, 2)
    active.stage(v2)
    assert active.version == 1 and "max_position_risk" in active.registry.rule_ids
    ids()                                                   # a pass starts
    assert active.version == 2 and "max_position_risk" not in active.registry.rule_ids
    assert active.config.max_position_risk.enabled is False and active.pending is None


def test_a_failing_install_changes_nothing_and_is_retried_at_the_next_boundary(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """4.9 skeptic: config and registry are installed together or not at all (the sweep keeps running on the
    installed ruleset after `ruleset_install_failed`), and the staged ruleset is kept for the next boundary."""
    from src.oversee.rules import RuleRegistry

    clock = Clock()
    active = ActiveRuleset(OverseeConfig(), clock_ms=clock)
    v2 = parse_ruleset({"max_position_risk": {"enabled": False}, "sweep": {"cool_down_ms": 1234}}, 2)
    active.stage(v2)
    real = RuleRegistry.from_config

    def broken(cfg: OverseeConfig) -> RuleRegistry:
        raise RuntimeError("rule build failed")

    monkeypatch.setattr(RuleRegistry, "from_config", staticmethod(broken))
    with pytest.raises(RuntimeError):
        active.at_boundary()
    assert active.version == 1 and active.config.sweep.cool_down_ms == OverseeConfig().sweep.cool_down_ms
    assert "max_position_risk" in active.registry.rule_ids and active.pending == v2
    monkeypatch.setattr(RuleRegistry, "from_config", real)
    assert active.at_boundary() and active.version == 2 and "max_position_risk" not in active.registry.rule_ids
    assert active.pending is None


def test_staged_ruleset_installs_directly_when_the_sweep_is_not_running() -> None:
    clock = Clock()
    active = ActiveRuleset(OverseeConfig(), clock_ms=clock, stale_install_ms=60_000)
    active.stage(parse_ruleset({}, 2))
    clock.t += 59_000
    assert not active.install_if_sweep_idle() and active.version == 1
    clock.t += 1_000
    assert active.install_if_sweep_idle() and active.version == 2


class _StagingReader:
    """Stages a new ruleset in the middle of a pass (between the account listing and the read)."""

    def __init__(self, db: InMemoryOverseeDb, active: ActiveRuleset) -> None:
        self.db = db
        self.active = active
        self.stage: OverseeConfig | None = None

    async def list_accounts(self) -> Sequence[str]:
        return await self.db.list_accounts()

    async def read_account(self, account_id: str, now_ms: int) -> AccountSnapshot:
        if self.stage is not None:
            self.active.stage(self.stage)
            self.stage = None
        return await self.db.read_account(account_id, now_ms)


async def test_reload_never_splits_a_pass_and_resolves_removed_rules() -> None:
    clock = Clock()
    db = InMemoryOverseeDb()
    active = ActiveRuleset(OverseeConfig(), clock_ms=clock)
    reader = _StagingReader(db, active)
    sweep = RiskSweep(reader=reader, writer=db, registry=active.registry, halt=HaltSwitch(db, db),
                      config=active.config, clock=clock, epoch=lambda: db.engine_epoch,
                      sweep_ids=active.wrap_sweep_ids(HlcSweepIds(clock)))
    db.set_snapshot(snap(now=T0, positions=(position("P", stop="1.0840"),), account_live=live("1000"),
                         quotes={(AD, "EURUSD"): quote("1.1000", T0)}))           # 1.6 % > 1.5 %: SOFT
    await sweep.run_pass()
    assert db.t.breaches[POS_KEY].active
    reader.stage = parse_ruleset({"max_position_risk": {"enabled": False}}, 2)
    clock.t += 1000
    db.set_snapshot(snap(now=clock.t, positions=(position("P", stop="1.0840"),), account_live=live("1000"),
                         quotes={(AD, "EURUSD"): quote("1.1000", clock.t)}))
    await sweep.run_pass()            # staged mid-pass: this pass still runs v1 (state unchanged)
    assert active.version == 1 and db.t.breaches[POS_KEY].resolved_at is None
    clock.t += 1000
    db.set_snapshot(snap(now=clock.t, positions=(position("P", stop="1.0840"),), account_live=live("1000"),
                         quotes={(AD, "EURUSD"): quote("1.1000", clock.t)}))
    await sweep.run_pass()            # next boundary installs v2: the removed rule's state resolves
    assert active.version == 2
    state = db.t.breaches[POS_KEY]
    assert state.resolved_at == clock.t and state.resolved_by == "reload"


# ============================================================================================ reloader
class FakeSource:
    def __init__(self, rows: Sequence[RulesetRow] = (), active: int | None = None) -> None:
        self.rows = {r.version: r for r in rows}
        self.active_v = active
        self.audits: list[tuple[int | None, str, str]] = []
        self.error: Exception | None = None

    async def active_version(self) -> int | None:
        if self.error is not None:
            raise self.error
        return self.active_v

    async def active(self) -> RulesetRow | None:
        return None if self.active_v is None else self.rows.get(self.active_v)

    async def earlier(self, below_version: int, limit: int) -> Sequence[RulesetRow]:
        return [self.rows[v] for v in sorted(self.rows, reverse=True) if v < below_version][:limit]

    async def audit(self, version: int | None, outcome: Any, detail: str) -> None:
        self.audits.append((version, outcome, detail))


def row(v: int, config: object) -> RulesetRow:
    return RulesetRow(v, config, "ops", f"change {v}", 1)


def reloader(src: FakeSource, base: OverseeConfig = OverseeConfig()) -> tuple[RulesetReloader, ActiveRuleset,
                                                                                list[tuple[str, str, str]],
                                                                                list[str]]:
    pages: list[tuple[str, str, str]] = []
    woken: list[str] = []
    active = ActiveRuleset(base, clock_ms=Clock())
    rl = RulesetReloader(src, active, pager=lambda *p: pages.append(p), accounts=lambda: ["B", "A", "A"],
                         sweep_now=woken.append, poll_s=0.01)
    return rl, active, pages, woken


async def test_leader_init_installs_the_active_row_and_audits() -> None:
    src = FakeSource([row(3, {"max_daily_loss": {"limit_pct_of_sod_equity": "2.5"}})], active=3)
    rl, active, pages, _ = reloader(src)
    res = await rl.load_initial()
    assert res.outcome == "installed" and active.version == 3
    assert active.config.max_daily_loss.limit_pct_of_sod_equity == D("2.5")
    assert src.audits == [(3, "installed", "by ops: change 3")] and pages == []


async def test_leader_init_falls_back_to_the_newest_valid_earlier_row() -> None:
    src = FakeSource([row(1, {}), row(2, {"margin_usage": {"enabled": False}}), row(3, {"bad": 1})], active=3)
    rl, active, pages, _ = reloader(src)
    res = await rl.load_initial()
    assert res.outcome == "fallback" and active.version == 2 and not active.config.margin_usage.enabled
    assert [a[:2] for a in src.audits] == [(3, "rejected"), (2, "fallback")]
    assert pages[0][0] == "critical" and "v3 rejected" in pages[0][1]


async def test_leader_init_without_a_valid_row_runs_the_base_config() -> None:
    base = replace(OverseeConfig(), ruleset_version=0)
    rl, active, pages, _ = reloader(FakeSource([row(5, "{")], active=5), base)
    assert (await rl.load_initial()).outcome == "fallback" and active.current is base
    assert pages and pages[0][0] == "critical"
    rl2, active2, pages2, _ = reloader(FakeSource(), base)
    assert (await rl2.load_initial()).outcome == "base" and active2.current is base and pages2 == []


async def test_poll_stages_a_new_version_and_wakes_every_account() -> None:
    src = FakeSource([row(1, {})], active=1)
    rl, active, pages, woken = reloader(src)
    await rl.load_initial()
    assert (await rl.poll_once()).outcome is None                 # unchanged: nothing happens
    src.rows[2] = row(2, {"max_aggregate_open_risk": {"halt_above_pct": "2.5"}})
    src.active_v = 2
    res = await rl.poll_once()
    assert res.outcome == "installed" and active.pending is not None and active.version == 1
    assert woken == ["A", "B"]                                    # sweep_now() for every known account
    assert src.audits[-1][:2] == (2, "installed") and pages[-1][0] == "info"
    active.at_boundary()
    assert active.version == 2 and active.config.max_aggregate_open_risk.halt_above_pct == D("2.5")


async def test_invalid_new_version_keeps_the_last_good_one_and_pages_once() -> None:
    src = FakeSource([row(1, {"margin_usage": {"enabled": False}})], active=1)
    rl, active, pages, woken = reloader(src)
    await rl.load_initial()
    src.rows[2] = row(2, {"max_position_risk": {"flag_above_pct": "3", "halt_above_pct": "2"}})
    src.active_v = 2
    assert (await rl.poll_once()).outcome == "rejected"
    assert (await rl.poll_once()).outcome is None                 # same bad version: no second page
    assert active.version == 1 and active.pending is None and not active.config.margin_usage.enabled
    assert [p[0] for p in pages] == ["high"] and "v2 rejected" in pages[0][1] and woken == []
    assert src.audits[-1][:2] == (2, "rejected")


async def test_deactivating_every_row_returns_to_the_base_config() -> None:
    src = FakeSource([row(1, {"margin_usage": {"enabled": False}})], active=1)
    rl, active, _, _ = reloader(src)
    await rl.load_initial()
    src.active_v = None
    assert (await rl.poll_once()).outcome == "base"
    active.at_boundary()
    assert active.current == OverseeConfig() and active.config.margin_usage.enabled


async def test_sweep_interval_is_pinned_to_the_term() -> None:
    src = FakeSource([row(1, {"sweep": {"interval_ms": 2000}})], active=1)
    rl, active, pages, _ = reloader(src)
    await rl.load_initial()
    assert active.version == 1 and active.config.sweep.interval_ms == 1000
    assert pages and pages[0][0] == "info" and "interval_ms ignored" in pages[0][1]


async def test_run_loop_survives_read_errors() -> None:
    src = FakeSource([row(1, {})], active=1)
    rl, active, _, _ = reloader(src)
    await rl.load_initial()
    src.error = ConnectionError("postgres down")
    stop = asyncio.Event()
    stamps: list[int] = []
    task = asyncio.create_task(rl.run(stop, stamp=lambda: stamps.append(1)))
    await asyncio.sleep(0.05)
    src.error = None
    src.rows[2] = row(2, {})
    src.active_v = 2
    for _ in range(100):
        if active.pending is not None:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 1)
    assert active.pending is not None and active.pending.ruleset_version == 2 and len(stamps) >= 2
