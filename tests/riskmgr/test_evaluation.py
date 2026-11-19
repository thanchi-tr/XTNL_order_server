"""S36b (plan RT-3): the S21 join (`src/riskmgr/evaluation.py`): the BarClock's `EvaluationPort` over the pipeline.

Synthetic ids, levels and times only; no assertion message carries a value."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from src.marketdata.hashing import HashStatus, verify
from src.marketdata.ports import HandleRef
from src.marketdata.timing import COLD_SKEW, decision_cap_ms, pre_fire_ms
from src.marketdata.types import M15, BarSpec, MdGeneration
from src.riskmgr.barclock import EvalRequest, Evaluated, NotReady, Prepared, Verdict
from src.riskmgr.config import ArmSettings
from src.db.arm_ports import DecisionMemory
from src.riskmgr.envelope import stamp
from src.riskmgr.evaluation import (
    DECISIONS_UNREADABLE,
    FIRST_BAR_SCAN_BUCKETS,
    IMAGE_MISSING,
    PIN_UNREADABLE,
    POSITION_NOT_OPEN,
    STATE_UNREADABLE,
    PipelineEvaluator,
    decode_pre_basis,
    encode_pre_basis,
    evaluated,
    final_at_target,
    handle_role_map,
    pre_levels_of,
    proposed_level,
    risk_multiple,
)
from src.riskmgr.outcomes import BROKER_BAR_NOT_FINAL, Outcome, OutcomeCode
from src.riskmgr.pipeline import PipelineResult, ResultKind
from src.riskmgr.stopbasis import PreBasis
from src.riskmgr.types import ArmEvent
from tests.riskmgr.failed_fakes import FakeCalendar
from tests.riskmgr.test_arm_join import KEYRING, TF, Join, join_rig
from tests.riskmgr.tracker_fakes import ADAPTER, SYMBOL, FakeGrid, image, position

D = Decimal


# ---------------------------------------------------------------------------------------------------- pure helpers
def test_pre_basis_round_trip_and_unreadable_forms() -> None:
    basis = PreBasis((("POSITION_FIELD", D("1.19")), ("CHILD_ORDER:0", D("1.18"))), 1_000)
    text = encode_pre_basis(basis)
    assert text is not None and decode_pre_basis(text) == basis and encode_pre_basis(None) is None
    for bad in (None, "", "not json", "[]", '{"bar": true, "levels": []}', '{"bar": 1, "levels": [["X", "1"]]}',
                '{"bar": 1, "levels": [["POSITION_FIELD", "nan?"]]}', '{"bar": 1, "levels": []}'):
        assert decode_pre_basis(bad) is None


def test_roles_proposed_level_and_risk_multiple() -> None:
    f, c1, c2 = HandleRef("POSITION_FIELD", "P1"), HandleRef("CHILD_ORDER", "S1"), HandleRef("CHILD_ORDER", "S2")
    roles = handle_role_map("BUY", D("1.20"), [(c1, D("1.19")), (c2, D("1.18")), (f, D("1.17"))])
    assert roles == {f: "POSITION_FIELD", c2: "CHILD_ORDER:0", c1: "CHILD_ORDER:1"}
    short = handle_role_map("SELL", D("1.20"), [(c1, D("1.21")), (c2, D("1.22"))])
    assert short == {c2: "CHILD_ORDER:0", c1: "CHILD_ORDER:1"}
    assert proposed_level("BUY", [(c1, D("1.20")), (c2, D("1.201"))]) == D("1.20")
    assert proposed_level("SELL", [(c1, D("1.20")), (c2, D("1.201"))]) == D("1.201")
    assert proposed_level("BUY", [(c1, D("1.20")), (f, D("1.205"))]) == D("1.205") and proposed_level("BUY", []) is None
    assert risk_multiple("BUY", D("1.2"), D("1.19"), D("0.01")) == D("1.00")
    assert risk_multiple("SELL", D("1.2"), D("1.2"), D("0.01")) == D("0.00")
    assert risk_multiple("BUY", D("1.2"), None, D("0.01")) is None and risk_multiple("BUY", D("1.2"), D("1"), D(0)) is None


# ---------------------------------------------------------------------------------------------------- prepare
def request(j: Join, event: ArmEvent, bar_end: int, now: int) -> EvalRequest:
    fire = until = None
    if event is ArmEvent.PRE:
        fire = pre_fire_ms(bar_end, COLD_SKEW, ArmSettings().pre_offset_s)
        until = min(fire + 10_000, bar_end - 30_000)
    return EvalRequest(row=j.row(), spec=M15, event=event, bar_end_ms=bar_end, now_ms=now, skew=COLD_SKEW,
                       decision_cap_ms=decision_cap_ms(bar_end, M15, COLD_SKEW), generation=MdGeneration(1, 1),
                       fire_ms=fire, valid_until_ms=until)


async def evaluate(j: Join, req: EvalRequest) -> Evaluated | NotReady:
    got = await j.evaluator.prepare(req)
    if isinstance(got, NotReady):
        return got
    assert isinstance(got, Prepared) and got.spec.strategy_scope == "strategy:break_even_on_risk_close:1"
    assert got.spec.guard_scopes == ("guard:first_bar:1",) and got.spec.decimal_context is not None
    return got.job()


async def armed() -> tuple[Join, int]:
    j = join_rig()
    await j.tracker.step()
    cursor = j.row().last_decided_bar
    assert cursor is not None
    return j, cursor + TF


async def test_not_ready_for_an_unloadable_pin_a_bad_state_a_missing_image_or_a_closed_position() -> None:
    j, b1 = await armed()
    try:
        row = j.row()
        now = b1 + 2_000
        cases = [(replace(row, strategy_params_json="not json"), PIN_UNREADABLE),
                 (replace(row, strategy_impl_version=99), PIN_UNREADABLE),
                 (replace(row, strategy_state_json="[]"), STATE_UNREADABLE),
                 (replace(row, state="NOT_A_STATE"), STATE_UNREADABLE)]
        for bad, reason in cases:
            got = await j.evaluator.prepare(replace(request(j, ArmEvent.POST, b1, now), row=bad))
            assert isinstance(got, NotReady) and got.reason == reason, reason
        j.snapshot.images = [image(position("P1", status="CLOSED"))]
        j.evaluator._images = None                                      # noqa: SLF001 (a new pass)
        got = await j.evaluator.prepare(request(j, ArmEvent.POST, b1, now + 5_000))
        assert isinstance(got, NotReady) and got.reason == POSITION_NOT_OPEN
        j.snapshot.images = []
        got = await j.evaluator.prepare(request(j, ArmEvent.POST, b1, now + 10_000))
        assert isinstance(got, NotReady) and got.reason == IMAGE_MISSING
    finally:
        j.barclock.close()


async def test_a_post_without_the_final_broker_bar_is_pending_with_its_expiry_reason() -> None:
    j, b1 = await armed()
    try:
        got = await evaluate(j, request(j, ArmEvent.POST, b1, b1 + 2_000))
        assert isinstance(got, Evaluated) and got.verdict is Verdict.PENDING and got.reason == BROKER_BAR_NOT_FINAL
    finally:
        j.barclock.close()


async def test_a_post_row_carries_the_keyed_trigger_input_and_no_market_value() -> None:
    j, b1 = await armed()
    try:
        j.md.final_bar(b1, "1.2150")
        j.md.live_quote("1.2150", b1 + 1_000)
        got = await evaluate(j, request(j, ArmEvent.POST, b1, b1 + 2_000))
        assert isinstance(got, Evaluated) and got.decision is not None and got.final
        d = got.decision
        assert (d.outcome, d.trigger_met, d.close_source, d.stop_basis) == ("ADJUST", True, "BROKER", "STREAM")
        series, bar_end, digest = d.series_inputs[0].split("|")
        assert series == f"{ADAPTER}:{SYMBOL}:15m:BID:BROKER" and int(bar_end) == b1 and got.trigger_hash == digest
        bar = j.md.bars_by_series[series][0]
        assert verify(KEYRING, digest, series, b1, bar.open, bar.high, bar.low, bar.close) is HashStatus.VALID
        assert d.inputs_digest is not None and len(d.inputs_digest) == 34
        assert d.proposed_stop is not None and d.current_risk_r == D("1.00")
        assert [(h.handle_role, h.target_level is not None, h.level_at_bar_end is not None) for h in got.handles] == [
            ("POSITION_FIELD", True, True)]
        assert got.strategy_state_json is not None and got.sm11_events
        # the row holds codes, ids, order-state levels and R values only: no field carries the bar or the quote
        bar_texts = {str(bar.close), "1.215", "1.2150"}
        assert not ({str(getattr(d, f)) for f in ("proposed_stop", "current_risk_r", "first_bar_ratio")} & bar_texts)
    finally:
        j.barclock.close()


async def test_pre_then_a_bar_end_below_the_trigger_stores_pre_basis_until_confirmed() -> None:
    """C1 default (option (a), §9.2, still unconfirmed): after a PRE ADJUST, a bar end that does not confirm the
    trigger is `PRE_NOT_CONFIRMED` and the PRE levels are stored as `pre_basis`."""
    j, b1 = await armed()
    try:
        b2 = b1 + TF
        fire = pre_fire_ms(b2, COLD_SKEW, ArmSettings().pre_offset_s)
        j.md.final_bar(b1, "1.2050")
        j.md.live_quote("1.2150", fire - 1_000)
        await j.at(fire + 100)
        pre = [r for r in j.decisions("PRE") if r.bar_end_ms == b2]
        assert [r.outcome for r in pre] == ["ADJUST"] and pre[0].series_inputs == ()
        j.md.final_bar(b2, "1.2050")
        await j.at(b2 + 2_000)
        post = [r for r in j.decisions("POST") if r.bar_end_ms == b2]
        assert [(r.outcome, r.category) for r in post] == [("PRE_NOT_CONFIRMED", "INFO")]
        basis = decode_pre_basis(j.row().pre_basis_json)
        assert basis is not None and basis.bar_end_ms == b2 and j.row().pre_basis_bar == b2
        assert basis.s_pre == D("1.1900")
    finally:
        j.barclock.close()


async def test_a_new_term_between_pre_and_post_keeps_the_pre_basis() -> None:
    """S36b review (major): the PRE `ADJUST`'s levels are re-read from its committed row, so a leader change (a fresh
    evaluator and BarClock, no memory) between PRE and POST still decides `PRE_NOT_CONFIRMED` and stores the basis,
    never a bar-end test at the stop the user tightened."""
    j, b1 = await armed()
    try:
        b2 = b1 + TF
        fire = pre_fire_ms(b2, COLD_SKEW, ArmSettings().pre_offset_s)
        j.md.final_bar(b1, "1.2050")
        j.md.live_quote("1.2150", fire - 1_000)
        await j.at(fire + 100)
        assert [r.outcome for r in j.decisions("PRE") if r.bar_end_ms == b2] == ["ADJUST"]
        j.new_term()
        j.md.final_bar(b2, "1.2050")
        await j.at(b2 + 2_000)
        post = [r for r in j.decisions("POST") if r.bar_end_ms == b2]
        assert [(r.outcome, r.category) for r in post] == [("PRE_NOT_CONFIRMED", "INFO")]
        basis = decode_pre_basis(j.row().pre_basis_json)
        assert basis is not None and basis.bar_end_ms == b2 and basis.s_pre == D("1.1900")
    finally:
        j.barclock.close()


async def test_a_pre_adjust_that_never_committed_leaves_no_pre_basis() -> None:
    """S36b review: the evaluator's memory follows commits only. A PRE `ADJUST` whose transaction rolled back was
    never sent, so the bar end is tested at `S_at_T` alone (a strategy reason, no basis stored)."""
    j, b1 = await armed()
    try:
        b2 = b1 + TF
        fire = pre_fire_ms(b2, COLD_SKEW, ArmSettings().pre_offset_s)
        j.md.final_bar(b1, "1.2050")
        await j.at(b1 + 2_000)                                         # the POST of b1 commits first
        j.md.live_quote("1.2150", fire - 1_000)
        j.store.fail_commits = 1
        await j.at(fire + 100)
        assert [r for r in j.decisions("PRE") if r.bar_end_ms == b2] == []
        j.md.final_bar(b2, "1.2050")
        await j.at(b2 + 2_000)
        post = [r for r in j.decisions("POST") if r.bar_end_ms == b2]
        assert [(r.outcome, r.reason) for r in post] == [("STRATEGY_REASON", "BELOW_TRIGGER")]
        assert j.row().pre_basis_json is None
    finally:
        j.barclock.close()


async def test_an_unreadable_decision_memory_is_not_ready() -> None:
    """A bar end never decides on a guessed basis: unreadable committed decisions are `NotReady`."""
    j, b1 = await armed()
    try:
        async def boom(*_a: object) -> DecisionMemory:
            raise ConnectionError("down")

        j.store.decision_memory = boom                     # type: ignore[method-assign, assignment]
        got = await j.evaluator.prepare(request(j, ArmEvent.POST, b1, b1 + 2_000))
        assert isinstance(got, NotReady) and got.reason == DECISIONS_UNREADABLE
        assert j.evaluator.counters.decision_read_errors == 1
    finally:
        j.barclock.close()


def test_pre_levels_and_final_at_target_from_the_committed_rows() -> None:
    assert pre_levels_of(DecisionMemory(), 1_000) is None
    got = pre_levels_of(DecisionMemory(pre_levels=(("CHILD_ORDER:10", D("1.15")), ("CHILD_ORDER:2", D("1.17")),
                                                   ("POSITION_FIELD", D("1.18")))), 1_000)
    assert got is not None and [r for r, _ in got.levels] == ["POSITION_FIELD", "CHILD_ORDER:2", "CHILD_ORDER:10"]
    assert got.bar_end_ms == 1_000
    cases = [("AT_TARGET_UNCONFIRMED", None, None, True), ("COMPLETE", None, None, True),
             ("PRE_NOT_CONFIRMED", "AT_TARGET_UNCONFIRMED", None, True),
             ("PRE_NOT_CONFIRMED", "BELOW_TRIGGER", "break_even_on_risk_close", False),
             ("UNRESOLVED_RANGE", "broker_bar_not_final", None, False),    # a span ends an at-target run
             ("ADJUST", None, None, False), (None, None, None, False)]
    for outcome, reason, kind, at in cases:
        mem = DecisionMemory(last_outcome=outcome, last_reason=reason, last_reason_kind=kind)
        assert final_at_target(mem) is at, outcome


async def test_the_job_changes_no_memory_and_the_commit_hook_does() -> None:
    """S36b review: `evaluated` (the job) is pure; a shown `LOOSENED_AFTER_COMPLETE` carries a `committed` hook that
    the BarClock runs on the loop after the commit, the only way `loosened_told` is set."""
    j, b1 = await armed()
    try:
        req = request(j, ArmEvent.POST, b1, b1 + 2_000)
        j.md.final_bar(b1, "1.2150")
        cells = await j.evaluator.prepare(req)
        assert isinstance(cells, Prepared) and cells.job.__closure__ is not None
        reads = next(c.cell_contents for c in cells.job.__closure__ if type(c.cell_contents).__name__ == "_Reads")
        told: list[str] = []
        result = PipelineResult(event=ArmEvent.POST, kind=ResultKind.DECISION, envelope=stamp(ArmSettings().envelope),
                                state=reads.inp.state, code=OutcomeCode.engine(Outcome.LOOSENED_AFTER_COMPLETE),
                                final=True, shown=True, at_target=False)
        got = evaluated(req, reads, result, keyring=KEYRING, remember_told=told.append)
        assert got.committed is not None and told == []                # nothing remembered by the job itself
        got.committed()
        assert told == [j.row().ref]
        silent = evaluated(req, reads, replace(result, shown=False), keyring=KEYRING, remember_told=told.append)
        assert silent.committed is None
    finally:
        j.barclock.close()


def test_bar_index_is_bounded_and_finds_bar_zero() -> None:
    """S36b review: bar 0 is found in short windows from the fill; a position held for months costs the same few
    grid calls (never a walk from the fill to `T`)."""
    calls: list[tuple[int, int]] = []

    class CountingGrid(FakeGrid):
        def open_bar_ends(self, adapter_id: str, symbol: str, spec: BarSpec, from_ms: int,
                          to_ms: int) -> list[int]:
            calls.append((from_ms, to_ms))
            return list(super().open_bar_ends(adapter_id, symbol, spec, from_ms, to_ms))

    j = join_rig()
    try:
        ev = PipelineEvaluator(md=j.md, snapshot=j.snapshot, versions=j.versions,  # type: ignore[arg-type]
                               decisions=j.store, grid=CountingGrid(), calendar=FakeCalendar(),
                               settings=ArmSettings(), keyring=lambda: KEYRING)
        bucket = 1_700_000_000_000 - 1_700_000_000_000 % TF
        fill = bucket + 60_000
        bar0 = bucket + TF
        assert ev._bar_index(ADAPTER, SYMBOL, M15, fill, bar0) == 0                  # noqa: SLF001
        assert ev._bar_index(ADAPTER, SYMBOL, M15, fill, bar0 + TF) == 1             # noqa: SLF001
        assert ev._bar_index(ADAPTER, SYMBOL, M15, fill, bucket) is None             # noqa: SLF001
        calls.clear()
        assert ev._bar_index(ADAPTER, SYMBOL, M15, fill, bar0 + 200 * 96 * TF) == 1  # noqa: SLF001 (~200 days)
        assert len(calls) <= 2 and all(to - frm <= FIRST_BAR_SCAN_BUCKETS * TF for frm, to in calls)
    finally:
        j.barclock.close()


async def test_a_pre_quote_received_after_fire_is_never_used() -> None:
    """Round 13: the PRE reads the quote as it was at fire (`quote_at`); with only a quote received after fire there
    is no quote at fire, so the PRE is `pre_quote_unavailable` (never computed from the later quote)."""
    j, b1 = await armed()
    try:
        b2 = b1 + TF
        fire = pre_fire_ms(b2, COLD_SKEW, ArmSettings().pre_offset_s)
        j.md.live_quote("1.2150", fire + 50)
        got = await evaluate(j, request(j, ArmEvent.PRE, b2, fire + 100))
        assert isinstance(got, Evaluated) and got.decision is not None
        assert (got.decision.outcome, got.decision.reason) == ("DATA_UNAVAILABLE", "pre_quote_unavailable")
        assert not got.final and got.sm11_events == ()
    finally:
        j.barclock.close()
