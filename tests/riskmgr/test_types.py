"""ARM core values (plan S19): σ, typed metrics, contexts that hide levels, keyed input ids and the HMAC
`inputs_digest`, outcome categories, the Evaluation / Proposal / GuardResult invariants, strategy state, the envelope
stamp and the non-database ports. Synthetic values only."""
from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal, localcontext

import pytest

from src.marketdata.hashing import Keyring, content_hash, key_id_of
from src.marketdata.ports import HandleRef
from src.marketdata.types import M15, Side, Source
from src.riskmgr.envelope import ENVELOPE_VERSION, EnvelopeParams, EnvelopeStamp, envelope_changed, stamp
from src.riskmgr.guard import GuardResult, GuardStatus, guard_scope_id, strategy_scope_id
from src.riskmgr.outcomes import (
    ARMED_LATE,
    BROKER_BAR_NOT_FINAL,
    STOP_STALE,
    Category,
    Outcome,
    OutcomeCode,
    unresolved_category,
)
from src.riskmgr.ports import ENGINE_PG_DOWN_AFTER_MS, Pager, SystemClock, engine_pg_down
from src.riskmgr.strategy import (
    Evaluation,
    HistoryNeed,
    IndicatorNeed,
    ParamError,
    Proposal,
    StrategyState,
    read_decimal,
)
from src.riskmgr.types import (
    ArmEvent,
    BarEvent,
    IndicatorInput,
    Ratio,
    RMultiple,
    SeriesInput,
    inputs_digest,
    sigma,
    window_hash,
)
from tests.riskmgr.toys import BID_SERIES, ToyStrategy, arm_ctx, ctx

RING = Keyring.of(3, bytes(range(32)))
OTHER = Keyring.of(4, bytes(range(32, 64)), ((3, bytes(range(32))),))
D = Decimal


def h(close: str, ring: Keyring = RING) -> str:
    c = D(close)
    return content_hash(ring, BID_SERIES.canonical, 900_000, c, c, c, c)


# ---------------------------------------------------------------------------------------------------- σ, metrics
def test_sigma() -> None:
    assert sigma("BUY") == 1 and sigma("SELL") == -1
    with pytest.raises(ValueError):
        sigma("FLAT")  # type: ignore[arg-type]
    assert ctx("1.1").r == D("0.00100") and ctx("1.1", side="SELL").r == D("0.00100")


def test_metrics_are_quantised_under_a_fixed_context() -> None:
    assert RMultiple.of(D("1.235")).value == D("1.24") and RMultiple.of(D("1.245")).value == D("1.24")
    assert Ratio.of(D("0.12345")).value == D("0.123")
    with localcontext() as c:
        c.prec = 2
        assert RMultiple.of(D("123.456")).value == D("123.46")
    with pytest.raises(ValueError):
        RMultiple(D("1.234"))
    with pytest.raises(ValueError):
        Ratio(D("NaN"))


@pytest.mark.parametrize("value", ["1.0", "1E+1", "1.000", "-0.00", "1"])
def test_r_multiples_are_canonical(value: str) -> None:
    """Equal metrics have one text: exactly two decimals, never a negative zero."""
    with pytest.raises(ValueError):
        RMultiple(D(value))
    assert str(RMultiple.of(D(value)).value) in ("1.00", "10.00", "0.00")


def test_metrics_never_keep_a_negative_zero() -> None:
    assert str(RMultiple.of(D("-0.004")).value) == "0.00" and not RMultiple.of(D("-0.004")).value.is_signed()
    assert str(Ratio.of(D("-0.0004")).value) == "0.000"
    assert RMultiple.of(D("-0.006")).value == D("-0.01")
    assert str(Ratio.of(D("1E+1")).value) == "10.000"
    for bad in ("0.10", "-0.000", "1E+1"):
        with pytest.raises(ValueError):
            Ratio(D(bad))
    RMultiple(D("1.00"))
    Ratio(D("0.100"))


def test_read_decimal_refuses_non_canonical_text() -> None:
    assert read_decimal({"x": "0.10"}, "x", D(0), D(1), D(0), "o") == D("0.10")
    assert read_decimal({"x": ".5"}, "x", D(0), D(1), D(0), "o") == D("0.5")
    for junk in (" 0.1 ", "0.1_0", "1e-1", "NaN", "٣", "0.1\n", "", "+"):
        with pytest.raises(ParamError) as e:
            read_decimal({"x": junk}, "x", D(0), D(1), D(0), "o")
        assert junk.strip() == "" or junk not in str(e.value)


def test_bar_event_accepts_pre_and_post_only() -> None:
    BarEvent(M15, ArmEvent.PRE)
    with pytest.raises(ValueError):
        BarEvent(M15, ArmEvent.LATE)


def test_contexts_hide_levels_and_prices() -> None:
    c = ctx("1.23457", stop="1.09876")
    text = repr(c) + repr(arm_ctx()) + repr(c.handles)
    for planted in ("1.23457", "1.09876", "1.10000", "1.099", "0.00001"):
        assert planted not in text
    assert "P1" in text and "POST" in text
    with pytest.raises(FrozenInstanceError):
        c.entry = D(1)  # type: ignore[misc]
    assert c.history_of(BID_SERIES) == () and c.indicator_values("x", "atr") == ()
    assert arm_ctx().fill_bucket_ms is not None and arm_ctx().sigma == 1


def test_requirement_templates_resolve_to_broker_series() -> None:
    spec = IndicatorNeed("ATR", 1, M15, Side.BID, (("length", "14"),)).resolve("mock", "SYN.A", D("0.00001"))
    assert spec.series.source is Source.BROKER and spec.series.side is Side.BID
    assert HistoryNeed(M15, Side.ASK, 3).resolve("mock", "SYN.A").source is Source.BROKER
    with pytest.raises(ValueError):
        HistoryNeed(M15, Side.ASK, 0)


# ---------------------------------------------------------------------------------------------------- keyed inputs
def test_input_ids_need_keyed_hashes() -> None:
    SeriesInput(BID_SERIES.canonical, 900_000, h("1.1"))
    with pytest.raises(ValueError):
        SeriesInput(BID_SERIES.canonical, 900_000, "deadbeef")
    with pytest.raises(ValueError):
        SeriesInput("not-a-series", 900_000, h("1.1"))
    with pytest.raises(ValueError):
        IndicatorInput("abc", "atr", 900_000, 0, "x" * 34)


def test_inputs_digest() -> None:
    s1 = SeriesInput(BID_SERIES.canonical, 900_000, h("1.1"))
    s2 = SeriesInput(BID_SERIES.canonical, 1_800_000, h("1.2"))
    wh = window_hash(RING, "0123456789abcdef", "atr", 1_800_000, 0, (h("1.1"), h("1.2")))
    ind = IndicatorInput("0123456789abcdef", "atr", 1_800_000, 0, wh)
    d = inputs_digest(RING, (s1, s2), (ind,))
    assert key_id_of(d) == 3 and key_id_of(wh) == 3
    assert d == inputs_digest(RING, (s2, s1), (ind,))       # order of reading never matters
    assert d != inputs_digest(RING, (s1,), (ind,))
    assert d != inputs_digest(RING, (s1, SeriesInput(BID_SERIES.canonical, 1_800_000, h("1.3"))), (ind,))
    rotated = inputs_digest(OTHER, (s1, s2), (ind,))
    assert key_id_of(rotated) == 4 and rotated[2:] != d[2:]
    assert "1.1" not in d and "1.2" not in wh
    with pytest.raises(ValueError):
        window_hash(RING, "0123456789abcdef", "atr", 1, 0, ("plain",))


def test_window_hash_refuses_the_separator() -> None:
    """('a|b', 'c') and ('a', 'b|c') would otherwise hash the same message."""
    for indicator_id, output in (("a|b", "c"), ("a", "b|c")):
        with pytest.raises(ValueError):
            window_hash(RING, indicator_id, output, 1, 0, (h("1.1"),))


# ---------------------------------------------------------------------------------------------------- outcomes
def test_engine_categories() -> None:
    assert OutcomeCode.engine(Outcome.ADJUST).actionable
    assert OutcomeCode.engine(Outcome.DATA_UNAVAILABLE, STOP_STALE).category is Category.FAULT
    assert OutcomeCode.engine(Outcome.AT_TARGET_UNCONFIRMED).category is Category.HOLD
    with pytest.raises(ValueError):
        OutcomeCode.engine(Outcome.STRATEGY_REASON)
    with pytest.raises(ValueError):
        OutcomeCode(Outcome.ADJUST, Category.INFO)


def test_kind_reasons_take_their_owner_category() -> None:
    code = OutcomeCode.kind_reason(Outcome.STRATEGY_REASON, "toy_break_even", "BELOW_TRIGGER", ToyStrategy.REASONS)
    assert code.category is Category.INFO and code.owner == "toy_break_even"
    held = OutcomeCode.kind_reason(Outcome.GUARD_REASON, "toy_calm", "TOY_VOLATILE", {"TOY_VOLATILE": Category.HOLD})
    assert held.category is Category.HOLD and not held.actionable
    with pytest.raises(ValueError):
        OutcomeCode.kind_reason(Outcome.STRATEGY_REASON, "toy_break_even", "NOPE", ToyStrategy.REASONS)
    with pytest.raises(ValueError):
        OutcomeCode(Outcome.STRATEGY_REASON, Category.ACTIONABLE, "BELOW_TRIGGER", "toy_break_even")


def test_pre_not_confirmed_carries_its_reason() -> None:
    """§B6.3 (7a): a guard HOLD, a strategy reason, or AT_TARGET_UNCONFIRMED; always INFO."""
    guard = OutcomeCode.pre_not_confirmed("first_bar", "FIRST_BAR_VOLATILE")
    strategy = OutcomeCode.pre_not_confirmed("break_even_on_risk_close", "BELOW_TRIGGER")
    at_target = OutcomeCode.pre_not_confirmed("", "AT_TARGET_UNCONFIRMED")
    for code in (guard, strategy, at_target):
        assert code.outcome is Outcome.PRE_NOT_CONFIRMED and code.category is Category.INFO and not code.actionable
    assert guard == OutcomeCode(Outcome.PRE_NOT_CONFIRMED, Category.INFO, "FIRST_BAR_VOLATILE", "first_bar")
    assert at_target == OutcomeCode.engine(Outcome.PRE_NOT_CONFIRMED, "AT_TARGET_UNCONFIRMED")
    assert OutcomeCode.pre_not_confirmed("first_bar", "FIRST_BAR_UNKNOWN", detail="timeout").detail == "timeout"
    for bad in (lambda: OutcomeCode.pre_not_confirmed("", "FIRST_BAR_VOLATILE"),      # a KIND reason needs its owner
                lambda: OutcomeCode.pre_not_confirmed("first_bar", "AT_TARGET_UNCONFIRMED"),   # engine name, owned
                lambda: OutcomeCode.pre_not_confirmed("first_bar", "stop_stale"),
                lambda: OutcomeCode.pre_not_confirmed("First-Bar", "FIRST_BAR_VOLATILE"),
                lambda: OutcomeCode.engine(Outcome.PRE_NOT_CONFIRMED),
                lambda: OutcomeCode.engine(Outcome.PRE_NOT_CONFIRMED, "FIRST_BAR_VOLATILE"),
                lambda: OutcomeCode(Outcome.PRE_NOT_CONFIRMED, Category.HOLD, "FIRST_BAR_VOLATILE", "first_bar")):
        with pytest.raises(ValueError):
            bad()


def test_kind_reason_detail() -> None:
    """`FIRST_BAR_UNKNOWN(atr_degenerate | timeout | stale)` (§B6.5, §B6.6): a code-only detail."""
    reasons = {"FIRST_BAR_UNKNOWN": Category.HOLD}
    for detail in ("atr_degenerate", "timeout", "stale"):
        code = OutcomeCode.kind_reason(Outcome.GUARD_REASON, "first_bar", "FIRST_BAR_UNKNOWN", reasons, detail=detail)
        assert code.detail == detail
    for bad in (lambda: OutcomeCode.kind_reason(Outcome.GUARD_REASON, "first_bar", "FIRST_BAR_UNKNOWN", reasons,
                                                detail="1.2345"),
                lambda: OutcomeCode(Outcome.DATA_UNAVAILABLE, Category.FAULT, STOP_STALE, detail="timeout"),
                lambda: OutcomeCode(Outcome.UNRESOLVED_RANGE, Category.INFO, ARMED_LATE, detail="timeout")):
        with pytest.raises(ValueError):
            bad()


def test_unresolved_range_categories() -> None:
    for reason in ("armed_late", "downtime", "paused", "failed"):
        assert unresolved_category(reason) is Category.INFO
    assert OutcomeCode.unresolved(ARMED_LATE).category is Category.INFO
    assert OutcomeCode.unresolved(BROKER_BAR_NOT_FINAL).category is Category.FAULT
    with pytest.raises(ValueError):
        OutcomeCode(Outcome.UNRESOLVED_RANGE, Category.INFO, BROKER_BAR_NOT_FINAL)


# ---------------------------------------------------------------------------------------------------- evaluation
def test_evaluation_and_proposal_invariants() -> None:
    ref = HandleRef("CHILD_ORDER", "O1")
    Evaluation(True, Proposal(((ref, D("1.1")),)))
    Evaluation(True, Proposal())                       # trigger met, every handle already at the target
    Evaluation(False, reason="BELOW_TRIGGER", metrics=(("risk_r", RMultiple.of(D(1))),))
    with pytest.raises(ValueError):
        Evaluation(False)
    with pytest.raises(ValueError):
        Evaluation(True, Proposal(), reason="BELOW_TRIGGER")
    with pytest.raises(ValueError):
        Evaluation(False, reason="X", metrics=(("a", D(1)),))  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Proposal(((ref, D(1)), (ref, D(2))))
    assert "1.1" not in repr(Proposal(((ref, D("1.1")),)))
    assert Proposal(((ref, D("1.1")),)).target(ref) == D("1.1")


def test_guard_result_invariants() -> None:
    assert GuardResult.passed().status is GuardStatus.PASS
    assert GuardResult.pending(5).until_ms == 5
    assert GuardResult.unknown("FIRST_BAR_UNKNOWN", "atr_degenerate").detail == "atr_degenerate"
    assert GuardResult.hold("FIRST_BAR_VOLATILE").detail == ""
    for bad in (lambda: GuardResult(GuardStatus.HOLD), lambda: GuardResult(GuardStatus.PASS, "X"),
                lambda: GuardResult(GuardStatus.PENDING), lambda: GuardResult.hold("lower_case"),
                lambda: GuardResult.unknown("FIRST_BAR_UNKNOWN", "Not A Code"),
                lambda: GuardResult(GuardStatus.PASS, detail="timeout"),
                lambda: GuardResult(GuardStatus.PENDING, until_ms=5, detail="timeout")):
        with pytest.raises(ValueError):
            bad()
    assert guard_scope_id("first_bar", 1) == "guard:first_bar:1"
    assert strategy_scope_id("toy_break_even", 2) == "strategy:toy_break_even:2"


def test_strategy_state_round_trip() -> None:
    s = StrategyState.of(triggered=True, n=3, level=D("1.10"), skip=None)
    assert s.flag("triggered") and s.text("n") == "3" and s.text("level") == "1.1" and s.text("skip") is None
    assert StrategyState.decode(s.encode()) == s and s.names() == {"triggered", "n", "level"}
    for junk in ("", "[]", '{"a": 1}', "{"):
        assert StrategyState.decode(junk) is None
    assert "1.1" not in repr(s)


# ---------------------------------------------------------------------------------------------------- envelope, ports
def test_envelope_stamp() -> None:
    a = stamp(EnvelopeParams())
    assert a == EnvelopeStamp(ENVELOPE_VERSION, EnvelopeParams(D("0.00")).params_hash)
    assert not envelope_changed(a, stamp(EnvelopeParams(D("0"))))
    assert envelope_changed(a, stamp(EnvelopeParams(D("0.1"))))
    assert envelope_changed(a, stamp(EnvelopeParams(), version=ENVELOPE_VERSION + 1))
    with pytest.raises(ValueError):
        EnvelopeParams(D("0.6"))


def test_pager_is_a_slots_wrapper() -> None:
    pages: list[tuple[str, str, str]] = []
    mirrors: list[tuple[str, str]] = []
    p = Pager(lambda s, t, b: pages.append((s, t, b)), lambda t, b: mirrors.append((t, b)))
    p.page("critical", "ARM FAILED", "scope series")
    p.log_mirror("ARM FAILED", "scope series")
    assert pages == [("critical", "ARM FAILED", "scope series")] and mirrors == [("ARM FAILED", "scope series")]
    assert not hasattr(p, "__dict__")
    with pytest.raises(AttributeError):
        p.extra = 1  # type: ignore[attr-defined]
    with pytest.raises(ValueError):
        p.page("loud", "t", "b")


class Pg:
    def __init__(self, last: int | None, alert: bool = False) -> None:
        self.last, self.alert = last, alert

    def last_fenced_commit_ms(self) -> int | None:
        return self.last

    def sweep_pg_unreachable(self) -> bool:
        return self.alert


def test_engine_pg_down() -> None:
    now = 1_000_000
    assert not engine_pg_down(Pg(now - ENGINE_PG_DOWN_AFTER_MS), now)
    assert engine_pg_down(Pg(now - ENGINE_PG_DOWN_AFTER_MS - 1), now)
    assert engine_pg_down(Pg(None), now)
    assert engine_pg_down(Pg(now, alert=True), now)


def test_system_clock() -> None:
    c = SystemClock()
    assert c.now_ms() > 1_600_000_000_000 and c.monotonic_ms() >= 0
