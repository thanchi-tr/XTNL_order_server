"""Plan S38f (round 14, fix 1): the S07 Oversee stop rule is behind `XTNL_OVERSEE_STOP_AT_ENTRY=1`.

Off (the default), `effective_stop` / `effective_stop_detail` / MissingStopLossRule / the stop_watch deadline give
exactly the verdicts of commit 31d9b58 (before S07): a linked reduce-only child counts only strictly on the loss side
of the entry, whatever its kind, and no stop caveat exists. `REFERENCE` below is that commit's `effective_stop`, copied
verbatim (it is the definition the flag-off path must equal). The ARM always reads stops by the S07 rule
(`at_entry=True`), and the go-live gate `oversee_c4` counts as accepted only with the switch on. Synthetic prices only.
"""
from __future__ import annotations

import random
from collections.abc import Sequence
from decimal import Decimal

import pytest

from src.dto import OrderDTO, OrderKind, OrderStatus, PositionDTO, Side
from src.oversee import risk
from src.oversee.config import OverseeConfig
from src.oversee.risk import (
    STOP_KIND_INCONSISTENT,
    Mark,
    MarkQuality,
    _units_to_cover,
    effective_stop,
    effective_stop_detail,
)
from src.oversee.rules import EvalContext, MissingStopLossRule, run_rule
from src.oversee.stop_rule import STOP_AT_ENTRY_VAR, configure_stop_rule, stop_at_entry_from_env
from src.oversee.sweep import stop_watch_changes
from src.oversee.types import Level, Result, Unknown
from src.riskmgr import golive
from tests.oversee.helpers import AD, T0, order, position, quote, snap

D = Decimal
ZERO = D(0)
CFG = OverseeConfig()
SIDES: tuple[Side, ...] = ("BUY", "SELL")
KINDS: tuple[OrderKind | None, ...] = ("STOP", "LIMIT", None)
LEVELS = ("1.0800", "1.0900", "1.0950", "1.1000", "1.1050", "1.1100", "1.1200")
STATUSES = (OrderStatus.PENDING, OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED,
            OrderStatus.CANCELLED)


def REFERENCE(e: OrderDTO | PositionDTO, positions: Sequence[PositionDTO],  # noqa: N802
              orders: Sequence[OrderDTO]) -> Decimal | None:
    """`src/oversee/risk.py::effective_stop` at commit 31d9b58 (before S07), verbatim apart from the name."""
    if e.stop_loss is not None:
        return e.stop_loss
    if e.entry_price is None:
        return None
    link = e.position_id if isinstance(e, PositionDTO) else e.broker_order_id
    children = [
        o for o in orders
        if o.is_reduce_only and not o.status.terminal and o.linked_position_id == link
        and o.symbol == e.symbol and o.side != e.side and o.entry_price is not None
        and o.remaining_units is not None
        and ((e.side == "BUY" and o.entry_price < e.entry_price)
             or (e.side == "SELL" and o.entry_price > e.entry_price))
    ]
    if not children:
        return None
    covered = sum((o.remaining_units or ZERO for o in children), ZERO)
    if covered < _units_to_cover(e, positions, orders):
        return None
    prices = [o.entry_price for o in children if o.entry_price is not None]
    return min(prices) if e.side == "BUY" else max(prices)


def _entities(rng: random.Random) -> tuple[list[PositionDTO], list[OrderDTO]]:
    positions = [position(f"P{i}", rng.choice(SIDES), units=str(rng.choice([1000, 5000])), entry=rng.choice(LEVELS),
                          stop=rng.choice([None, None, None, rng.choice(LEVELS)])) for i in range(rng.randint(1, 3))]
    orders = [order(f"E{i}", rng.choice(SIDES), rng.choice(STATUSES[:3]), ordered=str(rng.choice([1000, 5000])),
                    entry=rng.choice([None, *LEVELS])) for i in range(rng.randint(0, 3))]
    targets = [p.position_id for p in positions] + [o.broker_order_id for o in orders]
    for i in range(rng.randint(0, 6)):
        status = rng.choice(STATUSES)
        orders.append(order(f"C{i}", rng.choice(SIDES), status, ordered=str(rng.choice([1000, 2500, 5000])),
                            remaining="0" if status.terminal else None, entry=rng.choice(LEVELS),
                            reduce_only=rng.random() < 0.9, linked=rng.choice(targets), kind=rng.choice(KINDS)))
    return positions, orders


def _mark(price: str, quality: MarkQuality = MarkQuality.CLEAN) -> Mark:
    return Mark((AD, "EURUSD"), D(price), quality, T0)


# ---------------------------------------------------------------------------------------------------- the setting
@pytest.mark.parametrize(("raw", "on", "problem"), [
    (None, False, False), ("", False, False), ("0", False, False), (" 1 ", True, False), ("1", True, False),
    ("true", False, True), ("yes", False, True), ("2", False, True)])
def test_the_switch_is_on_only_for_1(raw: str | None, on: bool, problem: bool) -> None:
    env = {} if raw is None else {STOP_AT_ENTRY_VAR: raw}
    got, why = stop_at_entry_from_env(env)
    assert got is on and (why is not None) is problem
    assert configure_stop_rule(env) == why and risk.stop_at_entry() is on


def test_off_by_default() -> None:
    assert risk.stop_at_entry() is False          # the suite's default, as an engine without the setting
    assert golive.STOP_AT_ENTRY_VAR == STOP_AT_ENTRY_VAR


# ---------------------------------------------------------------------------------------------------- flag off
def test_flag_off_equals_the_pre_s07_rule_on_generated_entity_sets() -> None:
    rng = random.Random(20261117)
    marks = [None, _mark("1.1000"), _mark("1.0900", MarkQuality.LAST_MID), _mark("1.1100", MarkQuality.LIQUIDATION),
             _mark("1.1050", MarkQuality.CLOSED_LAST_CLEAN)]
    checked = 0
    for _ in range(600):
        positions, orders = _entities(rng)
        for e in [*positions, *orders]:
            want = REFERENCE(e, positions, orders)
            assert effective_stop(e, positions, orders) == want
            for m in marks:
                detail = effective_stop_detail(e, positions, orders, m)
                assert (None if detail is None else detail.level) == want
                if detail is not None:
                    checked += 1
                    assert detail.caveats == () and detail.inconsistent_ids == ()
    assert checked > 200


@pytest.mark.parametrize(("level", "kind", "off", "on"), [
    ("1.1000", "STOP", None, D("1.1000")),          # break-even STOP child: no stop before S07
    ("1.1200", "STOP", None, D("1.1200")),          # beyond entry
    ("1.0900", "LIMIT", D("1.0900"), None),         # a LIMIT on the loss side counted before S07
    ("1.0900", None, D("1.0900"), D("1.0900")),
    ("1.1000", None, None, None),
])
def test_the_differences_the_switch_makes(level: str, kind: OrderKind | None, off: Decimal | None,
                                          on: Decimal | None) -> None:
    p = position("P", "BUY", entry="1.1000")
    c = order("C", "SELL", ordered="100000", entry=level, reduce_only=True, linked="P", kind=kind)
    assert effective_stop(p, [p], [c]) == off
    assert effective_stop(p, [p], [c], at_entry=True) == on
    risk.set_stop_at_entry(True)
    assert effective_stop(p, [p], [c]) == on


def _rule(s: object, now: int = T0) -> Result | Unknown:
    c = EvalContext(s, CFG, now)  # type: ignore[arg-type]
    p = c.snap.positions[0]
    return run_rule(MissingStopLossRule(CFG.missing_stop_loss), c, c.entity_subject(p))


def test_flag_off_a_mislabelled_take_profit_is_never_unknown() -> None:
    """Before S07 a STOP child beyond the entry is no stop: the rule waits for its grace (`stop_watch`) and never
    reads UNKNOWN `stop_kind_inconsistent`; with the switch on it is the S07 UNKNOWN."""
    p = position("P", entry="1.1000")
    tp = order("T", "SELL", ordered="100000", entry="1.1200", reduce_only=True, linked="P", kind="STOP")
    s = snap(positions=(p,), orders=(tp,), quotes={(AD, "EURUSD"): quote("1.1060", T0)})
    r = _rule(s)
    assert isinstance(r, Result) and r.level is Level.PASS and r.reason != "stop_present"
    to_set, _ = stop_watch_changes(s, T0)
    assert len(to_set) == 1                    # the grace deadline starts: no stop is seen
    risk.set_stop_at_entry(True)
    r = _rule(s)
    assert isinstance(r, Unknown) and r.reason == STOP_KIND_INCONSISTENT


def test_flag_off_a_break_even_stop_child_is_missing_after_the_grace() -> None:
    p = position("P", entry="1.1000")
    be = order("C", "SELL", ordered="100000", entry="1.1000", reduce_only=True, linked="P", kind="STOP")
    s = snap(positions=(p,), orders=(be,), quotes={(AD, "EURUSD"): quote("1.1060", T0)},
             stop_watch={("position", f"{AD}:P"): T0 - CFG.missing_stop_loss.grace_ms})
    r = _rule(s)
    assert isinstance(r, Result) and r.level is Level.SOFT_HALT
    risk.set_stop_at_entry(True)
    r = _rule(s)
    assert isinstance(r, Result) and r.level is Level.PASS and r.reason == "stop_present"


# ---------------------------------------------------------------------------------------------------- ARM and gate
def test_the_arm_reads_stops_by_the_s07_rule_whatever_the_switch() -> None:
    p = position("P", "BUY", entry="1.1000")
    c = order("C", "SELL", ordered="100000", entry="1.1000", reduce_only=True, linked="P", kind="STOP")
    d = effective_stop_detail(p, [p], [c], at_entry=True)
    assert d is not None and d.level == D("1.1000")
    import ast
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    calls = 0
    for f in sorted((root / "src/riskmgr").rglob("*.py")):
        for node in ast.walk(ast.parse(f.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) in ("effective_stop_detail",
                                                                                "effective_stop"):
                calls += 1
                kw = {k.arg: k.value for k in node.keywords}
                assert isinstance(kw.get("at_entry"), ast.Constant) and kw["at_entry"].value is True, f.name
    assert calls >= 5


def test_accepting_oversee_c4_counts_only_with_the_switch_on() -> None:
    open_ids = [g.gate_id for g in golive.GATES if g.state is golive.GateState.OPEN]
    env = {golive.ACCEPT_VAR: ",".join(open_ids)}
    v = golive.verdict(env)
    assert not v.allowed and [g.gate_id for g in v.refused] == ["oversee_c4"]
    assert any(STOP_AT_ENTRY_VAR in p for p in v.problems)
    for bad in ("0", "", "true"):
        assert not golive.verdict({**env, STOP_AT_ENTRY_VAR: bad}).allowed
    v = golive.verdict({**env, STOP_AT_ENTRY_VAR: "1"})
    # oversee_c4 is accepted in the table (user, 2026-11-18); it counts once the switch is on
    assert golive.BY_ID["oversee_c4"].state is golive.GateState.ACCEPTED
    assert v.allowed and v.problems == ()


def test_oversee_c4_needs_the_rule_oversee_actually_runs() -> None:
    """R14 review: the setting says 1 but this process runs the pre-S07 rule (the process setting off): refused."""
    open_ids = [g.gate_id for g in golive.GATES if g.state is golive.GateState.OPEN]
    env = {golive.ACCEPT_VAR: ",".join(open_ids), STOP_AT_ENTRY_VAR: "1"}
    v = golive.verdict(env, stop_rule_in_force=False)
    assert not v.allowed and [g.gate_id for g in v.refused] == ["oversee_c4"]
    assert any("Oversee runs the S07 stop rule" in p for p in v.problems)
    assert golive.verdict(env, stop_rule_in_force=True).allowed
    assert not golive.verdict({golive.ACCEPT_VAR: ",".join(open_ids)}, stop_rule_in_force=True).allowed
    from pathlib import Path
    text = (Path(__file__).resolve().parents[2] / "src/runtime/optional.py").read_text(encoding="utf-8")
    assert "golive_verdict(self.deps.environ, stop_rule_in_force=stop_at_entry())" in text


def test_build_engine_configures_the_rule_from_the_environment() -> None:
    from pathlib import Path
    text = (Path(__file__).resolve().parents[2] / "src/runtime/wiring.py").read_text(encoding="utf-8")
    assert "configure_stop_rule(raw)" in text
