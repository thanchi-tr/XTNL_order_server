"""The resolver: rows in force at t, tombstones, future-dated rows, precedence (plan S25; design §B9)."""
from __future__ import annotations

from src.armconfig.resolver import PositionScope, assignments_in_force, resolve, route_policy_at
from src.armconfig.validate import RoutePolicy
from tests.armconfig.docs import FIRST_BAR, assignment_row, raw_doc, route_row, strategy_row, valid

POS = PositionScope(adapter_id="mock", account_id="acct-1", symbol="SYN.A", edge_code="default")
SL = "STOP_LEVEL"


def strategies() -> list[dict[str, object]]:
    return [strategy_row(1, code="s_one"), strategy_row(2, code="s_two"), strategy_row(3, code="s_three"),
            strategy_row(4, code="s_four"), strategy_row(5, code="s_one", version=2,
                                                         params={"guards": FIRST_BAR, "be_offset_r": "0.2"})]


def code_at(doc_rows: list[dict[str, object]], at_ms: int, scope: PositionScope = POS) -> str | None:
    doc = valid(raw_doc(strategies=strategies(), assignments=doc_rows))
    got = resolve(doc, scope, SL, at_ms)
    return None if got is None else f"{got.strategy.code}:{got.strategy.version}"


def test_empty_document_resolves_nothing() -> None:
    doc = valid(raw_doc(strategies=[], assignments=[], route_policy=[]))
    assert resolve(doc, POS, SL, 10_000) is None
    assert assignments_in_force(doc, 10_000) == ()
    assert route_policy_at(doc, "main", 10_000) == RoutePolicy(route="main")


def test_row_in_force_is_the_newest_effective_from_at_or_before_t() -> None:
    rows = [assignment_row(1, code="s_one", effective_from_ms=1_000),
            assignment_row(2, code="s_two", effective_from_ms=2_000),
            assignment_row(3, code="s_three", effective_from_ms=3_000)]
    assert code_at(rows, 999) is None
    assert code_at(rows, 1_000) == "s_one:1"
    assert code_at(rows, 2_500) == "s_two:1"
    assert code_at(rows, 3_000) == "s_three:1"


def test_changed_row_is_a_new_row_naming_a_new_version() -> None:
    rows = [assignment_row(1, code="s_one", effective_from_ms=1_000),
            assignment_row(2, code="s_one", version=2, effective_from_ms=5_000)]
    assert code_at(rows, 4_999) == "s_one:1"
    assert code_at(rows, 5_000) == "s_one:2"


def test_future_dated_row_applies_only_from_its_effective_from() -> None:
    rows = [assignment_row(1, code="s_one", effective_from_ms=1_000),
            assignment_row(2, code="s_two", effective_from_ms=9_000_000)]
    assert code_at(rows, 8_999_999) == "s_one:1"
    assert code_at(rows, 9_000_000) == "s_two:1"


def test_tombstone_ends_the_key_and_a_later_row_restores_it() -> None:
    rows = [assignment_row(1, code="s_one", effective_from_ms=1_000),
            assignment_row(2, code="s_one", enabled=False, effective_from_ms=2_000),
            assignment_row(3, code="s_two", effective_from_ms=3_000)]
    assert code_at(rows, 1_500) == "s_one:1"
    assert code_at(rows, 2_000) is None
    assert code_at(rows, 3_000) == "s_two:1"


def test_tombstone_of_a_specific_key_falls_back_to_a_wider_one() -> None:
    rows = [assignment_row(1, code="s_one"),
            assignment_row(2, code="s_two", symbol="SYN.A"),
            assignment_row(3, code="s_two", symbol="SYN.A", enabled=False, effective_from_ms=2_000)]
    assert code_at(rows, 1_500) == "s_two:1"
    assert code_at(rows, 2_000) == "s_one:1"


def test_precedence_symbol_over_account_over_adapter_set_beats_null() -> None:
    rows = [assignment_row(1, code="s_one"),                                   # all wildcard
            assignment_row(2, code="s_two", adapter="mock"),
            assignment_row(3, code="s_three", account="main", adapter="mock"),
            assignment_row(4, code="s_four", symbol="SYN.A")]
    assert code_at(rows, 1_000) == "s_four:1"                                   # symbol beats account + adapter
    assert code_at(rows, 1_000, PositionScope("mock", "acct-1", "SYN.B")) == "s_three:1"
    assert code_at(rows, 1_000, PositionScope("mock", "acct-2", "SYN.B")) == "s_two:1"
    assert code_at(rows, 1_000, PositionScope("mock2", "acct-2", "SYN.B")) == "s_one:1"
    both = rows + [assignment_row(5, code="s_one", version=2, symbol="SYN.A", adapter="mock")]
    assert code_at(both, 1_000) == "s_one:2"                                    # symbol + adapter beats symbol


def test_at_most_one_row_per_scope_and_resource() -> None:
    rows = [assignment_row(1, code="s_one"), assignment_row(2, code="s_two", effective_from_ms=2_000),
            assignment_row(3, code="s_three", symbol="SYN.A"),
            assignment_row(4, code="s_four", symbol="SYN.A", effective_from_ms=500)]
    doc = valid(raw_doc(strategies=strategies(), assignments=rows))
    in_force = assignments_in_force(doc, 5_000)
    keys = [a.key for a in in_force]
    assert len(keys) == len(set(keys)) == 2
    assert [a.row_id for a in in_force] == [2, 3]


def test_other_resource_or_position_resolves_nothing() -> None:
    doc = valid(raw_doc(strategies=strategies(), assignments=[assignment_row(1, code="s_one", symbol="SYN.B")]))
    assert resolve(doc, POS, SL, 5_000) is None
    assert resolve(doc, PositionScope("mock", "acct-1", "SYN.B"), "TAKE_PROFIT", 5_000) is None


def test_resolved_strategy_carries_the_pin_source() -> None:
    doc = valid(raw_doc(strategies=strategies(), assignments=[assignment_row(1, code="s_one", version=2)]))
    got = resolve(doc, POS, SL, 1_000)
    assert got is not None and got.assignment.row_id == 1
    assert got.strategy.params.param("be_offset_r") == "0.2"
    assert got.strategy.params.guards[0].kind == "first_bar"


def test_route_policy_in_force_at_bar_end() -> None:
    doc = valid(raw_doc(route_policy=[route_row(1, effective_from_ms=1_000),
                                      route_row(2, mode="actionable", effective_from_ms=5_000),
                                      route_row(3, route="second", post=["arm_email"], effective_from_ms=8_000)]))
    assert route_policy_at(doc, "main", 999) == RoutePolicy(route="main")
    assert route_policy_at(doc, "main", 4_999).row_id == 1
    assert route_policy_at(doc, "main", 5_000).mode == "actionable"
    assert route_policy_at(doc, "second", 7_999).row_id == 0
    assert route_policy_at(doc, "second", 8_000).post_channels == ("arm_email",)
    default = route_policy_at(doc, "nobody", 9_000)
    assert (default.mode, default.pre_channels, default.post_channels) == ("every_bar", ("arm_email",), ("arm_email",))
