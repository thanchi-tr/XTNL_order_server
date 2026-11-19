"""Row validation (plan S25; design §B9 Row validation, §B8 Policy, §B6.7, §B5.2 timing rows)."""
from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import ClassVar

import pytest

from src.armconfig.document import Document, parse_document
from src.armconfig.validate import KnownIds, ValidationStatus, escape_text, validate_document
from src.marketdata.types import BarSpec
from src.riskmgr.registry import REGISTRY, ArmRegistry
from src.riskmgr.strategy import StrategyParams, param_pairs, read_choice, reject_unknown
from src.riskmgr.types import ArmEvent, BarEvent
from tests.armconfig.docs import (
    FIRST_BAR,
    KIND_A,
    KNOWN,
    assignment_row,
    parsed,
    raw_doc,
    route_row,
    strategy_row,
    valid,
    validated,
)
from tests.riskmgr.toys import ToyGuard, ToyStrategy, toy_row


def codes(raw: Mapping[str, object]) -> list[tuple[str, str]]:
    result = validated(raw)
    assert result.status is ValidationStatus.INVALID and result.document is None
    return [(p.code, p.ref.rsplit(":", 1)[0] if p.row_hash else p.ref) for p in result.problems]


# ---------------------------------------------------------------------------------------------------- valid documents
def test_valid_document() -> None:
    doc = valid(raw_doc(strategies=[strategy_row(1), strategy_row(2, code="be_b", params={
                            "be_offset_r": "0.1", "trigger_basis": "bid", "guards": FIRST_BAR})],
                        assignments=[assignment_row(1), assignment_row(2, symbol="SYN.A", account="main",
                                                                         adapter="mock", code="be_b")],
                        route_policy=[route_row(1), route_row(2, route="second", mode="actionable",
                                                              pre=["arm_push", "arm_email"], post=["arm_email"])]))
    assert [s.code for s in doc.strategies] == ["be_a", "be_b"]
    entry = doc.strategy("be_b", 1)
    assert entry is not None and entry.kind == KIND_A and entry.impl_version == 1 and entry.resource == "STOP_LEVEL"
    assert entry.params == REGISTRY.parse_params(KIND_A, 1, {"be_offset_r": "0.1", "trigger_basis": "bid",
                                                              "guards": FIRST_BAR})
    a2 = doc.assignments[1]
    assert (a2.symbol, a2.account, a2.adapter, a2.resource) == ("SYN.A", "acct-1", "mock", "STOP_LEVEL")
    assert doc.route_policies[1].pre_channels == ("arm_push", "arm_email")
    assert len(doc.row_hashes) == 6 and len(doc.doc_hash) == 64


def test_empty_read_is_a_valid_empty_document() -> None:
    doc = valid(raw_doc(strategies=[], assignments=[], route_policy=[]))
    assert doc.strategies == () and doc.assignments == () and doc.route_policies == ()


def test_schema_ahead_is_never_validated() -> None:
    result = validated(raw_doc(schema_version=2, strategies=[{"id": 1, "new_column": "x"}]))
    assert result.status is ValidationStatus.SCHEMA_AHEAD and result.document is None
    assert [p.code for p in result.problems] == ["schema_ahead"]


def test_document_problem_becomes_invalid() -> None:
    result = validate_document(parse_document(raw_doc(sinks=[])), KNOWN)
    assert result.status is ValidationStatus.INVALID and result.problems[0].page_text == "sink_key document"


# ---------------------------------------------------------------------------------------------------- every row
@pytest.mark.parametrize("raw, expected", [
    (raw_doc(strategies=[strategy_row(note="x")]), ("unknown_column", "strategies:1")),
    (raw_doc(assignments=[assignment_row(guards=[])]), ("unknown_column", "assignments:1")),
    (raw_doc(route_policy=[{k: v for k, v in route_row().items() if k != "mode"}]),
     ("missing_column", "route_policy:1")),
    (raw_doc(strategies=[strategy_row(pre_offset=40)]), ("envelope_key", "strategies:1")),
    (raw_doc(route_policy=[route_row(pre_offset_s=40)]), ("envelope_key", "route_policy:1")),
    (raw_doc(strategies=[strategy_row(params={"guards": FIRST_BAR, "min_gap_r": "0.1"})]),
     ("envelope_key", "strategies:1")),
    (raw_doc(strategies=[strategy_row(params={"guards": [{"kind": "first_bar", "version": 1,
                                                          "params": {"pre_offset": 40}}]})]),
     ("envelope_key", "strategies:1")),
    (raw_doc(assignments=[assignment_row(sinks=["decision"])]), ("sink_key", "assignments:1")),
    (raw_doc(strategies=[strategy_row(params={"guards": FIRST_BAR, "arm_sinks": []})]), ("sink_key", "strategies:1")),
    (raw_doc(strategies=[strategy_row(set_at_ms="now")]), ("metadata_form", "strategies:1")),
    (raw_doc(strategies=[strategy_row(current_user="Robert'); --")]), ("metadata_form", "strategies:1")),
])
def test_every_row_checks(raw: Mapping[str, object], expected: tuple[str, str]) -> None:
    assert expected in codes(raw)


# ---------------------------------------------------------------------------------------------------- strategies
@pytest.mark.parametrize("row, code", [
    (strategy_row(code="Bad-Code"), "code_form"),
    (strategy_row(code="x" * 33), "code_form"),
    (strategy_row(code=""), "code_form"),
    (strategy_row(version=0), "version_form"),
    (strategy_row(version=Decimal("1.5")), "version_form"),
    (strategy_row(kind="no_such_kind"), "strategy_unknown"),
    (strategy_row(impl_version=99), "strategy_unknown"),
    (strategy_row(params=[]), "params_invalid"),
    (strategy_row(params={"guards": FIRST_BAR, "be_offset_r": "0.9"}), "params_invalid"),
    (strategy_row(params={"guards": FIRST_BAR, "unknown_key": 1}), "params_invalid"),
    (strategy_row(params={}), "params_invalid"),                                       # no first_bar entry
    (strategy_row(params={"guards": FIRST_BAR * 2}), "params_invalid"),                 # first_bar twice
    (strategy_row(params={"guards": [{"kind": "first_bar", "version": 7, "params": {}}]}), "params_invalid"),
    (strategy_row(params={"guards": [{"kind": "first_bar", "version": 1, "params": {"multiplier": "9"}}]}),
     "params_invalid"),
    (strategy_row(params={"guards": FIRST_BAR, "first_bar": {"multiplier": "1.5"}}), "guard_key_outside_guards"),
    (strategy_row(params={"guards": FIRST_BAR, "first_bar.multiplier": "1.5"}), "guard_key_outside_guards"),
    (strategy_row(params={"guards": FIRST_BAR, "bar_spec": "1h"}), "params_invalid"),   # kind (a) admits 15m only
])
def test_strategy_rows(row: dict[str, object], code: str) -> None:
    assert (code, "strategies:1") in codes(raw_doc(strategies=[row]))


def test_duplicate_strategy_code_and_version() -> None:
    found = codes(raw_doc(strategies=[strategy_row(1), strategy_row(2)]))
    assert found == [("duplicate_strategy", "strategies:1"), ("duplicate_strategy", "strategies:2")]


def test_deprecated_strategy_is_refused_for_new_documents() -> None:
    registry = ArmRegistry(strategies=[ToyStrategy()], guards=[ToyGuard()])
    registry.deprecate_strategy("toy_break_even", 1)
    doc = parse_document(raw_doc(strategies=[strategy_row(kind="toy_break_even", params=toy_row())]))
    result = validate_document(doc, KNOWN, registry)
    assert [p.code for p in result.problems] == ["strategy_deprecated"]


class HourlyToy(ToyStrategy):
    """A toy strategy with a `bar_spec` choice wider than the timing table (15m only in v1)."""

    KEYS: ClassVar[frozenset[str]] = frozenset({"comparison", "offset_r", "bar_spec"})

    def parse_params(self, raw: Mapping[str, object]) -> tuple[tuple[str, str], ...]:
        reject_unknown(raw, self.KEYS, self.KIND)
        own = {k: v for k, v in raw.items() if k != "bar_spec"}
        return (*super().parse_params(own),
                *param_pairs(bar_spec=read_choice(raw, "bar_spec", ("15m", "1h"), "15m", self.KIND)))

    def triggers(self, params: StrategyParams) -> tuple[BarEvent, ...]:
        spec = BarSpec.parse(params.param("bar_spec"))
        return (BarEvent(spec, ArmEvent.PRE), BarEvent(spec, ArmEvent.POST))


def test_strategy_whose_bar_spec_has_no_timing_row_is_refused() -> None:
    registry = ArmRegistry(strategies=[HourlyToy()], guards=[ToyGuard()])
    ok = parse_document(raw_doc(strategies=[strategy_row(kind="toy_break_even", params=toy_row(bar_spec="15m"))]))
    assert validate_document(ok, KNOWN, registry).status is ValidationStatus.VALID
    hourly = parse_document(raw_doc(strategies=[strategy_row(kind="toy_break_even", params=toy_row(bar_spec="1h"))]))
    result = validate_document(hourly, KNOWN, registry)
    assert [(p.code, p.detail) for p in result.problems] == [
        ("no_timing_row", "strategy toy_break_even v1: bar spec 1h has no timing row")]


def test_assignment_of_a_failing_strategy_row_names_only_the_strategy() -> None:
    found = codes(raw_doc(strategies=[strategy_row(params={"guards": FIRST_BAR, "be_offset_r": "2"})]))
    assert found == [("params_invalid", "strategies:1")]


# ---------------------------------------------------------------------------------------------------- assignments
@pytest.mark.parametrize("row, code", [
    (assignment_row(edge_code="edge_b"), "edge_code_set"),
    (assignment_row(symbol="NOT.HELD"), "symbol_unknown"),
    (assignment_row(symbol=5), "symbol_unknown"),
    (assignment_row(adapter="stonex_unknown"), "adapter_unknown"),
    (assignment_row(account="acct-9"), "account_unknown"),
    (assignment_row(code="be_zz"), "strategy_ref_unknown"),
    (assignment_row(version=2), "strategy_ref_unknown"),
    (assignment_row(code="BAD"), "code_form"),
    (assignment_row(enabled="false"), "enabled_form"),
    (assignment_row(enabled=0), "enabled_form"),
    (assignment_row(effective_from_ms=-1), "effective_from_form"),
    (assignment_row(effective_from_ms=Decimal("1.5")), "effective_from_form"),
])
def test_assignment_rows(row: dict[str, object], code: str) -> None:
    assert (code, "assignments:1") in codes(raw_doc(assignments=[row]))


def test_account_alias_maps_to_the_account_id() -> None:
    doc = valid(raw_doc(assignments=[assignment_row(account="main")]))
    assert doc.assignments[0].account == "acct-1"


def test_duplicate_effective_from_per_key() -> None:
    found = codes(raw_doc(assignments=[assignment_row(1, symbol="SYN.A"), assignment_row(2, symbol="SYN.A",
                                                                                         enabled=False)]))
    assert found == [("duplicate_effective_from", "assignments:1"), ("duplicate_effective_from", "assignments:2")]
    # an alias and its account id are one key
    found = codes(raw_doc(assignments=[assignment_row(1, account="main"), assignment_row(2, account="acct-1")]))
    assert [c for c, _ in found] == ["duplicate_effective_from"] * 2
    # another key, or another time on the same key, is fine
    valid(raw_doc(assignments=[assignment_row(1, symbol="SYN.A"), assignment_row(2, symbol="SYN.B"),
                               assignment_row(3, symbol="SYN.A", effective_from_ms=2_000)]))


# ---------------------------------------------------------------------------------------------------- route policy
@pytest.mark.parametrize("row, code", [
    (route_row(route="nobody"), "route_unknown"),
    (route_row(mode="digest"), "mode_invalid"),
    (route_row(pre=[]), "channels_invalid"),
    (route_row(pre=["email"]), "channels_invalid"),
    (route_row(pre=["arm_email", "arm_email"]), "channels_invalid"),
    (route_row(post=["arm_push"]), "channels_invalid"),
    (route_row(post="arm_email"), "channels_invalid"),
    (route_row(effective_from_ms=None), "effective_from_form"),
])
def test_route_policy_rows(row: dict[str, object], code: str) -> None:
    assert (code, "route_policy:1") in codes(raw_doc(route_policy=[row]))


def test_duplicate_route_effective_from() -> None:
    found = codes(raw_doc(route_policy=[route_row(1), route_row(2, mode="actionable")]))
    assert [c for c, _ in found] == ["duplicate_effective_from"] * 2


# ---------------------------------------------------------------------------------------------------- text
def test_problems_name_rows_by_id_and_hash_never_by_text() -> None:
    planted = "planted‮TEXT"
    raw = raw_doc(strategies=[strategy_row(code=planted)], assignments=[assignment_row(symbol=planted)],
                  route_policy=[route_row(route=planted)])
    doc = parsed(raw)
    result = validate_document(doc, KNOWN)
    hashes = doc.row_hashes()
    for p in result.problems:
        assert p.page_text == f"{p.code} {p.table}:{p.row_id}:{hashes[f'{p.table}:{p.row_id}']}"
        assert "planted" not in p.page_text and "planted" not in p.detail
    assert len(result.problems) == 3


def test_unknown_column_detail_is_escaped() -> None:
    result = validated(raw_doc(strategies=[strategy_row(**{"bad‮\x07col": 1})]))
    assert result.problems[0].code == "unknown_column"
    assert result.problems[0].detail == "bad\\u202e\\x07col"


@pytest.mark.parametrize("text, shown", [
    ("plain text", "plain text"),
    ("a\nb\tc", "a\\x0ab\\x09c"),
    ("back\\slash", "back\\\\slash"),
    ("\x1b[31mred", "\\x1b[31mred"),
    ("rtl‮override​", "rtl\\u202eoverride\\u200b"),
    ("\U000e0041tag", "\\U000e0041tag"),
    ("x" * 205, "x" * 200 + "..."),
])
def test_escape_text(text: str, shown: str) -> None:
    assert escape_text(text) == shown


# ---------------------------------------------------------------------------------------------------- seen rows
def _deprecated_toy() -> ArmRegistry:
    registry = ArmRegistry(strategies=[ToyStrategy()], guards=[ToyGuard()])
    registry.deprecate_strategy("toy_break_even", 1)
    return registry


def _toy_doc(assignments: list[dict[str, object]], strategies: list[dict[str, object]] | None = None) -> Document:
    return parsed(raw_doc(strategies=strategies or [strategy_row(1, kind="toy_break_even", params=toy_row())],
                          assignments=assignments))


def test_deprecated_strategy_only_in_seen_tombstoned_rows_stays_valid() -> None:
    registry = _deprecated_toy()
    seen_doc = _toy_doc([assignment_row(1), assignment_row(2, enabled=False, effective_from_ms=5_000)])
    seen = seen_doc.row_hashes()
    # without the seen rows every row is a new pin, so the document is refused
    assert [p.code for p in validate_document(seen_doc, KNOWN, registry).problems] == ["strategy_deprecated"]
    result = validate_document(seen_doc, KNOWN, registry, seen=seen, at_ms=6_000)
    assert result.status is ValidationStatus.VALID, [p.page_text for p in result.problems]
    # a later change (a pending route policy) still validates
    later = parsed(raw_doc(strategies=[strategy_row(1, kind="toy_break_even", params=toy_row())],
                           assignments=[assignment_row(1), assignment_row(2, enabled=False, effective_from_ms=5_000)],
                           route_policy=[route_row(1), route_row(2, effective_from_ms=7_000)]))
    assert validate_document(later, KNOWN, registry, seen=seen, at_ms=8_000).status is ValidationStatus.VALID


def test_new_pending_row_naming_a_deprecated_strategy_is_refused() -> None:
    registry = _deprecated_toy()
    seen = _toy_doc([assignment_row(1), assignment_row(2, enabled=False, effective_from_ms=5_000)]).row_hashes()
    # a pending enabled assignment would pin it: refused on the assignment row
    doc = _toy_doc([assignment_row(1), assignment_row(2, enabled=False, effective_from_ms=5_000),
                    assignment_row(3, symbol="SYN.A", effective_from_ms=9_000)])
    result = validate_document(doc, KNOWN, registry, seen=seen, at_ms=6_000)
    assert [(p.code, p.table, p.row_id) for p in result.problems] == [("strategy_deprecated", "assignments", 3)]
    # a pending tombstone of it is fine
    doc = _toy_doc([assignment_row(1), assignment_row(2, enabled=False, effective_from_ms=5_000),
                    assignment_row(3, symbol="SYN.A", enabled=False, effective_from_ms=9_000)])
    assert validate_document(doc, KNOWN, registry, seen=seen, at_ms=6_000).status is ValidationStatus.VALID
    # a pending strategy row of the deprecated kind is refused
    doc = _toy_doc([assignment_row(1), assignment_row(2, enabled=False, effective_from_ms=5_000)],
                   strategies=[strategy_row(1, kind="toy_break_even", params=toy_row()),
                               strategy_row(2, code="be_new", kind="toy_break_even", params=toy_row())])
    result = validate_document(doc, KNOWN, registry, seen=seen, at_ms=6_000)
    assert [(p.code, p.ref.rsplit(":", 1)[0]) for p in result.problems] == [("strategy_deprecated", "strategies:2")]


def test_seen_row_with_another_hash_is_checked_as_pending() -> None:
    registry = _deprecated_toy()
    doc = _toy_doc([assignment_row(1)])
    seen = {ref: "0" * 64 for ref in doc.row_hashes()}
    assert [p.code for p in validate_document(doc, KNOWN, registry, seen=seen, at_ms=2_000).problems] == [
        "strategy_deprecated"]


def test_removed_id_in_a_superseded_seen_row_stays_valid() -> None:
    raw = raw_doc(assignments=[assignment_row(1, symbol="SYN.OLD", account="old-alias", adapter="gone"),
                               assignment_row(2, symbol="SYN.OLD", account="old-alias", adapter="gone",
                                              enabled=False, effective_from_ms=5_000)],
                  route_policy=[route_row(1, route="gone_route"),
                                route_row(2, route="gone_route", effective_from_ms=5_000)])
    then = KnownIds(symbols=KNOWN.symbols | {"SYN.OLD"}, adapters=KNOWN.adapters | {"gone"},
                    accounts={**KNOWN.accounts, "old-alias": "acct-9"}, routes=KNOWN.routes | {"gone_route"})
    doc = parsed(raw)
    assert validate_document(doc, then).status is ValidationStatus.VALID
    seen = doc.row_hashes()
    # the ids are removed later; row 1 is superseded by the tombstone, route row 1 by route row 2
    result = validate_document(doc, KNOWN, seen=seen, at_ms=6_000)
    assert [(p.code, p.table, p.row_id) for p in result.problems] == [("route_unknown", "route_policy", 2)]
    # the route policy in force still needs a known route; the tombstone needs none
    raw["route_policy"] = [route_row(1)]
    doc = parsed(raw)
    result = validate_document(doc, KNOWN, seen=doc.row_hashes(), at_ms=6_000)
    assert result.status is ValidationStatus.VALID, [p.page_text for p in result.problems]
    # before the tombstone applies, row 1 is in force (and the tombstone is future-dated): both are checked
    result = validate_document(doc, KNOWN, seen=doc.row_hashes(), at_ms=2_000)
    assert [(p.code, p.row_id) for p in result.problems] == [("symbol_unknown", 1), ("symbol_unknown", 2)]
    # with no time every seen row counts as live
    assert validate_document(doc, KNOWN, seen=doc.row_hashes()).status is ValidationStatus.INVALID


def test_a_pending_typo_is_refused_only_while_it_is_live() -> None:
    accepted = parsed(raw_doc())
    seen = accepted.row_hashes()
    typo = assignment_row(2, symbol="TYPO")
    # live (enabled and in force for its key): refused
    result = validate_document(parsed(raw_doc(assignments=[assignment_row(), typo])), KNOWN, seen=seen, at_ms=6_000)
    assert [(p.code, p.row_id) for p in result.problems] == [("symbol_unknown", 2)]
    # future-dated: live, refused
    future = assignment_row(2, symbol="TYPO", effective_from_ms=9_000)
    result = validate_document(parsed(raw_doc(assignments=[assignment_row(), future])), KNOWN, seen=seen,
                               at_ms=6_000)
    assert [(p.code, p.row_id) for p in result.problems] == [("symbol_unknown", 2)]
    # tombstoned by a later pending row (append-only: the typo can never be deleted): the document stays valid
    tomb = assignment_row(3, symbol="TYPO", enabled=False, effective_from_ms=2_000)
    doc = parsed(raw_doc(assignments=[assignment_row(), typo, tomb]))
    result = validate_document(doc, KNOWN, seen=seen, at_ms=6_000)
    assert result.status is ValidationStatus.VALID, [p.page_text for p in result.problems]
    # before the tombstone applies the typo is in force again: refused (the tombstone is future-dated: refused too)
    result = validate_document(doc, KNOWN, seen=seen, at_ms=1_500)
    assert [(p.code, p.row_id) for p in result.problems] == [("symbol_unknown", 2), ("symbol_unknown", 3)]
    # a typo route row superseded by a later row of the same route is not live; the later one is (route_policy
    # has no tombstone, so a typo route stays refused: the plan records this for the user's decision)
    routes = [route_row(), route_row(2, route="typo_route"), route_row(3, route="typo_route", effective_from_ms=2_000)]
    result = validate_document(parsed(raw_doc(route_policy=routes)), KNOWN, seen=seen, at_ms=6_000)
    assert [(p.code, p.row_id) for p in result.problems] == [("route_unknown", 3)]
