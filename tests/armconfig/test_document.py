"""The configuration document: shape, canonical form, row and document hashes (plan S25; design §B9)."""
from __future__ import annotations

import json
from decimal import Context, Decimal, Inexact, localcontext

import pytest

from src.armconfig.document import (
    INT_LIMIT,
    MAX_ROWS_PER_TABLE,
    CanonicalError,
    Document,
    DocumentProblem,
    canonical_json,
    parse_document,
)
from tests.armconfig.docs import FIRST_BAR, assignment_row, parsed, raw_doc, route_row, strategy_row


def test_canonical_form_is_stable_and_compact() -> None:
    assert canonical_json({"b": [1, Decimal("1.50"), None, True], "a": "x"}) == '{"a":"x","b":[1,1.5,null,true]}'
    assert canonical_json(Decimal("5.0")) == "5" == canonical_json(5)
    assert canonical_json(Decimal("0.000")) == "0"
    assert canonical_json(Decimal("1E+3")) == "1000"
    assert canonical_json("‮\n") == '"\\u202e\\n"'


@pytest.mark.parametrize("value", [1.5, Decimal("NaN"), Decimal("Infinity"), Decimal("1E+999"), {1: "x"}, object(),
                                   [[[[[[[[[[1]]]]]]]]]], Decimal("1e1000000"), Decimal("-1e1000000"),
                                   Decimal("1e-1000000"), pytest.param(10**5000, id="int_5001_digits"),
                                   pytest.param(-(10**5000), id="neg_int_5001_digits"), INT_LIMIT,
                                   Decimal("1." + "1" * 200)])
def test_values_without_a_canonical_form(value: object) -> None:
    with pytest.raises(CanonicalError):
        canonical_json(value)


def test_numbers_are_never_rounded_and_never_depend_on_the_decimal_context() -> None:
    precise = Decimal("1.00000000000000000000000000001")      # 30 significant digits: past the default 28
    assert canonical_json(precise) == "1.00000000000000000000000000001"
    assert canonical_json(Decimal("1." + "0" * 300)) == "1"     # only trailing zeros go
    assert canonical_json(INT_LIMIT - 1) == canonical_json(Decimal(INT_LIMIT - 1))
    row = raw_doc(strategies=[strategy_row(params={"guards": FIRST_BAR, "be_offset_r": precise})])
    outside = parsed(row)
    for prec in (5, 28, 200):
        with localcontext(Context(prec=prec, traps=[Inexact])):
            assert canonical_json(precise) == "1.00000000000000000000000000001"
            assert parsed(row).doc_hash == outside.doc_hash


def test_untrusted_json_numbers_never_raise() -> None:
    def read(text: str) -> object:
        return parse_document(json.loads(text, parse_float=Decimal))

    meta = '"set_at_ms":1,"session_user":"a","current_user":"b"'
    for value in ("1e1000000", "-1e1000000", "1e-999999999", "1" * 100, "-" + "1" * 100, "1." + "1" * 200):
        text = ('{"schema_version":1,"assignments":[],"route_policy":[],"strategies":[{"id":1,"code":"x",'
                f'"version":1,"kind":"k","impl_version":1,{meta},"params":{{"x":{value}}}}}]}}')
        problem = read(text)
        assert isinstance(problem, DocumentProblem) and problem.code == "value_form", value
    for bad_id in ("1e999999999", "1" * 100):   # json.loads itself refuses ints over 4300 digits
        text = f'{{"schema_version":1,"assignments":[],"route_policy":[],"strategies":[{{"id":{bad_id}}}]}}'
        problem = read(text)
        assert isinstance(problem, DocumentProblem) and problem.code == "row_id_form"
    problem = read('{"schema_version":1e999999999,"strategies":[],"assignments":[],"route_policy":[]}')
    assert isinstance(problem, DocumentProblem) and problem.code == "schema_version_form"


def test_rows_in_document_order_with_hashes() -> None:
    doc = parsed(raw_doc(strategies=[strategy_row(2, code="be_b"), strategy_row(1)],
                         assignments=[assignment_row(5)], route_policy=[route_row(3)]))
    assert [r.ref for r in doc.rows] == ["strategies:1", "strategies:2", "assignments:5", "route_policy:3"]
    assert all(len(r.row_hash) == 64 for r in doc.rows)
    assert doc.rows[0].audit_ref == f"strategies:1:{doc.rows[0].row_hash}"
    assert doc.row_hashes()["assignments:5"] == doc.rows[2].row_hash
    assert len(doc.doc_hash) == 64 and not doc.schema_ahead


def test_row_text_stays_out_of_repr() -> None:
    doc = parsed(raw_doc(strategies=[strategy_row(code="secret_code")]))
    assert "secret_code" not in repr(doc)


def test_hashes_are_stable_across_reads_and_encodings() -> None:
    a = parsed(raw_doc(strategies=[strategy_row(params={"be_offset_r": Decimal("0.10"), "guards": [
        {"kind": "first_bar", "version": 1, "params": {}}]})]))
    b = parsed(raw_doc(strategies=[strategy_row(params={"guards": [
        {"kind": "first_bar", "version": Decimal("1"), "params": {}}], "be_offset_r": Decimal("0.1")})]))
    assert a.doc_hash == b.doc_hash
    assert a.row_hashes() == b.row_hashes()


def test_any_change_changes_the_row_and_document_hash() -> None:
    a = parsed(raw_doc())
    for changed in (raw_doc(strategies=[strategy_row(version=2)]),
                    raw_doc(strategies=[strategy_row(set_at_ms=1)]),
                    raw_doc(strategies=[strategy_row(session_user="someone_else")])):
        b = parsed(changed)
        assert b.row_hashes()["strategies:1"] != a.row_hashes()["strategies:1"]
        assert b.doc_hash != a.doc_hash
    assert parsed(raw_doc(schema_version=2)).doc_hash != a.doc_hash


def test_same_content_in_two_tables_hashes_differently() -> None:
    doc = parsed(raw_doc(route_policy=[route_row(1)], assignments=[assignment_row(1)]))
    assert doc.row_hashes()["assignments:1"] != doc.row_hashes()["route_policy:1"]


def test_empty_read() -> None:
    doc = parsed(raw_doc(strategies=[], assignments=[], route_policy=[]))
    assert doc.empty and doc.rows == () and len(doc.doc_hash) == 64


def test_schema_ahead_parses() -> None:
    doc = parse_document(raw_doc(schema_version=2))
    assert isinstance(doc, Document) and doc.schema_ahead


def test_schema_ahead_may_add_a_table() -> None:
    doc = parse_document(raw_doc(schema_version=2, edges=[{"id": 1, "code": "e"}]))
    assert isinstance(doc, Document) and doc.schema_ahead
    assert doc.doc_hash == parsed(raw_doc(schema_version=2)).doc_hash      # only the known tables are hashed
    assert [r.table for r in doc.rows] == ["strategies", "assignments", "route_policy"]
    # envelope and sink keys are still refused by name; the current schema still refuses the new table
    for raw, code in ((raw_doc(schema_version=2, pre_offset=40), "envelope_key"),
                      (raw_doc(schema_version=2, sinks=[]), "sink_key"),
                      (raw_doc(schema_version=1, edges=[]), "unknown_key")):
        problem = parse_document(raw)
        assert isinstance(problem, DocumentProblem) and problem.code == code


@pytest.mark.parametrize("raw, code", [
    ([], "not_an_object"),
    (raw_doc(extra_key=1), "unknown_key"),
    (raw_doc(pre_offset=40), "envelope_key"),
    (raw_doc(min_gap_r=0), "envelope_key"),
    (raw_doc(sinks=["x"]), "sink_key"),
    ({"schema_version": 1, "strategies": [], "assignments": []}, "missing_key"),
    (raw_doc(schema_version=0), "schema_version_form"),
    (raw_doc(schema_version=True), "schema_version_form"),
    (raw_doc(strategies={"id": 1}), "table_form"),
    (raw_doc(strategies=["row"]), "row_form"),
    (raw_doc(strategies=[strategy_row(id="1")]), "row_id_form"),
    (raw_doc(strategies=[strategy_row(id=0)]), "row_id_form"),
    (raw_doc(strategies=[strategy_row(1), strategy_row(1, code="other")]), "duplicate_row_id"),
    (raw_doc(strategies=[strategy_row(params={"be_offset_r": 0.1})]), "value_form"),
    (raw_doc(strategies=[strategy_row(note="x" * 9000)]), "row_too_large"),
    (raw_doc(assignments=[assignment_row(n) for n in range(1, MAX_ROWS_PER_TABLE + 2)]), "too_many_rows"),
])
def test_document_problems(raw: object, code: str) -> None:
    problem = parse_document(raw)
    assert isinstance(problem, DocumentProblem)
    assert problem.code == code


def test_document_problems_never_echo_text() -> None:
    problem = parse_document(raw_doc(**{"evil‮key": "planted-value"}))
    assert isinstance(problem, DocumentProblem)
    assert "planted" not in f"{problem} {problem.page_text}" and "evil" not in problem.page_text
    dup = parse_document(raw_doc(strategies=[strategy_row(7, code="planted"), strategy_row(7)]))
    assert isinstance(dup, DocumentProblem) and dup.page_text == "duplicate_row_id strategies:7"
