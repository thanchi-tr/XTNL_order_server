"""The seen-row chain: added, changed and deleted rows; pending versus tampered (plan S25; design §B9)."""
from __future__ import annotations

import hashlib

import pytest

from src.armconfig.chain import (
    CHAIN_SEED,
    ChainError,
    ChainState,
    SeenRows,
    TamperedRow,
    TamperKind,
    chain_head,
    check_rows,
    verify_stored,
)
from src.db.arm_ports import ConfigCacheRow
from tests.armconfig.docs import assignment_row, parsed, raw_doc, route_row, strategy_row


def base() -> dict[str, object]:
    return raw_doc(strategies=[strategy_row(1)], assignments=[assignment_row(1)], route_policy=[route_row(1)])


def accepted() -> SeenRows:
    return SeenRows().extend(parsed(base()).row_hashes())


def check(seen: SeenRows, raw: dict[str, object]) -> tuple[ChainState, tuple[str, ...], tuple[TamperedRow, ...]]:
    v = check_rows(seen.rows, seen.head, seen.count, parsed(raw).row_hashes())
    return v.state, v.pending, v.tampered


def test_chain_head_is_a_fold_in_canonical_order() -> None:
    h1, h2 = "a" * 64, "b" * 64
    expected = hashlib.sha256(f"{CHAIN_SEED}\nstrategies:2\n{h1}".encode()).hexdigest()
    expected = hashlib.sha256(f"{expected}\nassignments:1\n{h2}".encode()).hexdigest()
    assert chain_head([("assignments:1", h2), ("strategies:2", h1)]) == expected
    assert chain_head([]) == CHAIN_SEED
    assert SeenRows.build([("strategies:10", h1), ("strategies:9", h2)]).rows[0][0] == "strategies:9"


def test_unchanged_read_is_clean() -> None:
    assert check(accepted(), base()) == (ChainState.CLEAN, (), ())


def test_empty_read_with_nothing_seen_is_clean() -> None:
    assert check(SeenRows(), raw_doc(strategies=[], assignments=[], route_policy=[])) == (ChainState.CLEAN, (), ())


def test_added_row_is_pending_until_seen() -> None:
    seen = accepted()
    raw = base()
    raw["assignments"] = [assignment_row(1), assignment_row(2, symbol="SYN.A", effective_from_ms=5_000)]
    assert check(seen, raw) == (ChainState.PENDING, ("assignments:2",), ())
    after = seen.extend(parsed(raw).row_hashes())                  # accepted: now seen
    assert after.count == seen.count + 1 and after.head != seen.head
    assert check(after, raw) == (ChainState.CLEAN, (), ())


def test_superseding_row_is_pending_not_tampered() -> None:
    raw = base()
    raw["assignments"] = [assignment_row(1), assignment_row(2, enabled=False, effective_from_ms=9_000)]
    state, pending, tampered = check(accepted(), raw)
    assert (state, pending, tampered) == (ChainState.PENDING, ("assignments:2",), ())


def test_changed_row_is_tampered() -> None:
    raw = base()
    raw["strategies"] = [strategy_row(1, params={"guards": [{"kind": "first_bar", "version": 1,
                                                              "params": {"multiplier": "2"}}]})]
    assert check(accepted(), raw) == (ChainState.TAMPERED, (), (TamperedRow("strategies:1", TamperKind.CHANGED),))


def test_deleted_row_is_tampered_even_with_a_new_row() -> None:
    raw = base()
    raw["route_policy"] = [route_row(2)]
    state, pending, tampered = check(accepted(), raw)
    assert state is ChainState.TAMPERED
    assert pending == ("route_policy:2",)
    assert tampered == (TamperedRow("route_policy:1", TamperKind.VANISHED),)


def test_empty_read_after_seen_rows_is_tampered() -> None:
    state, _, tampered = check(accepted(), raw_doc(strategies=[], assignments=[], route_policy=[]))
    assert state is ChainState.TAMPERED and {t.kind for t in tampered} == {TamperKind.VANISHED} and len(tampered) == 3


@pytest.mark.parametrize("mutate", [
    lambda s: (s.rows, "0" * 64, s.count),                               # head replaced
    lambda s: (s.rows, s.head, s.count + 1),                             # count does not match
    lambda s: (s.rows[1:], s.head, s.count - 1),                         # a stored row removed from the cache
    lambda s: (((s.rows[0][0], "f" * 64),) + s.rows[1:], s.head, s.count),   # a stored hash rewritten
    lambda s: ((("strategies:x", "f" * 64),) + s.rows, s.head, s.count + 1),  # ref out of form
    lambda s: (s.rows + s.rows[:1], s.head, s.count + 1),                # a ref twice
])
def test_broken_stored_chain_is_tampered(mutate: object) -> None:
    seen = accepted()
    rows, head, count = mutate(seen)  # type: ignore[operator]
    v = check_rows(rows, head, count, parsed(base()).row_hashes())
    assert v.state is ChainState.TAMPERED and v.tampered == (TamperedRow("chain", TamperKind.CHAIN_BROKEN),)
    assert verify_stored(rows, head, count) is None


def test_extend_never_takes_a_changed_seen_row() -> None:
    seen = accepted()
    changed = dict(seen.hashes())
    changed["strategies:1"] = "e" * 64
    with pytest.raises(ChainError):
        seen.extend(changed)
    assert seen.extend(seen.hashes()) == seen


def test_seen_rows_map_onto_the_cache_row() -> None:
    doc = parsed(base())
    seen = accepted()
    cache = ConfigCacheRow(doc_hash=doc.doc_hash, schema_version=doc.schema_version, document_json="{}",
                           row_hashes=seen.rows, chain_head=seen.head, chain_rows=seen.count, updated_at_ms=1)
    assert check_rows(cache.row_hashes, cache.chain_head, cache.chain_rows, doc.row_hashes()).state is ChainState.CLEAN
    assert verify_stored(cache.row_hashes, cache.chain_head, cache.chain_rows) == seen


def test_schema_ahead_read_skips_only_changed() -> None:
    seen = accepted()
    ahead = base()
    ahead["schema_version"] = 2
    ahead["strategies"] = [strategy_row(1, new_column="x")]          # a new column changes the row hash
    current = parsed(ahead).row_hashes()
    assert check_rows(seen.rows, seen.head, seen.count, current).state is ChainState.TAMPERED
    v = check_rows(seen.rows, seen.head, seen.count, current, schema_ahead=True)
    assert (v.state, v.tampered) == (ChainState.CLEAN, ())
    # a seen row deleted under a schema bump is still tampered
    ahead["route_policy"] = []
    v = check_rows(seen.rows, seen.head, seen.count, parsed(ahead).row_hashes(), schema_ahead=True)
    assert v.state is ChainState.TAMPERED and v.tampered == (TamperedRow("route_policy:1", TamperKind.VANISHED),)
    # and a broken stored chain is still broken
    v = check_rows(seen.rows, "0" * 64, seen.count, current, schema_ahead=True)
    assert v.tampered == (TamperedRow("chain", TamperKind.CHAIN_BROKEN),)


@pytest.mark.parametrize("rows", [
    [(1, "a" * 64)],                       # ref not text
    [("strategies:1", 5)],                 # hash not text
    [("strategies:1",)],                   # not a pair
    [None],
])
def test_stored_rows_out_of_type_are_a_broken_chain_never_an_exception(rows: list[object]) -> None:
    v = check_rows(rows, CHAIN_SEED, len(rows), {"strategies:1": "a" * 64})  # type: ignore[arg-type]
    assert v.tampered == (TamperedRow("chain", TamperKind.CHAIN_BROKEN),)
