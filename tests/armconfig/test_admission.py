"""The admission check (plan S33, R11-M20): series count, §A5 bytes, BARS reads per bar end with the PR9 stop reads
and the REOPEN cadence. Synthetic instruments only."""
from __future__ import annotations

from src.armconfig.admission import (
    MEMBER_BYTES,
    PREWARM_CAP,
    STOP_ANCHOR_READS,
    AdapterLoad,
    AdmissionLimits,
    admission,
    live_assignments,
)
from src.armconfig.validate import KnownIds
from tests.armconfig.docs import assignment_row, raw_doc, valid

SYMS = tuple(f"SYN.{c}" for c in "ABCDEFGHIJKL")
KNOWN = KnownIds(symbols=frozenset(SYMS), adapters=frozenset({"mock", "mock2"}),
                 accounts={"acct-1": "acct-1"}, routes=frozenset({"main"}))
NOW = 10**13


def load(**kw: object) -> AdapterLoad:
    base: dict[str, object] = {"adapter_id": "mock", "symbols": frozenset(SYMS), "prewarm": SYMS[:8],
                               "bars_per_min": 60}
    base.update(kw)
    return AdapterLoad(**base)  # type: ignore[arg-type]


def explicit(n: int, adapter: str | None = "mock") -> list[dict[str, object]]:
    return [assignment_row(i + 1, symbol=SYMS[i], adapter=adapter) for i in range(n)]


def test_v1_demand_of_eight_symbols_is_admitted_with_its_numbers() -> None:
    doc = valid(raw_doc(assignments=explicit(8)), KNOWN)
    report = admission(doc, [load()], at_ms=NOW)
    assert report.admitted, report.refusals
    (a,) = report.adapters
    assert a.symbols == 8 and a.tick_series == 8 * 4 and a.broker_series >= 8 and a.indicators >= 8
    assert a.reads_per_bar_end == a.broker_series * 2 and a.stop_reads_per_bar_end == 0
    assert a.read_budget == 60 and a.reopen_budget == 120
    # §A5: about 50-60 MB a symbol; 8 symbols plus md:events fit the 512 MiB target
    assert 40 * 2**20 < a.bytes // 8 < 70 * 2**20
    assert report.total_bytes <= report.byte_limit == 2**29


def test_more_symbols_than_the_adapter_ceiling_are_refused() -> None:
    doc = valid(raw_doc(assignments=explicit(9)), KNOWN)
    report = admission(doc, [load()], at_ms=NOW)
    assert not report.admitted
    assert any("9 decision symbols > ceiling 8" in r for r in report.refusals)


def test_a_wildcard_covers_the_prewarm_set_capped_per_adapter() -> None:
    doc = valid(raw_doc(assignments=[assignment_row(1)]), KNOWN)
    report = admission(doc, [load(prewarm=SYMS), load(adapter_id="mock2", prewarm=SYMS[:2])], at_ms=NOW)
    assert [a.symbols for a in report.adapters] == [PREWARM_CAP, 2]


def test_bars_reads_over_the_budget_are_refused_with_the_numbers() -> None:
    doc = valid(raw_doc(assignments=explicit(4)), KNOWN)
    report = admission(doc, [load(bars_per_min=5)], at_ms=NOW)
    (a,) = report.adapters
    assert any(f"{a.reads_per_bar_end} BARS reads per bar end" in r and "> budget 5" in r for r in report.refusals)
    assert any("BARS reads at a reopen" in r for r in report.refusals)


def test_stop_reads_are_one_list_read_per_account_per_anchor_not_per_handle() -> None:
    doc = valid(raw_doc(assignments=explicit(2)), KNOWN)
    off = admission(doc, [load(armed_handles=16, stop_accounts=1)], at_ms=NOW).adapters[0]
    on = admission(doc, [load(stop_reads=True, armed_handles=16, stop_accounts=1)], at_ms=NOW).adapters[0]
    two = admission(doc, [load(stop_reads=True, armed_handles=16, stop_accounts=2)], at_ms=NOW).adapters[0]
    assert off.stop_reads_per_bar_end == 0
    assert on.stop_reads_per_bar_end == STOP_ANCHOR_READS and on.armed_handles == 16
    assert two.stop_reads_per_bar_end == 2 * STOP_ANCHOR_READS
    assert on.reads_per_bar_end == off.reads_per_bar_end + STOP_ANCHOR_READS
    assert on.reopen_reads == off.reopen_reads + 2 * STOP_ANCHOR_READS
    tight = admission(doc, [load(stop_reads=True, armed_handles=16, stop_accounts=1,
                                 bars_per_min=off.reads_per_bar_end)], at_ms=NOW)
    assert any("stop reads" in r for r in tight.refusals)


def test_bytes_over_the_memory_target_are_refused() -> None:
    doc = valid(raw_doc(assignments=explicit(2)), KNOWN)
    report = admission(doc, [load()], AdmissionLimits(maxmemory_bytes=128 * 2**20), at_ms=NOW)
    assert any(r.startswith("redis-md:") and "bytes at full retention" in r for r in report.refusals)
    assert MEMBER_BYTES == 270


def test_tombstones_and_superseded_rows_add_nothing_future_rows_do() -> None:
    rows = [assignment_row(1, symbol="SYN.A", adapter="mock", effective_from_ms=1_000),
            assignment_row(2, symbol="SYN.A", adapter="mock", effective_from_ms=2_000, enabled=False),
            assignment_row(3, symbol="SYN.B", adapter="mock", effective_from_ms=NOW + 10)]
    doc = valid(raw_doc(assignments=rows), KNOWN)
    assert [a.row_id for a in live_assignments(doc, NOW)] == [3]
    assert admission(doc, [load()], at_ms=NOW).adapters[0].symbols == 1


def test_adapters_without_bars_or_cap_are_noted_not_refused() -> None:
    doc = valid(raw_doc(assignments=[assignment_row(1, symbol="SYN.A")]), KNOWN)
    report = admission(doc, [load(bar_capable=False), load(adapter_id="mock2", bars_per_min=None)], at_ms=NOW)
    assert report.admitted and [a.symbols for a in report.adapters] == [0, 1]
    assert any("serves no broker bars" in n for n in report.notes)
    assert any("unmetered" in n for n in report.notes)
