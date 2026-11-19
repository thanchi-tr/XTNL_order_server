"""Round-13 hardening on the market-data side (plan S38d).

* D4 finality: until PR3 proves the broker stops amending bar `T` once `T + 1` appears, a later bucket alone never
  makes a bar final; two equal reads `broker_bar_settle_s` apart do. `awaiting_verification` holds until the
  verification re-read has answered; `verified` only once it positively confirmed the bar (round 13 review).
* D6: the block threshold K is at least 2 (one mismatch only flags).
* The PRE quote ring: `MdHook.quote_at(at_ms)` is the quote as it was at `at_ms`.
* `md-indicators` runs on its own bounded executor: hung indicator jobs never starve the loop's default executor.

Synthetic instruments only; nothing here prints a price.
"""
from __future__ import annotations

import asyncio
import threading
from decimal import Decimal

import pytest

from src.marketdata.config import MISMATCH_K_MAX, MISMATCH_K_MIN
from src.marketdata.crosscheck import CrossChecker
from src.marketdata.indicators.engine import THREAD_PREFIX, IndicatorSettings, MdIndicators
from src.marketdata.ticks import QUOTE_KEEP_MS, MdHook
from src.marketdata.types import SeriesId
from tests.marketdata.test_brokerbars import B, S1, T, make
from tests.marketdata.test_ticks import AD, SYM, dto

D = Decimal


# ---------------------------------------------------------------------------------------------------- finality
async def test_a_later_bucket_alone_is_not_finality_until_pr3() -> None:
    rig = make(next_bucket=False)
    await rig.run_to(T + 5_000)
    assert rig.sink.of(S1, B) == []                      # one read, T + 1 in the answer: not final on that alone
    await rig.run_to(T + 16_000)
    [bar] = rig.sink.of(S1, B)                           # two equal reads at least settle (10 s) apart
    assert bar.final_at_ms is not None and bar.final_at_ms - T >= 10_000


async def test_awaiting_verification_until_the_reread_answers() -> None:
    rig = make()
    await rig.run_to(T + 5_000)
    assert rig.sink.of(S1, B) != []
    assert rig.bars.awaiting_verification(S1, B)         # final, verification re-read due at final + 60 s
    await rig.run_to(T + 70_000)
    assert not rig.bars.awaiting_verification(S1, B)
    assert not rig.bars.awaiting_verification(S1, B - 900_000)


async def test_verified_only_after_a_confirming_reread() -> None:
    """Round 13 review: `verified` is True only once the verification re-read confirmed the bucket (the same hash,
    or a revision that was stored); the ARM releases only on such a bar."""
    rig = make()
    await rig.run_to(T + 30_000)
    assert not rig.bars.verified(S1, B)                  # final, not re-read yet
    await rig.run_to(T + 70_000)
    assert rig.bars.verified(S1, B) and rig.bars.counters.verified >= 1
    revised = make()
    await revised.run_to(T + 30_000)
    revised.broker.put(B, close="1.1007")
    await revised.run_to(T + 70_000)
    assert [b.revision for b in revised.sink.of(S1, B)] == [0, 1] and revised.bars.verified(S1, B)


@pytest.mark.parametrize("case", ["missing_from_answer", "failed_reread", "store_unreadable"])
async def test_an_inconclusive_reread_never_verifies(case: str) -> None:
    from src.marketdata.types import Transient
    rig = make()
    await rig.run_to(T + 30_000)
    if case == "missing_from_answer":
        rig.broker.hidden.add(B)
    elif case == "failed_reread":
        rig.broker.fail = lambda symbol, lo, hi: Transient("timeout")
    else:
        assert rig.stored is not None
        rig.stored.down = True
    await rig.run_to(T + 70_000)
    assert rig.bars.counters.verify_inconclusive >= 1
    assert not rig.bars.verified(S1, B) and not rig.bars.awaiting_verification(S1, B)


async def test_no_verification_time_left_and_untracked_never_verify() -> None:
    rig = make(start=T - 5_000)
    rig.broker.lag_ms = 880_000                          # the bar appears 20 s before its cap
    await rig.run_to(T + 905_000, every=5_000)
    assert B in rig.bars.unverified(S1) and not rig.bars.verified(S1, B)
    other = SeriesId(S1.adapter_id, "SYN9", S1.spec, S1.side, S1.source)
    assert not rig.bars.verified(other, B)               # untracked (e.g. after a restart: deviation 4)


def test_the_verified_book_is_bounded() -> None:
    from src.marketdata.brokerbars import VERIFIED_KEEP, _Series
    rig = make()
    st = _Series(S1, None, 4)
    for k in range(VERIFIED_KEEP + 10):
        rig.bars._note_verified(st, k)  # noqa: SLF001
    assert len(st.verified) == VERIFIED_KEEP and 0 not in st.verified and VERIFIED_KEEP + 9 in st.verified


# ---------------------------------------------------------------------------------------------------- D6
def test_one_mismatch_never_blocks() -> None:
    assert (MISMATCH_K_MIN, MISMATCH_K_MAX) == (2, 10)
    with pytest.raises(ValueError):
        CrossChecker(object(), mismatch_k=1)  # type: ignore[arg-type]
    CrossChecker(object(), mismatch_k=2)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------------------------------- quote ring
def _tick(hook: MdHook, received_ms: int, bid: str) -> None:
    hook.on_price(dto(bid, received_ms, received_ms=received_ms))


def test_quote_at_returns_the_quote_as_it_was_at_fire() -> None:
    now = [1_000_000]
    hook = MdHook(clock_ms=lambda: now[0])
    fire = 1_010_000
    for k, t in enumerate(range(fire - 2_000, fire + 1_000, 100)):     # 10 ticks a second across the fire
        _tick(hook, t, f"1.{1000 + k}")
    at_fire = hook.quote_at(AD, SYM, fire)
    assert at_fire is not None and at_fire.received_ms == fire
    assert hook.quote(AD, SYM).received_ms > fire  # type: ignore[union-attr]
    between = hook.quote_at(AD, SYM, fire + 50)
    assert between is not None and between.received_ms == fire
    assert hook.quote_at(AD, SYM, fire - 10_000) is None        # nothing received by then
    assert hook.quote_at("other", "SYN1", fire) is None


def test_a_failing_tick_clears_the_quote_from_its_time_on() -> None:
    hook = MdHook(clock_ms=lambda: 0)
    _tick(hook, 1_000, "1.1000")
    hook.on_price(dto("1.2", 2_000, received_ms=2_000, ask="1.1"))                        # ask below bid
    assert hook.quote_at(AD, SYM, 1_500) is not None
    assert hook.quote_at(AD, SYM, 2_500) is None


def test_the_ring_is_bounded_in_time_and_keeps_one_older_answer() -> None:
    hook = MdHook(clock_ms=lambda: 0)
    _tick(hook, 1_000, "1.1000")
    late = 1_000 + 3 * QUOTE_KEEP_MS
    for t in range(late, late + 3_000, 500):
        _tick(hook, t, "1.1001")
    got = hook.quote_at(AD, SYM, late - 1)
    assert got is not None and got.received_ms == 1_000     # the newest older entry still answers
    m = hook._markets[(AD, SYM)]  # noqa: SLF001
    assert len(m.quotes) <= 8


# ---------------------------------------------------------------------------------------------------- own executor
async def test_hung_indicator_jobs_leave_the_default_executor_responsive() -> None:
    """N hung indicator jobs (more than the default executor would hold for them) run on md-indicators' own
    executor: `run_in_executor(None, ...)` still answers at once, and `close()` reports the hung threads."""
    eng = MdIndicators(object(), settings=IndicatorSettings(max_threads=3, max_hung_threads=3,  # type: ignore[arg-type]
                                                            job_timeout_s=0.05))
    release = threading.Event()
    names: list[str] = []

    def hang() -> int:
        names.append(threading.current_thread().name)
        release.wait(5)
        return 1

    for iid in ("i1", "i2", "i3"):
        with pytest.raises(TimeoutError):
            await eng._in_thread(iid, hang)  # noqa: SLF001
    assert eng.hung_threads == 3
    assert all(n.startswith(THREAD_PREFIX) for n in names)
    loop = asyncio.get_running_loop()
    got = await asyncio.wait_for(loop.run_in_executor(None, lambda: 7), 1.0)
    assert got == 7
    assert eng.close() is True                            # a hung worker: the stop path must hard-exit
    release.set()
    await asyncio.sleep(0.1)
    with pytest.raises(RuntimeError):
        await eng._own_executor(lambda: 1)  # noqa: SLF001
