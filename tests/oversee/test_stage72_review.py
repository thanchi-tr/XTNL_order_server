"""IMPLEMENTATION_PLAN 7.2 review: a `stale_mark` state that only a closed market (or its roll break) explains is
not re-paged (operations.md §10 "While CLOSED or in ROLLOVER, only price staleness is suppressed"); a frozen feed on
an OPEN symbol still re-pages every escalate_after_ms (oversee-rules.md §10)."""
from __future__ import annotations

from src.oversee.rules import STALE_MARK
from src.oversee.snapshot import SymbolQuotes
from tests.oversee.helpers import AD, ACCT, T0, clean_mark, position, quote
from tests.oversee.test_stage72 import ESC, MARK_MS, repages, with_dispatcher
from tests.oversee.test_sweep import Env

KEY = (STALE_MARK, "account", ACCT)


def closed(session: str = "CLOSED") -> dict[tuple[str, str], SymbolQuotes]:
    q = quote("1.1000", T0)
    return {(AD, "EURUSD"): SymbolQuotes(latest=q.latest, last_clean=clean_mark("1.1000", T0),
                                         session=session)}  # type: ignore[arg-type]


async def test_a_closed_market_stale_mark_is_alerted_once_and_never_repaged_all_weekend() -> None:
    env = Env(positions=(position("P", stop="1.0990"),), quotes=closed())
    disp = with_dispatcher(env)
    await env.at(T0 + 1_000)
    assert env.state(KEY).active
    await disp.drain(T0 + 1_000)
    episodes = [r for r in env.db.t.outbox if r.kind == "episode" and STALE_MARK in r.title]
    assert len(episodes) == 1                                  # the activation still alerts (one email)
    for n in range(1, 9):                                      # two hours of a closed market
        await env.at(T0 + 1_000 + n * ESC)
    assert repages(env, STALE_MARK) == []


async def test_a_roll_break_stale_mark_is_not_repaged_either() -> None:
    env = Env(positions=(position("P", stop="1.0990"),), quotes=closed("ROLLOVER"))
    with_dispatcher(env)
    await env.at(T0 + 1_000)
    await env.at(T0 + 1_000 + 2 * ESC)
    assert env.state(KEY).active and repages(env, STALE_MARK) == []


async def test_a_frozen_feed_on_an_open_symbol_after_the_close_repages_again() -> None:
    env = Env(positions=(position("P", stop="1.0990"),), quotes=closed())
    disp = with_dispatcher(env)
    await env.at(T0 + 1_000)
    await disp.drain(T0 + 1_000)
    await env.at(T0 + 1_000 + ESC)
    assert repages(env, STALE_MARK) == []
    env.kw.pop("quotes")                                       # the symbol opens, but its feed is frozen
    env.stale_quotes = True
    await env.at(T0 + 1_000 + ESC + MARK_MS + 1_000)
    assert env.state(KEY).active
    assert repages(env, STALE_MARK) == ["info"]

