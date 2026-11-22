"""2026-11-20 (oversee-rules.md §10, accepted deviation): a flat account re-pages nothing.

Seen live: after every position was closed, the day's `max_daily_loss` FLAG (`transfers_unverified`) was re-paged
every escalate_after_ms all night, and again at each restart. Flat = no OPEN position, no working order, no margin in
use, no integrity incident, and an image that is current (no lost orders / positions / account stream, no active
stale_account_image). While flat, the per-day FLAG alerts are not sent either (`_quiet_day_flag`). Unsure = nothing
open but the image may be missing something: re-pages wait at most FLAT_UNSURE_MAX_MS, per-day FLAG alerts are sent.
The states, every SOFT / HARD alert and the halt level are unchanged; re-pages resume on the first pass that sees
exposure again. Clock injected; in-memory store."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from src.dto import OrderStatus
from src.oversee.dayroll import NEW_YORK
from src.oversee.rules import STALE_ACCOUNT_IMAGE
from src.oversee.snapshot import StreamHealthRow
from src.oversee.sweep import FLAT_UNSURE_MAX_MS, FlatView, _flat_account, _flat_view, _quiet_day_flag
from src.oversee.types import BreachState, Cap, Level, SubjectType, Transition, TransitionKind
from tests.oversee.helpers import ACCT, AD, T0, account, live, order, position, snap
from tests.oversee.test_stage72 import ESC, flag_state, repages, with_dispatcher
from tests.oversee.test_sweep import Env, day_state, losing_day

DAY = "max_daily_loss"
NO_MARGIN = live("0")


def flat(**kw: object) -> bool:
    states = kw.pop("states", [])
    return _flat_account(snap(**({"account_live": NO_MARGIN} | kw)), states)  # type: ignore[arg-type]


# ---------------------------------------------------------------- the pure predicate
def test_no_position_no_order_and_no_margin_is_flat() -> None:
    assert flat()
    assert flat(account_live=None)                                   # no live fields at all: nothing says exposure
    assert flat(account_live=live(None))                             # margin not reported


def test_an_open_position_or_a_working_order_is_exposure() -> None:
    assert not flat(positions=(position("P", stop="1.09"),))
    for status in (OrderStatus.WORKING, OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED):
        assert not flat(orders=(order("O", status=status),))


def test_closed_positions_and_terminal_orders_are_flat() -> None:
    closed = replace(position("P", stop="1.09"), status="CLOSED")
    assert flat(positions=(closed,), orders=(order("O", status=OrderStatus.FILLED),
                                             order("C", status=OrderStatus.CANCELLED)))


def test_margin_in_use_is_exposure_the_image_may_be_missing() -> None:
    assert not flat(account_live=live("1"))
    margin = flag_state(rule_id="margin_usage", reason="margin_usage")
    assert not flat(states=[margin])


def test_a_stale_or_fenced_image_is_never_flat() -> None:
    for stream in ("orders", "positions", "account"):
        assert not flat(stream_health=(StreamHealthRow(AD, stream, T0 - 1),))
    assert not flat(integrity_incident=True)
    image = flag_state(rule_id=STALE_ACCOUNT_IMAGE, reason=STALE_ACCOUNT_IMAGE)
    assert not flat(states=[image])
    assert flat(states=[replace(image, level=Level.PASS)])           # resolved: the image is current again


def test_a_lost_prices_feed_or_another_adapters_stream_does_not_block_flat() -> None:
    assert flat(stream_health=(StreamHealthRow(AD, "prices", T0 - 1),           # hides no order or position
                               StreamHealthRow("other", "orders", T0 - 1),
                               StreamHealthRow(AD, "orders", None)))



def view(**kw: object) -> FlatView:
    states = kw.pop("states", [])
    return _flat_view(snap(**({"account_live": NO_MARGIN} | kw)), states)  # type: ignore[arg-type]


def test_nothing_open_with_a_lost_stream_or_a_stale_image_is_unsure_since_the_earliest() -> None:
    lost = (StreamHealthRow(AD, "orders", T0 - 5_000), StreamHealthRow(AD, "account", T0 - 2_000))
    assert view(stream_health=lost) == FlatView(False, T0 - 5_000)
    image = flag_state(rule_id=STALE_ACCOUNT_IMAGE, reason=STALE_ACCOUNT_IMAGE, active_since=T0 - 9_000)
    assert view(stream_health=lost, states=[image]) == FlatView(False, T0 - 9_000)
    assert view(states=[replace(image, active_since=None)]) == FlatView(False)       # no time to wait from: re-page


def test_exposure_in_the_image_or_an_integrity_incident_is_never_unsure() -> None:
    lost = (StreamHealthRow(AD, "positions", T0),)
    assert view(stream_health=lost, positions=(position("P", stop="1.09"),)) == FlatView(False)
    assert view(stream_health=lost, orders=(order("O"),)) == FlatView(False)
    assert view(stream_health=lost, account_live=live("1")) == FlatView(False)
    assert view(stream_health=lost, integrity_incident=True) == FlatView(False)


def test_unsure_defers_repages_for_at_most_the_bound() -> None:
    assert FlatView(True).repages_held(T0, T0)
    unsure = FlatView(False, T0)
    assert unsure.repages_held(T0, T0) and unsure.repages_held(T0 + FLAT_UNSURE_MAX_MS - 1, T0)
    assert not unsure.repages_held(T0 + FLAT_UNSURE_MAX_MS, T0)
    assert not FlatView(False).repages_held(T0, T0)


def test_the_unsure_wait_runs_from_no_earlier_than_this_terms_start() -> None:
    """Review 2026-11-20: conn_lost_at and the stale_account_image row outlive a crashed / SAFE_HOLD-ended term."""
    left_over = FlatView(False, T0)                                  # set by a term that ended 8 min earlier
    start = T0 + 480_000
    assert left_over.repages_held(start + 30_000, start)
    assert not left_over.repages_held(start + FLAT_UNSURE_MAX_MS, start)

# ---------------------------------------------------------------- through the RiskSweep
async def _hard_day_then_flat(env: Env) -> None:
    disp = with_dispatcher(env)
    await env.at(T0)
    await env.at(T0 + 1_000)
    assert day_state(env).level is Level.HARD_HALT
    await disp.drain(T0 + 1_000)                                    # the activation alert is delivered
    assert len([r for r in env.db.t.outbox if r.kind == "episode" and DAY in r.title]) == 1
    env.kw.update(positions=(), account_live=NO_MARGIN)             # every position closed; the day's state stays


async def test_a_state_left_after_every_position_closed_is_not_repaged_while_flat() -> None:
    env = losing_day()
    await _hard_day_then_flat(env)
    for n in range(1, 9):                                           # two hours of the default cadence
        await env.at(T0 + 1_000 + n * ESC)
        assert day_state(env).active
    assert repages(env, DAY) == []


async def test_repages_resume_on_the_first_pass_with_exposure_again_then_once_per_cadence() -> None:
    env = losing_day()
    held = env.kw["positions"]
    await _hard_day_then_flat(env)
    await env.at(T0 + 1_000 + 3 * ESC)
    assert repages(env, DAY) == []
    back = T0 + 1_000 + 3 * ESC + 1_000
    env.kw["orders"] = (order("O"),)                                # a working order: exposure again
    await env.at(back)
    assert day_state(env).active and repages(env, DAY) == ["critical"]
    env.kw.update(orders=(), positions=held)
    for t in range(back + 60_000, back + ESC, 60_000):              # every minute of the next cadence: no repeat
        await env.at(t)
    assert repages(env, DAY) == ["critical"]


async def test_a_flat_account_with_a_lost_positions_stream_still_repages() -> None:
    env = losing_day()
    await _hard_day_then_flat(env)
    env.kw["stream_health"] = (StreamHealthRow(AD, "positions", T0 + 2_000),)    # the image may hide a position
    await env.at(T0 + 1_000 + ESC)
    assert repages(env, DAY) == ["critical"]



async def test_a_flat_account_at_a_restart_catch_up_fence_is_not_repaged() -> None:
    """Seen live 2026-11-20: LEADER_INIT fences every stream and the catch-up holds the fence for 7-22 s on every
    start; the flat account's FLAG, last delivered hours earlier, was re-paged at once."""
    env = losing_day()
    await _hard_day_then_flat(env)
    at = T0 + 1_000 + ESC
    env.kw["stream_health"] = (StreamHealthRow(AD, "positions", at), StreamHealthRow(AD, "orders", at))
    for t in range(at, at + 25_000, 1_000):                         # a slow catch-up, the stale image active
        await env.at(t)
    assert repages(env, DAY) == []
    env.kw["stream_health"] = ()                                    # the catch-up found nothing: still flat
    await env.at(at + 26_000)
    assert repages(env, DAY) == []


async def test_a_flat_looking_account_whose_streams_stay_lost_repages_after_the_bound() -> None:
    env = losing_day()
    await _hard_day_then_flat(env)
    at = T0 + 1_000 + ESC
    env.kw["stream_health"] = (StreamHealthRow(AD, "positions", at),)
    await env.at(at + FLAT_UNSURE_MAX_MS - 1_000)
    assert repages(env, DAY) == []
    await env.at(at + FLAT_UNSURE_MAX_MS)
    assert repages(env, DAY) == ["critical"]



async def test_a_restart_after_a_term_that_ended_fenced_does_not_repage_a_flat_account_at_once() -> None:
    env = losing_day()
    await _hard_day_then_flat(env)
    at = T0 + 1_000 + ESC
    env.kw["stream_health"] = (StreamHealthRow(AD, "positions", at - 480_000),)   # left over by the dead term
    env.db.engine_epoch += 1                                        # the new term
    for t in range(at, at + 25_000, 1_000):                         # its catch-up
        await env.at(t)
    assert repages(env, DAY) == []
    await env.at(at + FLAT_UNSURE_MAX_MS)                           # still lost after the bound: fail loud
    assert repages(env, DAY) == ["critical"]

async def test_the_day_roll_flag_alerts_go_out_while_the_image_may_be_missing_a_position() -> None:
    """A start across the day roll: the image shows nothing open, but the catch-up has not read the broker yet."""
    env = unverified_day()
    with_dispatcher(env)
    await env.every(T0, T0 + 2_000)
    env.kw.update(sod=None, stream_health=(StreamHealthRow(AD, "positions", NEXT_ROLL),))
    await env.every(NEXT_ROLL + 1_000, NEXT_ROLL + 3_000)
    titles = day_rows(env)
    assert any(t.startswith("FLAG") for t in titles) and any(t.startswith("resolved") for t in titles)


async def test_a_flat_account_with_margin_in_use_still_repages() -> None:
    env = losing_day()
    await _hard_day_then_flat(env)
    env.kw["account_live"] = live("1")                              # the broker still holds margin somewhere
    await env.at(T0 + 1_000 + ESC)
    assert repages(env, DAY) == ["critical"]


async def test_an_integrity_incident_on_a_flat_account_still_repages() -> None:
    env = losing_day()
    await _hard_day_then_flat(env)
    env.kw["integrity_incident"] = True
    await env.at(T0 + 1_000 + ESC)
    assert repages(env, DAY) != []


# ---------------------------------------------------------------- the day roll's FLAG alerts while flat
# Owner request 2026-11-20 (oversee-rules.md §10): while flat, a per-day (`account_day`) transition that stays at FLAG
# or below on both sides sends no alert; its oversee_events row is still written. SOFT / HARD always alert.
NEXT_ROLL = int(datetime(2026, 11, 11, 17, tzinfo=NEW_YORK).timestamp() * 1000)
DAY_KEY = (DAY, "account_day", f"{ACCT}:2026-11-12")


def tr(kind: TransitionKind, level: Level, subject_type: SubjectType = "account_day") -> Transition:
    return Transition(DAY, subject_type, DAY_KEY[2], ACCT, kind, 9, T0, level, Cap.NONE, "transfers_unverified")


def held_state(level: Level) -> dict[tuple[str, str, str], BreachState]:
    return {DAY_KEY: BreachState(DAY, "account_day", DAY_KEY[2], ACCT, level=level, active_since=T0)}


def test_quiet_day_flag_is_a_per_day_transition_at_flag_or_below_on_both_sides() -> None:
    assert _quiet_day_flag(tr("activated", Level.FLAG), {})                         # the new day's FLAG
    assert _quiet_day_flag(tr("resolved", Level.PASS), held_state(Level.FLAG))       # yesterday's FLAG, day_end
    assert _quiet_day_flag(tr("capped", Level.FLAG), held_state(Level.FLAG))
    assert not _quiet_day_flag(tr("activated", Level.HARD_HALT), {})
    assert not _quiet_day_flag(tr("raised", Level.HARD_HALT), held_state(Level.FLAG))
    assert not _quiet_day_flag(tr("lowered", Level.FLAG), held_state(Level.SOFT_HALT))
    assert not _quiet_day_flag(tr("resolved", Level.PASS), held_state(Level.HARD_HALT))
    assert not _quiet_day_flag(tr("activated", Level.FLAG, "account"), {})           # not a per-day state


def unverified_day(**kw: object) -> Env:
    return Env(**({"transfer_source": False, "account_live": NO_MARGIN, "positions": ()} | kw))  # type: ignore[arg-type]


def day_rows(env: Env) -> list[str]:
    return [r.title for r in env.db.t.outbox if DAY in r.title]


async def test_a_flat_day_roll_sends_no_flag_alerts_but_logs_the_events() -> None:
    env = unverified_day()
    with_dispatcher(env)
    await env.every(T0, T0 + 2_000)
    old = day_state(env)
    assert old.level is Level.FLAG and old.reason == "transfers_unverified"
    env.kw.update(sod=None)
    await env.every(NEXT_ROLL + 1_000, NEXT_ROLL + 3_000)
    assert env.db.t.breaches[old.key].resolved_by == "day_end"
    new = next(s for s in env.db.t.breaches.values() if s.rule_id == DAY and s.active)
    assert new.key != old.key and new.level is Level.FLAG
    assert env.events(DAY) == ["activated", "resolved", "activated"]
    assert day_rows(env) == []


async def test_the_day_roll_flag_alerts_still_go_out_with_a_position_open() -> None:
    env = unverified_day(positions=(position("P", stop="1.09"),))
    with_dispatcher(env)
    await env.every(T0, T0 + 2_000)
    env.kw.update(sod=None)
    await env.every(NEXT_ROLL + 1_000, NEXT_ROLL + 3_000)
    titles = day_rows(env)
    assert len([t for t in titles if t.startswith("FLAG")]) == 2 and any(t.startswith("resolved") for t in titles)


async def test_a_held_day_flag_repages_once_exposure_returns() -> None:
    env = unverified_day()
    with_dispatcher(env)
    await env.every(T0, T0 + 2_000)
    assert day_rows(env) == []
    await env.at(T0 + 1_000 + ESC, orders=(order("O"),))                             # a working order again
    assert repages(env, DAY) == ["info"]


async def test_a_hard_day_resolved_at_the_roll_still_alerts_while_flat() -> None:
    env = losing_day()
    await _hard_day_then_flat(env)
    env.kw.update(sod=None, account=account("100000"))
    await env.at(NEXT_ROLL + 1_000)
    assert any(t.startswith("resolved") for t in day_rows(env))
