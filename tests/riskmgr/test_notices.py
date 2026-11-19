"""Notices (S28, design §B1, §B5.2, §B8, D2, D3, C5, R11-M14, R11-M19): parts 0-4, the post-part-4 item notices,
part 5 at the cap, LATE items, suppression, folding, the timer notices and the ids-only outbox body."""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from src.db.arm_ports import DecisionHandleRow
from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.types import M15, Bar, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.guards.first_bar import pre_label
from src.riskmgr.notices import (
    ALIVE,
    END_PART,
    GLOBAL_ROUTE,
    MD_RESET,
    NoticeMeta,
    NoticeSettings,
    RouteBook,
    expires_at_ms,
    item_key_of,
    shown_by_mode,
    supersedes,
)
from src.riskmgr.render import (
    ALIVE_KEYS,
    BODY_KEYS,
    EXTRA_KEYS,
    FAILED_KEYS,
    FIRST_BAR_CONDITIONAL,
    ITEM_KEYS,
    PENDING_KEYS,
    PRE_HEADER,
    STILL_WORSE,
    render,
)
from src.riskmgr.sinks import ARM_SINKS
from src.riskmgr.tracker import NoticeKind, TrackerNotice
from tests.riskmgr.notice_fakes import (
    CAP_AFTER,
    ENTRY,
    ROUTE,
    S0,
    T1,
    T2,
    T3,
    TF,
    decision,
    rig,
    row,
    takeover,
)
from tests.riskmgr.tracker_fakes import ACCOUNT, ADAPTER

D = Decimal
KEY = Keyring.of(1, b"k" * 32)
BANNED = ("bid", "ask", "open", "high", "low", "close", "mid", "spread", "atr", "favour", "quote")


def hold(r, **kw):  # type: ignore[no-untyped-def]
    return decision(r, outcome="MARKET_CLOSED", category="HOLD", **kw)


def pids(n: int) -> list[str]:
    return [f"P{i}" for i in range(n)]


# ---------------------------------------------------------------------------------------------------- parts
async def test_eight_symbols_final_at_eight_times_each_get_their_adjust_in_their_own_pass() -> None:
    """R11-M19: parts 0-4, then each later ADJUST goes out at once as an item-keyed POST notice (part 0)."""
    g = rig()
    rows = [row(p, symbol=f"SYN.{i}") for i, p in enumerate(pids(8))]
    g.add(*rows)
    for i, r in enumerate(rows):
        before = len(g.outbox.rows)
        await g.commit("POST", T1, (decision(r, at=T1 + 70_000 + i * 10_000), True), at=T1 + 70_000 + i * 10_000)
        assert len(g.outbox.rows) == before + 1                 # its ADJUST left in this very commit
        assert g.store.notice_of(r.key.position_id) not in (None, 0)
    bodies = g.outbox.bodies("POST")
    assert [b.part for b in bodies] == [0, 1, 2, 3, 4, 0, 0, 0]
    assert [b.item_key for b in bodies[:5]] == [""] * 5
    assert [b.item_key for b in bodies[5:]] == [f"{ADAPTER}:P{i}:STOP_LEVEL:position:0" for i in (5, 6, 7)]
    assert all(len(b.items) == 1 and b.items[0].outcome == "ADJUST" for b in bodies)


async def test_six_positions_after_part_zero_give_parts_one_to_four_and_part_five_at_the_cap() -> None:
    g = rig()
    rows = [row(p) for p in pids(7)]
    g.add(*rows)
    for i, r in enumerate(rows):
        await g.commit("POST", T1, (hold(r, at=T1 + 70_000 + i * 1_000), True), at=T1 + 70_000 + i * 1_000)
    assert [b.part for b in g.outbox.bodies("POST")] == [0, 1, 2, 3, 4]
    assert g.store.notice_of("P5") is None and g.store.notice_of("P6") is None   # waiting for part 5
    g.clock.now = T1 + CAP_AFTER - 1
    await g.notices.step()
    assert [b.part for b in g.outbox.bodies("POST")] == [0, 1, 2, 3, 4]
    g.clock.now = T1 + CAP_AFTER
    await g.notices.step()
    last = g.outbox.bodies("POST")[-1]
    assert last.part == END_PART and [i.position_id for i in last.items] == ["P5", "P6"]
    assert g.store.notice_of("P5") == g.store.notice_of("P6") == last.notice_id
    assert g.notices.post_sent(rows[0], M15.name, T1)
    g.clock.now += 5_000
    await g.notices.step()                                    # nothing left: no second part 5
    assert len(g.outbox.bodies("POST")) == 6


async def test_part_five_only_with_items_and_a_post_after_it_goes_out_as_an_item() -> None:
    g = rig()
    rows = [row(p) for p in pids(7)]
    g.add(*rows)
    for i, r in enumerate(rows[:5]):
        await g.commit("POST", T1, (hold(r), True), at=T1 + 70_000 + i)
    g.clock.now = T1 + CAP_AFTER
    await g.notices.step()
    assert [b.part for b in g.outbox.bodies("POST")] == [0, 1, 2, 3, 4]   # no empty part 5
    assert g.notices.post_sent(rows[0], M15.name, T1)
    await g.commit("POST", T1, (hold(rows[5]), True), at=T1 + CAP_AFTER + 1_000)
    assert g.outbox.bodies("POST")[-1].part == END_PART                   # the window's one end part
    assert not g.outbox.bodies("POST")[-1].items[0].late
    await g.commit("POST", T1, (hold(rows[6]), True), at=T1 + CAP_AFTER + 2_000)
    late = g.outbox.bodies("POST")[-1]
    assert late.item_key == f"{ADAPTER}:P6:STOP_LEVEL:position:0"
    assert late.items[0].late and "LATE (decided after its bar's notice window)" in render(late).text   # §B8


async def test_two_late_items_in_one_bar_are_both_queued() -> None:
    g = rig()
    a, b = row("P1"), row("P2")
    g.add(a, b)
    await g.commit("LATE", T1, (decision(a, event="LATE"), True), (decision(b, event="LATE", outcome="BROKER_AHEAD",
                                                                            category="INFO"), True), at=T3)
    late = g.outbox.bodies("LATE")
    assert len(late) == 2 and {x.part for x in late} == {0}
    assert {x.item_key for x in late} == {f"{ADAPTER}:P1:STOP_LEVEL:position:0", f"{ADAPTER}:P2:STOP_LEVEL:position:0"}
    assert g.store.notice_of("P1", "LATE") != g.store.notice_of("P2", "LATE")


async def test_reopen_and_corrected_items_are_part_zero_with_their_own_keys() -> None:
    g = rig()
    a = row("P1")
    g.add(a)
    await g.commit("REOPEN", T1, (decision(a, event="REOPEN", at=T3 + 5), True), at=T3 + 5)
    await g.commit("CORRECTED", T1, (decision(a, event="CORRECTED", revision=1, outcome="BROKER_AHEAD",
                                              category="INFO", supersedes=1), True), at=T3 + 9)
    reopen, = g.outbox.bodies("REOPEN")
    corrected, = g.outbox.bodies("CORRECTED")
    assert reopen.item_key == f"{ADAPTER}:P1:STOP_LEVEL:position:{T3 + 5}"
    assert corrected.item_key == f"{ADAPTER}:P1:STOP_LEVEL:position:1"
    assert "earlier advice is withdrawn" in render(corrected).text


# ---------------------------------------------------------------------------------------------------- what is shown
async def test_at_target_unconfirmed_is_shown_once_not_on_every_bar() -> None:
    """D3: the repeated hold is silent even when the evaluation's flag is missing."""
    g = rig()
    r = row("P1")
    g.add(r)
    for i, t in enumerate((T1, T2, T3)):
        await g.commit("POST", t, (decision(r, bar_end=t, outcome="AT_TARGET_UNCONFIRMED", category="HOLD",
                                            seq=i + 1), True), at=t + 70_000)
    assert len(g.outbox.bodies("POST")) == 1
    assert g.store.notice_of("P1", bar_end=T2) == 0 and g.store.notice_of("P1", bar_end=T3) == 0


async def test_a_no_broker_bar_between_at_target_bars_does_not_repeat_the_notice() -> None:
    """S38c review: a bucket the broker did not trade (`NO_BROKER_BAR`, recorded only) changes no state, so it does
    not break the at-target run: the next traded bar's AT_TARGET_UNCONFIRMED stays silent."""
    g = rig()
    r = row("P1")
    g.add(r)
    await g.commit("POST", T1, (decision(r, bar_end=T1, outcome="AT_TARGET_UNCONFIRMED", category="HOLD", seq=1),
                                True), at=T1 + 70_000)
    await g.commit("POST", T2, (decision(r, bar_end=T2, outcome="NO_BROKER_BAR", category="INFO", seq=2), False),
                   at=T2 + 70_000)
    memory = await g.store.decision_memory(r.key, r.bar_spec, T3)
    assert memory.last_outcome == "AT_TARGET_UNCONFIRMED"                  # the evaluation reads past it too
    await g.commit("POST", T3, (decision(r, bar_end=T3, outcome="AT_TARGET_UNCONFIRMED", category="HOLD", seq=3),
                                True), at=T3 + 70_000)
    assert len(g.outbox.bodies("POST")) == 1 and g.store.notice_of("P1", bar_end=T3) == 0


async def test_every_position_released_gives_no_bar_notice() -> None:
    g = rig()
    rows = [row(p, state="STRATEGY_COMPLETE") for p in pids(3)]
    g.add(*rows)
    await g.commit("POST", T1, *((decision(r, outcome="COMPLETE", category="INFO"), False) for r in rows),
                   at=T1 + 70_000)
    assert g.outbox.rows == []
    assert all(s.notice_id == 0 for s in g.store.decisions)
    g.clock.now = T1 + CAP_AFTER + 1
    await g.notices.step()
    assert g.outbox.bodies("POST") == []


async def test_actionable_mode_shows_adjust_holds_faults_and_changes_from_pre() -> None:
    g = rig(mode="actionable")
    a, b, c = row("P1"), row("P2"), row("P3")
    g.add(a, b, c)
    await g.commit("PRE", T1, (decision(a, event="PRE", outcome="BROKER_AHEAD", category="INFO"), True),
                   (decision(b, event="PRE"), True), at=T1 - 40_000)
    pre, = g.outbox.bodies("PRE")
    assert [i.position_id for i in pre.items] == ["P2"]
    await g.commit("POST", T1, (decision(a, outcome="BROKER_AHEAD", category="INFO", seq=2), True),
                   (decision(b, outcome="BROKER_AHEAD", category="INFO", seq=2), True),
                   (decision(c, outcome="DATA_UNAVAILABLE", category="FAULT", reason="stop_stale", seq=2), True),
                   at=T1 + 70_000)
    post, = g.outbox.bodies("POST")
    assert [i.position_id for i in post.items] == ["P2", "P3"]    # P2 changed from PRE; P3 a FAULT
    assert g.store.notice_of("P1") == 0


def test_shown_by_mode_rules() -> None:
    r = row("P1")
    assert shown_by_mode(decision(r, outcome="BROKER_AHEAD", category="INFO"), "every_bar")
    assert not shown_by_mode(decision(r, outcome="BROKER_AHEAD", category="INFO"), "actionable")
    assert shown_by_mode(decision(r, outcome="COMPLETE", category="INFO"), "actionable")
    assert shown_by_mode(decision(r, outcome="PRE_NOT_CONFIRMED", category="INFO"), "actionable")
    assert not shown_by_mode(decision(r, event="PRE", outcome="MARKET_CLOSED", category="HOLD"), "actionable")
    assert shown_by_mode(decision(r, outcome="BROKER_AHEAD", category="INFO"), "actionable", pre_outcome="ADJUST")


# ---------------------------------------------------------------------------------------------------- PRE, grace
async def test_pre_notice_of_bar_zero_carries_the_conditional_label() -> None:
    g = rig()
    r = row("P1")
    g.add(r)
    await g.commit("PRE", T1, (decision(r, event="PRE", caveats=(FIRST_BAR_CONDITIONAL,)), True), at=T1 - 40_000)
    out, = g.outbox.rows
    assert out.channels == ("arm_email",) and out.expires_at_ms == T1
    body, = g.outbox.bodies("PRE")
    assert body.items[0].valid_until_ms == T1 - 30_000 and body.items[0].trigger_input is None
    text = render(body).text
    assert text.startswith(PRE_HEADER) and pre_label(True) in text


async def test_pre_adjust_renders_no_favour_whatever_bars_the_md_fake_holds() -> None:
    """C5: a PRE item has no input bar; even a supplied, verified bar of its symbol gives no favour figure."""
    g = rig()
    r = row("P1")
    g.add(r)
    series = SeriesId(ADAPTER, "SYN.A", M15, QuoteSide.BID, Source.BROKER)
    bar = Bar(series=series, bucket_start_ms=T1 - TF, open=D("1.24"), high=D("1.25"), low=D("1.23"), close=D("1.245"),
              final_at_ms=T1)
    h = content_hash(KEY, series.canonical, T1, bar.open, bar.high, bar.low, bar.close)
    await g.commit("PRE", T1, (decision(r, event="PRE", series=(f"{series.canonical}|{T1}|{h}",)), True),
                   at=T1 - 40_000)
    body, = g.outbox.bodies("PRE")
    text = render(body, bars={(series.canonical, T1): bar}, keyring=KEY).text
    assert "favour" not in text and "provisional" in text


async def test_post_part_zero_at_grace_lists_the_pending_positions() -> None:
    g = rig()
    a, b = row("P1"), row("P2")
    g.add(a, b)
    g.clock.now = T1 + 59_999
    await g.notices.step()
    assert g.outbox.bodies("POST") == []
    g.clock.now = T1 + 60_000
    await g.notices.step()
    part0, = g.outbox.bodies("POST")
    assert part0.part == 0 and [p.position_id for p in part0.pending] == ["P1", "P2"]
    assert "awaiting the broker's final bar" in render(part0).text
    await g.commit("POST", T1, (hold(a), True), at=T1 + 80_000)
    assert [x.part for x in g.outbox.bodies("POST")] == [0, 1]


async def test_pre_items_past_their_window_are_counted_not_sent() -> None:
    g = rig()
    rows = [row(p) for p in pids(7)]
    g.add(*rows)
    for i, r in enumerate(rows[:6]):
        await g.commit("PRE", T1, (decision(r, event="PRE", outcome="MARKET_CLOSED", category="HOLD"), True),
                       at=T1 - 40_000 + i)
    assert [b.part for b in g.outbox.bodies("PRE")] == [0, 1, 2, 3, 4]
    g.clock.now = T1 - 30_000
    await g.notices.step()
    last = g.outbox.bodies("PRE")[-1]
    assert last.part == END_PART and [i.position_id for i in last.items] == ["P5"]
    assert g.notices.counters.pre_expired == 0
    await g.commit("PRE", T1, (decision(rows[6], event="PRE", outcome="MARKET_CLOSED", category="HOLD"), True),
                   at=T1 - 29_000)                          # read after its window closed: counted, not sent
    assert g.store.notice_of("P6", "PRE") == 0 and g.notices.counters.pre_expired == 1
    assert len(g.outbox.bodies("PRE")) == 6


# ---------------------------------------------------------------------------------------------------- body content
async def test_the_outbox_insert_payload_carries_no_price_derived_field() -> None:
    """R11-M14: ids, codes and order state only; the bar's prices never reach the body."""
    g = rig()
    r = row("P1")
    g.add(r)
    series = SeriesId(ADAPTER, "SYN.A", M15, QuoteSide.BID, Source.BROKER)
    o, hi, lo, cl = D("1.24111"), D("1.25222"), D("1.23333"), D("1.24444")
    h = content_hash(KEY, series.canonical, T1, o, hi, lo, cl)
    await g.commit("POST", T1, (decision(r, series=(f"{series.canonical}|{T1}|{h}",)), True), at=T1 + 70_000)
    out, = g.outbox.rows
    for text in (out.body, out.subject):
        for price in (o, hi, lo, cl):
            assert format(price, "f") not in text
        assert "favour" not in text.lower()

    def keys(obj: object, allowed: frozenset[str]) -> None:
        assert isinstance(obj, dict) and set(obj) <= allowed
        for k in obj:
            assert not any(b in k for b in BANNED), k

    raw = json.loads(out.body)
    keys(raw, BODY_KEYS)
    for item in raw["items"]:
        keys(item, ITEM_KEYS)
    for part, allowed in (("pending", PENDING_KEYS), ("extras", EXTRA_KEYS), ("failed", FAILED_KEYS)):
        for x in raw[part]:
            keys(x, allowed)
    assert raw["alive"] is None or set(raw["alive"]) <= ALIVE_KEYS
    item = raw["items"][0]
    assert item["entry"] == format(ENTRY, "f") and item["s0"] == format(S0, "f")   # order state, for send time
    assert item["trigger_input"] == f"{series.canonical}|{T1}|{h}"
    bar = Bar(series=series, bucket_start_ms=T1 - TF, open=o, high=hi, low=lo, close=cl, final_at_ms=T1)
    assert "favour" in render(g.outbox.bodies()[0], bars={(series.canonical, T1): bar}, keyring=KEY).text


# ---------------------------------------------------------------------------------------------------- extras, timers
def tracker_item(kind: NoticeKind = NoticeKind.SUSPENDED, at: int = T1) -> TrackerNotice:
    return TrackerNotice(kind=kind, adapter_id=ADAPTER, account_id=ACCOUNT, position_id="P9", symbol="SYN.B",
                         side="SELL", reason="units_increase", at_ms=at)


async def test_extras_ride_on_the_next_bar_notice() -> None:
    g = rig()
    r = row("P1")
    g.add(r)
    g.notices.notice(tracker_item())
    await g.commit("POST", T1, (hold(r), True), at=T1 + 70_000)
    post, = g.outbox.bodies("POST")
    assert [e.kind for e in post.extras] == ["SUSPENDED"]
    g.clock.now = T1 + 2 * TF
    await g.notices.step()
    assert g.outbox.bodies("SUSPENDED") == []


async def test_extras_ride_on_the_post_notice_never_on_a_pre_notice() -> None:
    """A PRE notice can expire (local T + p50) or be superseded by its POST before it is sent: extras wait for POST."""
    g = rig()
    r = row("P1")
    g.add(r)
    g.notices.notice(tracker_item())
    await g.commit("PRE", T1, (decision(r, event="PRE"), True), at=T1 - 40_000)
    pre, = g.outbox.bodies("PRE")
    assert pre.extras == ()
    await g.commit("POST", T1, (hold(r, seq=2), True), at=T1 + 70_000)
    post, = g.outbox.bodies("POST")
    assert [e.kind for e in post.extras] == ["SUSPENDED"]


async def test_adjust_items_carry_the_handles_and_the_stop_read_time() -> None:
    g = rig()
    r = row("P1")
    g.add(r)
    h = DecisionHandleRow(handle_id="O1", handle_kind="CHILD_ORDER", handle_role="STOP", read_at_ms=T1 + 5_000,
                          target_level=D("1.23500"))
    other = DecisionHandleRow(handle_id="O2", handle_kind="CHILD_ORDER", handle_role="STOP", read_at_ms=T1 + 9_000)
    await g.commit("POST", T1, (decision(r), True), at=T1 + 70_000, handles=(h, other))
    post, = g.outbox.bodies("POST")
    item = post.items[0]
    assert item.handles == ("O1",) and item.stop_read_at_ms == T1 + 5_000      # only the handle to move
    text = render(post).text
    assert "(handles O1; stop read at " in text and STILL_WORSE in text


async def test_actionable_mode_sends_a_post_notice_for_a_folded_item_alone() -> None:
    g = rig(mode="actionable")
    r = row("P1")
    g.add(r)
    g.notices.notice(tracker_item())
    await g.commit("POST", T1, (decision(r, outcome="BROKER_AHEAD", category="INFO"), True), at=T1 + 70_000)
    post, = g.outbox.bodies("POST")
    assert post.items == () and [e.kind for e in post.extras] == ["SUSPENDED"]


async def test_extras_go_out_alone_one_bar_later_separate_at_once_and_off_drops() -> None:
    g = rig()
    g.notices.notice(tracker_item(NoticeKind.POSITION_NOT_MANAGED))
    g.clock.now = T1 + TF - 1
    await g.notices.step()
    assert g.outbox.bodies("POSITION_NOT_MANAGED") == []
    g.clock.now = T1 + TF
    await g.notices.step()
    alone, = g.outbox.bodies("POSITION_NOT_MANAGED")
    assert alone.item_key == f"{ADAPTER}:P9:units_increase" and alone.bar_end_ms == 0
    g.notices.notice(tracker_item(NoticeKind.POSITION_NOT_MANAGED, at=T2))   # one-shot for good
    g.clock.now = T3 + TF
    await g.notices.step()
    assert len(g.outbox.bodies("POSITION_NOT_MANAGED")) == 1
    s = rig(fold="separate")
    s.notices.notice(tracker_item())
    await s.notices.step()
    assert len(s.outbox.bodies("SUSPENDED")) == 1
    off = rig(fold="off")
    off.notices.notice(tracker_item())
    await off.notices.step()
    assert off.outbox.rows == [] and off.notices.counters.extras_dropped == 1
    with pytest.raises(ValueError):
        NoticeSettings(fold="sometimes")


async def test_md_reset_is_one_info_notice_keyed_by_generation() -> None:
    g = rig()
    g.notices.md_reset("gen-7", T1)
    g.notices.md_reset("gen-7", T1 + 5)
    await g.notices.step()
    await g.notices.step()
    reset, = g.outbox.bodies(MD_RESET)
    assert reset.route == GLOBAL_ROUTE and reset.item_key == "gen-7"
    assert "gen-7" in render(reset).text


async def test_alive_once_per_hour_and_the_daily_test_per_channel() -> None:
    g = rig()
    g.add(row("P1"))
    hour = T1 - T1 % 3_600_000
    g.clock.now = hour + 1
    await g.notices.step()
    await g.notices.step()
    alive, = g.outbox.bodies(ALIVE)
    assert alive.alive is not None and alive.alive.armed == 1 and alive.bar_end_ms == hour
    t = rig()
    t.add(row("P1"))
    day = T1 - T1 % 86_400_000
    t.clock.now = day + 86_399_999
    await t.notices.step()
    tests = t.outbox.bodies("TEST")
    assert sorted(b.channel or "" for b in tests) == ["arm_email", "arm_push"]
    assert {r.channels for r in t.outbox.rows if r.meta.event == "TEST"} == {("arm_email",), ("arm_push",)}


async def test_alive_skipped_in_every_bar_mode_after_an_hour_with_a_notice() -> None:
    g = rig()
    r = row("P1")
    g.add(r)
    hour = T1 - T1 % 3_600_000
    await g.commit("POST", T1, (hold(r), True), at=hour + 3_600_000 - 1)
    g.clock.now = hour + 3_600_000 + 1
    await g.notices.step()
    assert g.outbox.bodies(ALIVE) == []


async def test_a_decision_without_a_notice_for_60_s_pages_once_per_route_episode() -> None:
    """A decision committed with `notice_id` NULL (here: the pass-through sink) is noticed by the timer pass; once it
    has been NULL for more than 60 s, that pass pages once."""
    g = rig()
    r = row("P1")
    g.add(r)
    g.notices.learn(r)
    async with g.store.transaction() as tx:
        await tx.record_decision(hold(r, at=T1 + 70_000))
    g.clock.now = T1 + 130_000
    await g.notices.step()
    assert g.rec.pages == [] and g.store.notice_of("P1") not in (None, 0)
    async with g.store.transaction() as tx:
        await tx.record_decision(hold(r, bar_end=T2, at=T2 + 70_000))
    g.clock.now = T2 + 131_000
    await g.notices.step()
    assert len(g.rec.pages) == 1 and "without a notice" in g.rec.pages[0][1]
    assert g.store.notice_of("P1", bar_end=T2) not in (None, 0)
    assert all(ROUTE in p[2] for p in g.rec.pages)


async def test_a_part_five_waiter_pages_only_60_s_after_its_window_end() -> None:
    g = rig()
    rows = [row(p) for p in pids(6)]
    g.add(*rows)
    for i, r in enumerate(rows):
        await g.commit("POST", T1, (hold(r, at=T1 + 70_000 + i * 1_000), True), at=T1 + 70_000 + i * 1_000)
    assert g.store.notice_of("P5") is None                  # waiting for part 5
    g.clock.now = T1 + 200_000
    await g.notices.step()
    assert g.rec.pages == []                                 # NULL for 125 s, but a waiter
    g.store.fail_commits = 1
    g.clock.now = T1 + CAP_AFTER
    await g.notices.step()                                   # part 5 rolls back: the window stays open
    assert g.store.notice_of("P5") is None and g.notices.counters.errors == 1 and g.rec.pages == []
    g.store.fail_commits = 1
    g.clock.now = T1 + CAP_AFTER + 60_000
    await g.notices.step()
    assert g.rec.pages == []
    g.clock.now = T1 + CAP_AFTER + 60_001
    await g.notices.step()
    assert len(g.rec.pages) == 1
    last = g.outbox.bodies("POST")[-1]
    assert last.part == END_PART and g.store.notice_of("P5") == last.notice_id
    g.clock.now += 5_000
    await g.notices.step()
    assert len(g.rec.pages) == 1


async def test_a_takeover_does_not_page_for_a_part_five_waiter() -> None:
    """The new leader has no part counts: parts 0-4 are key conflicts, so the decision is a waiter, not overdue."""
    g = rig()
    rows = [row(p) for p in pids(6)]
    g.add(*rows)
    for i, r in enumerate(rows):
        await g.commit("POST", T1, (hold(r, at=T1 + 70_000 + i * 1_000), True), at=T1 + 70_000 + i * 1_000)
    new = takeover(g)
    g.clock.now = T1 + 200_000
    await new.step()
    assert g.rec.pages == [] and g.store.notice_of("P5") is None
    assert [b.part for b in g.outbox.bodies("POST")] == [0, 1, 2, 3, 4]
    g.clock.now = T1 + CAP_AFTER
    await new.step()
    last = g.outbox.bodies("POST")[-1]
    assert last.part == END_PART and [i.position_id for i in last.items] == ["P5"] and g.rec.pages == []


# ---------------------------------------------------------------------------------------------------- rollback
async def test_a_failed_barclock_commit_keeps_the_extra_and_the_part_number() -> None:
    """The memory follows the commit: five rolled-back commits neither skip part 0 nor drop the folded extra."""
    g = rig()
    r = row("P1")
    g.add(r)
    g.notices.notice(tracker_item())
    for i in range(5):
        g.store.fail_commits = 1
        with pytest.raises(ConnectionError):
            await g.commit("POST", T1, (hold(r), True), at=T1 + 70_000 + i)
    assert g.outbox.rows == [] and g.store.notices == {} and g.store.rollbacks == 5
    await g.commit("POST", T1, (hold(r), True), at=T1 + 80_000)
    post, = g.outbox.bodies("POST")
    assert post.part == 0 and [e.kind for e in post.extras] == ["SUSPENDED"]
    assert g.store.notice_of("P1") == post.notice_id
    g.clock.now = T1 + 3 * TF
    await g.notices.step()
    assert g.outbox.bodies("SUSPENDED") == []                # sent once, folded


async def test_a_rolled_back_repeat_hold_stays_silent_on_the_retry() -> None:
    """D3 memory is staged too: a rolled-back at-target hold does not make its retry look new."""
    g = rig()
    r = row("P1")
    g.add(r)
    await g.commit("POST", T1, (decision(r, outcome="AT_TARGET_UNCONFIRMED", category="HOLD"), True), at=T1 + 70_000)
    again = decision(r, bar_end=T2, outcome="AT_TARGET_UNCONFIRMED", category="HOLD", seq=2)
    g.store.fail_commits = 1
    with pytest.raises(ConnectionError):
        await g.commit("POST", T2, (again, True), at=T2 + 70_000)
    await g.commit("POST", T2, (again, True), at=T2 + 71_000)
    assert len(g.outbox.bodies("POST")) == 1 and g.store.notice_of("P1", bar_end=T2) == 0


async def test_a_failed_alive_or_test_is_sent_on_the_next_pass() -> None:
    g = rig()
    g.add(row("P1"))
    hour = T1 - T1 % 3_600_000
    g.clock.now = hour + 1
    g.store.fail_events = {ALIVE}
    await g.notices.step()
    assert g.outbox.bodies(ALIVE) == [] and g.notices.counters.errors == 1
    g.store.fail_events = set()
    await g.notices.step()
    assert len(g.outbox.bodies(ALIVE)) == 1
    t = rig()
    t.add(row("P1"))
    day = T1 - T1 % 86_400_000
    t.clock.now = day + 86_399_999
    t.store.fail_events = {"TEST"}
    await t.notices.step()
    assert t.outbox.bodies("TEST") == []
    t.store.fail_events = set()
    await t.notices.step()
    assert sorted(b.channel or "" for b in t.outbox.bodies("TEST")) == ["arm_email", "arm_push"]


async def test_a_failing_route_does_not_hold_back_the_others() -> None:
    g = rig()
    g.add(row("P1"), row("P2", account="acct2"))
    g.notices.md_reset("gen-1", T1)
    g.outbox.refuse = {ROUTE}                                # "main" sorts first and fails on every row
    g.clock.now = T1 - T1 % 86_400_000 + 86_399_999
    await g.notices.step()
    assert g.notices.counters.errors == 1
    routes = {r.route for r in g.outbox.rows}
    assert ROUTE not in routes and f"{ADAPTER}:acct2" in routes and GLOBAL_ROUTE in routes
    assert len(g.outbox.bodies(MD_RESET)) == 1 and len(g.outbox.bodies("TEST")) == 2
    assert {b.route for b in g.outbox.bodies(ALIVE)} == {f"{ADAPTER}:acct2"}


async def test_memory_of_positions_gone_is_pruned() -> None:
    g = rig()
    r = row("P1")
    g.add(r)
    await g.commit("POST", T1, (hold(r), True), at=T1 + 70_000)
    g.clock.now = T1 + 80_000
    await g.notices.step()
    assert g.notices._info and g.notices._members and g.notices._alive_done
    del g.store.rows[r.key]                                  # closed and gone
    g.clock.now = T1 + 3 * 86_400_000
    await g.notices.step()
    assert g.notices._info == {} and g.notices._members == {} and g.notices._alive_done == {}
    assert g.notices._last == {} and g.notices._parts == {}


async def test_unrouted_accounts_route_by_adapter_and_account() -> None:
    g = rig(aliases=False)
    r = row("P1")
    g.add(r)
    await g.commit("POST", T1, (hold(r), True), at=T1 + 70_000)
    post, = g.outbox.bodies("POST")
    assert post.route == f"{ADAPTER}:{ACCOUNT}"
    assert RouteBook().route_of("a", "b") == "a:b"


# ---------------------------------------------------------------------------------------------------- pure rules
def test_supersede_and_expiry_rules() -> None:
    pre = NoticeMeta(route=ROUTE, bar_spec="15m", event="PRE", bar_end_ms=T1)
    post0 = NoticeMeta(route=ROUTE, bar_spec="15m", event="POST", bar_end_ms=T1)
    post1 = NoticeMeta(route=ROUTE, bar_spec="15m", event="POST", bar_end_ms=T1, part=1)
    next0 = NoticeMeta(route=ROUTE, bar_spec="15m", event="POST", bar_end_ms=T2)
    part5 = NoticeMeta(route=ROUTE, bar_spec="15m", event="POST", bar_end_ms=T1, part=END_PART)
    late = NoticeMeta(route=ROUTE, bar_spec="15m", event="LATE", bar_end_ms=T1, item_key="x")
    alive = NoticeMeta(route=ROUTE, bar_spec="", event="ALIVE", bar_end_ms=T1)
    assert supersedes(post0, pre, old_sent=False)
    assert not supersedes(post1, post0, old_sent=False)
    assert supersedes(next0, post0, old_sent=False) and supersedes(next0, late, old_sent=False)
    assert not supersedes(next0, part5, old_sent=False) and supersedes(next0, part5, old_sent=True)
    assert not supersedes(next0, alive, old_sent=True)
    assert not supersedes(NoticeMeta(route="other", bar_spec="15m", event="POST", bar_end_ms=T2), post0,
                          old_sent=True)
    from src.marketdata.timing import COLD_SKEW
    assert expires_at_ms("PRE", bar_end_ms=T1, created_ms=T1 - 40_000, spec=M15, skew=COLD_SKEW) == T1
    assert expires_at_ms("POST", bar_end_ms=T1, created_ms=T1 + 5, spec=M15, skew=COLD_SKEW) == T1 + 5 + TF
    assert expires_at_ms("ALIVE", bar_end_ms=T1, created_ms=T1, spec=M15, skew=COLD_SKEW) == T1 + 3_600_000
    assert item_key_of(decision(row("P1"), event="LATE", revision=2)) == f"{ADAPTER}:P1:STOP_LEVEL:position:2"


def test_the_notice_sink_takes_the_second_arm_sink_place() -> None:
    g = rig()
    assert g.chain.names == ARM_SINKS


async def test_the_barclock_hands_the_shown_flag_to_the_notice_sink() -> None:
    """The BarClock with the real chain (decision sink, notice sink): a silent evaluation is suppressed in the same
    transaction, the other position gets its POST part 0; `post_sent` is the BarClock's `PostNoticeView`."""
    from dataclasses import replace as _replace

    from src.riskmgr.barclock import BarClock, BarClockSettings, EvalRequest
    from src.riskmgr.config import ArmSettings
    from src.riskmgr.jobs import JobRunner
    from src.riskmgr.ports import Pager
    from src.riskmgr.sinks import DecisionSink, SinkChain
    from src.riskmgr.stopreads import StopReads
    from tests.riskmgr.barclock_fakes import SETTLE, FakeControls, FakeEval, FakeGrid, FakeMdGen, arm_row, decided

    g = rig(now=T1 + SETTLE + 1_000)
    g.add(arm_row("P1"), arm_row("P2"))

    def rule(req: EvalRequest):  # type: ignore[no-untyped-def]
        return _replace(decided(), shown=req.row.key.position_id != "P2")

    settings = ArmSettings()
    bc = BarClock(store=g.store, controls=FakeControls(), md=FakeMdGen(), grid=FakeGrid(),  # type: ignore[arg-type]
                  evaluator=FakeEval(rule), jobs=JobRunner(), pager=Pager(g.rec.page, lambda t, b: None),
                  clock=g.clock, settings=settings, lease_valid=lambda: True,
                  stop_reads=StopReads(None, None, enabled=False, pre_offset_s=settings.pre_offset_s),
                  sinks=SinkChain((DecisionSink(), g.notices.sink())), notices=g.notices,
                  barclock=BarClockSettings(rows_ttl_ms=0))
    try:
        await bc.step()
    finally:
        bc.close()
    post, = g.outbox.bodies("POST")
    assert [i.position_id for i in post.items] == ["P1"] and post.part == 0
    assert g.store.notice_of("P2") == 0
