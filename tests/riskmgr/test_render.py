"""The send-time renderer and the ids-only body (S28, design §B8, R11-M04, R11-M14, C5, D2): a render test for every
outcome and every item kind, favour figures only from a verified bar, and the body allow-lists."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from src.marketdata.hashing import Keyring, content_hash
from src.marketdata.types import M15, Bar, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.guards.first_bar import CONDITIONAL_LABEL, PRE_ANCHOR_TEXT
from src.riskmgr.outcomes import Outcome
from src.riskmgr.render import (
    AT_TARGET_TEXT,
    COMPLETE_TEXT,
    DELAYED,
    EXTRA_TEXT,
    FIRST_BAR_CONDITIONAL,
    PRE_HEADER,
    PRE_NOT_CONFIRMED_RELEASE,
    PRE_NOT_CONFIRMED_TEXT,
    Q10_REARM,
    Q10_TELL,
    STILL_WORSE,
    AliveCounts,
    BodyError,
    ExtraBody,
    FailedLine,
    ItemBody,
    NoticeBody,
    PendingBody,
    RenderSettings,
    bar_reads,
    decode_body,
    encode_body,
    favour_figures,
    parse_input,
    render,
    subject,
    trigger_input,
)
from src.riskmgr.strategies.break_even_on_risk_close import BE_OFFSET_FIRST_SEEN, BE_OFFSET_FIRST_SEEN_TEXT

D = Decimal
T = 1_700_001_000_000
KEY = Keyring.of(1, b"k" * 32)
BID = SeriesId("mock", "SYN.A", M15, QuoteSide.BID, Source.BROKER)
ASK = SeriesId("mock", "SYN.A", M15, QuoteSide.ASK, Source.BROKER)
O, H, L, C = D("1.24111"), D("1.25222"), D("1.23333"), D("1.24456")
GOOD = content_hash(KEY, BID.canonical, T, O, H, L, C)
BAR = Bar(series=BID, bucket_start_ms=T - M15.timeframe_ms, open=O, high=H, low=L, close=C, final_at_ms=T)

OUTCOME_CASES: dict[Outcome, tuple[str, str | None, str | None, str]] = {
    Outcome.ADJUST: ("ACTIONABLE", None, None, "ADJUST: move the stop to 1.235"),
    Outcome.COMPLETE: ("INFO", None, None, COMPLETE_TEXT),
    Outcome.PRE_NOT_CONFIRMED: ("INFO", "BELOW_TRIGGER", "break_even_on_risk_close", PRE_NOT_CONFIRMED_RELEASE),
    Outcome.AT_TARGET_UNCONFIRMED: ("HOLD", None, None, AT_TARGET_TEXT),
    Outcome.LOOSENED_AFTER_COMPLETE: ("INFO", None, None, "moved back after release"),
    Outcome.BROKER_AHEAD: ("INFO", None, None, "already at or beyond"),
    Outcome.STRATEGY_REASON: ("HOLD", "NOT_IN_FAVOUR", "break_even_on_risk_close", "NOT_IN_FAVOUR"),
    Outcome.GUARD_REASON: ("HOLD", "PRE_ANCHOR", "first_bar", PRE_ANCHOR_TEXT),
    Outcome.MARKET_CLOSED: ("HOLD", None, None, "market closed"),
    Outcome.PRICE_THROUGH_LEVEL: ("HOLD", None, None, "through the proposed level"),
    Outcome.NO_STOP: ("FAULT", None, None, "no effective stop"),
    Outcome.DATA_UNAVAILABLE: ("FAULT", "stop_stale", None, "data unavailable (stop_stale)"),
    Outcome.STRATEGY_ERROR: ("FAULT", None, None, "strategy error"),
    Outcome.WRITE_ERROR: ("FAULT", None, None, "write error"),
    Outcome.UNRESOLVED_RANGE: ("FAULT", "broker_bar_not_final", None, "bars unresolved (broker_bar_not_final)"),
    Outcome.NO_BROKER_BAR: ("INFO", None, None, "the broker did not trade this bar"),
}


def item(**kw: object) -> ItemBody:
    base = dict(decision_id=7, decision_bar_end_ms=T, adapter_id="mock", position_id="P1", symbol="SYN.A",
                side="BUY", event="POST", bar_end_ms=T, outcome="ADJUST", category="ACTIONABLE",
                proposed_stop=D("1.23500"), current_risk_r=D("0.50"), handles=("h1",),
                trigger_input=f"{BID.canonical}|{T}|{GOOD}", entry=D("1.23456"), s0=D("1.21456"),
                strategy_kind="break_even_on_risk_close", strategy_impl_version=1, trigger_source="BROKER")
    base.update(kw)
    return ItemBody(**base)  # type: ignore[arg-type]


def body(*items: ItemBody, event: str = "POST", **kw: object) -> NoticeBody:
    base = dict(notice_id=42, route="main", event=event, bar_spec="15m", bar_end_ms=T, created_ms=T + 70_000,
                expires_at_ms=T + 970_000, items=items)
    base.update(kw)
    return NoticeBody(**base)  # type: ignore[arg-type]


@pytest.mark.parametrize("outcome", list(Outcome))
def test_every_outcome_renders(outcome: Outcome) -> None:
    category, reason, kind, expected = OUTCOME_CASES[outcome]
    i = item(outcome=outcome.value, category=category, reason=reason, reason_kind=kind,
             proposed_stop=D("1.23500") if outcome is Outcome.ADJUST else None)
    text = render(body(i)).text
    assert expected in text
    assert "position P1 (mock)" in text and "SYN.A BUY" in text and "decision 7" in text
    assert "current risk 0.50 R" in text
    if outcome is Outcome.ADJUST:                       # §B6.3 (13): the handles, the stop's read time, the qualifier
        read = replace(i, stop_read_at_ms=T - 5_000)
        post = render(body(read)).text
        assert "ADJUST: move the stop to 1.23500 (handles h1; stop read at " in post and STILL_WORSE in post
        assert "UTC / " in post.split("stop read at ")[1]
        pre = render(body(replace(read, event="PRE"), event="PRE")).text
        assert "provisional: the rule would move the stop to 1.23500 (handles h1; stop read at " in pre
        assert STILL_WORSE in pre


@pytest.mark.parametrize("event", ["PRE", "POST", "LATE", "REOPEN", "CORRECTED"])
def test_every_event_item_renders_with_its_label(event: str) -> None:
    text = render(body(item(event=event, revision=2, supersedes=3 if event == "CORRECTED" else None),
                       event=event)).text
    label = {"PRE": PRE_HEADER, "POST": "ADJUST: move the stop", "LATE": "LATE (", "REOPEN": "REOPEN (",
             "CORRECTED": "CORRECTED (revision 2)"}[event]
    assert label in text


@pytest.mark.parametrize("kind", sorted(EXTRA_TEXT))
def test_every_extra_item_renders(kind: str) -> None:
    e = ExtraBody(kind=kind, adapter_id="mock", position_id="P9", symbol="SYN.B", side="SELL", reason="units_increase",
                  at_ms=T)
    text = render(body(event=kind, extras=(e,), bar_spec="", item_key="mock:P9:units_increase")).text
    assert EXTRA_TEXT[kind] in text and "(units_increase)" in text


def test_pending_alive_test_md_reset_and_failed_lines_render() -> None:
    pending = render(body(pending=(PendingBody(adapter_id="mock", position_id="P2", symbol="SYN.A", side="BUY"),)))
    assert "INFO: awaiting the broker's final bar" in pending.text
    alive = render(body(event="ALIVE", alive=AliveCounts(armed=3, faulting=1), bar_spec="",
                        failed=(FailedLine(scope_kind="SERIES", scope_id="mock:SYN.A:15m:BID:BROKER",
                                           reason="unresolved"),)))
    assert "3 position(s) managed, 1 in FAULT" in alive.text and "FAILED: SERIES" in alive.text
    assert "on channel arm_push" in render(body(event="TEST", channel="arm_push", item_key="arm_push")).text
    assert "generation g7" in render(body(event="MD_RESET", generation="g7", item_key="g7")).text


def test_release_texts_say_what_releases_the_position_now() -> None:
    """R11-M04: PRE_NOT_CONFIRMED says the bar end did not confirm and what releases the position now."""
    text = render(body(item(outcome="PRE_NOT_CONFIRMED", category="INFO", reason="AT_TARGET_UNCONFIRMED"))).text
    assert PRE_NOT_CONFIRMED_TEXT in text and PRE_NOT_CONFIRMED_RELEASE in text and "at target" in text
    done = item(outcome="COMPLETE", category="INFO")
    assert Q10_TELL in render(body(done)).text
    assert Q10_REARM not in render(body(done), settings=RenderSettings(show_levels=True)).text
    assert Q10_REARM in render(body(item(outcome="COMPLETE", category="INFO", loosen_policy="REARM"))).text


def test_favour_figures_only_from_a_verified_bar() -> None:
    i = item(trigger_met=True)          # favour / risk is shown only next to a decided trigger it agrees with
    bars = {(BID.canonical, T): BAR}
    fav = favour_figures(i, bars, KEY)
    assert fav is not None and str(fav.favour_r.value) == "0.50" and fav.per_risk is not None
    assert str(fav.per_risk.value) == "1.000"
    text = render(body(i), bars=bars, keyring=KEY).text
    assert "favour 0.50 R" in text and "favour / risk 1.000" in text
    assert favour_figures(i, None, KEY) is None                            # bar unavailable: omitted
    assert favour_figures(i, bars, None) is None
    tampered = {(BID.canonical, T): replace(BAR, close=C + D("0.001"))}
    assert favour_figures(i, tampered, KEY) is None                        # hash does not verify
    other_key = Keyring.of(2, b"z" * 32)
    assert favour_figures(i, bars, other_key) is None                      # key id not held
    assert favour_figures(replace(i, event="PRE"), bars, KEY) is None      # C5
    assert "favour" not in render(body(replace(i, event="PRE"), event="PRE"), bars=bars, keyring=KEY).text
    short = item(side="SELL", entry=D("1.25456"), s0=D("1.27456"))
    sfav = favour_figures(short, bars, KEY)
    assert sfav is not None and str(sfav.favour_r.value) == "0.50"


def test_conditional_first_bar_label_valid_until_delayed_and_caveats() -> None:
    pre = item(event="PRE", caveats=(FIRST_BAR_CONDITIONAL,), trigger_met=False, valid_until_ms=T - 30_000,
               trigger_input=None)
    text = render(body(pre, event="PRE")).text
    assert CONDITIONAL_LABEL in text and "trigger is not met" in text and "valid until" in text
    late = item(caveats=(DELAYED, BE_OFFSET_FIRST_SEEN), decided_at_ms=T + 180_000)
    text = render(body(late)).text
    assert "DELAYED: decided 3 min after bar end" in text and BE_OFFSET_FIRST_SEEN_TEXT in text


def test_entry_shown_only_with_q13_and_texts_are_escaped() -> None:
    i = item(position_id="P\x1b1")
    assert "entry " not in render(body(i)).text
    assert "entry 1.23456" in render(body(i), settings=RenderSettings(show_levels=True)).text
    assert "\x1b" not in render(body(i)).text and "P\\x1b1" in render(body(i)).text
    for outcome in ("STRATEGY_REASON", "GUARD_REASON", "UNRESOLVED_RANGE"):
        text = render(body(item(outcome=outcome, category="X<y>\x07", reason="r", reason_kind="k"))).text
        assert "\x07" not in text and "X<y>\\x07" in text


def test_a_post_after_part_five_renders_as_late() -> None:
    assert "LATE (decided after its bar's notice window)" in render(body(item(late=True))).text
    assert "LATE (" not in render(body(item())).text


def test_subject_carries_the_notice_id_bar_end_event_and_part() -> None:
    s = subject(body(part=3))
    assert s.startswith("ARM main POST part 3 |") and "UTC" in s and "Melbourne" in s and s.endswith("#42")
    assert "item" in subject(body(item_key="mock:P1:STOP_LEVEL:position:0"))


def test_body_round_trip_and_strict_decoding() -> None:
    b = body(item(caveats=("borderline",), quality_flags=("atr_warm",)),
             pending=(PendingBody(adapter_id="mock", position_id="P2"),),
             extras=(ExtraBody(kind="SUSPENDED", position_id="P3", reason="units_increase"),),
             failed=(FailedLine(scope_kind="ARM", scope_id="arm", reason="stuck_frontier"),),
             alive=AliveCounts(armed=1), channel=None)
    assert decode_body(encode_body(b)) == b
    with pytest.raises(BodyError):
        decode_body(encode_body(b).replace('"route"', '"close"'))
    with pytest.raises(BodyError):
        decode_body(encode_body(b).replace('"handles"', '"favour_r"'))
    with pytest.raises(BodyError):
        decode_body("[]")


def test_trigger_input_picks_the_trigger_bar_and_bar_reads_skip_pre() -> None:
    h_ask = content_hash(KEY, ASK.canonical, T, O, H, L, C)
    both = (f"{BID.canonical}|{T}|{GOOD}", f"S|{ASK.canonical}|{T}|{h_ask}",
            f"{BID.canonical}|{T - M15.timeframe_ms}|{GOOD}")
    assert trigger_input(both, bar_end_ms=T, bar_spec="15m", side="BUY") == f"{BID.canonical}|{T}|{GOOD}"
    assert trigger_input(both, bar_end_ms=T, bar_spec="15m", side="SELL") == f"{ASK.canonical}|{T}|{h_ask}"
    assert trigger_input(both[:1], bar_end_ms=T, bar_spec="15m", side="SELL") == f"{BID.canonical}|{T}|{GOOD}"
    assert trigger_input(("junk",), bar_end_ms=T, bar_spec="15m", side="BUY") is None
    assert parse_input("a|b") is None
    assert bar_reads(body(item(), item(event="PRE", decision_id=8))) == ((BID.canonical, T),)
