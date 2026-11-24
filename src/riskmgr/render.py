"""The ARM notice body and its send-time renderer (design §B1, §B5.2, §B6.3, §B6.5, §B8; plan S28, R11-M04,
R11-M14).

**The body** (`NoticeBody`) is what an ARM outbox row holds from its INSERT on: the notice id and key, decision ids,
outcome codes and order-state fields only (`encode_body` / `decode_body`, a canonical JSON text whose keys are held
to `BODY_KEYS` / `ITEM_KEYS` / ... both ways). Order state means the proposed stop, the pinned entry `E` and `S0`
(needed to turn a bar close into favour R at send time), the stop level the handles to move held when read
(`stop_now`, for the HTML email's stop block) and the R values computed from order state
(`current_risk_r = σ·(E − S)/R`); market data appears only as the trigger input's id with its keyed hash
(`series_id|bar_end_ms|key_id‖hash`), which cannot be turned back into a price. There is never an OHLC, quote,
spread, indicator value, favour R or favour / risk in a body.

**The renderer** (`render`) is pure: a body plus, optionally, the input bars the relay re-read through
`MarketDataPort` by `(series, bar_end_ms)` (`bar_reads` lists them) and the market-data keyring → subject and text.
Favour R (`σ·(close − E)/R`) and favour / risk (`favour / σ·(E − S)`) are shown **only** when the bar is supplied
and its keyed content hash verifies against the hash the decision stored (`src.marketdata.hashing.verify`);
otherwise they are omitted. A PRE item has no input bar (its quote was frozen at fire time and never stored), so a
PRE notice carries outcome codes and order state only, never favour figures, whatever bars are supplied (round-11
confirmation C5).

Text follows the §B8 content allow-list: account alias (the route), position id, symbol, side, bar times (UTC and
Melbourne), event, outcome and category, the proposed stop, the handles to move (ids) and the stop's read time with
"only if your stop is still worse than this" (§B6.3 step 13), R values (0.01 R), ratios (0.001), quality flags,
`borderline`, sources, caveats, the strategy kind and version, "valid until", the LATE label of a decision committed
after its event's part 5; the entry only with Q13 (`RenderSettings.show_levels`). Every text taken from data is escaped (`escape_text`). The release texts
are §B8's (`COMPLETE`, `PRE_NOT_CONFIRMED` with what releases the position now, R11-M04, and
`AT_TARGET_UNCONFIRMED`); bar 0's PRE carries the conditional first-bar label (D2, `FIRST_BAR_CONDITIONAL`).
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final, cast
from zoneinfo import ZoneInfo

from src.armconfig.validate import escape_text
from src.marketdata.hashing import HashStatus, Keyring, verify
from src.marketdata.types import Bar, SeriesId, Source
from src.marketdata.types import Side as QuoteSide
from src.riskmgr.guards.first_bar import FIRST_BAR_UNKNOWN, FIRST_BAR_VOLATILE, PRE_ANCHOR, PRE_ANCHOR_TEXT, pre_label
from src.riskmgr.outcomes import Outcome
from src.riskmgr.strategies.break_even_on_risk_close import BE_OFFSET_FIRST_SEEN, BE_OFFSET_FIRST_SEEN_TEXT
from src.riskmgr.types import RMultiple, Ratio, sigma

BODY_VERSION: Final = 1
MELBOURNE: Final = ZoneInfo("Australia/Melbourne")
TEXT_LIMIT: Final = 64
"""Escaped data texts (ids, codes, aliases) are cut to this length."""

# ---------------------------------------------------------------------------------------------------- codes and texts
FIRST_BAR_CONDITIONAL: Final = "first_bar_conditional"
"""The caveat code a PRE item carries while FirstBarGuard's check is pending at bar end (bar 0, §B6.5, D2): the S21
join maps the guard's `conditional_label` to it, and the renderer shows `pre_label(trigger_met)`."""
DELAYED: Final = "delayed"
"""The BarClock's caveat on a POST decided after `T + post_grace` (§B5.2): rendered as `DELAYED`."""
PREVIOUS_POST_PENDING: Final = "previous_post_pending"
"""The pipeline's PRE caveat (S21 `pipeline.PREVIOUS_POST_PENDING`, repeated here: no S28 module imports S21)."""
MARKET_CLOSES: Final = "market_closes"
"""A PRE whose bar end is a close (§B5.2: "market closes at bar end")."""

PRE_HEADER: Final = "PROVISIONAL, final advice after the bar closes"
COMPLETE_TEXT: Final = "released: the bar end confirmed break-even; the ARM no longer manages this position"
Q10_TELL: Final = "if the stop is moved back below the target later, you are told only"
Q10_REARM: Final = "if the stop is moved back below the target later, the ARM manages the position again"
"""`Q10_TELL` / `Q10_REARM` are the long (BUY) forms of `q10_text`, kept for existing readers."""
_LOOSER: Final = {"BUY": "below", "SELL": "above"}
"""The direction a stop moves when it is loosened past the target: below it for a long, above it for a short."""


def q10_text(side: str, rearm: bool) -> str:
    """The Q10 release sentence for a position of `side`: "below" the target for a long, "above" it for a short, and
    side-neutral ("past") for an empty or unknown side (an older body), never a guessed direction."""
    tail = "the ARM manages the position again" if rearm else "you are told only"
    return f"if the stop is moved back {_LOOSER.get(side, 'past')} the target later, {tail}"

NO_BROKER_BAR_PRE_REASON: Final = "pre_not_confirmed"
"""The `NO_BROKER_BAR` reason after this bar's PRE proposed a move (`pipeline.NO_TRADE_PRE_NOT_CONFIRMED`): the item
is shown as not confirmed (S38c review)."""
PRE_NOT_CONFIRMED_TEXT: Final = "bar end did not confirm; the rule would not have moved your stop"
PRE_NOT_CONFIRMED_RELEASE: Final = ("still managed until a bar end confirms the trigger against your stop as it was "
                                    "before this move")
AT_TARGET_TEXT: Final = "at target, not yet confirmed; still managed until a bar end confirms"
AWAITING_TEXT: Final = "awaiting the broker's final bar"
STILL_WORSE: Final = "only if your stop is still worse than this"
"""The ADJUST qualifier (§B6.3 step 13): the user may already have moved the stop since it was read."""

CAVEAT_TEXT: Final[Mapping[str, str]] = {
    BE_OFFSET_FIRST_SEEN: BE_OFFSET_FIRST_SEEN_TEXT,
    PREVIOUS_POST_PENDING: "the previous bar's final advice is still pending",
    MARKET_CLOSES: "market closes at bar end",
    "stop_kind_unknown": "a stop child of unknown order kind is counted",
    "stop_changed_after_bar_end": "the stop changed after the bar end",
    "borderline": "borderline",
    "roll_affected": "bar overlaps the 17:00 New York roll",
    "unverified_final": "broker bar not re-verified",
    "first_bar_unknown_stale": "first-bar check could not run on a stale bar",
    "pre_basis_held": "the bar end tests against your stop as it was before your heads-up move",
    "moved_on_final_advice": "your stop move answered the earlier final advice, so it counts at this bar end",
}

EXTRA_TEXT: Final[Mapping[str, str]] = {
    "POSITION_NOT_MANAGED": "not managed by the ARM",
    "SUSPENDED": "suspended (units increased); run arm rearm after checking the position",
    "REPLACED": "closed and replaced by another position",
    "REPINNED": "entry and first stop re-pinned after a fill",
    "ROLL_ADJUST": "the broker adjusted the entry at the roll; the pinned entry is kept",
    "ENTRY_CORRECTED": "entry corrected and re-pinned",
    "MD_RESET": "the market-data store was reset",
    "FAILED_CLEARED": "a FAILED scope was cleared",
}
"""Extras (Q9: armed, ended, ... items) by kind; their reason codes are shown escaped."""

# ---------------------------------------------------------------------------------------------------- the body
BODY_KEYS: Final = frozenset({
    "v", "notice_id", "route", "event", "bar_spec", "bar_end_ms", "part", "item_key", "created_ms", "expires_at_ms",
    "items", "pending", "extras", "failed", "alive", "channel", "generation",
})
ITEM_KEYS: Final = frozenset({
    "decision_id", "decision_bar_end_ms", "adapter_id", "position_id", "symbol", "side", "event", "bar_end_ms",
    "revision", "outcome", "category", "reason", "reason_kind", "trigger_met", "borderline", "advice",
    "proposed_stop", "current_risk_r", "first_bar_ratio", "caveats", "quality_flags", "stop_basis", "trigger_source",
    "handles", "trigger_input", "entry", "s0", "supersedes", "decided_at_ms", "strategy_kind",
    "strategy_impl_version", "valid_until_ms", "stop_read_at_ms", "late", "loosen_policy", "stop_now",
})
"""`stop_now` is additive (written only when known), so a body without it still decodes (as None); `BODY_VERSION`
stays 1."""
PENDING_KEYS: Final = frozenset({"adapter_id", "position_id", "symbol", "side"})
EXTRA_KEYS: Final = frozenset({"kind", "adapter_id", "position_id", "symbol", "side", "reason", "at_ms"})
FAILED_KEYS: Final = frozenset({"scope_kind", "scope_id", "reason"})
ALIVE_KEYS: Final = frozenset({"armed", "faulting", "unmanaged"})


class BodyError(ValueError):
    """A body that is not a well-formed ARM notice body (unknown key, wrong type)."""


@dataclass(frozen=True, kw_only=True)
class ItemBody:
    """One decision item: ids, codes and order state. `entry` / `s0` are the pinned order-state levels (for favour R
    at send time; shown only with Q13); `trigger_input` the trigger bar's id and keyed hash (never at PRE)."""

    decision_id: int
    decision_bar_end_ms: int
    adapter_id: str
    position_id: str
    symbol: str = ""
    side: str = ""
    event: str
    bar_end_ms: int
    revision: int = 0
    outcome: str
    category: str
    reason: str | None = None
    reason_kind: str | None = None
    trigger_met: bool | None = None
    borderline: bool = False
    advice: bool = False
    proposed_stop: Decimal | None = field(default=None, repr=False)
    current_risk_r: Decimal | None = None
    first_bar_ratio: Decimal | None = None
    caveats: tuple[str, ...] = ()
    quality_flags: tuple[str, ...] = ()
    stop_basis: str | None = None
    trigger_source: str | None = None
    handles: tuple[str, ...] = ()
    stop_read_at_ms: int | None = None
    """When the stop the handles hold was read (order-state time, the latest of the handles' reads; never a price)."""
    stop_now: Decimal | None = field(default=None, repr=False)
    """The order-state stop level the handles to move held when read (at `stop_read_at_ms`; the most distant one when
    they differ); None when not recorded (an older body, a timer-pass item). Shown only in the HTML email's stop
    block, never in the text."""
    late: bool = False
    """A bar-end decision committed after its event's part 5 (a replayed bar): it goes out only as a LATE item."""
    trigger_input: str | None = None
    entry: Decimal | None = field(default=None, repr=False)
    s0: Decimal | None = field(default=None, repr=False)
    supersedes: int | None = None
    decided_at_ms: int = 0
    strategy_kind: str = ""
    strategy_impl_version: int = 0
    valid_until_ms: int | None = None
    loosen_policy: str | None = None
    """The position's **pinned** Q10 policy (`REARM` | `TELL_ONLY`, an order-state code from
    `SlStrategy.loosen_policy(pinned params)`), carried on a `COMPLETE` item so its release text says what happens if
    the stop is moved back; None (an older body, or no pin readable) renders the tell-only text (round 13)."""


@dataclass(frozen=True, kw_only=True)
class PendingBody:
    """A position still awaiting the broker's final bar at POST part 0 (INFO, never a FAULT, §B8)."""

    adapter_id: str
    position_id: str
    symbol: str = ""
    side: str = ""


@dataclass(frozen=True, kw_only=True)
class ExtraBody:
    """A Q9 extra item (tracker notices, MD_RESET, FAILED_CLEARED): ids and a reason code."""

    kind: str
    adapter_id: str = ""
    position_id: str = ""
    symbol: str = ""
    side: str = ""
    reason: str = ""
    at_ms: int = 0


@dataclass(frozen=True, kw_only=True)
class FailedLine:
    """An open FAILED scope, shown as a status line (§B14): kind, id and reason code."""

    scope_kind: str
    scope_id: str
    reason: str


@dataclass(frozen=True, kw_only=True)
class AliveCounts:
    armed: int = 0
    faulting: int = 0
    unmanaged: int = 0


@dataclass(frozen=True, kw_only=True)
class NoticeBody:
    """One ARM notice as its outbox row holds it (ids, codes, order state)."""

    notice_id: int
    route: str
    event: str
    bar_spec: str = ""
    bar_end_ms: int
    part: int = 0
    item_key: str = ""
    created_ms: int
    expires_at_ms: int | None = None
    items: tuple[ItemBody, ...] = ()
    pending: tuple[PendingBody, ...] = ()
    extras: tuple[ExtraBody, ...] = ()
    failed: tuple[FailedLine, ...] = ()
    alive: AliveCounts | None = None
    channel: str | None = None
    generation: str | None = None


def _dec_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _item_json(i: ItemBody) -> dict[str, object]:
    out = _item_core(i)
    if i.stop_now is not None:      # additive key: absent when unknown, so earlier bodies keep their exact text
        out["stop_now"] = _dec_text(i.stop_now)
    return out


def _item_core(i: ItemBody) -> dict[str, object]:
    return {
        "decision_id": i.decision_id, "decision_bar_end_ms": i.decision_bar_end_ms, "adapter_id": i.adapter_id,
        "position_id": i.position_id, "symbol": i.symbol, "side": i.side, "event": i.event,
        "bar_end_ms": i.bar_end_ms, "revision": i.revision, "outcome": i.outcome, "category": i.category,
        "reason": i.reason, "reason_kind": i.reason_kind, "trigger_met": i.trigger_met, "borderline": i.borderline,
        "advice": i.advice, "proposed_stop": _dec_text(i.proposed_stop),
        "current_risk_r": _dec_text(i.current_risk_r), "first_bar_ratio": _dec_text(i.first_bar_ratio),
        "caveats": list(i.caveats), "quality_flags": list(i.quality_flags), "stop_basis": i.stop_basis,
        "trigger_source": i.trigger_source, "handles": list(i.handles), "trigger_input": i.trigger_input,
        "entry": _dec_text(i.entry), "s0": _dec_text(i.s0), "supersedes": i.supersedes,
        "decided_at_ms": i.decided_at_ms, "strategy_kind": i.strategy_kind,
        "strategy_impl_version": i.strategy_impl_version, "valid_until_ms": i.valid_until_ms,
        "stop_read_at_ms": i.stop_read_at_ms, "late": i.late, "loosen_policy": i.loosen_policy,
    }


def encode_body(body: NoticeBody) -> str:
    """The canonical JSON text of a body (sorted keys, ASCII, levels as plain decimal text)."""
    out: dict[str, object] = {
        "v": BODY_VERSION, "notice_id": body.notice_id, "route": body.route, "event": body.event,
        "bar_spec": body.bar_spec, "bar_end_ms": body.bar_end_ms, "part": body.part, "item_key": body.item_key,
        "created_ms": body.created_ms, "expires_at_ms": body.expires_at_ms,
        "items": [_item_json(i) for i in body.items],
        "pending": [{"adapter_id": p.adapter_id, "position_id": p.position_id, "symbol": p.symbol, "side": p.side}
                    for p in body.pending],
        "extras": [{"kind": e.kind, "adapter_id": e.adapter_id, "position_id": e.position_id, "symbol": e.symbol,
                    "side": e.side, "reason": e.reason, "at_ms": e.at_ms} for e in body.extras],
        "failed": [{"scope_kind": f.scope_kind, "scope_id": f.scope_id, "reason": f.reason} for f in body.failed],
        "alive": None if body.alive is None else {"armed": body.alive.armed, "faulting": body.alive.faulting,
                                                  "unmanaged": body.alive.unmanaged},
        "channel": body.channel, "generation": body.generation,
    }
    return json.dumps(out, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _obj(raw: object, keys: frozenset[str], what: str) -> Mapping[str, Any]:
    if not isinstance(raw, dict) or set(raw) - keys:
        raise BodyError(f"{what}: not an object of the allowed keys")
    return cast(Mapping[str, Any], raw)


def _int(v: object, *, none: bool = False) -> int | None:
    if v is None and none:
        return None
    if isinstance(v, bool) or not isinstance(v, int):
        raise BodyError("an integer field holds something else")
    return v


def _req_int(v: object) -> int:
    out = _int(v)
    assert out is not None
    return out


def _str(v: object, *, none: bool = False) -> str | None:
    if v is None and none:
        return None
    if not isinstance(v, str):
        raise BodyError("a text field holds something else")
    return v


def _req_str(v: object) -> str:
    out = _str(v)
    assert out is not None
    return out


def _dec(v: object) -> Decimal | None:
    if v is None:
        return None
    if not isinstance(v, str):
        raise BodyError("a level is decimal text")
    try:
        d = Decimal(v)
    except InvalidOperation:
        raise BodyError("a level is decimal text") from None
    if not d.is_finite():
        raise BodyError("a level is finite")
    return d


def _bool(v: object, *, none: bool = False) -> bool | None:
    if v is None and none:
        return None
    if not isinstance(v, bool):
        raise BodyError("a flag holds something else")
    return v


def _texts(v: object) -> tuple[str, ...]:
    if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
        raise BodyError("a list of codes holds something else")
    return tuple(cast(list[str], v))


def _list(v: object) -> list[object]:
    if not isinstance(v, list):
        raise BodyError("a list field holds something else")
    return cast(list[object], v)


LOOSEN_POLICIES: Final = frozenset({"REARM", "TELL_ONLY"})
"""The pinned Q10 codes an item may carry (`src.riskmgr.strategy.LoosenPolicy` values, repeated: no S28 module imports
the strategy protocol's enum for a body check)."""


def _policy(v: object) -> str | None:
    out = _str(v, none=True)
    if out is not None and out not in LOOSEN_POLICIES:
        raise BodyError("an unknown loosen policy")
    return out


def _item(raw: object) -> ItemBody:
    m = _obj(raw, ITEM_KEYS, "item")
    g = m.get
    return ItemBody(
        decision_id=_req_int(g("decision_id")), decision_bar_end_ms=_req_int(g("decision_bar_end_ms")),
        adapter_id=_req_str(g("adapter_id")), position_id=_req_str(g("position_id")),
        symbol=_req_str(g("symbol", "")), side=_req_str(g("side", "")), event=_req_str(g("event")),
        bar_end_ms=_req_int(g("bar_end_ms")), revision=_req_int(g("revision", 0)), outcome=_req_str(g("outcome")),
        category=_req_str(g("category")), reason=_str(g("reason"), none=True),
        reason_kind=_str(g("reason_kind"), none=True), trigger_met=_bool(g("trigger_met"), none=True),
        borderline=bool(_bool(g("borderline", False))), advice=bool(_bool(g("advice", False))),
        proposed_stop=_dec(g("proposed_stop")), current_risk_r=_dec(g("current_risk_r")),
        first_bar_ratio=_dec(g("first_bar_ratio")), caveats=_texts(g("caveats", [])),
        quality_flags=_texts(g("quality_flags", [])), stop_basis=_str(g("stop_basis"), none=True),
        trigger_source=_str(g("trigger_source"), none=True), handles=_texts(g("handles", [])),
        trigger_input=_str(g("trigger_input"), none=True), entry=_dec(g("entry")), s0=_dec(g("s0")),
        supersedes=_int(g("supersedes"), none=True), decided_at_ms=_req_int(g("decided_at_ms", 0)),
        strategy_kind=_req_str(g("strategy_kind", "")), strategy_impl_version=_req_int(g("strategy_impl_version", 0)),
        valid_until_ms=_int(g("valid_until_ms"), none=True), stop_read_at_ms=_int(g("stop_read_at_ms"), none=True),
        late=bool(_bool(g("late", False))), loosen_policy=_policy(g("loosen_policy")), stop_now=_dec(g("stop_now")))


def decode_body(text: str) -> NoticeBody:
    """Parse an ARM outbox body; every key is checked against the allow-lists (`BodyError` otherwise)."""
    try:
        raw = json.loads(text)
    except (ValueError, RecursionError):
        raise BodyError("not JSON") from None
    m = _obj(raw, BODY_KEYS, "body")
    if m.get("v") != BODY_VERSION:
        raise BodyError("unknown body version")
    alive_raw = m.get("alive")
    alive = None
    if alive_raw is not None:
        a = _obj(alive_raw, ALIVE_KEYS, "alive")
        alive = AliveCounts(armed=_req_int(a.get("armed", 0)), faulting=_req_int(a.get("faulting", 0)),
                            unmanaged=_req_int(a.get("unmanaged", 0)))
    pending = []
    for p in _list(m.get("pending", [])):
        pm = _obj(p, PENDING_KEYS, "pending")
        pending.append(PendingBody(adapter_id=_req_str(pm.get("adapter_id")),
                                   position_id=_req_str(pm.get("position_id")),
                                   symbol=_req_str(pm.get("symbol", "")), side=_req_str(pm.get("side", ""))))
    extras = []
    for e in _list(m.get("extras", [])):
        em = _obj(e, EXTRA_KEYS, "extra")
        extras.append(ExtraBody(kind=_req_str(em.get("kind")), adapter_id=_req_str(em.get("adapter_id", "")),
                                position_id=_req_str(em.get("position_id", "")),
                                symbol=_req_str(em.get("symbol", "")), side=_req_str(em.get("side", "")),
                                reason=_req_str(em.get("reason", "")), at_ms=_req_int(em.get("at_ms", 0))))
    failed = []
    for f in _list(m.get("failed", [])):
        fm = _obj(f, FAILED_KEYS, "failed")
        failed.append(FailedLine(scope_kind=_req_str(fm.get("scope_kind")), scope_id=_req_str(fm.get("scope_id")),
                                 reason=_req_str(fm.get("reason"))))
    return NoticeBody(
        notice_id=_req_int(m.get("notice_id")), route=_req_str(m.get("route")), event=_req_str(m.get("event")),
        bar_spec=_req_str(m.get("bar_spec", "")), bar_end_ms=_req_int(m.get("bar_end_ms")),
        part=_req_int(m.get("part", 0)), item_key=_req_str(m.get("item_key", "")),
        created_ms=_req_int(m.get("created_ms")), expires_at_ms=_int(m.get("expires_at_ms"), none=True),
        items=tuple(_item(i) for i in _list(m.get("items", []))), pending=tuple(pending), extras=tuple(extras),
        failed=tuple(failed), alive=alive, channel=_str(m.get("channel"), none=True),
        generation=_str(m.get("generation"), none=True))


# ---------------------------------------------------------------------------------------------------- input bars
@dataclass(frozen=True)
class InputRef:
    """A parsed trigger input: the series, the bar end and the keyed content hash the decision stored."""

    series: SeriesId
    bar_end_ms: int
    content_hash: str

    @property
    def text(self) -> str:
        return f"{self.series.canonical}|{self.bar_end_ms}|{self.content_hash}"


def parse_input(text: str) -> InputRef | None:
    """`series|bar_end|hash` (the `arm_decisions.series_inputs` form) or `S|series|bar_end|hash`
    (`SeriesInput.text`); None when malformed."""
    parts = text.split("|")
    if len(parts) == 4 and parts[0] == "S":
        parts = parts[1:]
    if len(parts) != 3:
        return None
    try:
        return InputRef(SeriesId.parse(parts[0]), int(parts[1]), parts[2])
    except ValueError:
        return None


def trigger_input(series_inputs: Sequence[str], *, bar_end_ms: int, bar_spec: str, side: str) -> str | None:
    """The trigger bar of a bar-end decision among its series inputs: a BROKER bar of the decision's spec ending at
    its bar end; with both quote sides present, the exit side (bid for a long, ask for a short). None if absent."""
    found: dict[str, InputRef] = {}
    for text in series_inputs:
        ref = parse_input(text)
        if (ref is not None and ref.bar_end_ms == bar_end_ms and ref.series.spec.name == bar_spec
                and ref.series.source is Source.BROKER):
            found[ref.series.canonical] = ref
    if len(found) == 1:
        return next(iter(found.values())).text
    exit_side = QuoteSide.BID if side == "BUY" else QuoteSide.ASK
    for ref in found.values():
        if ref.series.side is exit_side:
            return ref.text
    return None


def bar_reads(body: NoticeBody) -> tuple[tuple[str, int], ...]:
    """The `(series_id, bar_end_ms)` reads the relay makes before rendering (none for PRE items)."""
    out: list[tuple[str, int]] = []
    for item in body.items:
        if item.event == "PRE" or item.trigger_input is None:
            continue
        ref = parse_input(item.trigger_input)
        if ref is not None and (ref.series.canonical, ref.bar_end_ms) not in out:
            out.append((ref.series.canonical, ref.bar_end_ms))
    return tuple(out)


@dataclass(frozen=True)
class Favour:
    favour_r: RMultiple
    per_risk: Ratio | None


def favour_figures(item: ItemBody, bars: Mapping[tuple[str, int], Bar] | None,
                   keyring: Keyring | None) -> Favour | None:
    """Favour R and favour / risk from a re-read bar whose keyed hash verifies (R11-M14); None otherwise, and always
    None for a PRE item (C5). Favour / risk divides by `current_risk_r` (the stop read now), which is the stop the
    trigger tested only when nothing moved it after the bar end: it is shown only for a plain on-time POST with no
    `pre_basis_held` / `stop_changed_after_bar_end` caveat and no `AT_PRE` basis, and only when it agrees with the
    decision (`>= 1` with the trigger met, `< 1` without; within `PER_RISK_BAND` of 1 either way, or `borderline`,
    since `ge` / `gt` and the 0.01 R rounding blur it). Otherwise it is omitted rather than shown contradicting the
    decision (live review: a stop moved after the bar end gave "favour / risk 2.000" next to BELOW_TRIGGER)."""
    if item.event == "PRE" or bars is None or keyring is None or item.trigger_input is None:
        return None
    ref = parse_input(item.trigger_input)
    if ref is None or item.entry is None or item.s0 is None or item.side not in ("BUY", "SELL"):
        return None
    bar = bars.get((ref.series.canonical, ref.bar_end_ms))
    if bar is None or bar.series != ref.series or bar.bucket_end_ms != ref.bar_end_ms:
        return None
    if verify(keyring, ref.content_hash, ref.series.canonical, ref.bar_end_ms, bar.open, bar.high, bar.low,
              bar.close) is not HashStatus.VALID:
        return None
    sig = Decimal(sigma("BUY") if item.side == "BUY" else sigma("SELL"))
    r = sig * (item.entry - item.s0)
    if r <= 0:
        return None
    favour = RMultiple.of(sig * (bar.close - item.entry) / r)
    per_risk = None
    if item.current_risk_r is not None and item.current_risk_r > 0 and _per_risk_basis_known(item):
        ratio = favour.value / item.current_risk_r
        near = item.borderline or abs(ratio - 1) <= PER_RISK_BAND
        if near or (ratio >= 1) is item.trigger_met:
            per_risk = Ratio.of(ratio)
    return Favour(favour, per_risk)


PER_RISK_BAND: Final = Decimal("0.03")
"""Favour / risk this close to 1 is consistent with either decision (the strategy's borderline band)."""
_PER_RISK_UNKNOWN_BASIS: Final = frozenset({"pre_basis_held", "stop_changed_after_bar_end"})


def _per_risk_basis_known(item: ItemBody) -> bool:
    """`current_risk_r` is the risk the trigger tested: a plain on-time POST with a decided trigger, no caveat saying
    the tested stop differs from the current one, and no `AT_PRE` basis."""
    return (item.event == "POST" and not item.late and item.trigger_met is not None
            and item.stop_basis != "AT_PRE" and not _PER_RISK_UNKNOWN_BASIS.intersection(item.caveats))


# ---------------------------------------------------------------------------------------------------- the renderer
@dataclass(frozen=True, kw_only=True)
class RenderSettings:
    """Local render settings: `show_levels` is Q13 (entry shown too). The Q10 text is **not** a render setting: it
    follows each position's pinned `loosen_policy` carried on its item (round 13)."""

    show_levels: bool = False


@dataclass(frozen=True)
class Rendered:
    subject: str
    text: str


def esc(text: str | None) -> str:
    return "" if text is None else escape_text(text, TEXT_LIMIT)


def times(bar_end_ms: int) -> str:
    """`HH:MM UTC / HH:MM Melbourne` (with the UTC date)."""
    utc = datetime.fromtimestamp(bar_end_ms / 1000, tz=UTC)
    mel = utc.astimezone(MELBOURNE)
    return f"{utc:%Y-%m-%d %H:%M} UTC / {mel:%H:%M} Melbourne"


def subject(body: NoticeBody) -> str:
    """The subject: route, event, part, the bar end in UTC and Melbourne and the notice id, so a duplicate is
    recognisable (§B8). Computable from ids alone, so the outbox row carries it from its INSERT."""
    part = f" part {body.part}" if body.event in ("PRE", "POST") and not body.item_key else ""
    item = " item" if body.item_key else ""
    when = times(body.bar_end_ms) if body.bar_end_ms > 0 else times(body.created_ms)
    return f"ARM {esc(body.route)} {esc(body.event)}{part}{item} | {when} | #{body.notice_id}"


def _clock(ms: int) -> str:
    utc = datetime.fromtimestamp(ms / 1000, tz=UTC)
    return f"{utc:%H:%M:%S} UTC / {utc.astimezone(MELBOURNE):%H:%M:%S} Melbourne"


def _reason_text(item: ItemBody) -> str:
    reason = item.reason or ""
    if item.reason_kind == "first_bar":
        if reason == PRE_ANCHOR:
            return PRE_ANCHOR_TEXT
        if reason == FIRST_BAR_VOLATILE:
            return "the first bar was too volatile"
        if reason == FIRST_BAR_UNKNOWN:
            return "the first-bar check could not be decided"
    owner = f"{esc(item.reason_kind)}: " if item.reason_kind else ""
    return f"{owner}{esc(reason)}" if reason else "no reason code"


def outcome_text(item: ItemBody, settings: RenderSettings) -> str:
    """The outcome line of one item (§B8 texts)."""
    o = item.outcome
    if o == Outcome.ADJUST.value:
        stop = "" if item.proposed_stop is None else f" to {format(item.proposed_stop, 'f')}"
        what = [f"handles {', '.join(esc(h) for h in item.handles)}"] if item.handles else []
        if item.stop_read_at_ms is not None:
            what.append(f"stop read at {_clock(item.stop_read_at_ms)}")
        detail = f" ({'; '.join(what)})" if what else ""
        if item.event == "PRE":
            return f"provisional: the rule would move the stop{stop}{detail} {STILL_WORSE}"
        return f"ADJUST: move the stop{stop}{detail} {STILL_WORSE}"
    if o == Outcome.COMPLETE.value:
        return f"{COMPLETE_TEXT} ({q10_text(item.side, item.loosen_policy == 'REARM')})"
    if o == Outcome.PRE_NOT_CONFIRMED.value:
        why = "at target, not yet confirmed" if item.reason == Outcome.AT_TARGET_UNCONFIRMED.value \
            else _reason_text(item)
        return f"{PRE_NOT_CONFIRMED_TEXT} (reason: {why}); {PRE_NOT_CONFIRMED_RELEASE}"
    if o == Outcome.AT_TARGET_UNCONFIRMED.value:
        return AT_TARGET_TEXT
    if o == Outcome.LOOSENED_AFTER_COMPLETE.value:
        # the outcome exists only for a TELL_ONLY pin (a REARM pin re-arms instead): always the tell-only text
        return f"the stop was moved back after release ({q10_text(item.side, False)})"
    if o == Outcome.BROKER_AHEAD.value:
        return "the broker stop is already at or beyond the proposed level; nothing to move"
    if o in (Outcome.STRATEGY_REASON.value, Outcome.GUARD_REASON.value):
        return f"{esc(item.category)}: {_reason_text(item)}"
    if o == Outcome.MARKET_CLOSED.value:
        return "HOLD: market closed now; no move"
    if o == Outcome.NO_BROKER_BAR.value:
        if item.reason == NO_BROKER_BAR_PRE_REASON:
            return (f"no broker bar: the broker did not trade this bar, so the heads-up was not confirmed; "
                    f"{PRE_NOT_CONFIRMED_RELEASE}")
        return "no broker bar: the broker did not trade this bar; nothing to decide"
    if o == Outcome.PRICE_THROUGH_LEVEL.value:
        return "HOLD: price is too close to or through the proposed level; no move"
    if o == Outcome.NO_STOP.value:
        return "FAULT: no effective stop found"
    if o == Outcome.DATA_UNAVAILABLE.value:
        return f"FAULT: data unavailable ({esc(item.reason)})"
    if o == Outcome.STRATEGY_ERROR.value:
        return "FAULT: strategy error"
    if o == Outcome.WRITE_ERROR.value:
        return "FAULT: write error"
    if o == Outcome.UNRESOLVED_RANGE.value:
        return f"{esc(item.category)}: bars unresolved ({esc(item.reason)})"
    return f"{esc(item.category)}: {esc(o)}"


def _event_label(item: ItemBody) -> str:
    if item.event == "LATE" or item.late:
        return "LATE (decided after its bar's notice window) "
    if item.event == "REOPEN":
        return "REOPEN (re-check at the market open) "
    if item.event == "CORRECTED":
        withdrawn = "; the earlier advice is withdrawn" if item.supersedes is not None \
            and item.outcome != Outcome.ADJUST.value else ""
        return f"CORRECTED (revision {item.revision}{withdrawn}) "
    return ""


def item_lines(item: ItemBody, route: str, *, favour: Favour | None, settings: RenderSettings) -> list[str]:
    head = (f"- {esc(route)} | position {esc(item.position_id)} ({esc(item.adapter_id)}) | {esc(item.symbol)} "
            f"{esc(item.side)} | bar {times(item.bar_end_ms)}")
    lines = [head, f"  {_event_label(item)}{outcome_text(item, settings)}"]
    if item.event == "PRE" and FIRST_BAR_CONDITIONAL in item.caveats:
        lines.append(f"  {pre_label(bool(item.trigger_met))}")
    if item.event == "PRE" and item.valid_until_ms is not None:
        lines.append(f"  valid until {_clock(item.valid_until_ms)}")
    if DELAYED in item.caveats and item.decided_at_ms > item.bar_end_ms:
        lines.append(f"  DELAYED: decided {(item.decided_at_ms - item.bar_end_ms) // 60_000} min after bar end")
    figures: list[str] = []
    if item.current_risk_r is not None:
        figures.append(f"current risk {format(item.current_risk_r, 'f')} R")
    if favour is not None:
        figures.append(f"favour {format(favour.favour_r.value, 'f')} R")
        if favour.per_risk is not None:
            figures.append(f"favour / risk {format(favour.per_risk.value, 'f')}")
    if item.first_bar_ratio is not None:
        figures.append(f"first-bar ratio {format(item.first_bar_ratio, 'f')}")
    if item.borderline:
        figures.append("borderline")
    if settings.show_levels and item.entry is not None and item.event != "PRE":
        # a PRE ADJUST item carries its pinned entry for the HTML stop block only; the text stays as before
        figures.append(f"entry {format(item.entry, 'f')}")
    if figures:
        lines.append("  " + "; ".join(figures))
    sources = [f"stop basis {esc(item.stop_basis)}" if item.stop_basis else "",
               f"close source {esc(item.trigger_source)}" if item.trigger_source else "",
               f"quality {', '.join(esc(q) for q in item.quality_flags)}" if item.quality_flags else ""]
    caveats = [CAVEAT_TEXT.get(c, esc(c)) for c in item.caveats if c not in (FIRST_BAR_CONDITIONAL, DELAYED)]
    if caveats:
        sources.append("caveats: " + "; ".join(caveats))
    sources.append(f"strategy {esc(item.strategy_kind)} v{item.strategy_impl_version}" if item.strategy_kind else "")
    sources.append(f"decision {item.decision_id}")
    lines.append("  " + " | ".join(s for s in sources if s))
    return lines


def render(body: NoticeBody, *, bars: Mapping[tuple[str, int], Bar] | None = None, keyring: Keyring | None = None,
           settings: RenderSettings | None = None) -> Rendered:
    """Subject and text of one notice at send time. `bars` holds the re-read input bars keyed `(series_id,
    bar_end_ms)` (see `bar_reads`); favour figures need the keyring and a verified hash (C5: never at PRE)."""
    s = settings or RenderSettings()
    lines: list[str] = []
    if body.event == "PRE":
        lines.append(PRE_HEADER)
    for item in body.items:
        lines.extend(item_lines(item, body.route, favour=favour_figures(item, bars, keyring), settings=s))
    for p in body.pending:
        lines.append(f"- {esc(body.route)} | position {esc(p.position_id)} ({esc(p.adapter_id)}) | {esc(p.symbol)} "
                     f"{esc(p.side)} | INFO: {AWAITING_TEXT}")
    for e in body.extras:
        what = EXTRA_TEXT.get(e.kind, esc(e.kind))
        who = f"position {esc(e.position_id)} ({esc(e.adapter_id)}) {esc(e.symbol)} {esc(e.side)}: " \
            if e.position_id else ""
        reason = f" ({esc(e.reason)})" if e.reason else ""
        lines.append(f"- {who}{what}{reason}")
    if body.event == "MD_RESET" and body.generation:
        lines.append(f"market-data generation {esc(body.generation)}; advice resumes once the series are LIVE")
    if body.alive is not None:
        lines.append(f"ARM alive: {body.alive.armed} position(s) managed, {body.alive.faulting} in FAULT, "
                     f"{body.alive.unmanaged} not managed")
    if body.event == "TEST" and body.channel:
        lines.append(f"daily test notice on channel {esc(body.channel)}")
    for f in body.failed:
        lines.append(f"FAILED: {esc(f.scope_kind)} {esc(f.scope_id)} ({esc(f.reason)}); see arm failed show")
    return Rendered(subject(body), "\n".join(lines))


__all__ = [
    "ALIVE_KEYS", "AT_TARGET_TEXT", "AWAITING_TEXT", "AliveCounts", "BODY_KEYS", "BODY_VERSION", "BodyError",
    "CAVEAT_TEXT", "COMPLETE_TEXT", "DELAYED", "EXTRA_KEYS", "EXTRA_TEXT", "ExtraBody", "FAILED_KEYS",
    "FIRST_BAR_CONDITIONAL", "FailedLine", "Favour", "ITEM_KEYS", "InputRef", "ItemBody", "LOOSEN_POLICIES",
    "MARKET_CLOSES",
    "NoticeBody", "PENDING_KEYS", "PRE_HEADER", "PRE_NOT_CONFIRMED_RELEASE", "PRE_NOT_CONFIRMED_TEXT",
    "PREVIOUS_POST_PENDING", "PendingBody", "Q10_REARM", "Q10_TELL", "RenderSettings", "Rendered", "STILL_WORSE",
    "bar_reads",
    "decode_body", "encode_body", "esc", "favour_figures", "item_lines", "outcome_text", "parse_input", "q10_text",
    "render",
    "subject", "times", "trigger_input",
]
