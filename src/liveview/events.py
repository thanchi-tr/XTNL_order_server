"""Events, typed slots, title templates and the privacy guard (docs/design/live-view.md s4.4, s4.4.5).

Every event and every payload row passes `assert_safe` / `guard_payload` before it is cached or sent. The guard
raises `PrivacyError` (never `assert`, which `-O` strips). Two outcomes, never more:
- a forbidden or unknown name drops the whole item (PrivacyError);
- a value that fails its slot type is replaced by "(not shown)" and the item is kept.

No slot holds free text. A title is rendered only from its kind's template and typed slots; outside the level
slots a rendered title contains no decimal number.
"""
from __future__ import annotations

import logging
import re
import time
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Final

log = logging.getLogger("xtnl.liveview.guard")

NOT_SHOWN: Final = "(not shown)"

CODE_RE: Final = re.compile(r"^[A-Za-z0-9_():,-]{1,80}$")
ID_RE: Final = re.compile(r"^[A-Za-z0-9:_@/-]{1,64}$")
OPERATOR_RE: Final = re.compile(r"^[A-Za-z0-9._@-]{2,64}$")
COMMAND_RE: Final = re.compile(r"^[a-z_]+(\.[a-z_]+){0,3}$")
SYMBOL_RE: Final = re.compile(r"^[A-Za-z0-9._/:#?-]{1,40}$")
DECIMAL_IN_TEXT: Final = re.compile(r"\d[.,]\d")
LEVEL_RE: Final = re.compile(r"^-?[0-9]{1,12}(\.[0-9]{1,12})?$")

#: Whole tokens (names split on `_`) that never appear in a name the page receives.
DENY_TOKENS: Final = frozenset({"units", "qty", "size", "balance", "equity", "margin", "pnl", "amount", "commission",
                                "swap", "spread", "pip", "pips", "point", "bid", "ask", "mid", "price", "notional",
                                "currency", "payload", "body", "detail", "error"})
DENY_NAMES: Final = frozenset({"price_increment", "min_stop_distance"})

LEVEL_SLOTS: Final = frozenset({"entry", "stop", "take_profit", "from_level", "to_level"})
#: Kinds whose titles may carry levels (order, bracket and position kinds).
LEVEL_KIND_PREFIXES: Final = ("order.", "bracket.", "position.")

#: Slot name -> slot type. A kind's slots are the fields of its template, each of which must be named here.
SLOT_TYPES: Final[Mapping[str, str]] = {
    "symbol": "symbol", "side": "side", "oid": "id", "pid": "id", "id": "id", "adapter": "code", "stream": "code",
    "status": "code", "leg": "code", "kind": "code", "rule": "code", "level": "code", "from_halt": "code",
    "to_halt": "code", "reason": "code", "subject": "id", "account": "id", "node": "id", "run": "id",
    "transition": "code", "event_type": "code", "state": "code", "outcome": "code", "command": "command",
    "target": "id", "operator": "operator", "by": "operator", "epoch": "count", "term": "count", "n": "count",
    "dlq": "count", "reviewed": "count", "seen": "count", "new": "count", "updated": "count", "closed": "count",
    "anomalies": "count", "hold_count": "count", "secs": "count", "retries": "count", "sev": "code",
    "entry": "level", "stop": "level", "take_profit": "level", "from_level": "level", "to_level": "level",
    "at": "time", "since": "time", "until": "time", "gap": "duration", "offline": "duration", "lead": "code",
    "streams": "code", "cause": "code", "completeness": "code", "extra": "suffixes",
}

#: Optional title suffixes: the `extra` slot is a list of these names, each rendered from the same typed slots.
SUFFIXES: Final[Mapping[str, str]] = {
    "rule": " ({rule})", "reason": " ({reason})", "reviewed": " (reviewed event {reviewed})",
    "reason_in_audit": "; reason recorded in operator_audit, read it with src.ops",
    "forced": " (forced)", "latch": " (hard latch)", "offline": ", offline {offline}",
    "anomalies": ", {anomalies} anomalies", "target": " {target}", "approver": ", approved by {by}",
    "tp_cancelled": " (take-profit cancelled)", "sl_cancelled": " (stop-loss cancelled)",
    "closing_trade": " (closing trade)", "level": " ({level})", "capped": ", capped at SOFT",
    "end": " ({reason})", "abnormal": ": {hold_count} abnormal term ends within 15 min",
    "epoch_changes": ": {hold_count} epoch changes within 30 min", "already": " (was already set)",
    "account": " for account {account}", "subject": " ({subject})", "repage": " (re-page)",
    "delivered": " (delivered)",
}

#: Names allowed in Event.ids.
ID_NAMES: Final = frozenset({"position_id", "broker_order_id", "client_order_id", "account_id", "adapter_id",
                             "dlq_id", "origin_dlq_id", "entity_id", "term_id", "run_id", "epoch", "outbox_id",
                             "event_id", "halt_event_id", "audit_id", "catchup_id", "breach_id", "log_id", "rule_id",
                             "symbol", "node", "stream", "sweep_id", "reviewed_event_id", "linked_position_id",
                             "subject_id"})

CATEGORIES: Final = frozenset({"orders", "positions", "risk", "halt", "alerts", "operator", "dlq", "engine",
                               "streams", "catchup"})
SEVERITIES: Final = ("debug", "info", "notice", "warn", "alert")
SEVERITY_RANK: Final[Mapping[str, int]] = {s: i for i, s in enumerate(SEVERITIES)}


@dataclass(frozen=True)
class Kind:
    category: str
    severity: str
    template: str


#: The closed enum of event kinds. `{p}` is the "SYMBOL SIDE id" lead, rendered from symbol / side / id slots.
KINDS: Final[Mapping[str, Kind]] = {
    # orders
    "order.placed": Kind("orders", "info", "{p}{kind} order placed at {entry}"),
    "order.placed_nolevel": Kind("orders", "info", "{p}{kind} order placed"),
    "order.found": Kind("orders", "info", "{p}order found on reconnect ({status})"),
    "order.first_seen": Kind("orders", "info", "{p}first seen by live view: {status}"),
    "order.status": Kind("orders", "info", "{p}order {status}"),
    "order.cancelled": Kind("orders", "notice", "{p}order cancelled"),
    "order.filled": Kind("orders", "notice", "{p}entry filled"),
    "order.entry_moved": Kind("orders", "notice", "{p}entry moved {from_level} → {to_level}"),
    "order.closing_working": Kind("orders", "info", "{p}closing order working"),
    "order.closing_trade": Kind("positions", "notice", "{p}closed (closing trade)"),
    "order.no_change": Kind("orders", "debug", "{p}updated, no visible change"),
    "order.stale": Kind("orders", "debug", "{p}stale read dropped"),
    "bracket.attached": Kind("orders", "info", "{p}{leg} {entry} attached"),
    "bracket.attached_nolevel": Kind("orders", "info", "{p}{leg} attached"),
    "bracket.moved": Kind("orders", "notice", "{p}{leg} moved {from_level} → {to_level}"),
    "bracket.cancelled": Kind("orders", "info", "{p}{leg} cancelled"),
    "bracket.cancelled_with_close": Kind("orders", "debug", "{p}{leg} cancelled with the close"),
    "bracket.filled": Kind("positions", "notice", "{p}{leg} filled"),
    # positions
    "position.opened": Kind("positions", "notice", "{p}position opened at {entry}"),
    "position.opened_nolevel": Kind("positions", "notice", "{p}position opened"),
    "position.found": Kind("positions", "info", "{p}position found on reconnect"),
    "position.first_seen": Kind("positions", "info", "{p}first seen by live view: {status}"),
    "position.filled_opened": Kind("positions", "notice", "{p}entry filled → position opened at {entry}"),
    "position.filled_opened_nolevel": Kind("positions", "notice", "{p}entry filled → position opened"),
    "position.stop_moved": Kind("positions", "notice", "{p}stop-loss moved {from_level} → {to_level}"),
    "position.stop_set": Kind("positions", "notice", "{p}stop-loss set at {to_level}"),
    "position.stop_removed": Kind("positions", "notice", "{p}stop-loss removed"),
    "position.target_moved": Kind("positions", "notice", "{p}take-profit moved {from_level} → {to_level}"),
    "position.target_set": Kind("positions", "notice", "{p}take-profit set at {to_level}"),
    "position.target_removed": Kind("positions", "notice", "{p}take-profit removed"),
    "position.closed": Kind("positions", "notice", "{p}closed{extra}"),
    "position.closed_by": Kind("positions", "notice", "{p}closed by {leg}{extra}"),
    "position.no_change": Kind("positions", "debug", "{p}updated, no visible change"),
    "position.stale": Kind("positions", "debug", "{p}stale read dropped"),
    "account.updated": Kind("positions", "debug", "Account {account} record updated"),
    # risk
    "rule.transition": Kind("risk", "info", "{p}rule {rule} {transition}{extra}"),
    "rule.raised": Kind("risk", "warn", "{p}rule {rule} {transition} to {level}"),
    # halt
    "halt.raised": Kind("halt", "alert", "Halt {from_halt} → {to_halt}{extra}"),
    "halt.changed": Kind("halt", "notice", "Halt {from_halt} → {to_halt}{extra}"),
    "halt.cleared_operator": Kind("halt", "notice", "Halt cleared by {by}{extra}"),
    "halt.cleared": Kind("halt", "notice", "Halt cleared ({from_halt} → NONE)"),
    "halt.recorded": Kind("halt", "info", "Halt {to_halt} recorded{extra}"),
    # alerts
    "alert.retrying": Kind("alerts", "warn", "Alert retrying{extra}"),
    "alert.undeliverable": Kind("alerts", "warn", "Alert NOT delivered{extra}"),
    "alert.escalated": Kind("alerts", "notice", "Alert escalated{extra}"),
    "alert.notice": Kind("alerts", "info", "Engine notice e-mailed ({sev})"),
    "alert.rule_notice": Kind("alerts", "info", "Rule {rule} notice e-mailed"),
    "alert.badge_only": Kind("alerts", "debug", "Alert {state}{extra}"),
    # operator
    "operator.action": Kind("operator", "notice", "Operator {operator}: {command}{extra} ({outcome})"),
    # dlq
    "dlq.parked": Kind("dlq", "warn", "{event_type} {id} parked in DLQ{extra}"),
    "dlq.parked_again": Kind("dlq", "warn", "{event_type} {id} parked again (same lineage){extra}"),
    "dlq.retry": Kind("dlq", "debug", "DLQ row {dlq}: retry {retries}"),
    "dlq.cycle": Kind("dlq", "debug", "DLQ row {dlq}: {state}"),
    "dlq.quarantined": Kind("dlq", "alert", "DLQ row {dlq} quarantined after {retries} retries"),
    "dlq.released": Kind("dlq", "notice", "DLQ row {dlq} released from quarantine"),
    "dlq.resolved": Kind("dlq", "info", "DLQ row {dlq} resolved"),
    # catch-up
    "catchup.done": Kind("catchup", "info",
                         "{adapter} caught up ({reason}): {seen} orders seen, {new} new, {completeness}{extra}"),
    "catchup.incomplete": Kind("catchup", "warn",
                               "{adapter} catch-up ({reason}) incomplete: {seen} orders seen, {new} new{extra}"),
    # engine
    "engine.run_started": Kind("engine", "info", "Engine started on {node}"),
    "engine.run_stopped": Kind("engine", "info", "Engine stopped on {node}{extra}"),
    "engine.run_unclean": Kind("engine", "warn", "Previous run on {node} ended without a clean stop (last seen {at})"),
    "engine.run_superseded": Kind("engine", "info", "Run on {node} superseded"),
    "engine.term_started": Kind("engine", "info", "Term {term} started on {node} (epoch {epoch})"),
    "engine.leader": Kind("engine", "info", "{node} is LEADER (epoch {epoch})"),
    "engine.draining": Kind("engine", "notice", "Term {term} draining ({reason})"),
    "engine.term_ended": Kind("engine", "info", "Term {term} ended ({reason})"),
    "engine.term_closed_by": Kind("engine", "info", "Term {term} closed at the start of term {n}"),
    "engine.epoch": Kind("engine", "info", "Epoch changed to {epoch} on {node}"),
    "engine.safe_hold": Kind("engine", "warn", "SAFE_HOLD set{extra}"),
    "engine.safe_hold_resumed": Kind("engine", "notice", "SAFE_HOLD resumed by {by}"),
    "engine.safe_hold_resumed_unknown": Kind("engine", "notice", "SAFE_HOLD resumed"),
    "engine.not_responding": Kind("engine", "alert", "Engine stopped responding"),
    "engine.responding": Kind("engine", "info", "Engine responding again"),
    "engine.restarted": Kind("engine", "info", "Engine restarted (not responding for {secs} s)"),
    "engine.sweep_stalled": Kind("engine", "warn", "RiskSweep stalled for {secs} s"),
    "engine.sweep_passing": Kind("engine", "info", "RiskSweep passing again"),
    "engine.oversight_lapsed": Kind("risk", "warn", "Oversight lapsed: account {account}"),
    "engine.oversight_restored": Kind("risk", "info", "Oversight restored: account {account}"),
    "engine.auth_failed": Kind("engine", "warn", "{adapter}: AUTH_FAILED (re-arm needed)"),
    "engine.auth_rearmed": Kind("engine", "notice", "{adapter}: AUTH_FAILED re-armed"),
    "engine.auth_cleared": Kind("engine", "info", "{adapter}: AUTH_FAILED cleared (credentials changed)"),
    "engine.not_reading": Kind("engine", "info",
                               "Live view was not reading from {since} to {until}; rows from then are shown now, "
                               "marked late"),
    # streams
    "stream.fenced": Kind("streams", "info", "Streams fenced at leader start (epoch {epoch})"),
    "stream.lost": Kind("streams", "warn", "{adapter} {stream} stream lost"),
    "stream.reconciled": Kind("streams", "info", "{adapter} {stream} stream reconciled"),
    "stream.blip": Kind("streams", "warn", "{adapter} {stream} stream lost and reconciled between polls"),
    "stream.still_lost": Kind("streams", "warn", "{adapter} {stream} stream still lost 60 s after the start"),
}

_LEAD_SLOTS: Final = ("symbol", "side", "id")


class PrivacyError(Exception):
    """A forbidden or unknown name reached the guard: the whole item is dropped."""


@dataclass
class GuardStats:
    dropped: Counter[str] = field(default_factory=Counter)
    blanked: Counter[str] = field(default_factory=Counter)


STATS: Final = GuardStats()


def name_denied(name: str) -> bool:
    lowered = name.lower()
    return lowered in DENY_NAMES or bool(set(lowered.split("_")) & DENY_TOKENS)


def level_str(value: object) -> str | None:
    """A level as a plain decimal string, or None. Accepts Decimal, int and decimal strings (never float)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        text = format(value.normalize(), "f") if value == value.to_integral() else format(value, "f")
    elif isinstance(value, int):
        text = str(value)
    elif isinstance(value, str):
        try:
            d = Decimal(value.strip())
        except InvalidOperation:
            return None
        if not d.is_finite():
            return None
        text = format(d, "f")
    else:
        return None
    return text if LEVEL_RE.fullmatch(text) else None


def check_value(slot_type: str, value: object) -> bool:
    """Whether `value` is a valid value of `slot_type` (s4.4.5 rule 2)."""
    if slot_type == "code":
        return isinstance(value, str) and bool(CODE_RE.fullmatch(value)) and not DECIMAL_IN_TEXT.search(value)
    if slot_type == "command":
        return isinstance(value, str) and bool(COMMAND_RE.fullmatch(value))
    if slot_type == "id":
        return isinstance(value, str) and bool(ID_RE.fullmatch(value)) and not DECIMAL_IN_TEXT.search(value)
    if slot_type == "operator":
        return isinstance(value, str) and bool(OPERATOR_RE.fullmatch(value)) and not DECIMAL_IN_TEXT.search(value)
    if slot_type == "symbol":
        return isinstance(value, str) and bool(SYMBOL_RE.fullmatch(value)) and not DECIMAL_IN_TEXT.search(value)
    if slot_type == "side":
        return value in ("BUY", "SELL")
    if slot_type == "count":
        return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 10**9
    if slot_type in ("time", "duration"):
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0
    if slot_type == "bool":
        return isinstance(value, bool)
    if slot_type == "level":
        return isinstance(value, str) and bool(LEVEL_RE.fullmatch(value))
    if slot_type == "suffixes":
        return isinstance(value, (list, tuple)) and all(isinstance(v, str) and v in SUFFIXES for v in value)
    return False


def fmt_time(ms: int) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ms / 1000))


def fmt_duration(ms: int) -> str:
    s = ms // 1000
    if s < 60:
        return f"{s} s"
    m = s // 60
    if m < 60:
        return f"{m} min" if s % 60 == 0 or m >= 10 else f"{m} min {s % 60} s"
    h = m // 60
    return f"{h} h {m % 60} min" if m % 60 else f"{h} h"


def short_id(value: str, limit: int = 24) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def render(kind: str, slots: Mapping[str, object]) -> str:
    """The kind's template filled from typed slots (values already checked). Raises PrivacyError on an unknown
    kind or slot name, or when the title carries a decimal number outside the level slots."""
    spec = KINDS.get(kind)
    if spec is None:
        raise PrivacyError(f"unknown kind {kind!r}")
    shown: dict[str, str] = {}
    for name, value in slots.items():
        stype = SLOT_TYPES.get(name)
        if stype is None or name_denied(name):
            raise PrivacyError(f"unknown slot {name!r}")
        if stype == "level" and not kind.startswith(LEVEL_KIND_PREFIXES):
            raise PrivacyError(f"level slot {name!r} outside the level kinds")
        if value is None or stype == "suffixes":
            shown[name] = ""
        elif value == NOT_SHOWN:
            shown[name] = NOT_SHOWN
        elif stype == "time":
            shown[name] = fmt_time(int(value))   # type: ignore[call-overload]
        elif stype == "duration":
            shown[name] = fmt_duration(int(value))   # type: ignore[call-overload]
        elif stype == "id":
            shown[name] = short_id(str(value))
        else:
            shown[name] = str(value)
    lead = " ".join(shown[s] for s in _LEAD_SLOTS if s in shown and slots.get(s) is not None)
    fields = {**{n: "" for n in SLOT_TYPES}, **shown, "p": f"{lead}: " if lead else ""}
    extra = slots.get("extra")
    if isinstance(extra, (list, tuple)):
        fields["extra"] = "".join(SUFFIXES[x].format_map(fields) for x in extra if x in SUFFIXES)
    title = spec.template.format_map(fields)
    if len(title) > 200:
        title = title[:199] + "…"
    probe = spec.template.format_map({**fields, **{n: "L" for n in LEVEL_SLOTS}})
    suffixes = extra if isinstance(extra, (list, tuple)) else ()
    probe += "".join(SUFFIXES[x].format_map(fields) for x in suffixes if x in SUFFIXES)
    if DECIMAL_IN_TEXT.search(probe):
        raise PrivacyError(f"decimal outside the level slots in kind {kind!r}")
    return title.strip()


@dataclass
class Event:
    """One feed line (immutable once emitted). `n` and `sid` are set by the ring when it is emitted."""

    kind: str
    slots: dict[str, Any]
    at_ms: int | None
    clock: str
    detected_ms: int
    ids: dict[str, str] = field(default_factory=dict)
    severity: str | None = None
    category: str | None = None
    late: str | None = None
    story: str | None = None
    parent: str | None = None
    episode: str | None = None
    housekeeping: bool = False
    source: str = ""
    source_key: str = ""
    title: str = ""
    n: int = 0
    sid: str = ""

    def to_json(self) -> dict[str, Any]:
        return {"sid": self.sid, "n": self.n, "at_ms": self.at_ms, "clock": self.clock,
                "detected_ms": self.detected_ms, "late": self.late, "category": self.category,
                "severity": self.severity, "kind": self.kind, "title": self.title, "ids": dict(self.ids),
                "story": self.story, "parent": self.parent, "episode": self.episode,
                "housekeeping": self.housekeeping}


_STORY_KEY_RE: Final = re.compile(r"^(pos|ord|acct|halt|term|out|dlq):[A-Za-z0-9:._@/-]{1,120}$")
_EPISODE_RE: Final = re.compile(r"^[A-Za-z0-9_():,-]{1,80}\|[A-Z_]{1,20}\|[A-Za-z0-9:_@/-]{1,64}\|[0-9]{1,19}$")


def assert_safe(ev: Event) -> Event:
    """Check and finish an event in place: names on the allow-list (else PrivacyError), slot values typed (else
    "(not shown)"), the title rendered from the template. Returns the event."""
    spec = KINDS.get(ev.kind)
    if spec is None:
        raise PrivacyError(f"unknown kind {ev.kind!r}")
    for name in ev.slots:
        if name not in SLOT_TYPES or name_denied(name):
            raise PrivacyError(f"slot {name!r} not allowed")
    for name in ev.ids:
        if name not in ID_NAMES or name_denied(name):
            raise PrivacyError(f"id {name!r} not allowed")
    for name, value in list(ev.slots.items()):
        if value is None or value == NOT_SHOWN:
            continue
        if not check_value(SLOT_TYPES[name], value):
            ev.slots[name] = NOT_SHOWN
            STATS.blanked[f"{ev.kind}.{name}"] += 1
    for name, value in list(ev.ids.items()):
        stype = "symbol" if name == "symbol" else "id"
        if name == "epoch" and isinstance(value, int):
            ev.ids[name] = str(value)
            continue
        if not isinstance(value, str) or not check_value(stype, value):
            ev.ids[name] = NOT_SHOWN
            STATS.blanked[f"{ev.kind}.ids.{name}"] += 1
    ev.category = ev.category or spec.category
    ev.severity = ev.severity or spec.severity
    if ev.category not in CATEGORIES or ev.severity not in SEVERITY_RANK or ev.clock not in ("broker", "server",
                                                                                             "engine"):
        raise PrivacyError("bad category, severity or clock")
    for key in (ev.story, ev.parent):
        if key is not None and not _STORY_KEY_RE.fullmatch(key):
            raise PrivacyError("bad story key")
    if ev.episode is not None and not _EPISODE_RE.fullmatch(ev.episode):
        ev.episode = None
    ev.title = render(ev.kind, ev.slots)
    if ev.severity == "debug":
        ev.housekeeping = True
    return ev


# ---- payload guard (state, story, day, series rows) ------------------------------------------------------------
#: Every key a state / story / day / series / poll payload may carry.
PAYLOAD_KEYS: Final = frozenset({
    # state
    "halt", "id", "level", "effective_level", "hard_latch", "rule_id", "reason_code", "by_whom", "at_ms", "halt_why",
    "escalate_min", "capped", "cooldown_clear_ms", "acked", "engine", "status", "role", "since_ms", "sweep_age_s",
    "last_ok_ms", "leader", "term_id", "node", "epoch", "reached_leader_ms", "draining", "safe_hold", "on",
    "hold_code", "hold_count", "resumed_by", "last_resume_ms", "streams", "adapter", "stream", "adapter_state",
    "role_note", "auth", "shown", "oversight", "account_id", "alerts", "waiting", "oldest_ms", "undeliverable",
    "escalated_today", "dlq", "breaches", "FLAG", "SOFT", "HARD", "pending", "open_breaches", "breach_id",
    "subject_type", "subject_id", "symbol", "side", "cap", "repages", "story", "open_now", "kind", "entry", "stop",
    "take_profit", "history", "timing", "escalate_after_ms", "cool_down_ms", "unknown_grace_ms",
    # state.engine.deadman (W1: the leader's dead-man gauges)
    "deadman", "configured", "failures", "ok_age_s", "stale",
    # story header
    "key", "state", "ids", "first_ms", "last_ms", "worst", "counts", "header", "levels", "episodes", "episode",
    "last_repage_ms", "resolved", "badges", "related", "merged_into", "held", "gap_ms", "dlq_status", "events",
    # day / series / poll
    "start_ms", "now_ms", "buckets", "activity", "notice", "warn", "alert", "housekeeping", "partial", "from_ms",
    "to_ms", "inactive", "sweep_pass_age_s", "alerts_waiting", "dlq_queued", "db", "backlog",
})
#: Keys whose dict value has data keys (adapter names, sids, categories, statuses, id names).
DYNAMIC_KEY_PARENTS: Final = frozenset({"badges", "counts", "dlq", "ids", "streams"})
_DYNAMIC_KEY_RE: Final = re.compile(r"^[A-Za-z0-9:_.@/-]{1,80}$")
_SID_KEY_RE: Final = re.compile(r"^[0-9a-f-]{36}\.[0-9]{1,18}$")


def guard_payload(obj: Any, *, parent: str = "") -> Any:
    """Check a payload tree in place: every key allowed and not denied (else PrivacyError); level keys hold level
    strings (else "(not shown)"); no float anywhere. Returns the object."""
    if isinstance(obj, dict):
        dynamic = parent in DYNAMIC_KEY_PARENTS
        for key in list(obj):
            if not isinstance(key, str):
                raise PrivacyError("non-string key")
            if name_denied(key):
                raise PrivacyError(f"key {key!r} denied")
            if dynamic:
                if parent == "badges":
                    if not _SID_KEY_RE.fullmatch(key):
                        raise PrivacyError("bad badge key")
                elif not _DYNAMIC_KEY_RE.fullmatch(key) or DECIMAL_IN_TEXT.search(key):
                    raise PrivacyError("bad dynamic key")
            elif key not in PAYLOAD_KEYS:
                raise PrivacyError(f"key {key!r} not allowed")
            value = obj[key]
            if key in LEVEL_SLOTS and not dynamic:
                if isinstance(value, float):
                    raise PrivacyError("float in payload")
                if value is not None and not check_value("level", value):
                    obj[key] = NOT_SHOWN
                continue
            if dynamic:
                # day.streams is two levels of data keys (adapter, then stream)
                guard_payload(value, parent="streams" if parent == "streams" and isinstance(value, dict) else "")
            else:
                guard_payload(value, parent=key)
    elif isinstance(obj, list):
        for item in obj:
            guard_payload(item, parent=f"{parent}[]")
    elif isinstance(obj, float):
        raise PrivacyError("float in payload")
    return obj


def worse(a: str, b: str) -> str:
    return a if SEVERITY_RANK.get(a, 0) >= SEVERITY_RANK.get(b, 0) else b


__all__ = ["CATEGORIES", "DENY_TOKENS", "Event", "ID_NAMES", "KINDS", "Kind", "LEVEL_SLOTS", "NOT_SHOWN",
           "PAYLOAD_KEYS", "PrivacyError", "SEVERITIES", "SEVERITY_RANK", "SLOT_TYPES", "STATS", "SUFFIXES", "assert_safe",
           "check_value", "fmt_duration", "fmt_time", "guard_payload", "level_str", "name_denied", "render", "worse"]
