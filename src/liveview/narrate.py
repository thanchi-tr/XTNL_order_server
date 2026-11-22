"""Narration: rows -> feed lines and stories (docs/design/live-view.md s4.4).

Titles come only from event kinds and typed slots (events.py). The bracket model (s4.4.2) folds stop-loss and
take-profit children into their parent; fill and close pairs are folded by holding the opener up to 10 s of
database time. Stories (s4.4.3) are keyed by stored values only; start-up stories by the leader term and its epoch
(s4.4.7). Grouping is presentation only: every row is emitted exactly once, whatever story it lands in.
"""
from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from src.liveview.events import Event, PrivacyError, check_value, level_str
from src.liveview.stories import STORY_KEY_RE, Episode, StoryIndex

log = logging.getLogger("xtnl.liveview.narrate")

#: Adapters whose position id IS its entry order's id (StoneX OrderId; the mock copies it). Never cTrader.
POSITION_IS_ENTRY_ORDER: Final = frozenset({"stonex", "mock"})
OPEN_ORDER: Final = frozenset({"PENDING", "WORKING", "PARTIALLY_FILLED"})
TERMINAL_ORDER: Final = frozenset({"FILLED", "CANCELLED", "REJECTED"})
LEG_NAME: Final = {"STOP": "stop-loss", "LIMIT": "take-profit"}
ADAPTER_NAME: Final = {"stonex": "StoneX", "pepperstone": "cTrader", "mock": "Mock"}
STREAM_ORDER: Final = {"account": 0, "orders": 1, "prices": 2}
FOLD_HOLD_MS: Final = 10_000
START_STORY_MAX_MS: Final = 120_000
STILL_LOST_MS: Final = 60_000
ENGINE_HOLD_MS: Final = 60_000
LATE_MS: Final = 60_000
FACTS_CAP: Final = 20_000
AUTH_EVIDENCE: Final = frozenset({"SNAPSHOTTING", "STREAMING", "RESYNCING"})
JOIN_REASONS: Final = frozenset({"reconnect", "retry", "operator", "resume_after_sleep"})
PLACEHOLDER_TYPES: Final = frozenset({"CASH_FLOW", "INSTRUMENT"})
TRADE_TYPES: Final = frozenset({"ORDER", "POSITION"})
ACCOUNT_SUBJECTS: Final = frozenset({"ACCOUNT", "ACCOUNT_DAY"})

Row = dict[str, Any]
Emit = Callable[[Event], Event | None]
Merge = Callable[[str, str, bool], None]


def adapter_name(a: str | None) -> str:
    return ADAPTER_NAME.get(a or "", a or "?")


def _id_tail(entity_key: str | None) -> str | None:
    return entity_key.split(":", 1)[1] if entity_key and ":" in entity_key else entity_key


@dataclass
class Facts:
    entity_type: str
    adapter: str
    account: str | None
    f: dict[str, Any]

    @property
    def status(self) -> str | None:
        s = self.f.get("status")
        return str(s) if s is not None else None

    def level(self, name: str) -> str | None:
        return level_str(self.f.get(name))

    @property
    def open(self) -> bool:
        return (self.status in OPEN_ORDER) if self.entity_type == "ORDER" else self.status == "OPEN"

    @property
    def leg_kind(self) -> str | None:
        k = self.f.get("leg_kind")
        return str(k) if k in ("STOP", "LIMIT") else None

    @property
    def reduce_only(self) -> bool:
        return bool(self.f.get("is_reduce_only"))

    @property
    def linked(self) -> str | None:
        v = self.f.get("linked_position_id")
        return str(v) if v not in (None, "") else None

    @property
    def is_child(self) -> bool:
        return self.entity_type == "ORDER" and self.reduce_only and self.linked is not None and \
            self.leg_kind is not None

    @property
    def is_closing(self) -> bool:
        return self.entity_type == "ORDER" and self.reduce_only and self.leg_kind is None

    def visible(self) -> tuple[Any, ...]:
        return (self.status, self.level("entry_level"), self.level("stop_level"), self.level("target_level"),
                self.linked, self.leg_kind)


@dataclass
class Hold:
    """A fold opener held until its due partners arrive, or 10 s of database time after detection (s4.4.2)."""

    kind: str                     # "close" or "fill"
    story: str
    detected_ms: int
    at_ms: int | None
    clock: str
    ids: dict[str, str]
    child_leg: str | None = None
    closing_trade: bool = False
    position_closed: bool = False
    entry_filled: bool = False
    position_opened: bool = False
    entry: str | None = None
    due: set[str] = field(default_factory=set)
    cancelled_legs: list[str] = field(default_factory=list)
    cancelled_rows: list[tuple[str, dict[str, Any]]] = field(default_factory=list)   # (leg, the row's own common)
    late: str | None = None


@dataclass
class CatchupJoin:
    """A non-startup catch-up row R and its open set (s4.4.3 "Catch-up rows and outages")."""

    adapter: str
    story: str                    # the story R joined
    reason: str
    open_set: set[str]
    after_n_poll: int
    reconciled_after: set[tuple[str, str]] = field(default_factory=set)


class Narrator:
    def __init__(self, index: StoryIndex, emit: Emit, merge: Merge) -> None:
        self.index = index
        self._emit = emit
        self._merge = merge
        self.facts: OrderedDict[str, Facts] = OrderedDict()
        self.holds: dict[tuple[str, str], Hold] = {}
        self.after_hold_close: dict[str, int] = {}        # story -> db time the close hold ended
        self.terms: dict[int, Row] = {}
        self.term_by_epoch: dict[int, list[int]] = {}
        self.fenced_terms: set[int] = set()
        self.term_still_lost: set[tuple[int, str, str]] = set()
        self.outages: dict[tuple[str, str], str] = {}     # (adapter, stream) -> open outage key
        self.fence_rows: set[tuple[str, str]] = set()     # streams currently fenced at a leader start
        self.joins: list[CatchupJoin] = []
        self.poll_no = 0
        self.halt_last: Row | None = None
        self.halt_story: str | None = None
        self.halt_lines: dict[int, tuple[str, str | None]] = {}   # halt at_ms -> (sid, story)
        self.oversee_lines: dict[tuple[Any, ...], tuple[str, str | None]] = {}
        self.episode_lines: dict[str, tuple[str, str | None]] = {}
        self.alerts: dict[int, tuple[str | None, str | None, Row]] = {}   # outbox_id -> (sid, story, row)
        self.dlq_keys: dict[int, str] = {}                                  # dlq_id -> story key at first read
        self.open_at_start: set[str] = set()       # trade story keys of the entities open at start (lv_open_*)
        self.alert_lines_done: set[tuple[int, str]] = set()
        self.breaches: list[Row] = []
        self.active_adapters: set[str] | None = None
        self.last_role: str | None = None
        self.auth_seen_arrival: set[tuple[str, int]] = set()
        self.auth_cleared_emitted: set[tuple[str, int]] = set()
        self.auth_rows: dict[str, Row] = {}
        self.auth_read_poll: dict[tuple[str, int], int] = {}
        self.runs_reported_unclean: set[str] = set()
        self.runs: dict[str, Row] = {}
        self.engine_down_since: int | None = None
        self.engine_hold_until: int | None = None
        self.engine_alerted = False
        self.new_run_during_hold: Row | None = None
        self.sweep_stalled = False
        self.backfilling = False

    # ---- emission helpers -------------------------------------------------------------------------------------
    def emit(self, kind: str, slots: dict[str, Any], *, at_ms: int | None, clock: str, detected_ms: int,
             ids: Mapping[str, Any] | None = None, story: str | None = None, severity: str | None = None,
             late: str | None = None, episode: str | None = None, parent: str | None = None,
             source: str = "", source_key: str = "") -> Event | None:
        if late is None and at_ms is not None and not self.backfilling and at_ms < detected_ms - LATE_MS:
            late = "delayed"
        ev = Event(kind=kind, slots=dict(slots), at_ms=at_ms, clock=clock, detected_ms=detected_ms,
                   ids={k: (str(v) if isinstance(v, int) else v) for k, v in (ids or {}).items() if v is not None},
                   severity=severity, late=late, story=self.index.resolve(story) if story else None,
                   parent=parent, episode=episode, source=source, source_key=source_key)
        if ev.story is not None and not STORY_KEY_RE.fullmatch(ev.story):
            ev.story = None          # a key built from an unexpected id: the line stands alone, never dropped
        try:
            return self._emit(ev)
        except PrivacyError as exc:
            log.warning("event dropped by the privacy guard", extra={"event": "guard_drop", "kind": kind,
                                                                     "why": str(exc)[:80]})
            return None

    @staticmethod
    def late_of(version_path: str | None) -> str | None:
        if version_path == "history":
            return "history"
        if version_path in ("snapshot", "dlq"):
            return version_path
        return None

    # ---- facts cache ------------------------------------------------------------------------------------------
    def seed_open(self, orders: Sequence[Row], positions: Sequence[Row], at_ms: int) -> None:
        """Seed the facts cache from lv_open_orders / lv_open_positions (the current applied facts), and open
        their trade stories silently."""
        for p in positions:
            ekey = f"{p['adapter_id']}:{p['position_id']}"
            key = f"POSITION|{ekey}"
            self._put(key, Facts("POSITION", p["adapter_id"], p.get("account_id"), {
                "position_id": p["position_id"], "symbol": p.get("symbol"), "side": p.get("side"), "status": "OPEN",
                "entry_level": p.get("entry_level"), "stop_level": p.get("stop_level"),
                "target_level": p.get("target_level"), "opened_by_client_order_id": p.get("opened_by_client_order_id")}))
            st = self.index.open_story(f"pos:{ekey}", int(p.get("committed_at_ms") or at_ms), at_ms)
            self._trade_facts(st.key, key)
        for o in orders:
            key = f"ORDER|{o['adapter_id']}:{o['broker_order_id']}"
            self._put(key, Facts("ORDER", o["adapter_id"], o.get("account_id"), {
                "broker_order_id": o["broker_order_id"], "client_order_id": o.get("client_order_id"),
                "symbol": o.get("symbol"), "side": o.get("side"), "status": o.get("status"),
                "entry_level": o.get("entry_level"), "stop_level": o.get("stop_level"),
                "target_level": o.get("target_level"), "is_reduce_only": o.get("is_reduce_only"),
                "linked_position_id": o.get("linked_position_id"), "leg_kind": o.get("leg_kind")}))
        for o in orders:
            key = f"ORDER|{o['adapter_id']}:{o['broker_order_id']}"
            fx = self.facts[key]
            skey = self._order_story(fx)
            self.index.open_story(skey, int(o.get("committed_at_ms") or at_ms), at_ms)
            self._trade_facts(skey, key)
        for st in list(self.index.stories.values()):
            if st.kind == "trade":
                self._refresh_levels(st.key, at_ms)

    def _put(self, key: str, fx: Facts) -> None:
        self.facts[key] = fx
        self.facts.move_to_end(key)
        if len(self.facts) > FACTS_CAP:
            for k in list(self.facts):
                if len(self.facts) <= FACTS_CAP:
                    break
                if not self.facts[k].open:
                    del self.facts[k]

    def _position_known(self, adapter: str, pid: str) -> bool:
        return f"POSITION|{adapter}:{pid}" in self.facts or f"pos:{adapter}:{pid}" in self.index.stories

    def _order_story(self, fx: Facts) -> str:
        adapter = fx.adapter
        oid = str(fx.f.get("broker_order_id"))
        linked = fx.linked
        if linked and self._position_known(adapter, linked):
            return self.index.resolve(f"pos:{adapter}:{linked}") or f"pos:{adapter}:{linked}"
        if adapter in POSITION_IS_ENTRY_ORDER:
            if not fx.reduce_only and f"pos:{adapter}:{oid}" in self.index.stories:
                return self.index.resolve(f"pos:{adapter}:{oid}") or f"pos:{adapter}:{oid}"
            if linked and (f"ORDER|{adapter}:{linked}" in self.facts
                           or f"ord:{adapter}:{linked}" in self.index.stories):
                return self.index.resolve(f"ord:{adapter}:{linked}") or f"ord:{adapter}:{linked}"
        existing = self.index.resolve(f"ord:{adapter}:{oid}")
        return existing or f"ord:{adapter}:{oid}"

    def _trade_facts(self, skey: str, entity_key: str) -> None:
        st = self.index.get(skey)
        fx = self.facts.get(entity_key)
        if st is None or fx is None:
            return
        st.members.add(entity_key)
        sym, side = fx.f.get("symbol"), fx.f.get("side")
        parent_row = not fx.is_child and not fx.is_closing
        if (st.symbol is None or (parent_row and not st.data.get("lead_from_parent"))) and \
                check_value("symbol", sym):
            st.symbol = str(sym)
            st.side = str(side) if check_value("side", side) else None
            st.data["lead_from_parent"] = parent_row
            self.index.touched.add(skey)
        if fx.entity_type == "POSITION" and check_value("id", str(fx.f.get("position_id"))):
            st.ids["position_id"] = str(fx.f.get("position_id"))
        elif not fx.is_child and not fx.is_closing and check_value("id", str(fx.f.get("broker_order_id"))):
            st.ids.setdefault("broker_order_id", str(fx.f.get("broker_order_id")))

    def _lead(self, skey: str | None, fallback: Facts | None = None) -> dict[str, Any]:
        st = self.index.get(self.index.resolve(skey)) if skey else None
        symbol = st.symbol if st and st.symbol else (fallback.f.get("symbol") if fallback else None)
        side = st.side if st and st.side else (fallback.f.get("side") if fallback else None)
        sid = None
        if st is not None:
            sid = st.ids.get("position_id") or st.ids.get("broker_order_id")
        if sid is None and fallback is not None:
            sid = fallback.f.get("position_id") or fallback.f.get("broker_order_id")
        return {"symbol": symbol, "side": side, "id": str(sid) if sid is not None else None}

    def _effective(self, skey: str) -> dict[str, str | None]:
        st = self.index.get(skey)
        if st is None:
            return {"entry": None, "stop": None, "take_profit": None}
        pos = entry_order = None
        stop_child = tp_child = None
        for k in st.members:
            fx = self.facts.get(k)
            if fx is None:
                continue
            if fx.entity_type == "POSITION":
                pos = fx
            elif fx.is_child and fx.open:
                if fx.leg_kind == "STOP":
                    stop_child = fx
                else:
                    tp_child = fx
            elif not fx.is_child and not fx.is_closing:
                entry_order = fx
        parent = pos or entry_order
        entry = parent.level("entry_level") if parent else None
        stop = (parent.level("stop_level") if parent else None) or (stop_child.level("entry_level") if stop_child
                                                                    else None)
        tp = (parent.level("target_level") if parent else None) or (tp_child.level("entry_level") if tp_child
                                                                    else None)
        return {"entry": entry, "stop": stop, "take_profit": tp}

    def _refresh_levels(self, skey: str, at_ms: int) -> None:
        skey = self.index.resolve(skey) or skey
        st = self.index.get(skey)
        if st is None:
            return
        self.index.add_history(skey, at_ms, self._effective(skey))
        if st.kind == "trade":
            members = [self.facts[k] for k in st.members if k in self.facts]
            if members and not any(m.open for m in members):
                self.index.set_state(skey, "closed")
            elif any(m.open for m in members):
                self.index.set_state(skey, "open")
        self._header(skey)

    # ---- entity rows ------------------------------------------------------------------------------------------
    def entity_rows(self, rows: Sequence[Row], db_now: int, *, late_rows: bool = False) -> None:
        for r in sorted(rows, key=lambda x: (x.get("entity_key") or "", x.get("seq_id") or 0)):
            etype = r.get("entity_type")
            if etype in PLACEHOLDER_TYPES:
                continue
            at_ms, clock = (r["broker_ms"], "broker") if r.get("broker_ms") is not None else \
                (r.get("logged_at_ms"), "server")
            late = self.late_of(r.get("version_path")) or ("delayed" if late_rows else None)
            detected = db_now if not self.backfilling else int(at_ms or db_now)
            if etype == "ACCOUNT":
                self.emit("account.updated", {"account": r.get("account_id")}, at_ms=at_ms, clock=clock,
                          detected_ms=detected, ids={"account_id": r.get("account_id"),
                                                     "adapter_id": r.get("adapter_id")}, late=late,
                          source="lv_entity_events", source_key=str(r.get("log_id")))
                continue
            ekey = r.get("entity_key")
            raw = r.get("facts")
            facts: dict[str, Any] = raw if isinstance(raw, dict) else {}
            if not ekey or etype not in TRADE_TYPES:
                continue
            key = f"{etype}|{ekey}"
            fx = Facts(etype, r["adapter_id"], r.get("account_id"), dict(facts))
            if r.get("action") == "DROPPED_STALE":
                skey = self._order_story(fx) if etype == "ORDER" else f"pos:{ekey}"
                self.emit(f"{etype.lower()}.stale" if etype == "ORDER" else "position.stale",
                          self._lead(skey, fx), at_ms=at_ms, clock=clock, detected_ms=detected,
                          ids=self._ids(fx), story=skey if self.index.get(skey) else None, late=late,
                          source="lv_entity_events", source_key=str(r.get("log_id")))
                continue
            prev = self.facts.get(key)
            if etype == "ORDER":
                self._order(key, prev, fx, r, at_ms, clock, detected, late)
            else:
                self._position(key, prev, fx, r, at_ms, clock, detected, late)

    @staticmethod
    def _ids(fx: Facts) -> dict[str, Any]:
        out: dict[str, Any] = {"adapter_id": fx.adapter, "account_id": fx.account}
        for k in ("broker_order_id", "client_order_id", "position_id", "linked_position_id"):
            if fx.f.get(k) is not None:
                out[k] = str(fx.f[k])
        if fx.f.get("symbol"):
            out["symbol"] = str(fx.f["symbol"])
        return out

    def _order(self, key: str, prev: Facts | None, fx: Facts, r: Row, at_ms: int | None, clock: str,
               detected: int, late: str | None) -> None:
        self._put(key, fx)
        skey = self._order_story(fx)
        st = self.index.open_story(skey, int(at_ms or detected), detected)
        self._trade_facts(skey, key)
        common: dict[str, Any] = {"at_ms": at_ms, "clock": clock, "detected_ms": detected, "ids": self._ids(fx), "story": skey,
                  "late": late, "source": "lv_entity_events", "source_key": str(r.get("log_id"))}
        lead = self._lead(skey, fx)
        status = fx.status
        if fx.is_child:
            leg = LEG_NAME.get(fx.leg_kind or "", "bracket")
            if prev is None:
                if fx.open:
                    lvl = fx.level("entry_level")
                    self.emit("bracket.attached" if lvl else "bracket.attached_nolevel",
                              {**lead, "leg": leg, "entry": lvl}, **common)
                elif status == "FILLED":
                    self._close_part(st.key, fx, common, child_leg=leg)
                elif status in ("CANCELLED", "REJECTED") and self._absorb_cancel(st.key, leg, common):
                    pass
                else:
                    self.emit("order.first_seen", {**lead, "status": status}, **common)
            elif prev.visible() == fx.visible():
                self.emit("order.no_change", lead, **common)
            elif status != prev.status:
                if status == "FILLED":
                    self._close_part(st.key, fx, common, child_leg=leg)
                elif status in ("CANCELLED", "REJECTED"):
                    if not self._absorb_cancel(st.key, leg, common):
                        ended = self.after_hold_close.get(st.key)
                        self.emit("bracket.cancelled_with_close" if ended is not None else "bracket.cancelled",
                                  {**lead, "leg": leg}, **common)
                else:
                    self.emit("order.status", {**lead, "status": status}, **common)
            else:
                a, b = prev.level("entry_level"), fx.level("entry_level")
                if a != b and b is not None and a is not None:
                    self.emit("bracket.moved", {**lead, "leg": leg, "from_level": a, "to_level": b}, **common)
                elif a != b and b is not None:
                    self.emit("bracket.attached", {**lead, "leg": leg, "entry": b}, **common)
                else:
                    self.emit("order.no_change", lead, **common)
        elif fx.is_closing:
            if status == "FILLED" and (prev is None or prev.status != "FILLED"):
                self._close_part(st.key, fx, common, closing_trade=True)
            elif fx.open and (prev is None or prev.status != status):
                self.emit("order.closing_working", lead, **common)
            elif status in ("CANCELLED", "REJECTED") and (prev is None or prev.status != status):
                self.emit("order.cancelled", lead, **common)
            else:
                self.emit("order.no_change", lead, **common)
        else:
            kind_word = (fx.leg_kind or "").lower() or None
            if prev is None:
                if fx.open:
                    lvl = fx.level("entry_level")
                    if r.get("version_path") == "snapshot":
                        self.emit("order.found", {**lead, "status": status}, **common)
                    else:
                        self.emit("order.placed" if lvl else "order.placed_nolevel",
                                  {**lead, "kind": kind_word or "", "entry": lvl}, **common)
                elif status == "FILLED" and fx.adapter in POSITION_IS_ENTRY_ORDER:
                    self._fill_part(st.key, common, entry_filled=True)
                else:
                    self.emit("order.first_seen", {**lead, "status": status}, **common)
            elif prev.visible() == fx.visible():
                self.emit("order.no_change", lead, **common)
            else:
                emitted = False
                a, b = prev.level("entry_level"), fx.level("entry_level")
                if a != b and a is not None and b is not None and fx.open:
                    self.emit("order.entry_moved", {**lead, "from_level": a, "to_level": b}, **common)
                    emitted = True
                for name, leg in (("stop_level", "stop-loss"), ("target_level", "take-profit")):
                    x, y = prev.level(name), fx.level(name)
                    if x != y and fx.open:
                        if x is not None and y is not None:
                            self.emit("bracket.moved", {**lead, "leg": leg, "from_level": x, "to_level": y},
                                      **common)
                        elif y is not None:
                            self.emit("bracket.attached", {**lead, "leg": leg, "entry": y}, **common)
                        else:
                            self.emit("bracket.cancelled", {**lead, "leg": leg}, **common)
                        emitted = True
                if status != prev.status:
                    if status == "FILLED":
                        if fx.adapter in POSITION_IS_ENTRY_ORDER:
                            self._fill_part(st.key, common, entry_filled=True)
                        else:
                            self.emit("order.filled", lead, **common)
                    elif status in ("CANCELLED", "REJECTED"):
                        self.emit("order.cancelled", lead, **common)
                    else:
                        self.emit("order.status", {**lead, "status": status}, **common)
                    emitted = True
                if not emitted:
                    self.emit("order.no_change", lead, **common)
        self._refresh_levels(st.key, int(at_ms or detected))

    def _position(self, key: str, prev: Facts | None, fx: Facts, r: Row, at_ms: int | None, clock: str,
                  detected: int, late: str | None) -> None:
        self._put(key, fx)
        adapter, pid = fx.adapter, str(fx.f.get("position_id"))
        skey = f"pos:{key.split('|', 1)[1]}"
        st = self.index.open_story(skey, int(at_ms or detected), detected)
        self._trade_facts(skey, key)
        # an entry order story becomes this position's story: StoneX / mock by id, any adapter by client order id
        merges: list[str] = []
        if adapter in POSITION_IS_ENTRY_ORDER and f"ord:{adapter}:{pid}" in self.index.stories:
            merges.append(f"ord:{adapter}:{pid}")
        opened_by = fx.f.get("opened_by_client_order_id")
        if opened_by:
            for s in list(self.index.stories.values()):
                if s.key.startswith(f"ord:{adapter}:") and not s.merged_into and s.key not in merges:
                    for m in s.members:
                        o = self.facts.get(m)
                        if o is not None and str(opened_by) in (str(o.f.get("client_order_id")),
                                                                str(o.f.get("broker_order_id"))):
                            merges.append(s.key)
                            break
        for m in merges:
            if not self.index.stories[m].merged_into:
                for hk in [hk for hk in self.holds if hk[0] == m]:
                    h = self.holds.pop(hk)
                    h.story = skey
                    self.holds[(skey, h.kind)] = h
                self._merge(m, skey, False)
        common: dict[str, Any] = {"at_ms": at_ms, "clock": clock, "detected_ms": detected, "ids": self._ids(fx), "story": skey,
                  "late": late, "source": "lv_entity_events", "source_key": str(r.get("log_id"))}
        lead = self._lead(skey, fx)
        status = fx.status
        if prev is None:
            if status == "OPEN":
                if r.get("version_path") == "snapshot":
                    self.emit("position.found", lead, **common)
                elif adapter in POSITION_IS_ENTRY_ORDER:
                    self._fill_part(skey, common, position_opened=True, entry=fx.level("entry_level"))
                else:
                    lvl = fx.level("entry_level")
                    self.emit("position.opened" if lvl else "position.opened_nolevel", {**lead, "entry": lvl},
                              **common)
            elif status == "CLOSED":
                self._close_part(skey, fx, common, position_closed=True)
            else:
                self.emit("position.first_seen", {**lead, "status": status}, **common)
        elif prev.visible() == fx.visible():
            self.emit("position.no_change", lead, **common)
        else:
            emitted = False
            if fx.open:
                for name, word in (("stop_level", "stop"), ("target_level", "target")):
                    x, y = prev.level(name), fx.level(name)
                    if x == y:
                        continue
                    if x is not None and y is not None:
                        self.emit(f"position.{word}_moved", {**lead, "from_level": x, "to_level": y}, **common)
                    elif y is not None:
                        self.emit(f"position.{word}_set", {**lead, "to_level": y}, **common)
                    else:
                        self.emit(f"position.{word}_removed", lead, **common)
                    emitted = True
            if status != prev.status and status == "CLOSED":
                self._close_part(skey, fx, common, position_closed=True)
                emitted = True
            elif status != prev.status:
                self.emit("position.first_seen", {**lead, "status": status}, **common)
                emitted = True
            if not emitted:
                self.emit("position.no_change", lead, **common)
        self._refresh_levels(skey, int(at_ms or detected))

    # ---- fold holds -------------------------------------------------------------------------------------------
    def _working_children(self, skey: str) -> list[Facts]:
        st = self.index.get(skey)
        if st is None:
            return []
        return [self.facts[k] for k in st.members if k in self.facts and self.facts[k].is_child
                and self.facts[k].open]

    def _close_part(self, skey: str, fx: Facts, common: dict[str, Any], *, child_leg: str | None = None,
                    closing_trade: bool = False, position_closed: bool = False) -> None:
        skey = self.index.resolve(skey) or skey
        h = self.holds.get((skey, "close"))
        if h is None:
            h = Hold(kind="close", story=skey, detected_ms=int(common["detected_ms"]), at_ms=common["at_ms"],
                     clock=common["clock"], ids=dict(common["ids"]), late=common.get("late"))
            self.holds[(skey, "close")] = h
            h.due = {f"child:{LEG_NAME.get(c.leg_kind or '', '')}" for c in self._working_children(skey)}
            if not position_closed:
                h.due.add("position")
        if child_leg:
            h.child_leg = child_leg
            h.due.discard(f"child:{child_leg}")
        if closing_trade:
            h.closing_trade = True
        if position_closed:
            h.position_closed = True
            h.due.discard("position")
        h.ids.update({k: v for k, v in common["ids"].items() if k == "position_id"})
        self._maybe_release(h, int(common["detected_ms"]))

    def _absorb_cancel(self, skey: str, leg: str, common: dict[str, Any]) -> bool:
        h = self.holds.get((self.index.resolve(skey) or skey, "close"))
        if h is None:
            return False
        h.cancelled_legs.append(leg)
        h.cancelled_rows.append((leg, dict(common)))
        h.due.discard(f"child:{leg}")
        self._maybe_release(h, h.detected_ms)
        return True

    def _fill_part(self, skey: str, common: dict[str, Any], *, entry_filled: bool = False,
                   position_opened: bool = False, entry: str | None = None) -> None:
        skey = self.index.resolve(skey) or skey
        h = self.holds.get((skey, "fill"))
        if h is None:
            h = Hold(kind="fill", story=skey, detected_ms=int(common["detected_ms"]), at_ms=common["at_ms"],
                     clock=common["clock"], ids=dict(common["ids"]), late=common.get("late"))
            self.holds[(skey, "fill")] = h
            h.due = {"position"} if entry_filled else {"entry"}
        if entry_filled:
            h.entry_filled = True
            h.due.discard("entry")
        if position_opened:
            h.position_opened = True
            h.entry = entry
            h.due.discard("position")
            h.ids.update(common["ids"])
        self._maybe_release(h, int(common["detected_ms"]))

    def _maybe_release(self, h: Hold, db_now: int) -> None:
        if not h.due or db_now - h.detected_ms >= FOLD_HOLD_MS:
            self._release(h, db_now)

    def tick_holds(self, db_now: int) -> None:
        for h in list(self.holds.values()):
            self._maybe_release(h, db_now)

    def _release(self, h: Hold, db_now: int) -> None:
        if self.holds.get((h.story, h.kind)) is not h:
            return
        del self.holds[(h.story, h.kind)]
        skey = self.index.resolve(h.story) or h.story
        lead = self._lead(skey)
        common: dict[str, Any] = {"at_ms": h.at_ms, "clock": h.clock, "detected_ms": h.detected_ms, "ids": h.ids,
                                  "story": skey, "late": h.late, "source": "lv_entity_events"}
        if h.kind == "fill":
            if h.entry_filled and h.position_opened:
                self.emit("position.filled_opened" if h.entry else "position.filled_opened_nolevel",
                          {**lead, "entry": h.entry}, **common)
            elif h.position_opened:
                self.emit("position.opened" if h.entry else "position.opened_nolevel", {**lead, "entry": h.entry},
                          **common)
            else:
                self.emit("order.filled", lead, **common)
            return
        self.after_hold_close[skey] = db_now
        extra: list[str] = []
        for leg in h.cancelled_legs:
            if leg == "take-profit" and h.child_leg != "take-profit":
                extra.append("tp_cancelled")
            elif leg == "stop-loss" and h.child_leg != "stop-loss":
                extra.append("sl_cancelled")
        if h.position_closed and h.child_leg:
            self.emit("position.closed_by", {**lead, "leg": h.child_leg, "extra": extra}, **common)
        elif h.position_closed and h.closing_trade:
            self.emit("position.closed", {**lead, "extra": ["closing_trade", *extra]}, **common)
        elif h.position_closed:
            self.emit("position.closed", {**lead, "extra": extra}, **common)
        elif h.child_leg:
            self.emit("bracket.filled", {**lead, "leg": h.child_leg}, **common)
        else:
            self.emit("order.closing_trade", lead, **common)
        if not h.position_closed:
            # no close line carries the cancels this hold absorbed: each is its own line, so no row is lost
            for leg, own in h.cancelled_rows:
                self.emit("bracket.cancelled_with_close", {**lead, "leg": leg}, **{**own, "story": skey})

    # ---- oversee and breaches ---------------------------------------------------------------------------------
    def _subject_story(self, subject_type: str | None, subject_id: str | None, adapter: str | None,
                       account: str | None) -> str | None:
        if subject_type == "POSITION" and subject_id:
            return self.index.resolve(f"pos:{subject_id}") or f"pos:{subject_id}"
        if subject_type == "ORDER" and subject_id:
            fx = self.facts.get(f"ORDER|{subject_id}")
            if fx is not None:
                return self._order_story(fx)
            return self.index.resolve(f"ord:{subject_id}") or f"ord:{subject_id}"
        if subject_type in ACCOUNT_SUBJECTS and adapter and account:
            return f"acct:{adapter}:{account}"
        return None

    @staticmethod
    def episode_key(rule: Any, subject_type: Any, subject_id: Any, activation: Any) -> str:
        return f"{rule}|{subject_type}|{subject_id}|{int(activation or 0)}"

    def _startup_open_at(self, at_ms: int | None) -> bool:
        """Whether a start-up story was open at `at_ms`: live, any term story still starting; in the backfill, a
        term within its first 120 s, before it ended (the story states there are not yet the ones of that time)."""
        if not self.backfilling:
            return any(s.kind == "engine" and s.state == "starting" for s in self.index.stories.values())
        if at_ms is None:
            return False
        for t in self.terms.values():
            start = int(t.get("started_at") or 0)
            end = min(start + START_STORY_MAX_MS, int(t.get("ended_at") or start + START_STORY_MAX_MS))
            if start <= at_ms < end:
                return True
        return False

    def oversee_rows(self, rows: Sequence[Row], db_now: int, *, late_rows: bool = False) -> None:
        for r in rows:
            skey = self._subject_story(r.get("subject_type"), r.get("subject_id"), r.get("adapter_id"),
                                       r.get("account_id"))
            at_ms = r.get("at_ms")
            startup_open = self._startup_open_at(at_ms)
            detected = db_now if not self.backfilling else int(at_ms or db_now)
            if skey and skey.startswith(("pos:", "ord:")) and skey not in self.index.stories \
                    and skey not in self.open_at_start:
                skey = None      # no facts of that entity: a trade story would never close, so the line stands alone
            if skey:
                self.index.open_story(skey, int(at_ms or detected), detected)
            ep = self.episode_key(r.get("rule_id"), r.get("subject_type"), r.get("subject_id"), r.get("activation_id"))
            transition, level = r.get("transition"), r.get("level")
            sev = "info"
            if transition in ("activated", "raised"):
                sev = {"SOFT": "warn", "HARD": "alert"}.get(str(level), "info")
            if startup_open and (r.get("reason_code") == "stale_account_image" or transition in ("capped",
                                                                                                    "uncapped")):
                sev = "info"
            trade_subject = r.get("subject_type") in TRADE_TYPES
            lead = self._lead(skey) if skey and skey.startswith(("pos:", "ord:")) else {}
            if not lead.get("id") and trade_subject:
                lead["id"] = _id_tail(r.get("subject_id"))
            kind = "rule.raised" if transition in ("activated", "raised") and level else "rule.transition"
            extra: list[str] = []
            if kind == "rule.transition":
                if level:
                    extra.append("level")
                if transition == "capped":
                    extra = ["capped"]
            if skey and skey.startswith("acct:"):
                extra.append("account")
            ev = self.emit(kind, {**lead, "rule": r.get("rule_id"), "transition": transition, "level": level,
                                  "account": r.get("account_id"), "extra": extra},
                           at_ms=at_ms, clock="engine", detected_ms=detected, severity=sev,
                           ids={"event_id": r.get("event_id"), "rule_id": r.get("rule_id"),
                                "subject_id": r.get("subject_id"), "account_id": r.get("account_id"),
                                "sweep_id": r.get("sweep_id")},
                           story=skey, episode=ep, late="delayed" if late_rows else None,
                           source="lv_oversee_events", source_key=str(r.get("event_id")))
            if ev is None:
                continue
            k = (r.get("account_id"), transition, r.get("rule_id"), r.get("subject_type"), r.get("subject_id"),
                 at_ms)
            self.oversee_lines[k] = (ev.sid, ev.story)
            if len(self.oversee_lines) > 20_000:
                for kk in list(self.oversee_lines)[:5_000]:
                    del self.oversee_lines[kk]
            st = self.index.get(ev.story)
            if st is not None:
                e = st.episodes.get(ep) or Episode(episode=ep, rule_id=str(r.get("rule_id")))
                st.episodes[ep] = e
                if transition in ("activated", "raised"):
                    e.level, e.since_ms, e.resolved = level, e.since_ms or at_ms, False
                    e.raise_sid = ev.sid
                    self.episode_lines[ep] = (ev.sid, ev.story)
                elif transition == "lowered":
                    e.level = level
                elif transition == "resolved":
                    e.resolved = True
                if r.get("cap"):
                    e.cap = r.get("cap")
                if st.kind == "account":
                    self.index.set_state(st.key, "active" if any(not x.resolved for x in st.episodes.values())
                                         else "resolved")
                self._header(st.key)

    def set_breaches(self, rows: Sequence[Row], timing: Mapping[str, int]) -> None:
        """Latest open breaches (from the StateReader): episode timing and acks on their stories."""
        self.breaches = list(rows)
        for b in rows:
            skey = self._subject_story(b.get("subject_type"), b.get("subject_id"), b.get("adapter_id"),
                                       b.get("account_id"))
            st = self.index.get(self.index.resolve(skey)) if skey else None
            if st is None:
                continue
            ep = self.episode_key(b.get("rule_id"), b.get("subject_type"), b.get("subject_id"), b.get("activation_id"))
            e = st.episodes.get(ep)
            if e is None:
                e = Episode(episode=ep, rule_id=str(b.get("rule_id")))
                st.episodes[ep] = e
            esc = None
            if b.get("level") == "SOFT" and b.get("cap") == "NONE":
                esc = max(0, (int(timing.get("escalate_after_ms", 900_000)) - int(b.get("uncapped_ms") or 0))
                          // 60_000)
            new = (b.get("level"), b.get("cap"), b.get("active_since"), esc, bool(b.get("acked")))
            old = (e.level, e.cap, e.since_ms, e.escalate_min, e.acked)
            if new != old:
                e.level, e.cap, e.since_ms, e.escalate_min, e.acked = new
                self.index.touched.add(st.key)

    # ---- halt -------------------------------------------------------------------------------------------------
    def seed_halt(self, latest: Row | None) -> None:
        self.halt_last = latest

    def halt_rows(self, rows: Sequence[Row], db_now: int) -> None:
        for r in sorted(rows, key=lambda x: x["id"]):
            prev = (self.halt_last or {}).get("effective_level") or "NONE"
            cur = r.get("effective_level") or "NONE"
            at_ms = r.get("at_ms")
            detected = db_now if not self.backfilling else int(at_ms or db_now)
            ids = {"halt_event_id": r.get("id"), "rule_id": r.get("rule_id"),
                   "reviewed_event_id": r.get("reviewed_event_id")}
            common: dict[str, Any] = {"at_ms": at_ms, "clock": "engine", "detected_ms": detected, "ids": ids,
                      "source": "lv_halt_events", "source_key": str(r.get("id"))}
            extra: list[str] = ["rule"] if r.get("rule_id") else []
            if r.get("hard_latch"):
                extra.append("latch")
            if r.get("forced"):
                extra.append("forced")
            ev: Event | None
            if prev == "NONE" and cur != "NONE":
                skey = f"halt:{r['id']}"
                self.index.open_story(skey, int(at_ms or detected), detected)
                self.halt_story = skey
                ev = self.emit("halt.raised", {"from_halt": prev, "to_halt": cur, "rule": r.get("rule_id"),
                                               "extra": extra}, story=skey, **common)
                self._link_halt(skey, r.get("rule_id"))
            elif cur == "NONE" and prev != "NONE":
                skey = self.halt_story or f"halt:{r['id']}"
                self.index.open_story(skey, int(at_ms or detected), detected)
                if r.get("by_whom") and r.get("by_whom") != "system":
                    ev = self.emit("halt.cleared_operator", {"by": r.get("by_whom"), "reviewed":
                                   r.get("reviewed_event_id"), "extra": (["reviewed"] if r.get("reviewed_event_id")
                                                                         else []) + ["reason_in_audit"]},
                                   story=skey, **common)
                else:
                    ev = self.emit("halt.cleared", {"from_halt": prev}, story=skey, **common)
                self.index.set_state(skey, "resolved")
                self._header(skey)
                self.halt_story = None
            elif prev == cur:
                ev = self.emit("halt.recorded", {"to_halt": cur, "extra": extra}, story=self.halt_story, **common)
            else:
                skey = self.halt_story or ""
                sev = "alert" if (prev, cur) == ("SOFT", "HARD") else "notice"
                ev = self.emit("halt.changed", {"from_halt": prev, "to_halt": cur, "rule": r.get("rule_id"),
                                                "extra": extra}, story=skey, severity=sev, **common)
            if ev is not None and at_ms is not None:
                self.halt_lines[int(at_ms)] = (ev.sid, ev.story)
            if self.halt_story:
                self._header(self.halt_story)
            self.halt_last = r

    def _link_halt(self, skey: str, rule_id: Any) -> None:
        if not rule_id:
            return
        for b in self.breaches:
            if b.get("rule_id") == rule_id and b.get("level") in ("SOFT", "HARD"):
                other = self._subject_story(b.get("subject_type"), b.get("subject_id"), b.get("adapter_id"),
                                            b.get("account_id"))
                if other and self.index.get(self.index.resolve(other)):
                    self.index.relate(skey, self.index.resolve(other) or other)

    # ---- operator ---------------------------------------------------------------------------------------------
    def operator_rows(self, rows: Sequence[Row], db_now: int) -> None:
        for r in rows:
            at_ms = r.get("at_ms")
            extra = (["target"] if r.get("target_id") else []) + (["approver"] if r.get("approver") else [])
            self.emit("operator.action", {"operator": r.get("operator") or "operator", "command":
                                          r.get("command") or "unknown", "target": r.get("target_id"),
                                          "by": r.get("approver"), "outcome": r.get("outcome"), "extra": extra},
                      at_ms=at_ms, clock="server", detected_ms=db_now if not self.backfilling else int(at_ms or db_now),
                      ids={"audit_id": r.get("audit_id"), "epoch": r.get("epoch")},
                      source="lv_operator_actions", source_key=str(r.get("audit_id")))

    # ---- DLQ --------------------------------------------------------------------------------------------------
    def _dlq_story(self, r: Row, detected: int) -> str:
        """A row's story is fixed when the row is first read (its root then): a lineage merge later setting
        origin_dlq_id on a watched row must not move the row to another story."""
        dlq_id = int(r["dlq_id"])
        skey = self.dlq_keys.get(dlq_id)
        if skey is None:
            root = r.get("origin_dlq_id") or dlq_id
            skey = f"dlq:{root}"
            self.dlq_keys[dlq_id] = skey
            while len(self.dlq_keys) > 20_000:
                self.dlq_keys.pop(next(iter(self.dlq_keys)))
        st = self.index.open_story(skey, int(r.get("parked_at_ms") or detected), detected)
        st.ids.setdefault("dlq_id", skey.split(":", 1)[1])
        etype, adapter, eid = r.get("event_type"), r.get("adapter_id"), r.get("entity_id")
        if etype in TRADE_TYPES and eid:
            fx = self.facts.get(f"{etype}|{adapter}:{eid}")
            cands: list[str] = []
            if etype == "ORDER":
                cands = [self._order_story(fx)] if fx is not None else [f"ord:{adapter}:{eid}"]
            else:
                cands = [f"pos:{adapter}:{eid}"]
            if adapter in POSITION_IS_ENTRY_ORDER:      # an order id names its position only on these adapters
                cands += [f"pos:{adapter}:{eid}", f"ord:{adapter}:{eid}"]
            for cand in cands:
                real = self.index.resolve(cand)
                if real and real in self.index.stories:
                    self.index.relate(skey, real)
                    break
        return skey

    def dlq_rows(self, rows: Sequence[Row], db_now: int) -> None:
        for r in rows:
            detected = db_now if not self.backfilling else int(r.get("parked_at_ms") or db_now)
            skey = self._dlq_story(r, detected)
            again = r.get("origin_dlq_id") is not None
            self.emit("dlq.parked_again" if again else "dlq.parked",
                      {"event_type": r.get("event_type"), "id": f"{r.get('adapter_id')}:{r.get('entity_id')}",
                       "reason": r.get("dlq_reason"), "extra": ["reason"] if r.get("dlq_reason") else []},
                      at_ms=r.get("parked_at_ms"), clock="server", detected_ms=detected, story=skey,
                      ids={"dlq_id": r.get("dlq_id"), "origin_dlq_id": r.get("origin_dlq_id"),
                           "entity_id": r.get("entity_id"), "adapter_id": r.get("adapter_id")},
                      source="lv_dlq", source_key=str(r.get("dlq_id")))
            self._dlq_state(skey, r)

    def dlq_changes(self, pairs: Sequence[tuple[Row, Row]], db_now: int) -> None:
        for old, new in pairs:
            skey = self._dlq_story(old, db_now)      # the old row: a merge must not move the row to its origin
            a, b = old.get("status"), new.get("status")
            common: dict[str, Any] = {"at_ms": new.get("updated_at_ms"), "clock": "server", "detected_ms": db_now,
                                      "story": skey, "ids": {"dlq_id": new.get("dlq_id")}, "source": "lv_dlq",
                                      "source_key": str(new.get("dlq_id"))}
            origin = new.get("origin_dlq_id")
            if origin is not None and old.get("origin_dlq_id") is None:
                # merged into an older row of the same lineage: say so, then file this story under the origin's
                self.emit("dlq.parked_again", {"event_type": new.get("event_type"),
                                               "id": f"{new.get('adapter_id')}:{new.get('entity_id')}",
                                               "reason": new.get("dlq_reason"),
                                               "extra": ["reason"] if new.get("dlq_reason") else []}, **common)
                own = self.index.get(skey)
                if own is not None and own.ids.get("dlq_id") == str(new.get("dlq_id")):
                    self.index.set_state(skey, "resolved")
                target = f"dlq:{origin}"
                if target in self.index.stories and target != skey:
                    self._merge(skey, target, False)
                continue
            if b == "QUARANTINED" and a != b:
                self.emit("dlq.quarantined", {"dlq": new.get("dlq_id"), "retries": new.get("retry_count")}, **common)
            elif a == "QUARANTINED" and b != a:
                self.emit("dlq.released", {"dlq": new.get("dlq_id")}, **common)
            elif b == "RESOLVED" and a != b:
                self.emit("dlq.resolved", {"dlq": new.get("dlq_id")}, **common)
            elif new.get("retry_count") != old.get("retry_count"):
                self.emit("dlq.retry", {"dlq": new.get("dlq_id"), "retries": new.get("retry_count")}, **common)
            else:
                self.emit("dlq.cycle", {"dlq": new.get("dlq_id"), "state": b}, **common)
            self._dlq_state(skey, new)

    def _dlq_state(self, skey: str, r: Row) -> None:
        st = self.index.get(skey)
        if st is None or st.ids.get("dlq_id") != str(r.get("dlq_id")):
            return          # only the story's own (root) row sets its state
        st.dlq_status = r.get("status")
        self.index.set_state(skey, {"RESOLVED": "resolved", "QUARANTINED": "quarantined"}.get(str(r.get("status")),
                                                                                              "queued"))
        if r.get("status") == "QUARANTINED":
            st.worst = "alert"
        self._header(skey)

    # ---- alerts as badges -------------------------------------------------------------------------------------
    def alert_rows(self, rows: Sequence[Row], db_now: int) -> None:
        for r in rows:
            if r.get("kind") == "arm":
                continue
            target: tuple[str, str | None] | None = None
            kind = r.get("kind")
            if kind in ("episode", "repage"):
                ep = self.episode_key(r.get("ep_rule_id"), r.get("ep_subject_type"), r.get("ep_subject_id"),
                                      r.get("ep_activation_id"))
                target = self.episode_lines.get(ep)
                if kind == "repage" and target is not None:
                    st = self.index.get(self.index.resolve(target[1]))
                    if st is not None and ep in st.episodes:
                        st.episodes[ep].repages += 1
                        st.episodes[ep].last_repage_ms = r.get("created_at_ms")
                        self.index.touched.add(st.key)
            elif kind == "trip" or (kind == "info" and r.get("info_class") in ("halt_clear", "halt_clear_operator")):
                target = self.halt_lines.get(int(r.get("created_at_ms") or -1))
            elif kind == "info" and r.get("info_class") == "transition":
                k = (r.get("account_id"), r.get("tr_transition"), r.get("tr_rule_id"), r.get("tr_subject_type"),
                     r.get("tr_subject_id"), r.get("created_at_ms"))
                target = self.oversee_lines.get(k)
                if target is None:
                    self.emit("alert.rule_notice", {"rule": r.get("tr_rule_id")}, at_ms=r.get("created_at_ms"),
                              clock="engine", detected_ms=db_now, ids={"outbox_id": r.get("outbox_id")},
                              source="lv_alerts", source_key=str(r.get("outbox_id")))
            elif kind == "info":
                self.emit("alert.notice", {"sev": r.get("severity")}, at_ms=r.get("created_at_ms"), clock="engine",
                          detected_ms=db_now if not self.backfilling else int(r.get("created_at_ms") or db_now),
                          ids={"outbox_id": r.get("outbox_id")}, source="lv_alerts",
                          source_key=str(r.get("outbox_id")))
            sid, story = target if target else (None, None)
            self.alerts[int(r["outbox_id"])] = (sid, story, r)
            if sid:
                self.index.badge(story, sid, str(r.get("delivery") or "queued"))
                if r.get("escalated"):
                    self.index.badge(story, sid, "escalated")
            self._alert_lines(r, sid, story, db_now)

    def alert_changes(self, pairs: Sequence[tuple[Row, Row]], db_now: int) -> None:
        for _old, new in pairs:
            sid, story, _ = self.alerts.get(int(new["outbox_id"]), (None, None, new))
            self.alerts[int(new["outbox_id"])] = (sid, story, new)
            if sid:
                self.index.badge(story, sid, str(new.get("delivery") or "queued"))
                if new.get("escalated"):
                    self.index.badge(story, sid, "escalated")
            self._alert_lines(new, sid, story, db_now)

    def _alert_lines(self, r: Row, sid: str | None, story: str | None, db_now: int) -> None:
        oid = int(r["outbox_id"])
        for cond, kind in ((r.get("delivery") == "retrying", "alert.retrying"),
                           (r.get("delivery") == "undeliverable", "alert.undeliverable"),
                           (bool(r.get("escalated")), "alert.escalated")):
            if cond and (oid, kind) not in self.alert_lines_done:
                self.alert_lines_done.add((oid, kind))
                if self.backfilling and kind == "alert.retrying":
                    continue
                self.emit(kind, {"extra": ["repage"] if r.get("kind") == "repage" else []},
                          at_ms=r.get("created_at_ms"), clock="engine", detected_ms=db_now,
                          ids={"outbox_id": oid}, story=self.index.resolve(story) if story else None,
                          source="lv_alerts", source_key=str(oid))

    # ---- engine runs ------------------------------------------------------------------------------------------
    def seed_runs(self, runs: Sequence[Row]) -> None:
        for r in runs:
            self.runs[r["run_id"]] = r
            if r.get("stopped_at_ms") is None and any(o.get("node_id") == r.get("node_id") and
                                                      (o.get("started_at_ms") or 0) > (r.get("started_at_ms") or 0)
                                                      for o in runs):
                self.runs_reported_unclean.add(r["run_id"])

    def run_rows(self, new: Sequence[Row], changed: Sequence[tuple[Row, Row]], db_now: int) -> None:
        for r in new:
            self.runs[r["run_id"]] = r
            for o in list(self.runs.values()):
                if o["run_id"] != r["run_id"] and o.get("node_id") == r.get("node_id") and \
                        o.get("stopped_at_ms") is None and (o.get("started_at_ms") or 0) < (r.get("started_at_ms")
                                                                                            or 0) \
                        and o["run_id"] not in self.runs_reported_unclean:
                    self.runs_reported_unclean.add(o["run_id"])
                    self.emit("engine.run_unclean", {"node": o.get("node_id"), "at": o.get("last_seen_ms")},
                              at_ms=r.get("started_at_ms"), clock="engine", detected_ms=db_now,
                              ids={"run_id": o["run_id"], "node": o.get("node_id")}, source="lv_engine_runs",
                              source_key=o["run_id"])
            if self.engine_hold_until is not None and not self.engine_alerted:
                self.new_run_during_hold = r
            else:
                self.emit("engine.run_started", {"node": r.get("node_id")}, at_ms=r.get("started_at_ms"),
                          clock="engine", detected_ms=db_now, ids={"run_id": r["run_id"], "node": r.get("node_id")},
                          source="lv_engine_runs", source_key=r["run_id"])
        for old, r in changed:
            self.runs[r["run_id"]] = r
            if r.get("stopped_at_ms") is not None and old.get("stopped_at_ms") is None:
                self.emit("engine.run_stopped", {"node": r.get("node_id"), "reason": r.get("end_code"),
                                                 "extra": ["end"] if r.get("end_code") else []},
                          at_ms=r.get("stopped_at_ms"), clock="engine", detected_ms=db_now,
                          ids={"run_id": r["run_id"], "node": r.get("node_id")}, source="lv_engine_runs",
                          source_key=r["run_id"])

    # ---- leader terms (start-up stories) ----------------------------------------------------------------------
    def _term_key(self, term_id: int) -> str:
        return f"term:{term_id}"

    def _term_for_epoch(self, epoch: Any) -> str | None:
        if epoch is None:
            return None
        ids = self.term_by_epoch.get(int(epoch), [])
        if len(ids) != 1:
            return None
        key = self._term_key(ids[0])
        return key if key in self.index.stories else None

    def term_rows(self, rows: Sequence[Row], db_now: int, *, seed: bool = False) -> None:
        for r in sorted(rows, key=lambda x: x["term_id"]):
            tid = int(r["term_id"])
            known = tid in self.terms
            self.terms[tid] = r
            ids = self.term_by_epoch.setdefault(int(r["epoch_revision"]), [])
            if tid not in ids:
                ids.append(tid)
            skey = self._term_key(tid)
            detected = db_now if not self.backfilling else int(r.get("started_at") or db_now)
            st = self.index.open_story(skey, int(r.get("started_at") or detected), detected)
            st.ids.update({"term_id": str(tid), "epoch": str(r["epoch_revision"]), "node": str(r.get("node"))})
            st.data.update({"node": r.get("node"), "epoch": r.get("epoch_revision"), "term": tid})
            if (seed or self.backfilling) and not known and \
                    int(r.get("started_at") or 0) < db_now - START_STORY_MAX_MS:
                # a term already past its start-up window when this process first sees it (backfill or start
                # seed): its start-up is over, so it never shows "starting" again or raises still-lost lines
                st.state = "done"
            if seed or known:
                self._header(skey)
                continue
            common: dict[str, Any] = {"clock": "engine", "detected_ms": detected, "story": skey,
                      "ids": {"term_id": tid, "epoch": r["epoch_revision"], "node": r.get("node")},
                      "source": "lv_leader_terms", "source_key": str(tid)}
            self.emit("engine.term_started", {"term": tid, "node": r.get("node"), "epoch": r["epoch_revision"]},
                      at_ms=r.get("started_at"), **common)
            if r.get("reached_leader_at") is not None:
                self.emit("engine.leader", {"node": r.get("node"), "epoch": r["epoch_revision"]},
                          at_ms=r.get("reached_leader_at"), **common)
            if r.get("end_reason") is not None or r.get("ended_at") is not None:
                self._term_end(r, None, db_now)
            self._header(skey)

    def term_changes(self, pairs: Sequence[tuple[Row, Row]], db_now: int, successors: Sequence[Row]) -> None:
        for old, r in pairs:
            tid = int(r["term_id"])
            self.terms[tid] = r
            skey = self._term_key(tid)
            if skey not in self.index.stories:
                continue
            common: dict[str, Any] = {"clock": "engine", "detected_ms": db_now, "story": skey,
                      "ids": {"term_id": tid, "epoch": r["epoch_revision"], "node": r.get("node")},
                      "source": "lv_leader_terms", "source_key": str(tid)}
            if old.get("reached_leader_at") is None and r.get("reached_leader_at") is not None:
                self.emit("engine.leader", {"node": r.get("node"), "epoch": r["epoch_revision"]},
                          at_ms=r.get("reached_leader_at"), **common)
            if (old.get("end_reason"), old.get("ended_at")) != (r.get("end_reason"), r.get("ended_at")):
                succ = next((s for s in successors if r.get("ended_at") is not None
                             and s.get("started_at") == r.get("ended_at") and s["term_id"] != tid), None)
                self._term_end(r, succ, db_now, old=old)
            self._header(skey)

    def _term_end(self, r: Row, successor: Row | None, db_now: int, old: Row | None = None) -> None:
        tid = int(r["term_id"])
        skey = self._term_key(tid)
        common: dict[str, Any] = {"clock": "engine", "detected_ms": db_now, "story": skey,
                  "ids": {"term_id": tid, "epoch": r["epoch_revision"]}, "source": "lv_leader_terms",
                  "source_key": str(tid)}
        if r.get("ended_at") is None and r.get("end_reason") is not None:
            self.emit("engine.draining", {"term": tid, "reason": r.get("end_reason")}, at_ms=None, **common)
        elif r.get("ended_at") is not None:
            if successor is not None:
                self.emit("engine.term_closed_by", {"term": tid, "n": int(successor["term_id"])},
                          at_ms=r.get("ended_at"), **common)
            elif old is None or old.get("ended_at") is None:
                self.emit("engine.term_ended", {"term": tid, "reason": r.get("end_reason") or "unknown"},
                          at_ms=r.get("ended_at"), **common)
        self.index.set_state(skey, "done")

    def close_startups(self, db_now: int, streams: Sequence[Row]) -> None:
        """Close start-up stories (s4.4.7 "Closing") and raise the 60 s still-lost break-outs."""
        for tid, r in self.terms.items():
            skey = self._term_key(tid)
            st = self.index.get(skey)
            if st is None or st.state != "starting":
                continue
            epoch = int(r["epoch_revision"])
            rows = [s for s in streams if self.active_adapters is None or s["adapter_id"] in self.active_adapters]
            age = db_now - st.opened_detect_ms
            if age >= STILL_LOST_MS:
                for s in rows:
                    k = (tid, s["adapter_id"], s["stream"])
                    if s.get("conn_lost_at") is not None and k not in self.term_still_lost:
                        self.term_still_lost.add(k)
                        self.emit("stream.still_lost", {"adapter": adapter_name(s["adapter_id"]),
                                                        "stream": s["stream"]}, at_ms=None, clock="server",
                                  detected_ms=db_now, story=skey, ids={"adapter_id": s["adapter_id"],
                                                                       "stream": s["stream"]},
                                  source="lv_stream_health")
            reached = r.get("reached_leader_at") is not None
            all_rec = all(s.get("conn_lost_at") is None and s.get("reconciled_epoch") == epoch for s in rows)
            if (reached and all_rec) or age >= START_STORY_MAX_MS or r.get("end_reason") or r.get("ended_at"):
                self.index.set_state(skey, "done")
                self._header(skey)

    def refresh_headers(self) -> None:
        for key in list(self.index.stories):
            self._header(key)

    # ---- catch-ups --------------------------------------------------------------------------------------------
    def catchup_rows(self, rows: Sequence[Row], db_now: int, *, late_rows: bool = False) -> None:
        for r in rows:
            adapter = r.get("adapter_id")
            reason = r.get("reason")
            detected = db_now if not self.backfilling else int(r.get("finished_at_ms") or db_now)
            story: str | None = None
            parent = None
            if reason in JOIN_REASONS and not self.backfilling:
                open_keys = sorted(((k, ks) for k, ks in self.outages.items() if k[0] == adapter),
                                   key=lambda kv: (self._outage_lost_at(kv[1]), STREAM_ORDER.get(kv[0][1], 9)))
                if open_keys:
                    story = open_keys[0][1]
                    self.joins.append(CatchupJoin(adapter=str(adapter), story=story, reason=str(reason),
                                                  open_set={ks for _, ks in open_keys}, after_n_poll=self.poll_no))
            if story is None:
                story = self._term_for_epoch(r.get("epoch"))
                if story is None:
                    parent = None
            incomplete = (not r.get("complete")) or int(r.get("anomalies") or 0) > 0
            extra: list[str] = []
            if r.get("offline_from_ms") is not None:
                extra.append("offline")
            if int(r.get("anomalies") or 0) > 0:
                extra.append("anomalies")
            offline = max(0, int(r.get("gap_end_ms") or 0) - int(r.get("offline_from_ms") or r.get("gap_start_ms")
                                                                  or 0))
            self.emit("catchup.incomplete" if incomplete else "catchup.done",
                      {"adapter": adapter_name(adapter), "reason": reason, "seen": r.get("orders_seen"),
                       "new": r.get("orders_new"), "completeness": "complete", "offline": offline,
                       "anomalies": r.get("anomalies"), "extra": extra},
                      at_ms=r.get("finished_at_ms"), clock="engine", detected_ms=detected, story=story,
                      parent=parent, late="delayed" if late_rows else None,
                      ids={"catchup_id": r.get("id"), "adapter_id": adapter, "epoch": r.get("epoch"),
                           "run_id": r.get("run_id")}, source="lv_catchups", source_key=str(r.get("id")))
            if story:
                self._header(story)

    def _outage_lost_at(self, key: str) -> int:
        tail = key.rsplit(":", 1)[-1]
        return int(tail[1:]) if tail.startswith("r") else int(tail)

    # ---- stream health ----------------------------------------------------------------------------------------
    def stream_changes(self, pairs: Sequence[tuple[Row | None, Row]], db_now: int, day: Any = None) -> None:
        term_starts = {int(t["started_at"]): tid for tid, t in self.terms.items() if t.get("started_at") is not None}
        for old, new in pairs:
            lost_old = old.get("conn_lost_at") if old else None
            lost_new = new.get("conn_lost_at")
            rec_old = old.get("reconciled_at") if old else None
            rec_new = new.get("reconciled_at")
            lost = int(lost_new) if lost_new is not None and lost_new != lost_old else None
            rec_changed = rec_new is not None and rec_new != rec_old
            if lost is not None and rec_new is not None and rec_changed and lost >= int(rec_new):
                # lost, reconciled and lost again between two polls: the reconcile closes the earlier loss (or is
                # a blip when that loss was never seen), then the new loss opens its own outage
                self._stream_reconcile(old, new, lost_old, None, db_now, day)
                self._stream_loss(new, lost, term_starts, db_now, day)
                continue
            if lost is not None:
                self._stream_loss(new, lost, term_starts, db_now, day)
            if rec_changed:
                self._stream_reconcile(old, new, lost_old, lost_new, db_now, day)

    def _stream_loss(self, new: Row, lost: int, term_starts: Mapping[int, int], db_now: int, day: Any) -> None:
        """A new conn_lost_at: the leader-start fence (one info line per term), or an outage."""
        adapter, stream = new["adapter_id"], new["stream"]
        tid = term_starts.get(lost)
        if tid is not None:
            self.fence_rows.add((adapter, stream))
            if tid not in self.fenced_terms:
                self.fenced_terms.add(tid)
                self.emit("stream.fenced", {"epoch": self.terms[tid]["epoch_revision"]}, at_ms=lost,
                          clock="server", detected_ms=db_now, story=self._term_key(tid),
                          ids={"term_id": tid, "epoch": self.terms[tid]["epoch_revision"]}, source="lv_stream_health")
            if day is not None:
                day.stream_event(adapter, stream, lost, "fenced")
            return
        key = f"out:{adapter}:{stream}:{lost}"
        st = self.index.open_story(key, lost, db_now)
        st.ids.update({"adapter_id": adapter, "stream": stream})
        st.data.update({"adapter": adapter, "stream": stream, "lost_at": lost})
        self.outages[(adapter, stream)] = key
        self.emit("stream.lost", {"adapter": adapter_name(adapter), "stream": stream}, at_ms=lost, clock="server",
                  detected_ms=db_now, story=key, ids={"adapter_id": adapter, "stream": stream},
                  source="lv_stream_health")
        self._header(key)
        if day is not None:
            day.stream_event(adapter, stream, lost, "down")

    def _stream_reconcile(self, old: Row | None, new: Row, lost_old: Any, lost_new: Any, db_now: int,
                          day: Any) -> None:
        adapter, stream = new["adapter_id"], new["stream"]
        rec_new = int(new["reconciled_at"])
        ids = {"adapter_id": adapter, "stream": stream}
        name = adapter_name(adapter)
        first_in_epoch = old is None or old.get("reconciled_epoch") != new.get("reconciled_epoch")
        term_story = self._term_for_epoch(new.get("reconciled_epoch")) if first_in_epoch else None
        outage = self.outages.pop((adapter, stream), None)
        was_fence = (adapter, stream) in self.fence_rows
        self.fence_rows.discard((adapter, stream))
        if day is not None:
            day.stream_event(adapter, stream, rec_new, "up")
        if outage is not None:
            ost = self.index.get(outage)
            if ost is not None:
                ost.gap_ms = max(0, rec_new - int(ost.data.get("lost_at") or rec_new))
            if first_in_epoch:
                self.emit("stream.reconciled", {"adapter": name, "stream": stream}, at_ms=rec_new, clock="server",
                          detected_ms=db_now, story=term_story, ids=ids, source="lv_stream_health")
                if term_story:
                    self.index.relate(outage, term_story)
                self.index.set_state(outage, "reconciled")
            else:
                self.emit("stream.reconciled", {"adapter": name, "stream": stream}, at_ms=rec_new, clock="server",
                          detected_ms=db_now, story=outage, ids=ids, source="lv_stream_health")
                self.index.set_state(outage, "reconciled")
                self._fold(adapter, stream, outage)
            self._header(outage)
        elif lost_old is None and lost_new is None and not first_in_epoch:
            key = f"out:{adapter}:{stream}:r{rec_new}"
            st = self.index.open_story(key, rec_new, db_now, state="reconciled")
            st.ids.update(ids)
            st.data.update({"adapter": adapter, "stream": stream, "lost_at": rec_new})
            self.emit("stream.blip", {"adapter": name, "stream": stream}, at_ms=rec_new, clock="server",
                      detected_ms=db_now, story=key, ids=ids, source="lv_stream_health")
            for j in self.joins:
                if j.adapter == adapter and j.after_n_poll == self.poll_no:
                    j.open_set.add(key)
            self._fold(adapter, stream, key)
            self._header(key)
        else:
            # the stream's first reconcile in its epoch (clears the fence), or a loss seeded at start
            self.emit("stream.reconciled", {"adapter": name, "stream": stream}, at_ms=rec_new, clock="server",
                      detected_ms=db_now, story=term_story if (first_in_epoch or was_fence) else None, ids=ids,
                      source="lv_stream_health")
            if term_story:
                self._header(term_story)

    def _fold(self, adapter: str, stream: str, outage: str) -> None:
        """Fold an outage cleared by a catch-up's fence release into that catch-up's story ("held for catch-up")."""
        for j in self.joins:
            if j.adapter != adapter or (adapter, stream) in j.reconciled_after:
                continue
            if outage not in j.open_set:
                j.reconciled_after.add((adapter, stream))
                continue
            j.reconciled_after.add((adapter, stream))
            st = self.index.get(outage)
            if st is None or st.merged_into:
                return
            if outage == j.story:
                if j.reason != "reconnect":
                    st.info_only = True
                    self.index.touched.add(outage)
                return
            st.info_only = True
            self._merge(outage, self.index.resolve(j.story) or j.story, True)
            return
        if len(self.joins) > 50:
            self.joins = self.joins[-50:]

    # ---- other diffs ------------------------------------------------------------------------------------------
    def cluster_change(self, pair: tuple[Row, Row] | None, db_now: int) -> None:
        if pair is None:
            return
        old, new = pair
        if new.get("safe_hold") and not old.get("safe_hold"):
            extra = {"abnormal_term_ends": ["abnormal"], "epoch_changes": ["epoch_changes"],
                     "already_set": ["already"]}.get(str(new.get("hold_code")), [])
            self.emit("engine.safe_hold", {"hold_count": new.get("hold_count"), "extra": extra},
                      at_ms=new.get("safe_hold_since"), clock="engine", detected_ms=db_now, source="lv_cluster_state")
        elif (old.get("safe_hold") and not new.get("safe_hold")) or (new.get("last_resume_at") !=
                                                                     old.get("last_resume_at")
                                                                     and new.get("last_resume_at") is not None):
            if new.get("resumed_by"):
                self.emit("engine.safe_hold_resumed", {"by": new.get("resumed_by")}, at_ms=new.get("last_resume_at"),
                          clock="engine", detected_ms=db_now, source="lv_cluster_state")
            else:
                self.emit("engine.safe_hold_resumed_unknown", {}, at_ms=new.get("last_resume_at"), clock="engine",
                          detected_ms=db_now, source="lv_cluster_state")

    def epoch_change(self, pair: tuple[Row, Row] | None, db_now: int) -> None:
        if pair is None:
            return
        _old, new = pair
        self.emit("engine.epoch", {"epoch": new.get("epoch"), "node": new.get("node")}, at_ms=new.get("bumped_at_ms"),
                  clock="server", detected_ms=db_now, severity="debug", story=self._term_for_epoch(new.get("epoch")),
                  ids={"epoch": new.get("epoch"), "node": new.get("node")}, source="lv_engine_epoch")

    def quarantine_changes(self, pairs: Sequence[tuple[Row | None, Row]], db_now: int) -> None:
        for old, new in pairs:
            skey = f"acct:{new['adapter_id']}:{new['account_id']}"
            lapsed = new.get("released_at_ms") is None
            was = old is not None and old.get("released_at_ms") is None
            if lapsed and (not was or (old or {}).get("quarantined_at_ms") != new.get("quarantined_at_ms")):
                self.index.open_story(skey, int(new.get("quarantined_at_ms") or db_now), db_now)
                self.emit("engine.oversight_lapsed", {"account": new["account_id"]},
                          at_ms=new.get("quarantined_at_ms"), clock="engine", detected_ms=db_now, story=skey,
                          ids={"account_id": new["account_id"], "adapter_id": new["adapter_id"]},
                          source="lv_sweep_quarantine")
            elif not lapsed and was:
                self.emit("engine.oversight_restored", {"account": new["account_id"]},
                          at_ms=new.get("released_at_ms"), clock="engine", detected_ms=db_now,
                          story=skey if skey in self.index.stories else None,
                          ids={"account_id": new["account_id"], "adapter_id": new["adapter_id"]},
                          source="lv_sweep_quarantine")

    def seed_auth(self, rows: Sequence[Row]) -> None:
        for r in rows:
            self.auth_rows[r["adapter_id"]] = r
            self.auth_read_poll[(r["adapter_id"], int(r.get("failed_at_ms") or 0))] = self.poll_no

    def auth_changes(self, pairs: Sequence[tuple[Row | None, Row]], db_now: int) -> None:
        for _old, new in pairs:
            a = new["adapter_id"]
            self.auth_rows[a] = new
            fk = (a, int(new.get("failed_at_ms") or 0))
            self.auth_read_poll[fk] = self.poll_no
            if new.get("state") == "AUTH_FAILED":
                self.auth_seen_arrival.add(fk)
                self.emit("engine.auth_failed", {"adapter": adapter_name(a)}, at_ms=new.get("failed_at_ms"),
                          clock="server", detected_ms=db_now, ids={"adapter_id": a}, source="lv_auth_state")
            elif new.get("state") == "REARMED":
                self.emit("engine.auth_rearmed", {"adapter": adapter_name(a)}, at_ms=new.get("rearmed_at_ms"),
                          clock="server", detected_ms=db_now, ids={"adapter_id": a}, source="lv_auth_state")

    def auth_shown(self, probe: Any) -> list[dict[str, Any]]:
        """The band's auth items (s4.5 "Auth state shown"); emits "AUTH_FAILED cleared" once per failure this
        process saw arrive, on evidence from a LEADER answer read in a later poll."""
        out: list[dict[str, Any]] = []
        for a, r in sorted(self.auth_rows.items()):
            fk = (a, int(r.get("failed_at_ms") or 0))
            if r.get("state") == "REARMED":
                out.append({"adapter": a, "shown": "rearmed", "since_ms": r.get("rearmed_at_ms") or fk[1]})
                continue
            shown = "as_recorded"
            if probe is not None and probe.leader_answer and probe.poll_no > self.auth_read_poll.get(fk, 0):
                state = probe.adapters.get(a)
                if state in AUTH_EVIDENCE:
                    r.setdefault("_cleared", True)
                elif state == "AUTH_FAILED":
                    r.pop("_cleared", None)
                    shown = "failed_now"
            if r.get("_cleared"):
                shown = "cleared"
                if fk in self.auth_seen_arrival and fk not in self.auth_cleared_emitted:
                    self.auth_cleared_emitted.add(fk)
                    self.emit("engine.auth_cleared", {"adapter": adapter_name(a)}, at_ms=None, clock="server",
                              detected_ms=probe.at_ms if probe is not None else fk[1], ids={"adapter_id": a},
                              source="probe")
            out.append({"adapter": a, "shown": shown, "since_ms": fk[1]})
        return out

    # ---- engine probe -----------------------------------------------------------------------------------------
    def probe_result(self, probe: Any, db_now: int, quarantined: bool, reached_leader_ms: int | None) -> None:
        if probe is None:
            return
        if probe.leader_answer:
            self.active_adapters = set(probe.adapters)
        self.last_role = probe.role or self.last_role
        if probe.down and self.engine_down_since is None:
            self.engine_down_since = probe.last_ok_ms or db_now
            self.engine_hold_until = db_now + ENGINE_HOLD_MS
            self.engine_alerted = False
            self.new_run_during_hold = None
        if self.engine_hold_until is not None and not self.engine_alerted:
            if self.new_run_during_hold is not None:
                r = self.new_run_during_hold
                secs = max(0, (int(r.get("started_at_ms") or db_now) - (self.engine_down_since or db_now)) // 1000)
                self.emit("engine.restarted", {"secs": secs}, at_ms=r.get("started_at_ms"), clock="engine",
                          detected_ms=db_now, ids={"run_id": r["run_id"], "node": r.get("node_id")},
                          source="lv_engine_runs", source_key=r["run_id"])
                self.new_run_during_hold = None
                if probe.down:
                    # the new run has not answered yet: a fresh 60 s hold from now, so an engine that stays down
                    # still gets its "stopped responding" alert
                    self.engine_down_since = db_now
                    self.engine_hold_until = db_now + ENGINE_HOLD_MS
                else:
                    self.engine_down_since = None
                    self.engine_hold_until = None
            elif db_now >= self.engine_hold_until:
                self.engine_alerted = True
                self.emit("engine.not_responding", {}, at_ms=self.engine_down_since, clock="server",
                          detected_ms=db_now, source="probe")
        if not probe.down and self.engine_down_since is not None:
            if self.engine_alerted:
                self.emit("engine.responding", {}, at_ms=None, clock="server", detected_ms=db_now, source="probe")
            self.engine_down_since = None
            self.engine_hold_until = None
            self.engine_alerted = False
        age = probe.sweep_age_s
        grace = reached_leader_ms is not None and db_now - reached_leader_ms < 60_000
        stalled = bool(probe.leader_answer and age is not None and age > 10 and not quarantined and not grace)
        if stalled and not self.sweep_stalled:
            self.emit("engine.sweep_stalled", {"secs": int(age or 0)}, at_ms=None, clock="server", detected_ms=db_now,
                      source="probe")
        elif not stalled and self.sweep_stalled and probe.leader_answer:
            self.emit("engine.sweep_passing", {}, at_ms=None, clock="server", detected_ms=db_now, source="probe")
        if probe.leader_answer or not stalled:
            self.sweep_stalled = stalled

    # ---- memory bound (24/7) ------------------------------------------------------------------------------------
    def prune(self, now: int) -> None:
        """Bound every map of the narrator: entries older than a week (or beyond a size cap) are forgotten. A
        forgotten entry only stops later rows from joining or badging an old line; nothing already shown changes."""
        week = now - 7 * 86_400_000
        for oid in [k for k, (_s, _st, r) in self.alerts.items() if int(r.get("created_at_ms") or 0) < week]:
            del self.alerts[oid]
        self.alert_lines_done = {k for k in self.alert_lines_done if k[0] in self.alerts}
        for at in [k for k in self.halt_lines if k < week]:
            del self.halt_lines[at]
        while len(self.episode_lines) > 20_000:
            self.episode_lines.pop(next(iter(self.episode_lines)))
        for key in [k for k, t in self.after_hold_close.items() if now - t > 3_600_000]:
            del self.after_hold_close[key]
        if len(self.runs) > 200:
            keep = sorted(self.runs.values(), key=lambda r: int(r.get("started_at_ms") or 0))[-200:]
            self.runs = {r["run_id"]: r for r in keep}
        self.runs_reported_unclean &= set(self.runs)
        for tid in [t for t, r in self.terms.items()
                    if r.get("ended_at") is not None and int(r["ended_at"]) < week
                    and self._term_key(t) not in self.index.stories]:
            r = self.terms.pop(tid)
            ids = self.term_by_epoch.get(int(r["epoch_revision"]), [])
            if tid in ids:
                ids.remove(tid)
            if not ids:
                self.term_by_epoch.pop(int(r["epoch_revision"]), None)
            self.fenced_terms.discard(tid)
        self.term_still_lost = {k for k in self.term_still_lost if k[0] in self.terms}
        self.auth_read_poll = {k: v for k, v in self.auth_read_poll.items()
                               if k[0] in self.auth_rows and k[1] == int(self.auth_rows[k[0]].get("failed_at_ms") or 0)}
        for st in self.index.stories.values():
            if len(st.episodes) > 50:
                for ep in [k for k, e in st.episodes.items() if e.resolved][: len(st.episodes) - 50]:
                    del st.episodes[ep]
            while len(st.badges) > 200:
                st.badges.pop(next(iter(st.badges)))

    # ---- story header sentences -------------------------------------------------------------------------------
    def _header(self, key: str | None) -> None:
        st = self.index.get(key) if key else None
        if st is None:
            return
        parts: list[str] = []
        if st.kind == "trade":
            lead = " ".join(x for x in (st.symbol, st.side, st.ids.get("position_id") or st.ids.get("broker_order_id"))
                            if x)
            lv = st.levels
            levels = ", ".join(f"{n} {lv[k]}" for k, n in (("stop", "stop"), ("take_profit", "take-profit"))
                               if lv.get(k))
            state = "open" if st.state == "open" else "closed"
            parts.append(f"{lead}: {state}" + (f" at {lv['entry']}" if lv.get("entry") else "")
                         + (f", {levels}" if levels else ""))
            moves = sum(1 for i in range(1, len(st.history))
                        if None not in (st.history[i].get("stop"), st.history[i - 1].get("stop"))
                        and st.history[i].get("stop") != st.history[i - 1].get("stop"))
            if moves:
                parts.append(f"stop moved {moves}x")
        elif st.kind == "engine":
            d = st.data
            parts.append(f"{d.get('node', '?')} term {d.get('term', '?')}, epoch {d.get('epoch', '?')}")
            parts.append("starting" if st.state == "starting" else "start-up done")
        elif st.kind == "outage":
            d = st.data
            parts.append(f"{adapter_name(d.get('adapter'))} {d.get('stream')} stream "
                         + ("lost" if st.state == "lost" else "reconciled"))
            if st.gap_ms is not None:
                parts.append(f"gap {st.gap_ms // 1000} s")
            if st.info_only:
                parts.append("held for catch-up")
        elif st.kind == "dlq":
            parts.append(f"DLQ row {st.ids.get('dlq_id', '?')} {str(st.dlq_status or 'QUEUED').lower()}")
        elif st.kind == "halt":
            parts.append("Halt " + ("active" if st.state == "active" else "cleared"))
        elif st.kind == "account":
            parts.append(f"Account {st.key.split(':', 2)[-1]}")
            parts.append("rule active" if st.state == "active" else "resolved")
        for e in st.episodes.values():
            if e.level:
                parts.append(f"{e.level} {e.rule_id}" + (" resolved" if e.resolved else ""))
        header = " · ".join(p for p in parts if p)
        if len(header) > 200:
            header = header[:199] + "…"
        if header != st.header:
            st.header = header
            self.index.touched.add(st.key)


__all__ = ["ADAPTER_NAME", "Facts", "Narrator", "POSITION_IS_ENTRY_ORDER", "adapter_name"]
