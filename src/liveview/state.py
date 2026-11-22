"""StateReader: the status band, Open now and open breaches (docs/design/live-view.md s4.5).

Runs in the same READ ONLY REPEATABLE READ transaction as the tailer each poll. Every value comes from the lv_ views
or the allow-listed probe; the result passes the payload guard and is pushed only when it changes.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from src.liveview import sql as Q
from src.liveview.events import check_value, level_str
from src.liveview.narrate import LEG_NAME, POSITION_IS_ENTRY_ORDER, Narrator
from src.liveview.probe import ProbeAnswer
from src.liveview.tailer import Conn, Row, as_row

#: src/oversee/config.py SweepConfig defaults (tests/liveview pins them).
DEFAULT_TIMING: Final = {"escalate_after_ms": 900_000, "cool_down_ms": 300_000, "unknown_grace_ms": 30_000}
DLQ_STATUSES: Final = frozenset({"QUEUED", "LEASED", "BACKOFF", "QUARANTINED"})
WEEK_MS: Final = 7 * 86_400_000


@dataclass
class StateRows:
    halt: Row | None = None
    breaches: list[Row] = field(default_factory=list)
    orders: list[Row] = field(default_factory=list)
    positions: list[Row] = field(default_factory=list)
    timing: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_TIMING))
    alerts_waiting: int = 0
    alerts_oldest: int | None = None
    alerts_undeliverable: int = 0
    alerts_escalated: int = 0
    dlq: dict[str, int] = field(default_factory=dict)
    term: Row | None = None


async def read_state(conn: Conn, db_now: int, day_start_ms: int) -> StateRows:
    s = StateRows()
    rows = await conn.fetch(Q.SQL_HALT_LATEST)
    s.halt = as_row(rows[0]) if rows else None
    s.breaches = [as_row(r) for r in await conn.fetch(Q.SQL_BREACHES_OPEN)]
    s.orders = [as_row(r) for r in await conn.fetch(Q.SQL_OPEN_ORDERS)]
    s.positions = [as_row(r) for r in await conn.fetch(Q.SQL_OPEN_POSITIONS)]
    t = await conn.fetch(Q.SQL_TIMING)
    if t:
        row = as_row(t[0])
        for k, default in DEFAULT_TIMING.items():
            v = row.get(k)
            s.timing[k] = int(v) if isinstance(v, int) and v >= 0 else default
    w = await conn.fetch(Q.SQL_ALERTS_WAITING, db_now - WEEK_MS)
    if w:
        r0 = as_row(w[0])
        s.alerts_waiting = int(r0.get("waiting") or 0)
        s.alerts_oldest = int(r0["oldest_ms"]) if r0.get("oldest_ms") is not None else None
    d = await conn.fetch(Q.SQL_ALERTS_TODAY, day_start_ms)
    if d:
        r0 = as_row(d[0])
        s.alerts_undeliverable = int(r0.get("undeliverable") or 0)
        s.alerts_escalated = int(r0.get("escalated_today") or 0)
    for r in await conn.fetch(Q.SQL_DLQ_COUNTS):
        r0 = as_row(r)
        if r0.get("status") in DLQ_STATUSES:
            s.dlq[str(r0["status"])] = int(r0.get("n") or 0)
    tr = await conn.fetch(Q.SQL_TERM_LATEST)
    s.term = as_row(tr[0]) if tr else None
    return s


def _subject_lead(n: Narrator, b: Row) -> tuple[str | None, str | None, str | None]:
    """(symbol, side, story key) of a breach subject."""
    skey = n._subject_story(b.get("subject_type"), b.get("subject_id"), b.get("adapter_id"), b.get("account_id"))
    st = n.index.get(n.index.resolve(skey)) if skey else None
    if st is not None:
        return st.symbol, st.side, st.key
    return None, None, None


def open_now(n: Narrator, orders: Sequence[Row], positions: Sequence[Row]) -> list[dict[str, Any]]:
    """Open positions and working orders not attached to a position, folded by the bracket model (s4.4.2)."""
    out: list[dict[str, Any]] = []
    pos_ids = {(p["adapter_id"], str(p["position_id"])) for p in positions}
    entry_ids = {(o["adapter_id"], str(o["broker_order_id"])) for o in orders if not o.get("is_reduce_only")}
    children: dict[tuple[str, str], dict[str, str | None]] = {}
    for o in orders:
        if o.get("is_reduce_only") and o.get("linked_position_id") and o.get("leg_kind") in LEG_NAME:
            k = (o["adapter_id"], str(o["linked_position_id"]))
            name = "stop" if o["leg_kind"] == "STOP" else "take_profit"
            children.setdefault(k, {})[name] = level_str(o.get("entry_level"))
    for p in positions:
        k = (p["adapter_id"], str(p["position_id"]))
        ch = children.get(k, {})
        skey = n.index.resolve(f"pos:{k[0]}:{k[1]}") or f"pos:{k[0]}:{k[1]}"
        st = n.index.get(skey)
        out.append({"story": skey, "kind": "position", "adapter": k[0], "id": k[1], "symbol": p.get("symbol"),
                    "side": p.get("side"), "status": "OPEN", "entry": level_str(p.get("entry_level")),
                    "stop": level_str(p.get("stop_level")) or ch.get("stop"),
                    "take_profit": level_str(p.get("target_level")) or ch.get("take_profit"),
                    "history": list(st.history) if st else []})
    for o in orders:
        k = (o["adapter_id"], str(o["broker_order_id"]))
        if o.get("is_reduce_only"):
            linked = (o["adapter_id"], str(o.get("linked_position_id")))
            if linked in pos_ids or (o["adapter_id"] in POSITION_IS_ENTRY_ORDER and linked in entry_ids):
                continue           # a child folded into its parent's row
        ch = children.get(k, {}) if o["adapter_id"] in POSITION_IS_ENTRY_ORDER else {}
        fx = n.facts.get(f"ORDER|{k[0]}:{k[1]}")
        skey = n._order_story(fx) if fx is not None else f"ord:{k[0]}:{k[1]}"
        st = n.index.get(n.index.resolve(skey))
        out.append({"story": n.index.resolve(skey) or skey, "kind": "order", "adapter": k[0], "id": k[1],
                    "symbol": o.get("symbol"), "side": o.get("side"), "status": o.get("status"),
                    "entry": level_str(o.get("entry_level")),
                    "stop": level_str(o.get("stop_level")) or ch.get("stop"),
                    "take_profit": level_str(o.get("target_level")) or ch.get("take_profit"),
                    "history": list(st.history) if st else []})
    for row in out:
        if not check_value("symbol", row["symbol"]):
            row["symbol"] = "(not shown)"
        if not check_value("side", row["side"]):
            row["side"] = None
        if not check_value("id", row["id"]):
            row["id"] = "(not shown)"
    return out


#: The engine beats every 15 s (src/runtime/deadman.py DEADMAN_INTERVAL_S). The failed-beat streak only counts
#: beats that were sent and refused; a beat that is never sent (task dead, circuit open, loop stuck) leaves it at 0
#: while the age grows. So the age alone also sets the tone: two missed beats is "warn", over 60 s is "alert".
DEADMAN_ACK_WARN_S: Final = 30
DEADMAN_ACK_ALERT_S: Final = 60


def deadman_stale(ok_age_s: int | None) -> str | None:
    """None while the last acknowledged beat is fresh (or there is none yet), else "warn" or "alert"."""
    if ok_age_s is None:
        return None
    if ok_age_s > DEADMAN_ACK_ALERT_S:
        return "alert"
    if ok_age_s > DEADMAN_ACK_WARN_S:
        return "warn"
    return None


def deadman_view(probe: ProbeAnswer | None) -> dict[str, Any] | None:
    """W1 (home-server plan): the dead-man receiver as the leader's gauges report it, for the Engine line: configured
    (XTNL_DEADMAN_URL set), the current failed-beat streak, the age of the last acknowledged beat (None until the
    term's first one) and `stale` (None, "warn" or "alert" from that age, see deadman_stale). None when there is no
    answering leader (the gauges are leader-only). Counts and ages only."""
    if probe is None or not probe.ok or not probe.leader_answer or probe.deadman_configured is None:
        return None
    return {"configured": probe.deadman_configured, "failures": probe.deadman_failures,
            "ok_age_s": probe.deadman_ok_age_s, "stale": deadman_stale(probe.deadman_ok_age_s)}


def build_state(n: Narrator, s: StateRows, *, db_now: int, probe: ProbeAnswer | None, streams: Sequence[Row],
                cluster: Row | None, quarantine: Sequence[Row], db_ok: bool) -> dict[str, Any]:
    timing = s.timing
    halt = None
    if s.halt is not None:
        h = s.halt
        halt = {"id": h.get("id"), "level": h.get("level"), "effective_level": h.get("effective_level"),
                "hard_latch": bool(h.get("hard_latch")), "rule_id": h.get("rule_id"),
                "reason_code": h.get("reason_code"), "by_whom": h.get("by_whom"), "at_ms": h.get("at_ms")}
    open_rows = [b for b in s.breaches if b.get("level") is not None]
    pending = sum(1 for b in s.breaches if b.get("level") is None and b.get("pending_level") is not None)
    counts = {"FLAG": 0, "SOFT": 0, "HARD": 0, "pending": pending}
    breaches_out: list[dict[str, Any]] = []
    soft_esc: list[int] = []
    capped_any = False
    for b in open_rows:
        lvl = str(b.get("level"))
        if lvl in counts:
            counts[lvl] += 1
        esc = None
        if lvl == "SOFT" and b.get("cap") == "NONE":
            esc = max(0, (timing["escalate_after_ms"] - int(b.get("uncapped_ms") or 0)) // 60_000)
            soft_esc.append(esc)
        if b.get("cap") == "SOFT":
            capped_any = True
        symbol, side, story = _subject_lead(n, b)
        ep = n.episode_key(b.get("rule_id"), b.get("subject_type"), b.get("subject_id"), b.get("activation_id"))
        st = n.index.get(story) if story else None
        repages = st.episodes[ep].repages if st is not None and ep in st.episodes else 0
        breaches_out.append({"breach_id": b.get("breach_id"), "rule_id": b.get("rule_id"),
                             "subject_type": b.get("subject_type"), "subject_id": b.get("subject_id"),
                             "symbol": symbol, "side": side, "level": lvl, "cap": b.get("cap"),
                             "reason_code": b.get("reason_code"), "since_ms": b.get("active_since"),
                             "escalate_min": esc, "capped": b.get("cap") == "SOFT", "acked": bool(b.get("acked")),
                             "repages": repages, "story": story})
    cooldown = None
    if halt and halt["effective_level"] == "SOFT" and not any(b.get("level") in ("SOFT", "HARD") for b in open_rows):
        below = [int(b["below_soft_since"]) for b in s.breaches if b.get("below_soft_since") is not None]
        if below:
            cooldown = max(below) + timing["cool_down_ms"]
    halt_why = {"escalate_min": min(soft_esc) if soft_esc else None, "capped": capped_any and not soft_esc,
                "cooldown_clear_ms": cooldown,
                "acked": bool(open_rows) and all(bool(b.get("acked")) for b in open_rows)}
    # engine
    role = probe.role if probe is not None and probe.ok else None
    engine: dict[str, Any]
    if probe is None or probe.off:
        engine = {"status": "off", "role": None, "since_ms": None, "sweep_age_s": None, "last_ok_ms": None}
    elif probe.down:
        engine = {"status": "down", "role": None, "since_ms": probe.last_ok_ms, "sweep_age_s": None,
                  "last_ok_ms": probe.last_ok_ms}
    elif not probe.ok:
        engine = {"status": "unknown", "role": n.last_role, "since_ms": None, "sweep_age_s": None,
                  "last_ok_ms": probe.last_ok_ms}
    else:
        engine = {"status": "stalled" if n.sweep_stalled else "running", "role": role, "since_ms": None,
                  "sweep_age_s": probe.sweep_age_s, "last_ok_ms": probe.last_ok_ms}
    engine["deadman"] = deadman_view(probe)
    leader = None
    if s.term is not None:
        t = s.term
        leader = {"term_id": t.get("term_id"), "node": t.get("node"), "epoch": t.get("epoch_revision"),
                  "since_ms": t.get("started_at"), "reached_leader_ms": t.get("reached_leader_at"),
                  "draining": t.get("end_reason") if t.get("ended_at") is None else None}
    safe_hold = {"on": bool((cluster or {}).get("safe_hold")), "since_ms": (cluster or {}).get("safe_hold_since"),
                 "hold_code": (cluster or {}).get("hold_code"), "hold_count": (cluster or {}).get("hold_count"),
                 "resumed_by": (cluster or {}).get("resumed_by"), "last_resume_ms": (cluster or {}).get("last_resume_at")}
    active = n.active_adapters
    streams_out: list[dict[str, Any]] = []
    term_starts = {int(t["started_at"]) for t in n.terms.values() if t.get("started_at") is not None}
    for r in streams:
        a = r["adapter_id"]
        if active is not None and a not in active:
            status = "inactive"
        elif r.get("conn_lost_at") is None:
            status = "live"
        elif int(r["conn_lost_at"]) in term_starts:
            status = "fenced"
        else:
            status = "lost"
        role_note = None
        if active is None:
            role_note = "engine probe off" if probe is None or probe.off else (f"role: {n.last_role}" if n.last_role
                                                                               else None)
        streams_out.append({"adapter": a, "stream": r["stream"], "status": status,
                            "since_ms": r.get("conn_lost_at") if status in ("lost", "fenced") else r.get(
                                "reconciled_at"),
                            "adapter_state": probe.adapters.get(a) if probe is not None and probe.leader_answer
                            else None, "role_note": role_note})
    oversight = [{"adapter": q["adapter_id"], "account_id": q["account_id"], "since_ms": q.get("quarantined_at_ms")}
                 for q in quarantine if q.get("released_at_ms") is None]
    return {
        "halt": halt, "halt_why": halt_why, "engine": engine, "leader": leader, "safe_hold": safe_hold,
        "streams": streams_out, "auth": n.auth_shown(probe), "oversight": oversight,
        "alerts": {"waiting": s.alerts_waiting, "oldest_ms": s.alerts_oldest, "undeliverable": s.alerts_undeliverable,
                   "escalated_today": s.alerts_escalated},
        "dlq": {k: s.dlq.get(k, 0) for k in ("QUEUED", "LEASED", "BACKOFF", "QUARANTINED")},
        "breaches": counts, "open_breaches": breaches_out, "open_now": open_now(n, s.orders, s.positions),
        "timing": dict(timing),
    }


def series_values(state: Mapping[str, Any]) -> dict[str, int | None]:
    return {"sweep_pass_age_s": state["engine"].get("sweep_age_s"), "alerts_waiting": state["alerts"]["waiting"],
            "dlq_queued": state["dlq"].get("QUEUED", 0),
            "open_breaches": sum(state["breaches"][k] for k in ("FLAG", "SOFT", "HARD"))}


__all__ = ["DEFAULT_TIMING", "StateRows", "build_state", "deadman_view", "open_now", "read_state", "series_values"]
