"""Narration (docs/design/live-view.md s4.4, s6 "narration"): bracket model, fold holds, story keys, outages and
catch-up folds, start-up stories by epoch, alert badges, auth evidence and the engine hold."""
from __future__ import annotations

from typing import Any

from src.liveview.narrate import Narrator
from src.liveview.probe import ProbeAnswer
from src.liveview.server import Hub
from src.liveview.stories import DayModel, Series, StoryIndex

T0 = 1_795_000_000_000


class H:
    """A narrator wired to a real Hub (guard, ring, story index)."""

    def __init__(self) -> None:
        self.index = StoryIndex()
        self.hub = Hub(history=5000, index=self.index, day=DayModel(T0 - 3_600_000), series=Series())
        self.n = Narrator(self.index, self.hub.emit, self.hub.merge)
        self.now = T0
        self.log_id = 0

    @property
    def events(self) -> list[dict[str, Any]]:
        return [d for (_n, kind, d, _s) in self.hub.ring if kind == "activity"]

    @property
    def merges(self) -> list[dict[str, Any]]:
        return [d for (_n, kind, d, _s) in self.hub.ring if kind == "story_merge"]

    def titles(self, *, include_debug: bool = False) -> list[str]:
        return [e["title"] for e in self.events if include_debug or not e["housekeeping"]]

    def story_of(self, title_part: str) -> str | None:
        return next(e["story"] for e in self.events if title_part in e["title"])

    def entity(self, etype: str, key: str, path: str = "stream", action: str = "APPLIED", **facts: Any) -> None:
        self.log_id += 1
        row = {"log_id": self.log_id, "seq_id": self.now << 20, "entity_type": etype, "entity_key": key,
               "adapter_id": key.split(":")[0], "account_id": "acc1", "action": action, "logged_at_ms": self.now,
               "version_path": path, "broker_ms": self.now, "facts": facts}
        self.n.entity_rows([row], self.now)

    def tick(self, ms: int = 1000) -> None:
        self.now += ms
        self.n.poll_no += 1
        self.n.tick_holds(self.now)


def order(h: H, oid: str, status: str, *, adapter: str = "stonex", entry: str | None = "1.09903",
          side: str = "SELL", kind: str | None = "STOP", reduce: bool = False, linked: str | None = None,
          **kw: Any) -> None:
    f = {"broker_order_id": oid, "client_order_id": kw.pop("client", oid), "symbol": "EURUSD", "side": side,
         "status": status, "is_reduce_only": reduce, "source": "STREAM", **kw}
    if entry is not None:
        f["entry_level"] = entry
    if kind is not None:
        f["leg_kind"] = kind
    if linked is not None:
        f["linked_position_id"] = linked
    h.entity("ORDER", f"{adapter}:{oid}", **f)


def position(h: H, pid: str, status: str, *, adapter: str = "stonex", entry: str = "1.09903", stop: str | None = None,
             tp: str | None = None, path: str = "stream", **kw: Any) -> None:
    f = {"position_id": pid, "symbol": "EURUSD", "side": "SELL", "status": status, "entry_level": entry,
         "source": "STREAM", **kw}
    if stop:
        f["stop_level"] = stop
    if tp:
        f["target_level"] = tp
    h.entity("POSITION", f"{adapter}:{pid}", path=path, **f)


def stonex_trade(h: H) -> None:
    order(h, "100", "WORKING")
    order(h, "101", "WORKING", side="BUY", entry="1.10203", kind="STOP", reduce=True, linked="100")
    order(h, "102", "WORKING", side="BUY", entry="1.09303", kind="LIMIT", reduce=True, linked="100")
    h.tick()
    order(h, "100", "FILLED")
    h.tick(500)
    position(h, "100", "OPEN")
    h.tick()


# ---- bracket model and fold holds -------------------------------------------------------------------------------
def test_stonex_pending_entry_brackets_join_the_order_story_then_merge_into_the_position() -> None:
    h = H()
    stonex_trade(h)
    assert h.story_of("stop-loss 1.10203 attached") == "ord:stonex:100"
    assert h.merges == [{"n": h.merges[0]["n"], "from": "ord:stonex:100", "into": "pos:stonex:100", "held": False}]
    assert "EURUSD SELL 100: entry filled → position opened at 1.09903" in h.titles()
    order(h, "101", "WORKING", side="BUY", entry="1.10003", kind="STOP", reduce=True, linked="100")
    assert h.titles()[-1] == "EURUSD SELL 100: stop-loss moved 1.10203 → 1.10003"
    assert h.events[-1]["story"] == "pos:stonex:100"
    assert not any("entry changed" in t or "entry moved" in t for t in h.titles())


def test_close_by_stop_loss_folds_across_polls_with_the_sibling_cancel() -> None:
    h = H()
    stonex_trade(h)
    order(h, "101", "FILLED", side="BUY", entry=None, kind="STOP", reduce=True, linked="100")
    h.tick(3000)
    position(h, "100", "CLOSED")
    order(h, "102", "CANCELLED", side="BUY", entry="1.09303", kind="LIMIT", reduce=True, linked="100")
    h.tick()
    closes = [t for t in h.titles() if "closed" in t or "filled" in t and "stop-loss" in t]
    assert closes == ["EURUSD SELL 100: closed by stop-loss (take-profit cancelled)"]
    assert h.index.get("pos:stonex:100").state == "closed"


def test_a_partner_12s_later_gives_two_lines_and_a_late_sibling_cancel_is_housekeeping() -> None:
    h = H()
    stonex_trade(h)
    order(h, "101", "FILLED", side="BUY", entry=None, kind="STOP", reduce=True, linked="100")
    for _ in range(10):
        h.tick()
    assert h.titles()[-1] == "EURUSD SELL 100: stop-loss filled"
    h.tick(2000)
    position(h, "100", "CLOSED")
    h.tick(11_000)
    assert h.titles()[-1] == "EURUSD SELL 100: closed (take-profit cancelled)" or \
        h.titles()[-1] == "EURUSD SELL 100: closed"
    order(h, "102", "CANCELLED", side="BUY", entry="1.09303", kind="LIMIT", reduce=True, linked="100")
    assert h.events[-1]["housekeeping"] is True or "cancelled with the close" in h.events[-1]["title"]


def test_a_closing_trade_reads_closed_closing_trade_never_stop_loss() -> None:
    h = H()
    stonex_trade(h)
    order(h, "103", "FILLED", side="BUY", entry=None, kind=None, reduce=True, linked="100", path="history")
    h.tick(1000)
    position(h, "100", "CLOSED")
    for _ in range(12):
        h.tick()
    t = [x for x in h.titles() if "closed" in x]
    assert t and all("stop-loss" not in x for x in t)
    assert any("closing trade" in x for x in t)


def test_dropped_stale_does_not_change_the_cache_and_no_change_is_housekeeping() -> None:
    h = H()
    stonex_trade(h)
    before = dict(h.n.facts["POSITION|stonex:100"].f)
    position(h, "100", "OPEN", entry="9.99999", action="DROPPED_STALE")
    assert h.n.facts["POSITION|stonex:100"].f == before
    position(h, "100", "OPEN")
    assert h.events[-1]["housekeeping"] is True


def test_an_entity_open_at_start_reads_moved_not_first_seen() -> None:
    h = H()
    h.n.seed_open([], [{"adapter_id": "stonex", "position_id": "7", "account_id": "acc1", "symbol": "GBPUSD",
                        "side": "BUY", "entry_level": "1.25000", "stop_level": "1.24000", "target_level": None,
                        "opened_by_client_order_id": None, "committed_at_ms": T0}], T0)
    position(h, "7", "OPEN", entry="1.25000", stop="1.24500", symbol="GBPUSD", side="BUY")
    assert h.titles() == ["GBPUSD BUY 7: stop-loss moved 1.24000 → 1.24500"]


def test_ctrader_ids_never_pair_or_merge_by_number() -> None:
    h = H()
    order(h, "500", "WORKING", adapter="pepperstone")
    position(h, "900", "OPEN", adapter="pepperstone")          # unrelated id spaces
    order(h, "600", "WORKING", adapter="pepperstone", side="BUY", entry="1.1", kind="STOP", reduce=True,
          linked="500")                                        # names the entry's number, but no position 500
    order(h, "500", "FILLED", adapter="pepperstone")
    titles = h.titles()
    assert "EURUSD SELL 500: entry filled" in titles
    assert h.story_of("stop-loss 1.1 attached") == "ord:pepperstone:600"
    assert not h.merges
    # the same rows on StoneX pair and merge
    h2 = H()
    stonex_trade(h2)
    assert h2.merges


def test_ctrader_stop_fill_still_folds_into_the_close() -> None:
    h = H()
    position(h, "900", "OPEN", adapter="pepperstone")
    order(h, "601", "WORKING", adapter="pepperstone", side="BUY", entry="1.2", kind="STOP", reduce=True, linked="900")
    h.tick()
    order(h, "601", "FILLED", adapter="pepperstone", side="BUY", entry=None, kind="STOP", reduce=True, linked="900")
    position(h, "900", "CLOSED", adapter="pepperstone")
    h.tick()
    assert "EURUSD SELL 900: closed by stop-loss" in h.titles()


def test_an_entry_cancelled_unfilled_ends_its_story_children_included() -> None:
    h = H()
    order(h, "100", "WORKING")
    order(h, "101", "WORKING", side="BUY", entry="1.10203", kind="STOP", reduce=True, linked="100")
    order(h, "100", "CANCELLED")
    order(h, "101", "CANCELLED", side="BUY", entry="1.10203", kind="STOP", reduce=True, linked="100")
    assert h.index.get("ord:stonex:100").state == "closed"


# ---- rule episodes, halt, badges --------------------------------------------------------------------------------
def oversee(h: H, transition: str, level: str | None, *, subject_type: str = "POSITION",
            subject: str = "stonex:100", rule: str = "max_position_risk", activation: int | None = None,
            at: int | None = None) -> dict[str, Any]:
    r = {"event_id": len(h.events) + 1, "sweep_id": h.now << 20, "rule_id": rule, "subject_type": subject_type,
         "subject_id": subject, "transition": transition, "adapter_id": "stonex", "account_id": "acc1",
         "level": level, "cap": "NONE", "reason_code": "risk_over_soft", "at_ms": at or h.now,
         "activation_id": activation}
    h.n.oversee_rows([r], h.now)
    return r


def test_episode_alert_badges_the_raising_line_with_activation_null_and_zero() -> None:
    h = H()
    stonex_trade(h)
    oversee(h, "activated", "SOFT", activation=None)
    raise_sid = h.events[-1]["sid"]
    h.n.alert_rows([{"outbox_id": 1, "created_at_ms": h.now, "kind": "episode", "severity": "SOFT",
                     "delivery": "delivered", "escalated": False, "ep_rule_id": "max_position_risk",
                     "ep_subject_type": "POSITION", "ep_subject_id": "stonex:100", "ep_activation_id": 0,
                     "ep_level": 2}], h.now)
    assert h.index.get("pos:stonex:100").badges == {raise_sid: ["delivered"]}


def test_two_transitions_of_one_account_in_one_pass_each_badge_their_own_line() -> None:
    h = H()
    oversee(h, "capped", "SOFT", subject_type="ACCOUNT", subject="acc1", rule="margin_usage", at=T0)
    capped = h.events[-1]["sid"]
    oversee(h, "lowered", "FLAG", subject_type="ACCOUNT", subject="acc1", rule="margin_usage", at=T0)
    lowered = h.events[-1]["sid"]
    base = {"created_at_ms": T0, "kind": "info", "info_class": "transition", "severity": "INFO",
            "delivery": "delivered", "escalated": False, "account_id": "acc1", "tr_rule_id": "margin_usage",
            "tr_subject_type": "ACCOUNT", "tr_subject_id": "acc1"}
    h.n.alert_rows([{**base, "outbox_id": 1, "tr_transition": "capped"},
                    {**base, "outbox_id": 2, "tr_transition": "lowered"},
                    {**base, "outbox_id": 3, "tr_transition": "resolved"}], h.now)
    badges = h.index.get("acct:stonex:acc1").badges
    assert set(badges) == {capped, lowered}
    assert h.titles()[-1] == "Rule margin_usage notice e-mailed"      # the unmatched one stands alone


def test_a_halt_story_links_the_active_episode_of_its_rule_and_a_trip_badges_it() -> None:
    h = H()
    stonex_trade(h)
    oversee(h, "activated", "SOFT", activation=42)
    h.n.set_breaches([{"breach_id": 1, "rule_id": "max_position_risk", "subject_type": "POSITION",
                       "subject_id": "stonex:100", "adapter_id": "stonex", "account_id": "acc1", "level": "SOFT",
                       "cap": "NONE", "activation_id": 42, "uncapped_ms": 0, "acked": False}],
                     {"escalate_after_ms": 900_000})
    h.n.halt_rows([{"id": 2, "level": "SOFT", "effective_level": "SOFT", "hard_latch": False,
                    "rule_id": "max_position_risk", "reason_code": "x", "by_whom": "system", "at_ms": h.now}], h.now)
    assert h.index.get("halt:2").related == ["pos:stonex:100"]
    h.n.alert_rows([{"outbox_id": 9, "created_at_ms": h.now, "kind": "trip", "severity": "SOFT",
                     "delivery": "queued", "escalated": False}], h.now)
    assert list(h.index.get("halt:2").badges.values()) == [["queued"]]
    h.n.halt_rows([{"id": 3, "level": "NONE", "effective_level": "NONE", "hard_latch": False, "rule_id": None,
                    "reason_code": None, "by_whom": "june", "at_ms": h.now + 5, "reviewed_event_id": 2}], h.now)
    assert h.titles()[-1].startswith("Halt cleared by june (reviewed event 2)")
    assert h.index.get("halt:2").state == "resolved"


# ---- DLQ --------------------------------------------------------------------------------------------------------
def test_dlq_episode_and_lineage_and_link_to_the_trade() -> None:
    h = H()
    stonex_trade(h)
    row = {"dlq_id": 3, "origin_dlq_id": None, "event_type": "ORDER", "entity_id": "101", "adapter_id": "stonex",
           "dlq_reason": "illegal_transition", "retry_count": 0, "status": "QUEUED", "parked_at_ms": h.now,
           "updated_at_ms": h.now}
    h.n.dlq_rows([row], h.now)
    h.n.dlq_rows([{**row, "dlq_id": 4, "origin_dlq_id": 3}], h.now)
    h.n.dlq_changes([(row, {**row, "status": "LEASED"})], h.now)
    h.n.dlq_changes([(row, {**row, "status": "QUARANTINED", "retry_count": 5})], h.now)
    keys = {e["story"] for e in h.events if e["category"] == "dlq"}
    assert keys == {"dlq:3"}
    assert "pos:stonex:100" in h.index.get("dlq:3").related
    lease = [e for e in h.events if e["title"].endswith("LEASED")]
    assert lease and lease[0]["housekeeping"]
    assert h.index.get("dlq:3").state == "quarantined"


# ---- streams, outages, catch-ups, start-up stories ---------------------------------------------------------------
def term(h: H, tid: int = 1, epoch: int = 7, started: int | None = None, **kw: Any) -> dict[str, Any]:
    r = {"term_id": tid, "cluster_id": "c1", "epoch_revision": epoch, "node": "pc1", "started_at": started or h.now,
         "reached_leader_at": None, "ended_at": None, "end_reason": None, **kw}
    h.n.term_rows([r], h.now)
    return r


def sh(adapter: str, stream: str, lost: int | None, rec: int | None, epoch: int | None) -> dict[str, Any]:
    return {"adapter_id": adapter, "stream": stream, "conn_lost_at": lost, "reconciled_at": rec,
            "reconciled_epoch": epoch, "updated_at_ms": 0}


def catchup(h: H, reason: str, epoch: int = 7, cid: int = 1) -> None:
    h.n.catchup_rows([{"id": cid, "run_id": None, "epoch": epoch, "adapter_id": "stonex", "reason": reason,
                       "gap_start_ms": h.now - 5000, "gap_end_ms": h.now, "covered_from_ms": None,
                       "offline_from_ms": None, "started_at_ms": h.now - 1000, "finished_at_ms": h.now,
                       "orders_seen": 3, "orders_new": 0, "orders_updated": 0, "positions_seen": 0,
                       "positions_new": 0, "positions_closed": 0, "cash_flows_new": 0, "anomalies": 0,
                       "complete": True}], h.now)


def started_term(h: H) -> dict[str, dict[str, Any]]:
    """Term 1 (epoch 7), its fence, the start-up catch-up and every stream's first reconcile."""
    t = term(h)
    rows = {s: sh("stonex", s, None, 100, 6) for s in ("account", "orders", "prices")}
    fence = {s: sh("stonex", s, t["started_at"], 100, 6) for s in rows}
    h.n.stream_changes([(rows[s], fence[s]) for s in rows], h.now)
    h.tick()
    catchup(h, "startup", cid=1)
    rec = {s: sh("stonex", s, None, h.now, 7) for s in rows}
    h.n.stream_changes([(fence[s], rec[s]) for s in rows], h.now)
    h.tick()
    return rec


def test_leader_start_fence_gives_one_info_event_and_members_join_the_term_story() -> None:
    h = H()
    started_term(h)
    fenced = [e for e in h.events if e["kind"] == "stream.fenced"]
    assert len(fenced) == 1 and fenced[0]["severity"] == "info" and fenced[0]["story"] == "term:1"
    assert not any(e["kind"] == "stream.lost" for e in h.events)
    assert {e["story"] for e in h.events if e["kind"] in ("stream.reconciled", "catchup.done")} == {"term:1"}


def test_reconnect_catch_up_joins_the_oldest_outage_and_folds_the_hold_losses() -> None:
    h = H()
    rec = started_term(h)
    lost_o = sh("stonex", "orders", h.now - 4000, rec["orders"]["reconciled_at"], 7)
    h.n.stream_changes([(rec["orders"], lost_o)], h.now)
    h.tick()
    hold = {s: sh("stonex", s, h.now, rec[s]["reconciled_at"], 7) for s in ("account", "prices")}
    h.n.stream_changes([(rec[s], hold[s]) for s in hold], h.now)
    h.tick()
    catchup(h, "reconnect", cid=2)
    after = {s: sh("stonex", s, None, h.now, 7) for s in ("account", "orders", "prices")}
    h.n.stream_changes([(hold["account"], after["account"]), (lost_o, after["orders"]),
                        (hold["prices"], after["prices"])], h.now)
    joined = h.story_of("caught up (reconnect)")
    assert joined is not None and joined.startswith("out:stonex:orders:")
    held = [m for m in h.merges if m["held"]]
    assert {m["into"] for m in held} == {joined} and len(held) == 2
    assert h.index.get(joined).to_json()["worst"] == "warn"
    for m in held:
        assert h.index.get(m["from"]).to_json()["worst"] == "info"
    # every loss and reconcile line appears exactly once
    lines = [e for e in h.events if e["kind"].startswith("stream.") and e["n"] > 0]
    assert len({e["n"] for e in lines}) == len(lines)


def test_a_retry_catch_up_shows_even_the_joined_outage_at_info() -> None:
    h = H()
    rec = started_term(h)
    hold = {s: sh("stonex", s, h.now + i, rec[s]["reconciled_at"], 7) for i, s in enumerate(("account", "orders",
                                                                                              "prices"))}
    h.n.stream_changes([(rec[s], hold[s]) for s in hold], h.now)
    h.tick()
    catchup(h, "retry", cid=3)
    joined = h.story_of("caught up (retry)")
    assert joined is not None and joined.startswith("out:stonex:account:")
    h.n.stream_changes([(hold[s], sh("stonex", s, None, h.now, 7)) for s in hold], h.now)
    assert h.index.get(joined).to_json()["worst"] == "info"


def test_a_loss_and_reconcile_between_polls_is_one_closed_r_keyed_outage() -> None:
    h = H()
    rec = started_term(h)
    new = sh("stonex", "prices", None, h.now, 7)
    h.n.stream_changes([(rec["prices"], new)], h.now)
    key = f"out:stonex:prices:r{h.now}"
    assert h.story_of("lost and reconciled between polls") == key
    assert h.index.get(key).state == "reconciled"
    assert "None" not in key


def test_catch_up_with_no_open_outage_joins_its_term_and_an_old_epoch_never_joins_a_newer_term() -> None:
    h = H()
    started_term(h)
    term(h, tid=2, epoch=9)
    catchup(h, "retry", epoch=7, cid=5)
    assert h.events[-1]["story"] == "term:1"
    catchup(h, "startup", epoch=99, cid=6)
    assert h.events[-1]["story"] is None


def test_start_up_story_closes_at_120s_and_on_draining() -> None:
    h = H()
    t = term(h)
    streams = [sh("stonex", "orders", t["started_at"], None, 6)]
    h.n.close_startups(h.now + 61_000, streams)
    assert any(e["kind"] == "stream.still_lost" for e in h.events)
    assert h.index.get("term:1").state == "starting"
    h.n.close_startups(h.now + 121_000, streams)
    assert h.index.get("term:1").state == "done"
    h2 = H()
    t2 = term(h2)
    h2.n.term_changes([(t2, {**t2, "end_reason": "switchover"})], h2.now, [])
    assert h2.index.get("term:1").state == "done"
    assert h2.titles()[-1] == "Term 1 draining (switchover)"


def test_a_term_closed_by_its_successor_reads_closed_at_the_start_of_term_n() -> None:
    h = H()
    t1 = term(h, tid=1, epoch=7)
    h.tick()
    t2 = term(h, tid=2, epoch=8)
    h.n.term_changes([(t1, {**t1, "ended_at": t2["started_at"], "end_reason": "lost"})], h.now, [t2])
    assert "Term 1 closed at the start of term 2" in h.titles()


# ---- engine runs, probe, auth -----------------------------------------------------------------------------------
def probe(h: H, *, ok: bool = True, down: bool = False, leader: bool = True, adapters: dict[str, str] | None = None,
          age: int | None = 1) -> ProbeAnswer:
    return ProbeAnswer(ok=ok, at_ms=h.now, poll_no=h.n.poll_no, role="LEADER" if leader and ok else None,
                       leader_answer=leader and ok, adapters=adapters or {}, sweep_age_s=age if leader else None,
                       down=down, last_ok_ms=h.now - 3000 if down else h.now)


def test_engine_stopped_then_a_new_run_within_60s_is_one_info_restarted_line() -> None:
    h = H()
    h.n.probe_result(probe(h, ok=False, down=True), h.now, False, None)
    h.tick(20_000)
    h.n.run_rows([{"run_id": "r2", "node_id": "pc1", "started_at_ms": h.now, "last_seen_ms": h.now,
                   "stopped_at_ms": None, "end_code": None}], [], h.now)
    h.n.probe_result(probe(h), h.now, False, None)
    kinds = [e["kind"] for e in h.events]
    assert kinds == ["engine.restarted"] and h.events[0]["story"] is None and h.events[0]["severity"] == "info"


def test_engine_down_for_60s_without_a_new_run_alerts_once() -> None:
    h = H()
    h.n.probe_result(probe(h, ok=False, down=True), h.now, False, None)
    for _ in range(70):
        h.tick()
        h.n.probe_result(probe(h, ok=False, down=True), h.now, False, None)
    assert [e["kind"] for e in h.events] == ["engine.not_responding"]
    h.n.probe_result(probe(h), h.now, False, None)
    assert h.events[-1]["kind"] == "engine.responding"


def test_risksweep_stall_only_on_a_leader_and_not_while_quarantined_or_just_after_leader() -> None:
    h = H()
    h.n.probe_result(probe(h, age=30), h.now, True, None)
    h.n.probe_result(probe(h, age=30), h.now, False, h.now - 10_000)
    h.n.probe_result(probe(h, leader=False, age=None), h.now, False, None)
    assert h.events == []
    h.n.probe_result(probe(h, age=30), h.now, False, None)
    assert h.events[-1]["kind"] == "engine.sweep_stalled"


def test_auth_cleared_needs_a_leader_answer_from_a_later_poll_and_is_emitted_once() -> None:
    h = H()
    failed = {"adapter_id": "stonex", "state": "AUTH_FAILED", "failed_at_ms": T0 - 5000, "rearmed_at_ms": None}
    h.n.auth_changes([(None, failed)], h.now)
    same_poll = probe(h, adapters={"stonex": "STREAMING"})
    assert h.n.auth_shown(same_poll)[0]["shown"] == "as_recorded"
    h.tick()
    for state in ("AUTHENTICATING", "BACKOFF", "DISCONNECTED"):
        assert h.n.auth_shown(probe(h, adapters={"stonex": state}))[0]["shown"] == "as_recorded"
    h.tick()
    assert h.n.auth_shown(probe(h, adapters={"stonex": "STREAMING"}))[0]["shown"] == "cleared"
    h.n.auth_shown(probe(h, adapters={"stonex": "STREAMING"}))
    assert [e["kind"] for e in h.events].count("engine.auth_cleared") == 1


def test_auth_failure_present_at_start_clears_in_the_band_without_an_event() -> None:
    h = H()
    h.n.seed_auth([{"adapter_id": "stonex", "state": "AUTH_FAILED", "failed_at_ms": T0 - 30 * 86_400_000,
                    "rearmed_at_ms": None}])
    h.tick()
    assert h.n.auth_shown(probe(h, adapters={"stonex": "STREAMING"}))[0]["shown"] == "cleared"
    assert not any(e["kind"] == "engine.auth_cleared" for e in h.events)


def test_a_crashed_run_is_reported_unclean_once_and_runs_never_open_stories() -> None:
    h = H()
    r1 = {"run_id": "r1", "node_id": "pc1", "started_at_ms": T0 - 100_000, "last_seen_ms": T0 - 50_000,
          "stopped_at_ms": None, "end_code": None}
    h.n.seed_runs([r1])
    r2 = {"run_id": "r2", "node_id": "pc1", "started_at_ms": T0, "last_seen_ms": T0, "stopped_at_ms": None,
          "end_code": None}
    h.n.run_rows([r2], [], h.now)
    kinds = [e["kind"] for e in h.events]
    assert kinds == ["engine.run_unclean", "engine.run_started"]
    assert all(e["story"] is None for e in h.events)


def test_every_story_key_passes_the_story_route_pattern() -> None:
    import re
    pattern = re.compile(r"^(pos|ord|acct|halt|term|out|dlq):[A-Za-z0-9:._@/-]{1,120}$")
    h = H()
    stonex_trade(h)
    started_term(h)
    for key in h.index.stories:
        assert pattern.fullmatch(key) and "None" not in key, key


def test_backfill_lines_are_never_marked_delayed() -> None:
    h = H()
    h.n.backfilling = True
    h.now = T0 - 3_600_000
    position(h, "5", "OPEN", adapter="pepperstone")
    h.n.backfilling = False
    assert h.events[-1]["late"] is None


