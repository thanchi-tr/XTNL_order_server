"""Day panel, series and Open now (docs/design/live-view.md s4.5, s4.7, s6 "day and series")."""
from __future__ import annotations

from src.liveview.events import Event, guard_payload
from src.liveview.narrate import Narrator
from src.liveview.server import Hub
from src.liveview.state import DEFAULT_TIMING, open_now
from src.liveview.stories import BUCKET_MS, DayModel, Series, StoryIndex

DAY = 1_795_000_000_000


def test_halt_intervals_start_from_the_level_in_force_at_midnight() -> None:
    d = DayModel(DAY)
    before = {"id": 1, "effective_level": "SOFT", "hard_latch": False, "at_ms": DAY - 3_600_000}
    rows = [{"id": 2, "effective_level": "NONE", "hard_latch": False, "at_ms": DAY + 60_000},
            {"id": 3, "effective_level": "HARD", "hard_latch": True, "at_ms": DAY + 120_000}]
    d.set_halt(before, rows, DAY + 200_000)
    assert d.halt == [
        {"from_ms": DAY, "to_ms": DAY + 60_000, "level": "SOFT", "hard_latch": False},
        {"from_ms": DAY + 60_000, "to_ms": DAY + 120_000, "level": "NONE", "hard_latch": False},
        {"from_ms": DAY + 120_000, "to_ms": None, "level": "HARD", "hard_latch": True}]


def test_stream_rows_from_each_source_rule_and_not_observed_is_never_filled_in() -> None:
    d = DayModel(DAY)
    rows = [{"adapter_id": "stonex", "stream": "orders", "conn_lost_at": None, "reconciled_at": DAY + 600_000},
            {"adapter_id": "stonex", "stream": "prices", "conn_lost_at": DAY + 900_000, "reconciled_at": None}]
    gaps = [{"adapter_id": "stonex", "gap_start_ms": DAY + 100_000, "gap_end_ms": DAY + 200_000}]
    d.seed_streams(rows, gaps, {1: DAY + 900_000})
    orders = d.streams["stonex"]["orders"]
    assert [s["state"] for s in orders] == ["not_observed", "down", "not_observed", "up"]
    prices = d.streams["stonex"]["prices"]
    assert prices[-1] == {"from_ms": DAY + 900_000, "to_ms": None, "state": "fenced"}
    d.stream_event("stonex", "prices", DAY + 950_000, "up")
    assert d.streams["stonex"]["prices"][-1]["state"] == "up"
    assert all(s["state"] in ("not_observed", "down", "fenced", "up") for s in prices)


def test_engine_row_running_unknown_and_off() -> None:
    d = DayModel(DAY)
    runs = [{"run_id": "a", "started_at_ms": DAY + 1000, "last_seen_ms": DAY + 5000, "stopped_at_ms": None},
            {"run_id": "b", "started_at_ms": DAY + 9000, "last_seen_ms": DAY + 9000, "stopped_at_ms": None}]
    d.set_engine(runs, DAY + 20_000, True)
    assert [s["state"] for s in d.engine] == ["engine_off", "running", "unknown", "running"]
    d.set_engine(runs, DAY + 20_000, False)
    assert d.engine[-1]["state"] == "unknown"


def test_buckets_count_by_class_and_are_marked_partial_before_the_backfill_limit() -> None:
    d = DayModel(DAY)
    for sev, hk in (("info", False), ("warn", False), ("alert", False), ("debug", True)):
        d.count(Event(kind="engine.responding", slots={}, at_ms=DAY + 5, clock="server", detected_ms=DAY + 5,
                      severity=sev, housekeeping=hk))
    b = d.buckets[0]
    assert (b["activity"], b["warn"], b["alert"], b["housekeeping"]) == (1, 1, 1, 1)
    d.mark_partial(DAY + 2 * BUCKET_MS)
    assert d.buckets[0]["partial"] and d.buckets[1]["partial"] and not d.buckets[2]["partial"]
    guard_payload(d.to_json(DAY + 10))


def test_series_keeps_the_minute_maximum_and_60_points() -> None:
    s = Series()
    assert s.sample(DAY, {"sweep_pass_age_s": 1, "alerts_waiting": 0, "dlq_queued": 0, "open_breaches": 0})
    assert not s.sample(DAY + 10_000, {"sweep_pass_age_s": 5, "alerts_waiting": 0, "dlq_queued": 0,
                                       "open_breaches": 0})
    assert s.points["sweep_pass_age_s"][-1] == [DAY - DAY % 60_000, 5]
    for i in range(100):
        s.sample(DAY + (i + 1) * 60_000, {"sweep_pass_age_s": None, "alerts_waiting": i, "dlq_queued": 0,
                                          "open_breaches": 0})
    assert len(s.points["alerts_waiting"]) == 60
    guard_payload(s.to_json())


def test_open_now_folds_stonex_children_into_the_position_row() -> None:
    idx = StoryIndex()
    hub = Hub(history=100, index=idx, day=DayModel(DAY), series=Series())
    n = Narrator(idx, hub.emit, hub.merge)
    positions = [{"adapter_id": "stonex", "position_id": "100", "account_id": "a", "symbol": "EURUSD",
                  "side": "SELL", "entry_level": "1.09903", "stop_level": None, "target_level": None,
                  "opened_by_client_order_id": None, "committed_at_ms": DAY}]
    orders = [{"adapter_id": "stonex", "broker_order_id": "101", "symbol": "EURUSD", "side": "BUY",
               "status": "WORKING", "entry_level": "1.10203", "stop_level": None, "target_level": None,
               "is_reduce_only": True, "linked_position_id": "100", "leg_kind": "STOP"},
              {"adapter_id": "stonex", "broker_order_id": "102", "symbol": "EURUSD", "side": "BUY",
               "status": "WORKING", "entry_level": "1.09303", "stop_level": None, "target_level": None,
               "is_reduce_only": True, "linked_position_id": "100", "leg_kind": "LIMIT"}]
    n.seed_open(orders, positions, DAY)
    rows = open_now(n, orders, positions)
    assert len(rows) == 1
    assert (rows[0]["entry"], rows[0]["stop"], rows[0]["take_profit"]) == ("1.09903", "1.10203", "1.09303")
    assert rows[0]["story"] == "pos:stonex:100"
    guard_payload({"open_now": rows})


def test_default_timing_is_pinned_to_the_sweep_config() -> None:
    from src.oversee.config import SweepConfig
    cfg = SweepConfig()
    assert DEFAULT_TIMING == {"escalate_after_ms": cfg.escalate_after_ms, "cool_down_ms": cfg.cool_down_ms,
                              "unknown_grace_ms": cfg.unknown_grace_ms}
