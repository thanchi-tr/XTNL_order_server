"""Dead-man receiver paging rules (operations.md §9 thresholds; IMPLEMENTATION_PLAN 6.4)."""
from __future__ import annotations

import dataclasses
import json
from typing import Any

import pytest

from src.deadman_receiver.evaluate import (PAYLOAD_FIELDS, BeatError, Evaluator, Thresholds, parse_beat)
from src.runtime.deadman import AdapterBeat, DeadmanPayload

T0 = 1_700_000_000_000


def payload(**kw: Any) -> DeadmanPayload:
    base: dict[str, Any] = dict(
        node="node-a", epoch="7", sent_at_ms=T0, gate_state="OPEN", halt_level="NONE",
        adapters={"stonex": AdapterBeat("STREAMING", True, None)}, last_committed_broker_event_age_ms=500,
        oldest_unacked_feed_age_ms=None, oldest_risksweep_pass_age_ms=900, calendar_version=3,
        oldest_undelivered_notification_age_ms=None, sm8_state="READY", sm5_state="NONE", spill_bytes=0,
        degraded=[])
    base.update(kw)
    return DeadmanPayload(**base)


def body(p: DeadmanPayload) -> bytes:
    return p.to_json().encode("utf-8")


class Feed:
    """Beats every 15 s from T0, receiver clock == engine clock."""

    def __init__(self) -> None:
        self.ev = Evaluator(started_ms=T0)
        self.now = T0

    def beat(self, **kw: Any) -> None:
        self.ev.accept(parse_beat(body(payload(sent_at_ms=self.now, **kw))), self.now)

    def run(self, seconds: int, **kw: Any) -> set[str]:
        """Beat every 15 s for `seconds`, return the active keys at the end."""
        end = self.now + seconds * 1000
        while self.now < end:
            self.beat(**kw)
            self.now += 15_000
        self.now = end
        return set(self.ev.active(self.now))


def test_payload_fields_match_the_engine_sender() -> None:
    assert set(PAYLOAD_FIELDS) == {f.name for f in dataclasses.fields(DeadmanPayload)}
    assert {f.name for f in dataclasses.fields(AdapterBeat)} == {"sm4_state", "market_open", "maintenance_until_ms"}


def test_parse_round_trips_the_engine_json() -> None:
    b = parse_beat(body(payload(degraded=["outbox_relay"])))
    assert b.node == "node-a" and b.adapters["stonex"].sm4_state == "STREAMING" and b.degraded == ("outbox_relay",)


@pytest.mark.parametrize("mutate", [
    lambda d: d.pop("spill_bytes"),
    lambda d: d.update(sent_at_ms="now"),
    lambda d: d.update(adapters={"x": {"sm4_state": "STREAMING", "market_open": True}}),
    lambda d: d.update(adapters={"x": {"sm4_state": 1, "market_open": True, "maintenance_until_ms": None}}),
    lambda d: d.update(spill_bytes=True),
    lambda d: d.update(degraded="task"),
    lambda d: d.update(node=""),
])
def test_malformed_beats_are_rejected_naming_fields_only(mutate: Any) -> None:
    d = json.loads(body(payload()))
    mutate(d)
    with pytest.raises(BeatError) as info:
        parse_beat(json.dumps(d).encode())
    assert "STREAMING" not in str(info.value)


def test_non_json_and_oversized_bodies_are_rejected() -> None:
    for raw in (b"\xff\xfe", b"[]", b"{" * 10, b" " * 70_000):
        with pytest.raises(BeatError):
            parse_beat(raw)


def test_healthy_beats_page_nothing() -> None:
    f = Feed()
    assert f.run(600) == set()


def test_silence_after_60_s_and_from_receiver_start() -> None:
    f = Feed()
    assert set(f.ev.active(T0 + 60_000)) == set()
    assert "silence" in f.ev.active(T0 + 61_000)          # never heard a beat since start
    f.now = T0 + 61_000
    f.beat()
    assert set(f.ev.active(f.now + 60_000)) == set()
    assert set(f.ev.active(f.now + 60_001)) == {"silence"}


def test_adapter_not_streaming_pages_only_after_2_min_while_open() -> None:
    f = Feed()
    down = {"stonex": AdapterBeat("BACKOFF", True, None)}
    assert f.run(105, adapters=down) == set()               # 1:45 not streaming
    assert f.run(30, adapters=down) == {"adapter_not_streaming:stonex"}
    assert f.run(15) == set()                               # STREAMING again: resolved


def test_market_closed_or_maintenance_suppresses_the_adapter_page() -> None:
    f = Feed()
    assert f.run(600, adapters={"stonex": AdapterBeat("BACKOFF", False, None)}) == set()
    g = Feed()
    window = {"stonex": AdapterBeat("BACKOFF", True, T0 + 3_600_000)}
    assert g.run(600, adapters=window) == set()
    h = Feed()
    expired = {"stonex": AdapterBeat("BACKOFF", True, T0 - 1)}  # the window ended: no longer suppressed
    assert h.run(150, adapters=expired) == {"adapter_not_streaming:stonex"}


def test_null_fields_page_on_the_first_beat_that_lacks_them() -> None:
    """8.2 (operations.md §9 "pages ... when those fields are missing"): no 60 s grace any more (before, a null
    health field paged only after 60 s). A beat that has them again resolves it."""
    f = Feed()
    assert f.run(15, sm8_state="READY", gate_state=None) == {"fields_missing:gate_state"}
    assert f.run(15) == set()
    g = Feed()
    assert g.run(15, adapters={"stonex": AdapterBeat(None, None, None)}) == {"adapter_fields_missing:stonex"}
    h = Feed()
    assert h.run(15, adapters={}) == {"fields_missing:adapters"}
    for name in ("epoch", "halt_level", "calendar_version", "sm8_state", "sm5_state", "spill_bytes"):
        k = Feed()
        assert f"fields_missing:{name}" in k.run(15, **{name: None}), name


@pytest.mark.parametrize(("field", "ok", "bad", "key"), [
    ("oldest_unacked_feed_age_ms", 30_000, 30_001, "feed_lag"),
    ("oldest_risksweep_pass_age_ms", 10_000, 10_001, "risksweep_pass_age"),
    ("oldest_undelivered_notification_age_ms", 60_000, 60_001, "notification_undelivered"),
])
def test_age_thresholds(field: str, ok: int, bad: int, key: str) -> None:
    f = Feed()
    assert f.run(15, **{field: ok}) == set()
    assert key in f.run(15, **{field: bad})


def test_sm8_sm5_and_spill_page_after_60_s() -> None:
    f = Feed()
    assert f.run(45, sm8_state="BOOTSTRAPPING", sm5_state="UNKNOWN", spill_bytes=10) == set()
    assert f.run(30, sm8_state="BOOTSTRAPPING", sm5_state="UNKNOWN", spill_bytes=10) == {
        "sm8_not_ready", "sm5_unknown", "spill_nonempty"}
    assert f.run(15) == set()


def test_open_circuit_pages_at_once_per_task() -> None:
    f = Feed()
    assert f.run(15, degraded=["outbox_relay", "integrity"]) == {"circuit_open:integrity",
                                                                  "circuit_open:outbox_relay"}


def test_replayed_or_skewed_beats_are_refused() -> None:
    f = Feed()
    f.beat()
    with pytest.raises(BeatError, match="not newer"):
        f.ev.accept(parse_beat(body(payload(sent_at_ms=f.now))), f.now)
    with pytest.raises(BeatError, match="clock"):
        f.ev.accept(parse_beat(body(payload(sent_at_ms=f.now + 600_000))), f.now)
    assert "payload_invalid" in f.ev.active(f.now)
    f.now += 15_000
    f.beat()                                                 # a good beat clears it
    assert "payload_invalid" not in f.ev.active(f.now)


def test_thresholds_are_the_operations_values() -> None:
    t = Thresholds()
    assert (t.silence_ms, t.not_streaming_ms, t.feed_age_ms, t.pass_age_ms, t.notification_age_ms,
            t.sm8_sm5_ms, t.spill_ms) == (60_000, 120_000, 30_000, 10_000, 60_000, 60_000, 60_000)
