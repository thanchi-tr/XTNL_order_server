"""Optional ARM fields of the dead-man beat (S08: design PR4, §B14, §C2; RT-M7, ST-12).

Engine: `arm_oldest_undelivered_notification_age_ms`, `arm_overdue_positions`, `arm_failed` (count and global flag)
default to None and are left out of the JSON while None, so the payload is byte-identical with the ARM off.
Receiver: an old payload without them still parses; a set value is validated; it pages when `arm_failed.count` > 0
or `arm_overdue_positions` > 0 (at once), or when the ARM notice age is over its threshold."""
from __future__ import annotations

import json
from typing import Any

import pytest

from src.deadman_receiver.evaluate import SOURCE_LIVE_MS, BeatError, Evaluator, Thresholds, parse_beat
from src.runtime.deadman import AdapterBeat, ArmFailedBeat, DeadmanPayload, build_payload

T0 = 1_700_000_000_000
ARM_KEYS = {"arm_failed", "arm_overdue_positions", "arm_notification_undelivered"}


def payload(**kw: Any) -> DeadmanPayload:
    base: dict[str, Any] = dict(
        node="node-a", epoch="7", sent_at_ms=T0, gate_state="OPEN", halt_level="NONE",
        adapters={"stonex": AdapterBeat("STREAMING", True, None)}, last_committed_broker_event_age_ms=500,
        oldest_unacked_feed_age_ms=None, oldest_risksweep_pass_age_ms=900, calendar_version=3,
        oldest_undelivered_notification_age_ms=None, sm8_state="READY", sm5_state="NONE", spill_bytes=0,
        degraded=[])
    base.update(kw)
    return DeadmanPayload(**base)


def raw(p: DeadmanPayload) -> dict[str, Any]:
    d: dict[str, Any] = json.loads(p.to_json())
    return d


def keys_after(p: DeadmanPayload, at_ms: int = T0, thresholds: Thresholds | None = None) -> set[str]:
    ev = Evaluator(started_ms=T0, thresholds=thresholds or Thresholds())
    ev.accept(parse_beat(p.to_json().encode()), at_ms)
    return set(ev.active(at_ms)) & ARM_KEYS


# ---- engine ------------------------------------------------------------------------------------------------
def test_unset_fields_are_left_out_so_the_payload_is_unchanged() -> None:
    d = raw(payload())
    assert not {"arm_oldest_undelivered_notification_age_ms", "arm_overdue_positions", "arm_failed"} & set(d)
    assert len(d) == 15


def test_set_fields_are_sent() -> None:
    d = raw(payload(arm_oldest_undelivered_notification_age_ms=0, arm_overdue_positions=0,
                    arm_failed=ArmFailedBeat(2, True)))
    assert d["arm_oldest_undelivered_notification_age_ms"] == 0 and d["arm_overdue_positions"] == 0
    assert d["arm_failed"] == {"count": 2, "global_failed": True, "unrecorded": 0}


class Source:
    def __init__(self, arm: bool) -> None:
        self.arm = arm

    def __getattr__(self, name: str) -> Any:
        values: dict[str, Any] = {"node_id": "n1", "epoch_label": "7", "adapter_ids": (), "risksweep_pass_ms": {},
                                  "degraded": ()}
        return lambda *a: values.get(name)

    def arm_oldest_undelivered_notification_ms(self) -> int | None:
        return T0 - 4_000 if self.arm else None

    def arm_overdue_positions(self) -> int | None:
        return 1 if self.arm else None

    def arm_failed(self) -> ArmFailedBeat | None:
        return ArmFailedBeat(0, False) if self.arm else None


def test_build_payload_reads_the_source() -> None:
    on = build_payload(Source(True), T0)
    assert on.arm_oldest_undelivered_notification_age_ms == 4_000 and on.arm_overdue_positions == 1
    assert on.arm_failed == ArmFailedBeat(0, False)
    off = build_payload(Source(False), T0)
    assert (off.arm_oldest_undelivered_notification_age_ms, off.arm_overdue_positions, off.arm_failed) == (
        None, None, None)


def test_the_session_view_returns_none_until_the_arm_is_wired() -> None:
    """S32: the getters read the session's optional view; without one (the ARM off) they are None, a raising view
    reports None (but `arm_failed` reports one global FAILED while the ARM runs: fail closed), and a wired view's
    values reach the beat."""
    from types import SimpleNamespace

    from src.runtime.session import _DeadmanView

    view = _DeadmanView(SimpleNamespace(optional_view=None))  # type: ignore[arg-type]
    assert view.arm_oldest_undelivered_notification_ms() is None
    assert view.arm_overdue_positions() is None and view.arm_failed() is None

    class Raising:
        active = False

        def arm_active(self) -> bool:
            return self.active

        def arm_oldest_undelivered_notification_ms(self) -> int | None:
            raise RuntimeError("boom")

        def arm_overdue_positions(self) -> int | None:
            raise RuntimeError("boom")

        def arm_failed(self) -> ArmFailedBeat | None:
            raise RuntimeError("boom")

    view = _DeadmanView(SimpleNamespace(optional_view=Raising()))  # type: ignore[arg-type]
    assert (view.arm_oldest_undelivered_notification_ms(), view.arm_overdue_positions(), view.arm_failed()) == (
        None, None, None)
    running = Raising()
    running.active = True                    # the ARM runs: an unreadable FAILED state is a global FAILED
    view = _DeadmanView(SimpleNamespace(optional_view=running))  # type: ignore[arg-type]
    assert view.arm_failed() == ArmFailedBeat(1, True) and view.arm_overdue_positions() is None

    class Wired:
        def arm_oldest_undelivered_notification_ms(self) -> int | None:
            return T0 - 4_000

        def arm_overdue_positions(self) -> int | None:
            return None

        def arm_failed(self) -> ArmFailedBeat | None:
            return ArmFailedBeat(2, True)

    view = _DeadmanView(SimpleNamespace(optional_view=Wired()))  # type: ignore[arg-type]
    assert view.arm_oldest_undelivered_notification_ms() == T0 - 4_000
    assert view.arm_overdue_positions() is None and view.arm_failed() == ArmFailedBeat(2, True)


# ---- receiver ----------------------------------------------------------------------------------------------
def test_an_old_payload_without_the_optional_fields_still_parses() -> None:
    d = raw(payload())
    b = parse_beat(json.dumps(d).encode())
    assert (b.arm_oldest_undelivered_notification_age_ms, b.arm_overdue_positions, b.arm_failed) == (None, None, None)
    for name in ("arm_oldest_undelivered_notification_age_ms", "arm_overdue_positions", "arm_failed"):
        b2 = parse_beat(json.dumps({**d, name: None}).encode())   # an explicit null is the same
        assert getattr(b2, name) is None


def test_set_values_round_trip() -> None:
    b = parse_beat(payload(arm_oldest_undelivered_notification_age_ms=5, arm_overdue_positions=3,
                           arm_failed=ArmFailedBeat(1, False)).to_json().encode())
    assert b.arm_oldest_undelivered_notification_age_ms == 5 and b.arm_overdue_positions == 3
    assert b.arm_failed is not None and (b.arm_failed.count, b.arm_failed.global_failed) == (1, False)


@pytest.mark.parametrize("name,value", [
    ("arm_overdue_positions", -1), ("arm_overdue_positions", "2"), ("arm_overdue_positions", True),
    ("arm_oldest_undelivered_notification_age_ms", 1.5),
    ("arm_failed", 1), ("arm_failed", {"count": 1}), ("arm_failed", {"count": -1, "global_failed": False}),
    ("arm_failed", {"count": 1, "global_failed": "yes"}), ("arm_failed", {"count": True, "global_failed": False}),
])
def test_malformed_optional_values_are_rejected_naming_fields_only(name: str, value: object) -> None:
    d = {**raw(payload()), name: value}
    with pytest.raises(BeatError) as info:
        parse_beat(json.dumps(d).encode())
    assert name.split(".")[0] in str(info.value) and "yes" not in str(info.value)


def test_arm_off_pages_nothing() -> None:
    assert keys_after(payload()) == set()
    assert keys_after(payload(arm_oldest_undelivered_notification_age_ms=0, arm_overdue_positions=0,
                              arm_failed=ArmFailedBeat(0, False))) == set()


def test_failed_scopes_page_at_once() -> None:
    ev = Evaluator(started_ms=T0)
    ev.accept(parse_beat(payload(arm_failed=ArmFailedBeat(2, True)).to_json().encode()), T0)
    cond = ev.active(T0)["arm_failed"]
    assert "2 scopes" in cond.summary and "global" in cond.detail


def test_overdue_positions_page_at_once() -> None:
    assert keys_after(payload(arm_overdue_positions=1)) == {"arm_overdue_positions"}


def test_undelivered_arm_notice_pages_over_its_threshold() -> None:
    assert Thresholds().arm_notification_age_ms == 60_000
    assert keys_after(payload(arm_oldest_undelivered_notification_age_ms=60_000)) == set()
    assert keys_after(payload(arm_oldest_undelivered_notification_age_ms=60_001)) == {"arm_notification_undelivered"}
    assert keys_after(payload(arm_oldest_undelivered_notification_age_ms=90_000),
                      thresholds=Thresholds(arm_notification_age_ms=120_000)) == set()


def test_arm_fields_never_feed_the_oversee_conditions() -> None:
    """The Oversee notification age stays its own field: an ARM backlog never raises notification_undelivered."""
    ev = Evaluator(started_ms=T0)
    ev.accept(parse_beat(payload(arm_oldest_undelivered_notification_age_ms=600_000).to_json().encode()), T0)
    assert "notification_undelivered" not in ev.active(T0)


# ---- S38c review: unrecorded FAILED episodes (the AWS leader-change gap) -------------------------------------
def _beat(at: int, *, node: str = "node-a", epoch: str = "7", failed: ArmFailedBeat | None) -> bytes:
    return payload(node=node, epoch=epoch, sent_at_ms=at, arm_failed=failed).to_json().encode()


def test_an_unrecorded_failed_episode_pages_off_node_and_clears_once_recorded() -> None:
    ev = Evaluator(started_ms=T0)
    ev.accept(parse_beat(_beat(T0, failed=ArmFailedBeat(1, False, 1))), T0)
    active = ev.active(T0)
    assert "arm_failed_unrecorded" in active and "node-a" in active["arm_failed_unrecorded"].detail
    ev.accept(parse_beat(_beat(T0 + 15_000, failed=ArmFailedBeat(1, False, 0))), T0 + 15_000)   # recorded
    assert "arm_failed_unrecorded" not in ev.active(T0 + 15_000)


def test_a_leader_change_before_the_episode_was_recorded_latches_the_page() -> None:
    """The new leader starts with no episode (count 0): the receiver keeps paging that the episode may be lost,
    until a FAILED scope is reported again or the latch bound passes."""
    th = Thresholds(arm_unrecorded_latch_ms=600_000)
    ev = Evaluator(started_ms=T0, thresholds=th)
    ev.accept(parse_beat(_beat(T0, failed=ArmFailedBeat(1, True, 1))), T0)
    ev.accept(parse_beat(_beat(T0 + 30_000, node="node-b", epoch="8", failed=ArmFailedBeat(0, False, 0))),
              T0 + 30_000)
    active = ev.active(T0 + 30_000)
    assert "arm_failed_unrecorded" in active and "leader change" in active["arm_failed_unrecorded"].summary
    # the old leader's last beat still counts until SOURCE_LIVE_MS after it (review round 5), marked as its own
    assert "(from node-a epoch 7)" in active["arm_failed"].detail
    assert "arm_failed" not in ev.active(T0 + SOURCE_LIVE_MS + 1_000)    # the new leader reports nothing
    assert "arm_failed_unrecorded" in ev.active(T0 + 30_000 + 600_000)
    ev.accept(parse_beat(_beat(T0 + 30_000 + 600_001, node="node-b", epoch="8", failed=ArmFailedBeat(0, False, 0))),
              T0 + 30_000 + 600_001)
    assert "arm_failed_unrecorded" not in ev.active(T0 + 30_000 + 600_001)   # the latch bound passed
    # a FAILED scope reported (recorded) again ends the latch at once
    ev2 = Evaluator(started_ms=T0)
    ev2.accept(parse_beat(_beat(T0, failed=ArmFailedBeat(1, True, 1))), T0)
    ev2.accept(parse_beat(_beat(T0 + 15_000, node="node-b", epoch="8", failed=ArmFailedBeat(1, True, 0))),
               T0 + 15_000)
    assert "arm_failed_unrecorded" not in ev2.active(T0 + 15_000) and "arm_failed" in ev2.active(T0 + 15_000)


def test_an_older_engine_without_the_unrecorded_key_still_parses() -> None:
    d = raw(payload(arm_failed=ArmFailedBeat(1, False)))
    del d["arm_failed"]["unrecorded"]
    beat = parse_beat(json.dumps(d).encode())
    assert beat.arm_failed is not None and beat.arm_failed.unrecorded == 0
    d["arm_failed"]["unrecorded"] = -1
    with pytest.raises(BeatError):
        parse_beat(json.dumps(d).encode())
