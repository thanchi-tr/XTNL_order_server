"""Home-server plan W1: the engine alerts when the dead-man receiver is not acknowledging.

Gauges from the sender's counters, the episode page (once after the threshold, never before, never off the leader,
never without a URL, an info page on recovery), the bounded threshold setting and the start-up error for a URL
without its bearer token. No network: a scripted transport stands in for the receiver.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
from pathlib import Path
from typing import Any

import pytest

from src.observability.metrics import CONTRACT, MetricsRegistry
from src.runtime.deadman import (
    ACK_BACK_TITLE, ACK_LOST_TITLE, ACK_RUNBOOK, ALERT_AFTER_VAR, DEFAULT_ALERT_AFTER_S, DeadmanError, DeadmanPayload,
    DeadmanSender, ack_episode_keys, alert_after_s_from_env, deadman_samples, deadman_token_problem,
)
from src.runtime.wiring import EngineHooks, WiringError, build_engine, load_settings, log_config_issues
from tests.runtime.fakes import FakeClock, FakeEtcd
from tests.runtime.test_wiring import MOCK, cfg

T0 = 1_800_000_000_000
BEAT_MS = 15_000
HOST = "vps-a.example.invalid"


class _Src:
    def __getattr__(self, name: str) -> Any:
        values: dict[str, Any] = {"node_id": "node-a", "epoch_label": "91", "adapter_ids": (),
                                  "risksweep_pass_ms": {}, "degraded": ()}
        return lambda *a: values.get(name)


class _Receiver:
    """Answers each POST from `up` (True: 2xx, False: a refused / failed beat)."""

    host = HOST

    def __init__(self) -> None:
        self.up = True
        self.posts = 0

    async def post(self, p: DeadmanPayload) -> None:
        self.posts += 1
        if not self.up:
            raise DeadmanError("HTTP 401")

    async def aclose(self) -> None:
        return None


@dataclasses.dataclass
class Rig:
    sender: DeadmanSender
    receiver: _Receiver
    pages: list[tuple[str, str, str]]
    clock: list[int]
    leader: list[bool]
    lease: list[bool]

    async def beat(self, n: int = 1) -> None:
        for _ in range(n):
            await self.sender.send_once()
            self.clock[0] += BEAT_MS


def rig(*, alert_after_s: float = DEFAULT_ALERT_AFTER_S, page: bool = True) -> Rig:
    clock = [T0]
    leader = [True]
    lease = [True]
    pages: list[tuple[str, str, str]] = []
    receiver = _Receiver()
    sender = DeadmanSender(None, _Src(), lease_valid=lambda: lease[0], is_leader=lambda: leader[0],
                           clock_ms=lambda: clock[0], monotonic=lambda: clock[0] / 1000, transport=receiver,
                           page=(lambda sev, title, body: pages.append((sev, title, body))) if page else None,
                           alert_after_s=alert_after_s)
    return Rig(sender, receiver, pages, clock, leader, lease)


# ================================================================================================ setting
def test_alert_after_defaults_to_300_s_and_is_bounded() -> None:
    assert alert_after_s_from_env({}) == DEFAULT_ALERT_AFTER_S == 300
    assert alert_after_s_from_env({ALERT_AFTER_VAR: " 600 "}) == 600
    assert alert_after_s_from_env({ALERT_AFTER_VAR: "60"}) == 60
    assert alert_after_s_from_env({ALERT_AFTER_VAR: "3600"}) == 3600
    for bad in ("59", "3601", "0", "-5", "5m", "300.5"):
        with pytest.raises(ValueError, match=ALERT_AFTER_VAR):
            alert_after_s_from_env({ALERT_AFTER_VAR: bad})


@pytest.mark.parametrize("value", ["30", "abc"])
def test_a_bad_alert_after_refuses_to_start(tmp_path: Path, value: str) -> None:
    env = {**MOCK, ALERT_AFTER_VAR: value}
    with pytest.raises(WiringError, match=ALERT_AFTER_VAR):
        build_engine(cfg(env, tmp_path), environ=env,
                     hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))


def test_build_engine_hands_the_threshold_to_the_session_context(tmp_path: Path) -> None:
    env = {**MOCK, ALERT_AFTER_VAR: "120"}
    engine = build_engine(cfg(env, tmp_path), environ=env,
                          hooks=EngineHooks(etcd=FakeEtcd(FakeClock()), require_keeper_thread=False))
    try:
        assert engine.context.deadman_alert_after_s == 120
    finally:
        asyncio.run(engine.aclose())


# ================================================================================================ episode page
async def test_no_page_before_the_threshold() -> None:
    r = rig()
    r.receiver.up = False
    await r.beat(20)                          # 20 failures, the last one 285 s after the first
    assert r.sender.consecutive_failures == 20 and r.pages == [] and not r.sender.ack_paged


async def test_one_high_page_once_the_streak_reaches_the_threshold() -> None:
    r = rig()
    await r.beat()                            # one acknowledged beat first
    r.receiver.up = False
    await r.beat(21)                          # the 21st failure is 300 s after the first
    assert len(r.pages) == 1
    sev, title, body = r.pages[0]
    assert sev == "high" and title == ACK_LOST_TITLE == "dead-man receiver not acknowledging"
    assert body == f"receiver {HOST}: 21 failed beats in a row over 300 s ({ACK_RUNBOOK})"
    await r.beat(40)                          # still failing: the episode is paged once
    assert len(r.pages) == 1 and r.sender.ack_episodes == 1


async def test_an_info_page_when_beats_are_acknowledged_again_and_a_new_episode_pages_again() -> None:
    r = rig()
    r.receiver.up = False
    await r.beat(25)
    assert [p[0] for p in r.pages] == ["high"]
    r.receiver.up = True
    await r.beat()
    assert len(r.pages) == 2
    sev, title, body = r.pages[1]
    assert sev == "info" and title == ACK_BACK_TITLE
    assert body == f"receiver {HOST}: acknowledged again after 25 failed beats over 375 s"
    await r.beat(3)
    assert len(r.pages) == 2                  # nothing more while beats are acknowledged
    r.receiver.up = False
    await r.beat(21)
    assert [p[0] for p in r.pages] == ["high", "info", "high"] and r.sender.ack_episodes == 2


async def test_a_short_streak_that_never_paged_sends_no_info_page() -> None:
    r = rig()
    r.receiver.up = False
    await r.beat(5)
    r.receiver.up = True
    await r.beat()
    assert r.pages == [] and r.sender.failing_since_ms is None


async def test_no_page_when_not_leader() -> None:
    r = rig()
    r.leader[0] = False
    r.receiver.up = False
    await r.beat(40)
    assert r.receiver.posts == 0 and r.sender.consecutive_failures == 0 and r.pages == []
    assert r.sender.skipped == 40


async def test_a_lease_lost_at_the_page_moment_defers_the_page_to_the_next_valid_beat() -> None:
    r = rig()
    r.receiver.up = False
    await r.beat(20)
    calls = [0]

    def lease() -> bool:            # valid for the two checks of send_once, gone at the page check
        calls[0] += 1
        return calls[0] <= 2
    r.sender._lease_valid = lease
    await r.beat()
    assert r.sender.consecutive_failures == 21 and r.pages == [] and not r.sender.ack_paged
    r.sender._lease_valid = lambda: True
    await r.beat()
    assert [p[0] for p in r.pages] == ["high"]
    assert r.pages[0][2] == f"receiver {HOST}: 22 failed beats in a row over 315 s ({ACK_RUNBOOK})"


async def test_the_threshold_follows_the_setting() -> None:
    r = rig(alert_after_s=60)
    r.receiver.up = False
    await r.beat(4)                           # 45 s
    assert r.pages == []
    await r.beat()                            # 60 s
    assert [p[0] for p in r.pages] == ["high"]


async def test_a_raising_page_path_never_stops_the_beat_and_is_retried(caplog: pytest.LogCaptureFixture) -> None:
    r = rig()
    good = r.sender._page

    def boom(sev: str, title: str, body: str) -> None:
        raise RuntimeError("outbox down")
    r.sender._page = boom
    r.receiver.up = False
    with caplog.at_level(logging.WARNING, logger="src.runtime.deadman"):
        await r.beat(22)
    # nothing was queued, so the episode is not marked paged and no info page can follow on its own
    assert r.sender.consecutive_failures == 22 and not r.sender.ack_paged and r.sender.ack_episodes == 0
    assert any("page not queued: RuntimeError" in m for m in caplog.messages)
    r.sender._page = good
    await r.beat()                            # the page path works again: the next failed beat pages once
    assert [p[0] for p in r.pages] == ["high"] and r.sender.ack_paged and r.sender.ack_episodes == 1
    await r.beat(3)
    assert len(r.pages) == 1


async def test_a_page_path_without_a_lease_answers_false_and_is_retried() -> None:
    r = rig()
    queued = [False]

    def page(sev: str, title: str, body: str) -> bool:
        if queued[0]:
            r.pages.append((sev, title, body))
        return queued[0]
    r.sender._page = page
    r.receiver.up = False
    await r.beat(25)
    assert r.pages == [] and not r.sender.ack_paged
    queued[0] = True
    await r.beat()
    assert [p[0] for p in r.pages] == ["high"]


async def test_without_a_page_callback_only_the_log_lines_remain() -> None:
    r = rig(page=False)
    r.receiver.up = False
    await r.beat(30)
    assert not r.sender.ack_paged and r.sender.consecutive_failures == 30


async def test_page_text_has_no_amount_or_url_path() -> None:
    r = rig()
    r.receiver.up = False
    await r.beat(21)
    r.receiver.up = True
    await r.beat()
    for _sev, title, body in r.pages:
        text = title + body
        assert "AUD" not in text and "$" not in text and "/" not in text and "token" not in text.lower()


# ================================================================================================ gauges
async def test_gauges_follow_the_sender_counters() -> None:
    r = rig()
    got = {s.name: s.value for s in r.sender.samples(r.clock[0])}
    assert got == {"xtnl_deadman_consecutive_failures": 0.0}   # no acknowledged beat yet: no age (absent)
    await r.beat()                                              # acknowledged at T0
    r.receiver.up = False
    await r.beat(3)
    got = {s.name: s.value for s in r.sender.samples(r.clock[0])}
    assert got == {"xtnl_deadman_consecutive_failures": 3.0, "xtnl_deadman_last_ok_age_seconds": 60.0}
    r.receiver.up = True
    await r.beat()
    got = {s.name: s.value for s in r.sender.samples(r.clock[0])}
    assert got == {"xtnl_deadman_consecutive_failures": 0.0, "xtnl_deadman_last_ok_age_seconds": 15.0}


def test_deadman_samples_and_the_contract() -> None:
    for name in ("xtnl_deadman_configured", "xtnl_deadman_consecutive_failures", "xtnl_deadman_last_ok_age_seconds"):
        assert CONTRACT[name] == ("gauge", ())
    assert [(s.name, s.value) for s in deadman_samples(None, configured=False, now_ms=T0)] == [
        ("xtnl_deadman_configured", 0.0)]
    r = rig()
    reg = MetricsRegistry("node-a")
    reg.register("deadman", lambda: deadman_samples(r.sender, configured=True, now_ms=T0))
    text = reg.render()
    assert 'xtnl_deadman_configured{node="node-a"} 1' in text
    assert 'xtnl_deadman_consecutive_failures{node="node-a"} 0' in text
    assert "xtnl_deadman_last_ok_age_seconds" not in text and reg.dropped == 0


# ================================================================================================ token check
def test_url_without_token_is_a_start_up_error_naming_the_variable(caplog: pytest.LogCaptureFixture) -> None:
    assert deadman_token_problem(None, None) is None
    assert deadman_token_problem("https://r.example.invalid/beat", "tok-0123456789abcdef") is None
    for empty in (None, "", "   "):
        problem = deadman_token_problem("https://r.example.invalid/beat", empty)
        assert problem is not None and "XTNL_DEADMAN_TOKEN" in problem and "401" in problem
    env = {k: v for k, v in MOCK.items() if k != "XTNL_DEADMAN_TOKEN"}
    settings = load_settings(cfg(env), env)                       # the engine still starts
    assert len(settings.errors) == 1 and "XTNL_DEADMAN_TOKEN" in settings.errors[0]
    assert "example.invalid" not in settings.errors[0]            # names only, never the URL
    with caplog.at_level(logging.ERROR, logger="src.runtime.wiring"):
        log_config_issues(settings)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "XTNL_DEADMAN_TOKEN" in errors[0].getMessage()


def test_no_token_error_with_a_token_or_without_a_url() -> None:
    assert load_settings(cfg(MOCK), MOCK).errors == ()
    env = {k: v for k, v in MOCK.items() if k not in ("XTNL_DEADMAN_URL", "XTNL_DEADMAN_TOKEN")}
    env["XTNL_SINGLE_PC"] = "1"
    assert load_settings(cfg(env), env).errors == ()


# ================================================================================================ session wiring
from src.notifications.settings import DeadmanSettings  # noqa: E402
from src.runtime.supervisor import TaskSupervisor  # noqa: E402
from tests.runtime.test_session_56 import Built, built  # noqa: E402,F401  (the session fixture)

URL = "https://vps-a.example.invalid/xtnl/heartbeat"


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def test_the_leader_session_pages_through_its_outbox_once_per_episode(built: Built) -> None:
    s = built.session
    s.ctx = dataclasses.replace(s.ctx, deadman=DeadmanSettings(URL, "tok-0123456789abcdef"), deadman_alert_after_s=60)
    s.init_deps(TaskSupervisor([]))
    s._add = lambda spec: None                # type: ignore[method-assign]  (no background beat: the test drives it)
    s._start_notifications()
    sender = s.deadman
    assert sender is not None and sender._page == s.queue_page_durable and sender._alert_after_ms == 60_000
    assert sender._episodes is s.ports.dedupe            # the episode is kept in notification_dedupe
    receiver = _Receiver()
    receiver.up = False
    sender._transport = receiver
    clock = [T0]
    sender._clock_ms = lambda: clock[0]
    sender._mono = lambda: clock[0] / 1000
    for _ in range(8):
        await sender.send_once()
        clock[0] += BEAT_MS
    await _settle()
    rows = [r for r in s.ports.outbox.t.outbox if r.title.startswith("dead-man receiver")]
    assert [(r.severity, r.title) for r in rows] == [("high", ACK_LOST_TITLE)]
    assert rows[0].body == f"receiver {HOST}: 5 failed beats in a row over 60 s ({ACK_RUNBOOK})"
    got = {x.name: x.value for x in s.metrics_samples(leader=True) if x.name.startswith("xtnl_deadman_")}
    assert got == {"xtnl_deadman_configured": 1.0, "xtnl_deadman_consecutive_failures": 8.0}
    assert not {x.name for x in s.metrics_samples(leader=False)} & set(got)       # leader-only series
    receiver.up = True
    await sender.send_once()
    await _settle()
    rows = [r for r in s.ports.outbox.t.outbox if r.title.startswith("dead-man receiver")]
    assert [(r.severity, r.title) for r in rows] == [("high", ACK_LOST_TITLE), ("info", ACK_BACK_TITLE)]
    lost, back = ack_episode_keys(HOST)
    assert s.ports.dedupe.t.dedupe[back] >= s.ports.dedupe.t.dedupe[lost]   # type: ignore[attr-defined]


async def test_a_session_whose_lease_is_gone_queues_no_page(built: Built) -> None:
    s = built.session
    s.ctx = dataclasses.replace(s.ctx, deadman=DeadmanSettings(URL, "tok-0123456789abcdef"), deadman_alert_after_s=60)
    s.init_deps(TaskSupervisor([]))
    s._add = lambda spec: None                # type: ignore[method-assign]  (no background beat: the test drives it)
    s._start_notifications()
    sender = s.deadman
    assert sender is not None
    receiver = _Receiver()
    receiver.up = False
    sender._transport = receiver
    s.ctx.lease.valid = False                 # type: ignore[attr-defined]  (FakeLease: not the leader any more)
    for _ in range(10):
        await sender.send_once()
    await _settle()
    assert receiver.posts == 0 and not s.ports.outbox.t.outbox


async def test_without_a_url_there_is_no_sender_and_no_page(built: Built) -> None:
    s = built.session
    assert s.ctx.deadman.url is None
    s.init_deps(TaskSupervisor([]))
    s._add = lambda spec: None                # type: ignore[method-assign]  (no background beat: the test drives it)
    s._start_notifications()
    assert s.deadman is None
    got = {x.name: x.value for x in s.metrics_samples(leader=True) if x.name.startswith("xtnl_deadman_")}
    assert got == {"xtnl_deadman_configured": 0.0}
