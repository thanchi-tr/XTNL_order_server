"""IMPLEMENTATION_PLAN 7.4: the dashboard checks Redis freshness against the latest leader heartbeat's epoch
(operations.md §9 Dashboard). The leader POSTs a copy of its dead-man heartbeat to the dashboard's listener; no
fresh heartbeat (none configured, none received, a dead leader's beats stopped) means every live value is stale.
No network beyond loopback sockets the tests open themselves."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from src.cache import keys as K
from src.dashboard import heartbeat as H
from src.dashboard import views as V
from src.runtime.deadman import AdapterBeat, DeadmanPayload, DeadmanSender, HttpsDeadmanTransport
from tests.dashboard.test_dashboard import NOW, beats, scripted
from tests.db.conftest import FakeSource

TOKEN = "dashboard-heartbeat-token-0123456789"


def payload(epoch: str | None = "42", sent: int = NOW, node: str = "node-a") -> DeadmanPayload:
    return DeadmanPayload(node=node, epoch=epoch, sent_at_ms=sent, gate_state="OPEN", halt_level="NONE",
                          adapters={"mock": AdapterBeat("STREAMING", True, None)},
                          last_committed_broker_event_age_ms=10, oldest_unacked_feed_age_ms=None,
                          oldest_risksweep_pass_age_ms=500, calendar_version=3,
                          oldest_undelivered_notification_age_ms=None, sm8_state="READY", sm5_state="NONE",
                          spill_bytes=0, degraded=[])


async def leader_redis() -> fakeredis.FakeAsyncRedis:
    r = fakeredis.FakeAsyncRedis()
    await r.set(K.BOOT_ID, "run-y:42")
    await r.hset(K.price_key("pepperstone", "EURUSD"), mapping={"bid": "1.1", "ask": "1.2", "ts": NOW - 1_000})
    return r


def views_with(board: H.HeartbeatBoard | None, r: Any) -> V.DashboardViews:
    conn = scripted()                                         # PostgreSQL: term 42 open, engine_epoch 42
    conn.returns_all(V.SQL_OPEN_SYMBOLS, [("pepperstone", "EURUSD")])
    return V.DashboardViews(FakeSource(conn), {"node-a": r}, heartbeats=board)


async def test_without_a_heartbeat_listener_every_live_value_is_stale() -> None:
    """The production entrypoint's board without a heartbeat file (`heartbeat_board(None)`) is unconfigured."""
    from src.dashboard.__main__ import heartbeat_board
    r = await leader_redis()
    try:
        views = views_with(heartbeat_board(None), r)
        live = await views.live(await views.leader(), NOW)
        assert live["stale"] and live["prices"] == [] and live["source"] is None
        assert "no leader heartbeat listener configured" in live["reason"]
        unconfigured = views_with(H.HeartbeatBoard(), r)            # listening, nothing received yet
        live = await unconfigured.live(await unconfigured.leader(), NOW)
        assert live["stale"] and "no leader heartbeat received yet" in live["reason"]
    finally:
        await r.aclose()


async def test_a_dead_leaders_redis_is_stale_once_its_heartbeats_stop_although_its_term_is_open() -> None:
    """The divergence the critics found: PostgreSQL still shows the dead leader's term open and its Redis carries
    that epoch, but its heartbeats stopped more than max_age_ms ago."""
    r = await leader_redis()
    try:
        fresh = views_with(beats("42", at=NOW - 59_000), r)
        live = await fresh.live(await fresh.leader(), NOW)
        assert not live["stale"] and live["source"] == "node-a" and live["epoch"] == 42
        assert live["heartbeat"] == {"node": "node-a", "epoch": "42", "age_ms": 59_000}
        stopped = views_with(beats("42", at=NOW - 61_000), r)
        live = await stopped.live(await stopped.leader(), NOW)
        assert live["stale"] and live["prices"] == [] and "61 s old" in live["reason"]
    finally:
        await r.aclose()


async def test_a_heartbeat_epoch_postgresql_does_not_confirm_or_no_epoch_is_stale() -> None:
    r = await leader_redis()
    try:
        other = views_with(beats("41"), r)                         # PostgreSQL's current term is 42
        live = await other.live(await other.leader(), NOW)
        assert live["stale"] and live["epoch"] is None and "not the current leader term" in live["reason"]
        none = views_with(beats(None), r)
        live = await none.live(await none.leader(), NOW)
        assert live["stale"] and "carries no epoch" in live["reason"]
    finally:
        await r.aclose()


def test_the_board_keeps_the_newest_beat_and_takes_any_once_the_held_one_is_stale() -> None:
    clock = [NOW]
    board = H.HeartbeatBoard(clock_ms=lambda: clock[0], max_age_ms=60_000)
    assert board.offer("node-a", "42", NOW)
    assert not board.offer("node-a", "41", NOW - 15_000)            # a delayed older beat never replaces it
    assert board.latest() is not None and board.latest().epoch == "42"  # type: ignore[union-attr]
    clock[0] = NOW + 61_000
    assert board.offer("node-b", "43", NOW - 1)                     # held one stale: any beat is newer news
    beat, why = board.current(clock[0])
    assert beat is not None and beat.epoch_revision == 43 and why == ""


def test_heartbeat_settings_file(tmp_path: Path) -> None:
    assert H.HeartbeatListenSettings.from_file(tmp_path / "absent.json") is None
    f = tmp_path / "heartbeat.json"
    f.write_text(json.dumps({"listen": "10.0.9.10:8444", "token": TOKEN, "tls_cert": "/c", "tls_key": "/k"}))
    s = H.HeartbeatListenSettings.from_file(f)
    assert s is not None and (s.host, s.port, s.path, s.max_age_ms, s.tls) == (
        "10.0.9.10", 8444, "/xtnl/heartbeat", 60_000, True)
    assert TOKEN not in repr(s)
    bad = [({"listen": "10.0.9.10:8444", "token": TOKEN}, "plain http only on loopback"),
           ({"listen": "127.0.0.1:8444", "token": "short"}, "at least"),
           ({"listen": "127.0.0.1:8444", "token": TOKEN, "tls_cert": "/c"}, "go together"),
           ({"listen": "127.0.0.1:8444", "token": TOKEN, "max_age_ms": 1}, "max_age_ms"),
           ({"listen": "127.0.0.1:8444", "token": TOKEN, "extra": 1}, "unknown"),
           ([], "JSON object")]
    for doc, why in bad:
        f.write_text(json.dumps(doc))
        with pytest.raises(H.HeartbeatConfigError, match=why) as err:
            H.HeartbeatListenSettings.from_file(f)
        assert TOKEN not in str(err.value)


def test_main_check_config_refuses_a_bad_heartbeat_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.dashboard.__main__ as entry
    monkeypatch.setattr(entry, "configure_logging", lambda *a, **k: "text")
    monkeypatch.setenv("XTNL_DASHBOARD_DSN", "postgresql://d:pw@h/x")
    f = tmp_path / "heartbeat.json"
    f.write_text("{not json")
    assert entry.main(["--check-config", "--heartbeat-file", str(f)]) == 78
    f.write_text(json.dumps({"listen": "127.0.0.1:8444", "token": TOKEN}))
    assert entry.main(["--check-config", "--heartbeat-file", str(f)]) == 0


async def _request(port: int, raw: bytes) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    out = await reader.read()
    writer.close()
    return out


async def test_the_listener_takes_only_authorised_well_formed_newer_beats() -> None:
    clock = [NOW]
    board = H.HeartbeatBoard(clock_ms=lambda: clock[0])
    listener = H.HeartbeatListener(H.HeartbeatListenSettings("127.0.0.1", 0, TOKEN), board)
    server = await listener.start()
    port = server.sockets[0].getsockname()[1]
    try:
        body = payload().to_json().encode()

        def post(token: str, data: bytes, path: str = "/xtnl/heartbeat") -> bytes:
            return (f"POST {path} HTTP/1.1\r\nAuthorization: Bearer {token}\r\nContent-Length: {len(data)}\r\n\r\n"
                    .encode() + data)
        assert (await _request(port, post("wrong-token-wrong-token", body))).startswith(b"HTTP/1.1 401")
        assert board.latest() is None
        assert (await _request(port, post(TOKEN, b"{}"))).startswith(b"HTTP/1.1 400")
        assert (await _request(port, post(TOKEN, body, "/other"))).startswith(b"HTTP/1.1 404")
        assert (await _request(port, b"GET /xtnl/heartbeat HTTP/1.1\r\n\r\n")).startswith(b"HTTP/1.1 405")
        big = f"POST /xtnl/heartbeat HTTP/1.1\r\nAuthorization: Bearer {TOKEN}\r\nContent-Length: 70000\r\n\r\n"
        assert (await _request(port, big.encode())).startswith(b"HTTP/1.1 413")
        assert (await _request(port, post(TOKEN, body))).startswith(b"HTTP/1.1 204")
        beat = board.latest()
        assert beat is not None and (beat.node, beat.epoch, beat.sent_at_ms) == ("node-a", "42", NOW)
        older = payload(sent=NOW - 15_000).to_json().encode()
        assert (await _request(port, post(TOKEN, older))).startswith(b"HTTP/1.1 409")
    finally:
        server.close()
        await server.wait_closed()


class _Src:
    """A minimal DeadmanSource for the sender (values do not matter here)."""

    def __getattr__(self, name: str) -> Any:
        values: dict[str, Any] = {"node_id": "node-a", "epoch_label": "42", "adapter_ids": (),
                                  "risksweep_pass_ms": {}, "degraded": ()}
        return lambda *a: values.get(name)


async def test_the_engines_heartbeat_copy_reaches_the_dashboard_and_a_dead_receiver_does_not_stop_it() -> None:
    """End to end: the leader's DeadmanSender POSTs its copy to the dashboard listener (bearer token from the
    secrets source), whatever happens to the dead-man receiver POST."""
    board = H.HeartbeatBoard()
    listener = H.HeartbeatListener(H.HeartbeatListenSettings("127.0.0.1", 0, TOKEN), board)
    server = await listener.start()
    port = server.sockets[0].getsockname()[1]

    class DownReceiver:
        host = "receiver"

        async def post(self, p: DeadmanPayload) -> None:
            from src.runtime.deadman import DeadmanError
            raise DeadmanError("ConnectError")

        async def aclose(self) -> None:
            return None
    copy = HttpsDeadmanTransport(f"http://127.0.0.1:{port}/xtnl/heartbeat", token_source=lambda: TOKEN)
    sender = DeadmanSender(None, _Src(), lease_valid=lambda: True, is_leader=lambda: True,
                           clock_ms=lambda: NOW, transport=DownReceiver(), copies=[copy])
    try:
        assert await sender.send_once() is False                  # the dead-man POST failed ...
        assert sender.failures == 1 and sender.copies_sent == 1     # ... the copy still went out
        beat = board.latest()
        assert beat is not None and beat.epoch == "42" and beat.node == "node-a"
    finally:
        await copy.aclose()
        server.close()
        await server.wait_closed()
