"""PatroniRestControl over httpx.MockTransport (no network)."""
from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from src.integrity.ports import PatroniControl
from src.runtime.patroni import (
    PatroniError,
    PatroniMember,
    PatroniRestControl,
    default_endpoints,
    dsn_for_member,
    dsn_hosts,
    parse_members,
)

A = "https://10.0.1.10:8008"
B = "https://10.0.2.10:8008"


class FakePatroni:
    """Two members; POST /switchover swaps the roles; POST /reinitialize is recorded."""

    def __init__(self) -> None:
        self.primary, self.standby = "pg-a", "pg-b"
        self.standby_lag: Any = 0
        self.standby_state = "streaming"
        self.posts: list[tuple[str, dict[str, Any]]] = []
        self.down: set[str] = set()
        self.switch_status = 200

    def members(self) -> dict[str, Any]:
        def m(name: str, role: str, state: str, lag: Any) -> dict[str, Any]:
            host = "10.0.1.10" if name == "pg-a" else "10.0.2.10"
            return {"name": name, "role": role, "state": state, "host": host, "port": 5432,
                    "api_url": f"https://{host}:8008/patroni", "timeline": 3, "lag": lag}
        return {"members": [m(self.primary, "leader", "running", None),
                            m(self.standby, "sync_standby", self.standby_state, self.standby_lag)]}

    def handler(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host in self.down:
            raise httpx.ConnectError("down", request=request)
        if request.method == "GET" and request.url.path == "/cluster":
            return httpx.Response(200, json=self.members())
        body = json.loads(request.content or b"{}")
        self.posts.append((f"{host}{request.url.path}", body))
        if request.url.path == "/switchover":
            if self.switch_status != 200:
                return httpx.Response(self.switch_status, text="nope")
            assert body == {"leader": self.primary, "candidate": self.standby}
            self.primary, self.standby = self.standby, self.primary
            return httpx.Response(200, text="Successfully switched over")
        if request.url.path == "/reinitialize":
            return httpx.Response(200, text="reinitialize started")
        return httpx.Response(404)


def control(fake: FakePatroni, **kw: Any) -> tuple[PatroniRestControl, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=httpx.MockTransport(fake.handler))
    return PatroniRestControl([A, B], client=client, poll_s=0.0, **kw), client


async def ok_check(member: PatroniMember) -> bool:
    return member.name == "pg-b"


def test_endpoint_and_dsn_helpers() -> None:
    dsn = "postgresql://u:pw@10.0.1.10:5432,10.0.2.10:5432/xtnl?target_session_attrs=read-write&sslmode=verify-full"
    assert dsn_hosts(dsn) == ["10.0.1.10", "10.0.2.10"]
    assert default_endpoints(dsn) == [A, B]
    member = dsn_for_member(dsn, "10.0.2.10", 5433)
    assert member.startswith("postgresql://u:pw@10.0.2.10:5433/xtnl?")
    assert "target_session_attrs=any" in member and "sslmode=verify-full" in member
    assert dsn_hosts("postgresql://db.local/xtnl") == ["db.local"]
    with pytest.raises(PatroniError):
        parse_members({"nope": 1})


async def test_reads_primary_and_standby() -> None:
    fake = FakePatroni()
    p, client = control(fake, standby_check=ok_check)
    try:
        assert await p.primary_member() == "pg-a"
        sb = await p.sync_standby()
        assert sb is not None and sb.name == "pg-b" and sb.is_sync_standby and sb.port == 5432
        assert await p.standby_passes_check()
        fake.standby_lag = 50 << 20
        assert not await p.standby_passes_check()
        fake.standby_lag, fake.standby_state = 0, "stopped"
        assert not await p.standby_passes_check()
    finally:
        await client.aclose()


async def test_standby_unverifiable_without_a_check() -> None:
    p, client = control(FakePatroni())
    try:
        assert not await p.standby_passes_check()
    finally:
        await client.aclose()


async def test_fails_over_to_the_next_endpoint_and_raises_when_none_answers() -> None:
    fake = FakePatroni()
    p, client = control(fake)
    try:
        fake.down = {"10.0.1.10"}
        assert await p.primary_member() == "pg-a"
        fake.down = {"10.0.1.10", "10.0.2.10"}
        with pytest.raises(PatroniError):
            await p.primary_member()
    finally:
        await client.aclose()


async def test_switchover_and_reinit_only_from_the_incident_path() -> None:
    fake = FakePatroni()
    p, client = control(fake, standby_check=ok_check)
    try:
        with pytest.raises(PermissionError):
            await p.switchover()
        with pytest.raises(PermissionError):
            await p.reinit("pg-a")
        assert fake.posts == []
        incident = {"active": False}
        guarded: PatroniControl = p.with_incident_guard(lambda: incident["active"])
        with pytest.raises(PermissionError):
            await guarded.switchover()
        incident["active"] = True
        assert await guarded.switchover() == "pg-b"
        await guarded.reinit("pg-a")
        assert fake.posts[0] == ("10.0.1.10/switchover", {"leader": "pg-a", "candidate": "pg-b"})
        assert fake.posts[1] == ("10.0.1.10/reinitialize", {"force": True})
        with pytest.raises(PatroniError):
            await guarded.reinit("pg-zz")
    finally:
        await client.aclose()


async def test_switchover_refusal_raises() -> None:
    fake = FakePatroni()
    fake.switch_status = 401
    p, client = control(fake, incident_path=lambda: True)
    try:
        with pytest.raises(PatroniError, match="401"):
            await p.switchover()
        assert len(fake.posts) == 1           # an auth refusal is not retried on the other member
    finally:
        await client.aclose()


async def test_switchover_needs_a_healthy_sync_standby() -> None:
    fake = FakePatroni()
    fake.standby_state = "stopped"
    p, client = control(fake, incident_path=lambda: True)
    try:
        with pytest.raises(PatroniError, match="sync standby"):
            await p.switchover()
    finally:
        await client.aclose()


async def test_switchover_not_confirmed_times_out() -> None:
    fake = FakePatroni()
    original = fake.handler

    def no_swap(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/switchover":
            return httpx.Response(200, text="scheduled")
        return original(request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(no_swap))
    p = PatroniRestControl([A], client=client, poll_s=0.0, switchover_wait_s=0.0, incident_path=lambda: True)
    try:
        with pytest.raises(PatroniError, match="not confirmed"):
            await p.switchover()
    finally:
        await client.aclose()


async def test_rows_pass_check_over_the_integrity_store_sql() -> None:
    from src.db.integrity_store import PgIntegrityDb
    from src.runtime.patroni import rows_pass_check
    from tests.db.integrity_emulator import IntegrityEmulator
    from tests.integrity.fakes import T0, ManualClock, order

    emu = IntegrityEmulator(ManualClock())
    for i in range(7):
        emu.put(order(f"B{i}"), T0)
    assert await rows_pass_check(PgIntegrityDb(emu), page=3)
    emu.put(order("B9"), T0, checksum="0" * 64)
    assert not await rows_pass_check(PgIntegrityDb(emu), page=3)
    row = list(emu.put(order("B9"), T0))
    row[10] = None
    emu.t.current["orders_state"]["order:stonex:B9"] = tuple(row)
    assert not await rows_pass_check(PgIntegrityDb(emu), page=3)
