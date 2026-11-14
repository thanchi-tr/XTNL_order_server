"""PatroniControl over the Patroni REST API (operations.md §1, §7; tier-persistence.md §3 step 2).

The engine never restores PostgreSQL. On damage confined to the primary, SM3 asks Patroni to switch
over to the sync standby, after that standby passes the same Step 2 checks, and then to reinit the
damaged member. Everything else here is read-only:

    GET  {endpoint}/cluster                  members, roles, state, lag, api_url
    POST {endpoint}/switchover               {"leader": primary, "candidate": sync_standby}
    POST {member api}/reinitialize           {"force": true}

`switchover()` and `reinit()` refuse (PermissionError) unless the `incident_path` predicate allows it:
the runtime builds a guarded copy for SM3 only (`with_incident_guard`) whose predicate is "SM3 is
HALTED or SWITCHING_OVER". The health probe's copy can only read.

The REST endpoints default to the PostgreSQL hosts of ORDER_SERVER_DSN on Patroni's port 8008 over
https (deploy/patroni/patroni.yml `restapi.listen`); TLS trust comes from SSL_CERT_FILE. Logs name
hosts only, never credentials.

REST credentials (IMPLEMENTATION_PLAN 4.7): XTNL_PATRONI_USER / XTNL_PATRONI_PASSWORD, read through the
secrets source (src/secrets), are sent as HTTP Basic auth on the unsafe endpoints (switchover, reinit),
matching patroni.yml `restapi.authentication`. `patroni_auth_from_env` reads them.
"""
from __future__ import annotations

import asyncio
import importlib
import logging
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, Final
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx

from src.db.integrity_store import CorruptRowError, PgIntegrityDb
from src.db.pg import AsyncpgConnection, PgConnection, init_connection
from src.integrity.dbcheck import DbIntegrityChecker

log = logging.getLogger(__name__)

PATRONI_PORT: Final = 8008
PRIMARY_ROLES: Final = frozenset({"leader", "primary", "master"})
SYNC_ROLES: Final = frozenset({"sync_standby"})
HEALTHY_STATES: Final = frozenset({"running", "streaming"})


PATRONI_USER_VAR: Final = "XTNL_PATRONI_USER"
PATRONI_PASSWORD_VAR: Final = "XTNL_PATRONI_PASSWORD"


def patroni_auth_from_env(environ: Mapping[str, str] | None = None) -> tuple[str, str] | None:
    """(user, password) for the Patroni REST API, None when neither is set. ValueError (names only)
    when only one of them is set."""
    e = os.environ if environ is None else environ
    user = (e.get(PATRONI_USER_VAR) or "").strip()
    password = e.get(PATRONI_PASSWORD_VAR) or ""
    if not user and not password:
        return None
    if not user or not password:
        raise ValueError(f"{PATRONI_USER_VAR} and {PATRONI_PASSWORD_VAR} must be set together")
    return user, password


class PatroniError(RuntimeError):
    """Patroni could not be reached or refused the request."""


@dataclass(frozen=True)
class PatroniMember:
    name: str
    role: str
    state: str
    host: str | None
    port: int | None
    api_url: str | None
    lag: int | None
    timeline: int | None

    @property
    def is_primary(self) -> bool:
        return self.role in PRIMARY_ROLES

    @property
    def is_sync_standby(self) -> bool:
        return self.role in SYNC_ROLES


def _opt_int(v: object) -> int | None:
    try:
        return None if v is None or v == "" else int(str(v))
    except ValueError:
        return None          # Patroni reports lag "unknown" for a member it cannot measure


def parse_members(body: object) -> list[PatroniMember]:
    if not isinstance(body, dict) or not isinstance(body.get("members"), list):
        raise PatroniError("unexpected /cluster response")
    out: list[PatroniMember] = []
    for m in body["members"]:
        if not isinstance(m, dict) or "name" not in m:
            continue
        out.append(PatroniMember(
            name=str(m["name"]), role=str(m.get("role", "")).lower(), state=str(m.get("state", "")).lower(),
            host=None if m.get("host") is None else str(m["host"]), port=_opt_int(m.get("port")),
            api_url=None if m.get("api_url") is None else str(m["api_url"]), lag=_opt_int(m.get("lag")),
            timeline=_opt_int(m.get("timeline"))))
    return out


def dsn_hosts(dsn: str) -> list[str]:
    """The host names of a (multi-host) PostgreSQL URL: postgresql://u:p@h1:5432,h2:5432/db -> [h1, h2]."""
    rest = dsn.split("://", 1)[-1]
    netloc = rest.split("/", 1)[0].split("?", 1)[0]
    hosts = netloc.rsplit("@", 1)[-1]
    out: list[str] = []
    for h in hosts.split(","):
        h = h.strip()
        if not h:
            continue
        if h.startswith("["):
            out.append(h[1:h.index("]")])
        else:
            out.append(h.rsplit(":", 1)[0] if h.count(":") == 1 else h)
    return out


def default_endpoints(dsn: str, *, port: int = PATRONI_PORT, scheme: str = "https") -> list[str]:
    return [f"{scheme}://{h}:{port}" for h in dsn_hosts(dsn)]


def dsn_for_member(dsn: str, host: str, port: int | None) -> str:
    """The same credentials and database, pointed at one member, any session attrs (standby reads)."""
    scheme, rest = dsn.split("://", 1)
    path_q = rest.split("/", 1)[1] if "/" in rest else ""
    netloc = rest.split("/", 1)[0]
    userinfo = netloc.rsplit("@", 1)[0] + "@" if "@" in netloc else ""
    path, _, query = path_q.partition("?")
    params = [(k, v) for k, v in parse_qsl(query) if k != "target_session_attrs"]
    params.append(("target_session_attrs", "any"))
    hostpart = f"[{host}]" if ":" in host else host
    return f"{scheme}://{userinfo}{hostpart}:{port or 5432}/{path}?{urlencode(params)}"


StandbyCheck = Callable[[PatroniMember], Awaitable[bool]]


class PatroniRestControl:
    def __init__(self, endpoints: Sequence[str], *, client: httpx.AsyncClient | None = None,
                 auth: tuple[str, str] | None = None, timeout_s: float = 3.0,
                 standby_check: StandbyCheck | None = None, incident_path: Callable[[], bool] = lambda: False,
                 max_standby_lag_bytes: int = 1 << 20, switchover_wait_s: float = 60.0, poll_s: float = 1.0,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        if not endpoints:
            raise ValueError("at least one Patroni REST endpoint is required")
        self._endpoints = [e.rstrip("/") for e in endpoints]
        self._client = client
        self._owned: httpx.AsyncClient | None = None
        self._auth = auth
        self._timeout = timeout_s
        self._standby_check = standby_check
        self._incident_path = incident_path
        self._max_lag = max_standby_lag_bytes
        self._switch_wait = switchover_wait_s
        self._poll = poll_s
        self._mono = monotonic

    def __repr__(self) -> str:   # never the credentials
        hosts = [urlsplit(e).hostname for e in self._endpoints]
        return f"PatroniRestControl(hosts={hosts!r}, auth={self._auth is not None})"

    @property
    def has_auth(self) -> bool:
        return self._auth is not None

    def with_incident_guard(self, incident_path: Callable[[], bool]) -> PatroniRestControl:
        """A copy sharing the HTTP client whose switchover / reinit are allowed only while
        `incident_path()` is True (SM3 HALTED / SWITCHING_OVER)."""
        twin = PatroniRestControl(self._endpoints, client=self._http(), auth=self._auth, timeout_s=self._timeout,
                                  standby_check=self._standby_check, incident_path=incident_path,
                                  max_standby_lag_bytes=self._max_lag, switchover_wait_s=self._switch_wait,
                                  poll_s=self._poll, monotonic=self._mono)
        return twin

    def _http(self) -> httpx.AsyncClient:
        if self._client is not None:
            return self._client
        if self._owned is None:
            self._owned = httpx.AsyncClient(timeout=self._timeout)
        return self._owned

    async def aclose(self) -> None:
        if self._owned is not None:
            await self._owned.aclose()
            self._owned = None

    # --- reads ----------------------------------------------------------------------------------------
    async def cluster(self) -> list[PatroniMember]:
        errors: list[str] = []
        for ep in self._endpoints:
            try:
                resp = await self._http().get(f"{ep}/cluster", timeout=self._timeout)
            except httpx.HTTPError as exc:
                errors.append(f"{urlsplit(ep).hostname}: {type(exc).__name__}")
                continue
            if resp.status_code != 200:
                errors.append(f"{urlsplit(ep).hostname}: HTTP {resp.status_code}")
                continue
            try:
                return parse_members(resp.json())
            except ValueError as exc:
                errors.append(f"{urlsplit(ep).hostname}: {exc}")
        raise PatroniError("no Patroni endpoint answered: " + "; ".join(errors))

    async def primary(self) -> PatroniMember:
        members = await self.cluster()
        primaries = [m for m in members if m.is_primary]
        if len(primaries) != 1:
            raise PatroniError(f"expected one primary, found {len(primaries)}")
        return primaries[0]

    async def primary_member(self) -> str:
        return (await self.primary()).name

    async def sync_standby(self) -> PatroniMember | None:
        members = await self.cluster()
        return next((m for m in members if m.is_sync_standby and m.state in HEALTHY_STATES), None)

    async def standby_passes_check(self) -> bool:
        """The sync standby is streaming, caught up, and its rows pass the Step 2 checks."""
        sb = await self.sync_standby()
        if sb is None:
            log.warning("Patroni: no healthy sync standby")
            return False
        if sb.lag is not None and sb.lag > self._max_lag:
            log.warning("Patroni: sync standby %s lags %d bytes", sb.name, sb.lag)
            return False
        if self._standby_check is None:
            log.warning("Patroni: no standby check configured; the standby cannot be verified")
            return False
        return await self._standby_check(sb)

    # --- incident actions -------------------------------------------------------------------------------
    def _require_incident(self, what: str) -> None:
        if not self._incident_path():
            raise PermissionError(f"Patroni {what} is allowed only from the SM3 integrity incident path")

    async def _post(self, url: str, body: dict[str, Any]) -> httpx.Response:
        try:
            if self._auth is None:
                return await self._http().post(url, json=body, timeout=self._timeout)
            return await self._http().post(url, json=body, auth=httpx.BasicAuth(*self._auth), timeout=self._timeout)
        except httpx.HTTPError as exc:
            raise PatroniError(f"{urlsplit(url).hostname}: {type(exc).__name__}") from None

    async def switchover(self) -> str:
        self._require_incident("switchover")
        primary = await self.primary()
        sb = await self.sync_standby()
        if sb is None:
            raise PatroniError("switchover refused: no healthy sync standby")
        last: str = ""
        for ep in self._endpoints:
            resp = await self._post(f"{ep}/switchover", {"leader": primary.name, "candidate": sb.name})
            if resp.status_code == 200:
                log.critical("Patroni switchover requested: %s -> %s", primary.name, sb.name)
                return await self._await_primary(sb.name)
            last = f"HTTP {resp.status_code}"
            if resp.status_code in (401, 403, 412):
                break                         # not authorised / precondition failed: same on every node
        raise PatroniError(f"switchover refused: {last}")

    async def _await_primary(self, name: str) -> str:
        deadline = self._mono() + self._switch_wait
        while True:
            try:
                if (await self.primary()).name == name:
                    return name
            except PatroniError:
                pass
            if self._mono() >= deadline:
                raise PatroniError(f"switchover to {name} not confirmed within {self._switch_wait:.0f} s")
            await asyncio.sleep(self._poll)

    async def reinit(self, member: str) -> None:
        self._require_incident("reinit")
        target = next((m for m in await self.cluster() if m.name == member), None)
        if target is None or not target.api_url:
            raise PatroniError(f"reinit: member {member} unknown")
        base = target.api_url.rstrip("/")
        base = base[: -len("/patroni")] if base.endswith("/patroni") else base
        resp = await self._post(f"{base}/reinitialize", {"force": True})
        if resp.status_code not in (200, 202):
            raise PatroniError(f"reinit {member} refused: HTTP {resp.status_code}")
        log.critical("Patroni reinit of %s requested", member)


# --- the standby check ------------------------------------------------------------------------------------
class _OneConnection:
    def __init__(self, conn: PgConnection) -> None:
        self._conn = conn

    @asynccontextmanager
    async def _acq(self) -> AsyncIterator[PgConnection]:
        yield self._conn

    def acquire(self) -> AbstractAsyncContextManager[PgConnection]:
        return self._acq()


async def rows_pass_check(db: PgIntegrityDb, *, page: int = 500, max_rows: int | None = None) -> bool:
    """Step 2 content checks (checksum, digest hash, version label) over every current row of `db`."""
    checker = DbIntegrityChecker()
    after, seen = "", 0
    while True:
        try:
            rows = await db.rows_after_key(after, page)
        except CorruptRowError as exc:
            log.critical("standby check: row %s cannot be rebuilt", exc.key)
            return False
        if not rows:
            return True
        damage = [f for f in checker.check_rows(rows) if f.is_damage]
        if damage:
            log.critical("standby check: %d damaged rows (first %s: %s)", len(damage), damage[0].key, damage[0].kind)
            return False
        seen += len(rows)
        if max_rows is not None and seen >= max_rows:
            return True
        after = rows[-1].key


def asyncpg_standby_check(dsn: str, *, timeout_s: float = 5.0, page: int = 500) -> StandbyCheck:
    """Connect to the standby member (read only) and run `rows_pass_check` on it."""
    async def check(member: PatroniMember) -> bool:
        if member.host is None:
            return False
        asyncpg = importlib.import_module("asyncpg")
        raw = await asyncpg.connect(dsn_for_member(dsn, member.host, member.port), timeout=timeout_s)
        try:
            await init_connection(raw)
            return await rows_pass_check(PgIntegrityDb(_OneConnection(AsyncpgConnection(raw))), page=page)
        finally:
            await raw.close()

    return check
