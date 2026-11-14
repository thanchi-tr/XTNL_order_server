"""Dashboard HTTP server: settings, the read-only PostgreSQL pool and the routes (IMPLEMENTATION_PLAN 5.6).

Routes (GET / HEAD only, src/observability/http.py refuses anything else):

    /                server-rendered HTML (refreshes every 10 s), every value HTML-escaped
    /api/summary     everything below in one JSON object
    /api/orders /api/positions /api/accounts /api/halt /api/rules /api/dlq /api/streams /api/leader
    /api/live        Redis live values (only when fresh, see src/dashboard/views.py)
    /healthz         200 "ok"

The leader heartbeat copy arrives on a separate listener (src/dashboard/heartbeat.py), so this server stays
GET / HEAD only.

Settings (environment; deploy/env/dashboard.env.example, /etc/xtnl/dashboard.env on the Melbourne host):

    XTNL_DASHBOARD_DSN         PostgreSQL DSN of role xtnl_dashboard (required; never logged)
    XTNL_DASHBOARD_REDIS_URLS  comma-separated name=rediss://... of both nodes' Redis TLS proxies (dashboard ACL
                               user), optional
    XTNL_DASHBOARD_ADDR        bind address, default 127.0.0.1:8088 (VPN only, operations.md §16)
    XTNL_DASHBOARD_FRESH_MS    live-value freshness limit, default 30000
"""
from __future__ import annotations

import asyncio
import html
import importlib
import json
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

import redis.asyncio as aioredis

from src.cache.client import create_redis
from src.db.pg import AsyncpgSource
from src.observability.http import Request, Response
from src.dashboard.views import DEFAULT_FRESH_MS, DashboardViews, LeaderTerm

log = logging.getLogger(__name__)

DSN_VAR: Final = "XTNL_DASHBOARD_DSN"
REDIS_URLS_VAR: Final = "XTNL_DASHBOARD_REDIS_URLS"
ADDR_VAR: Final = "XTNL_DASHBOARD_ADDR"
FRESH_VAR: Final = "XTNL_DASHBOARD_FRESH_MS"
DEFAULT_ADDR: Final = "127.0.0.1:8088"
DEFAULT_PORT: Final = 8088
APPLICATION_NAME: Final = "xtnl-dashboard"
#: Every dashboard session is read-only: a write is refused by PostgreSQL itself, whatever the SQL.
READ_ONLY_SETTINGS: Final[Mapping[str, str]] = {
    "application_name": APPLICATION_NAME, "default_transaction_read_only": "on", "statement_timeout": "5000"}
REQUEST_TIMEOUT_S: Final = 8.0
#: Run on every new pooled connection before it is used: the role, whether it is a superuser, the session's
#: read-only default, and how many of the database's tables it could write (INSERT / UPDATE / DELETE /
#: TRUNCATE, inherited grants and ownership included). A DSN pointed at a writable role (the engine's, an
#: operator's, a superuser) is refused, so a read-only session never rests on the startup parameter alone.
SQL_ROLE_CHECK: Final = (
    "SELECT current_user::text, r.rolsuper, current_setting('default_transaction_read_only'), "
    "(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relkind IN ('r', 'p') AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%' "
    "AND (has_table_privilege(c.oid, 'INSERT') OR has_table_privilege(c.oid, 'UPDATE') "
    "OR has_table_privilege(c.oid, 'DELETE') OR has_table_privilege(c.oid, 'TRUNCATE'))) "
    "FROM pg_roles r WHERE r.rolname = current_user"
)


class DashboardConfigError(ValueError):
    """A dashboard setting is missing or invalid (the message names the variable, never a value)."""


class DashboardRoleError(RuntimeError):
    """The dashboard's PostgreSQL role could write (or the session is not read-only): the connection is refused."""


async def verify_read_only_connection(conn: Any) -> None:
    """asyncpg pool `init`: refuse a connection whose role is a superuser, can write any table, or whose session
    is not `default_transaction_read_only = on`. Raises DashboardRoleError (role name only, never the DSN)."""
    row = await conn.fetchrow(SQL_ROLE_CHECK)
    if row is None:
        raise DashboardRoleError("dashboard role not found in pg_roles")
    role, superuser, read_only, writable = str(row[0]), bool(row[1]), str(row[2]), int(row[3] or 0)
    problems: list[str] = []
    if superuser:
        problems.append("is a superuser")
    if writable:
        problems.append(f"can write {writable} table(s)")
    if read_only.lower() != "on":
        problems.append("session is not default_transaction_read_only")
    if problems:
        msg = (f"refusing PostgreSQL role {role!r} for the read-only dashboard: {', '.join(problems)} "
               f"(set {DSN_VAR} to role xtnl_dashboard, SELECT grants only)")
        log.critical("%s", msg)
        raise DashboardRoleError(msg)


@dataclass(frozen=True)
class DashboardSettings:
    dsn: str = field(repr=False)
    redis_urls: Mapping[str, str] = field(default_factory=dict, repr=False)
    addr: str = DEFAULT_ADDR
    fresh_ms: int = DEFAULT_FRESH_MS

    @staticmethod
    def from_env(environ: Mapping[str, str]) -> DashboardSettings:
        dsn = (environ.get(DSN_VAR) or "").strip()
        if not dsn:
            raise DashboardConfigError(f"{DSN_VAR} is required")
        urls: dict[str, str] = {}
        for i, part in enumerate(p.strip() for p in (environ.get(REDIS_URLS_VAR) or "").split(",")):
            if not part:
                continue
            name, sep, url = part.partition("=")
            if sep and "://" not in name:
                urls[name.strip()] = url.strip()
            else:
                urls[f"redis{i + 1}"] = part
        raw = (environ.get(FRESH_VAR) or "").strip()
        try:
            fresh = int(raw) if raw else DEFAULT_FRESH_MS
        except ValueError:
            raise DashboardConfigError(f"{FRESH_VAR} must be an integer (ms)") from None
        if fresh <= 0:
            raise DashboardConfigError(f"{FRESH_VAR} must be positive")
        return DashboardSettings(dsn, urls, (environ.get(ADDR_VAR) or DEFAULT_ADDR).strip(), fresh)


async def create_read_only_source(dsn: str, *, max_size: int = 4) -> AsyncpgSource:
    """asyncpg pool whose every session is read-only (`default_transaction_read_only = on`) and whose role cannot
    write (`verify_read_only_connection` runs on every new connection; a writable role raises DashboardRoleError
    and no pool is created)."""
    asyncpg = importlib.import_module("asyncpg")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=max_size, timeout=3.0,
                                     server_settings=dict(READ_ONLY_SETTINGS), init=verify_read_only_connection)
    return AsyncpgSource(pool)


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


# ============================================================================================ HTML
_CSS: Final = """
:root{--bg:#fafafa;--fg:#1d1d1f;--muted:#6b6b70;--line:#e2e2e6;--warn:#b35c00;--bad:#b3261e;--ok:#1b7f3b}
@media (prefers-color-scheme: dark){:root{--bg:#141416;--fg:#ececf0;--muted:#9a9aa3;--line:#2c2c31;
--warn:#f0a050;--bad:#ff8a80;--ok:#7bd88f}}
body{background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif;margin:0;padding:16px}
h1{font-size:18px;margin:0 0 4px}h2{font-size:15px;margin:24px 0 8px}
.muted{color:var(--muted)}.warn{color:var(--warn)}.bad{color:var(--bad)}.ok{color:var(--ok)}
.wrap{overflow-x:auto}table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
th,td{border-bottom:1px solid var(--line);padding:4px 8px;text-align:left;white-space:nowrap}
th{color:var(--muted);font-weight:600}
"""


def _cell(v: Any) -> str:
    return "" if v is None else html.escape(str(v))


def _table(rows: Sequence[Mapping[str, Any]], cols: Sequence[str]) -> str:
    if not rows:
        return '<p class="muted">none</p>'
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{_cell(r.get(c))}</td>" for c in cols) + "</tr>" for r in rows)
    return f'<div class="wrap"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def render_html(s: Mapping[str, Any], now_ms: int) -> str:
    leader = s.get("leader") or {}
    halt = s.get("halt") or {}
    live = s.get("live") or {}
    level = str(halt.get("effective_level") or "unknown")
    level_cls = "ok" if level == "NONE" else "bad" if level == "HARD" else "warn"
    hold = leader.get("safe_hold")
    live_note = (f'<p class="warn">Live values stale: {html.escape(str(live.get("reason") or ""))}</p>'
                 if live.get("stale") else
                 f'<p class="muted">Live values from {html.escape(str(live.get("source")))}, epoch '
                 f'{html.escape(str(live.get("epoch")))}; values older than '
                 f'{int(s.get("fresh_ms", 0)) // 1000} s are hidden.</p>')
    prices = [dict(p, bid=p.get("bid", "stale"), ask=p.get("ask", "stale")) for p in live.get("prices", [])]
    accts = [dict(a, equity=a.get("equity", "stale") if a.get("fresh") else "stale")
             for a in live.get("accounts", [])]
    parts = [
        "<!doctype html><html lang=en><head><meta charset=utf-8>",
        '<meta name=viewport content="width=device-width,initial-scale=1">',
        '<meta http-equiv=refresh content=10><title>XTNL Dashboard</title>',
        f"<style>{_CSS}</style></head><body>",
        "<h1>XTNL Phase 1 &middot; read-only</h1>",
        f'<p class="muted">Generated {html.escape(time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(now_ms / 1000)))}'
        " from PostgreSQL. Nothing on this page can change the engine.</p>",
        f'<p>Halt level: <b class="{level_cls}">{html.escape(level)}</b> &middot; leader '
        f'{_cell(leader.get("node")) or "<span class=bad>none</span>"} (epoch {_cell(leader.get("term_epoch"))}, '
        f'engine_epoch {_cell(leader.get("epoch"))}) &middot; SAFE_HOLD '
        f'<b class="{"bad" if hold else "ok"}">{_cell(hold)}</b></p>',
        "<h2>Live (Redis)</h2>", live_note,
        _table(prices, ("adapter_id", "symbol", "bid", "ask", "age_ms")),
        _table(accts, ("account_id", "equity", "age_ms", "sampled")),
        "<h2>Rule states (active)</h2>",
        _table(s.get("rules", []), ("rule_id", "subject_type", "subject_id", "account_id", "level", "pending_level",
                                    "cap", "reason", "active_since")),
        "<h2>Positions</h2>",
        _table(s.get("positions", []), ("adapter_id", "position_id", "account_id", "symbol", "side", "units",
                                        "entry_price", "stop_loss", "take_profit", "status", "seq_id")),
        "<h2>Orders</h2>",
        _table(s.get("orders", []), ("adapter_id", "broker_order_id", "client_order_id", "account_id", "symbol",
                                     "side", "status", "remaining_units", "stop_loss", "seq_id")),
        "<h2>Accounts</h2>",
        _table(s.get("accounts", []), ("adapter_id", "account_id", "currency", "balance", "realised_pnl_today",
                                       "seq_id")),
        "<h2>Stream health</h2>",
        _table(s.get("streams", []), ("adapter_id", "stream", "conn_lost_at", "reconciled_at", "reconciled_epoch")),
        "<h2>DLQ depth</h2>",
        _table([{"status": k, "rows": v} for k, v in sorted((s.get("dlq") or {}).items())], ("status", "rows")),
        "<h2>Escalated alerts (24 h)</h2>",
        _table(s.get("escalations", []), ("created_ms", "severity", "title", "channels", "delivered", "last_error")),
        "<h2>Leader terms</h2>",
        _table(leader.get("terms", []), ("epoch", "node", "started_at", "reached_leader_at", "ended_at",
                                         "end_reason")),
        "</body></html>",
    ]
    return "".join(parts)


# ============================================================================================ app
def _term_dict(t: LeaderTerm) -> dict[str, Any]:
    return {"epoch": t.epoch, "term_epoch": t.term_epoch, "node": t.node, "bumped_at_ms": t.bumped_at_ms,
            "safe_hold": t.safe_hold, "terms": t.terms}


class DashboardApp:
    def __init__(self, views: DashboardViews, *, clock_ms: Callable[[], int] = _wall_ms,
                 timeout_s: float = REQUEST_TIMEOUT_S) -> None:
        self.views = views
        self._clock = clock_ms
        self._timeout = timeout_s
        self._gate = asyncio.Semaphore(4)       # bounded concurrent reads per dashboard process

    async def summary(self) -> dict[str, Any]:
        v = self.views
        now = self._clock()
        term = await v.leader()
        accounts = await v.accounts()
        out: dict[str, Any] = {
            "generated_ms": now, "fresh_ms": v.fresh_ms, "leader": _term_dict(term), "halt": await v.halt(),
            "rules": await v.rule_states(), "positions": await v.positions(), "orders": await v.orders(),
            "accounts": accounts, "dlq": await v.dlq(), "streams": await v.streams()}
        try:
            out["escalations"] = await v.escalations(now)
        except Exception as exc:
            log.warning("dashboard: escalations unreadable: %s", type(exc).__name__)
            out["escalations"] = []
        out["live"] = await v.live(term, now, [str(a["account_id"]) for a in accounts])
        return out

    async def _one(self, path: str) -> Any:
        v = self.views
        now = self._clock()
        routes: dict[str, Callable[[], Awaitable[Any]]] = {
            "/api/summary": self.summary, "/api/orders": v.orders, "/api/positions": v.positions,
            "/api/accounts": v.accounts, "/api/halt": v.halt, "/api/rules": v.rule_states, "/api/dlq": v.dlq,
            "/api/streams": v.streams}
        if path in routes:
            return await routes[path]()
        if path == "/api/leader":
            return _term_dict(await v.leader())
        if path == "/api/live":
            accounts = [str(a["account_id"]) for a in await v.accounts()]
            return await v.live(await v.leader(), now, accounts)
        raise KeyError(path)

    async def handle(self, req: Request) -> Response:
        if req.path == "/healthz":
            return Response.text(200, "ok\n")
        if req.path != "/" and not req.path.startswith("/api/"):
            return Response.text(404, "not found\n")
        try:
            async with self._gate:
                if req.path == "/":
                    s = await asyncio.wait_for(self.summary(), self._timeout)
                    return Response.text(200, render_html(s, self._clock()), "text/html; charset=utf-8")
                data = await asyncio.wait_for(self._one(req.path), self._timeout)
        except KeyError:
            return Response.text(404, "not found\n")
        except TimeoutError:
            return Response.text(503, "PostgreSQL / Redis did not answer in time\n")
        except Exception as exc:
            log.warning("dashboard %s failed: %s", req.path, type(exc).__name__)
            return Response.text(503, f"unavailable ({type(exc).__name__})\n")
        return Response.text(200, json.dumps(data, default=str, separators=(",", ":")),
                             "application/json; charset=utf-8")


def redis_clients(urls: Mapping[str, str]) -> dict[str, aioredis.Redis]:
    return {name: create_redis(url) for name, url in urls.items()}




__all__ = ["ADDR_VAR", "DEFAULT_ADDR", "DashboardApp", "DashboardConfigError", "DashboardRoleError",
           "DashboardSettings", "DSN_VAR", "FRESH_VAR", "READ_ONLY_SETTINGS", "REDIS_URLS_VAR", "SQL_ROLE_CHECK",
           "create_read_only_source", "redis_clients", "render_html", "verify_read_only_connection"]
