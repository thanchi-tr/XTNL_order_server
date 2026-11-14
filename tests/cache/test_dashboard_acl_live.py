"""The dashboard's Redis ACL user (deploy/redis/redis.conf) on a real Redis (IMPLEMENTATION_PLAN 7.3). Skipped
unless ORDER_SERVER_TEST_REDIS points at a DISPOSABLE Redis >= 7 whose connection may run ACL SETUSER / DELUSER:
the test creates a temporary user with the exact rules of redis.conf and deletes it afterwards.

It proves the rules work as written: the dashboard's reads succeed, every write, KEYS / SCAN, FLUSHDB, EVAL and a
read of any other key (halt:state) are refused."""
from __future__ import annotations

import os
import secrets
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest
from redis.exceptions import NoPermissionError, ResponseError

from src.cache import keys as K
from src.cache.client import create_redis

URL = os.environ.get("ORDER_SERVER_TEST_REDIS")
pytestmark = [pytest.mark.redis, pytest.mark.skipif(not URL, reason="ORDER_SERVER_TEST_REDIS not set")]
CONF = Path(__file__).resolve().parents[2] / "deploy" / "redis" / "redis.conf"


def _rules() -> list[str]:
    (line,) = [ln.strip() for ln in CONF.read_text(encoding="utf-8").splitlines()
               if ln.strip().startswith("user xtnl_dashboard ")]
    return line.split()[2:]


async def test_dashboard_acl_user_reads_only_what_the_dashboard_needs() -> None:
    assert URL is not None
    user = f"xtnl_dashboard_test_{secrets.token_hex(4)}"
    password = secrets.token_hex(16)
    rules = [r if not r.startswith("#") else f">{password}" for r in _rules()]
    admin = create_redis(URL)
    parts = urlsplit(URL)
    host = parts.hostname or "127.0.0.1"
    netloc = f"{user}:{password}@{host}" + (f":{parts.port}" if parts.port else "")
    dash = create_redis(urlunsplit((parts.scheme, netloc, parts.path, parts.query, "")))
    try:
        await admin.execute_command("ACL", "SETUSER", user, "reset", *rules)
        await admin.set(K.BOOT_ID, "run:7")
        await admin.hset(K.price_key("mock", "EURUSD"), mapping={"bid": "1", "ask": "2", "ts": "1"})
        await admin.set(K.HALT_STATE, "x")
        assert await dash.ping()
        assert await dash.get(K.BOOT_ID) == b"run:7"
        pipe = dash.pipeline(transaction=False)
        pipe.hgetall(K.price_key("mock", "EURUSD"))
        pipe.hgetall(K.account_live_key("ACC"))
        assert (await pipe.execute())[0][b"bid"] == b"1"
        for refused in (lambda: dash.set(K.BOOT_ID, "forged"), lambda: dash.get(K.HALT_STATE),
                        lambda: dash.keys("*"), lambda: dash.scan(0), lambda: dash.flushdb(),
                        lambda: dash.hset(K.price_key("mock", "EURUSD"), "bid", "9"),
                        lambda: dash.eval("return 1", 0), lambda: dash.delete(K.BOOT_ID),
                        lambda: dash.xadd(K.FEED, {"k": "v"})):
            with pytest.raises((NoPermissionError, ResponseError)):
                await refused()
        assert await admin.get(K.BOOT_ID) == b"run:7"
    finally:
        try:
            await admin.execute_command("ACL", "DELUSER", user)
            await admin.delete(K.BOOT_ID, K.HALT_STATE, K.price_key("mock", "EURUSD"))
        finally:
            await dash.aclose()
            await admin.aclose()
