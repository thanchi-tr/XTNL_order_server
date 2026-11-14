"""HttpEtcdClient against an httpx.MockTransport (no network): JSON gateway encoding + failover."""
from __future__ import annotations

import base64
import json

import httpx
import pytest

from src.runtime.etcd import EtcdError, HttpEtcdClient, prefix_range_end


def b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


class Gateway:
    def __init__(self, dead: set[str] | None = None) -> None:
        self.dead = dead or set()
        self.requests: list[tuple[str, str, dict[str, object]]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        body = json.loads(request.content or b"{}")
        self.requests.append((host, request.url.path, body))
        if host in self.dead:
            raise httpx.ConnectError("refused", request=request)
        header = {"cluster_id": "14841639068965178418", "revision": "42"}
        path = request.url.path
        if path == "/v3/lease/grant":
            return httpx.Response(200, json={"header": header, "ID": "7587", "TTL": str(body["TTL"])})
        if path == "/v3/lease/keepalive":
            line = json.dumps({"result": {"header": header, "ID": body["ID"], "TTL": "15"}})
            return httpx.Response(200, text=line + "\n")
        if path == "/v3/lease/revoke":
            return httpx.Response(200, json={"header": header})
        if path == "/v3/kv/put":
            return httpx.Response(200, json={"header": header})
        if path == "/v3/kv/txn":
            return httpx.Response(200, json={"header": header})       # succeeded omitted = false
        if path == "/v3/kv/range":
            return httpx.Response(200, json={"header": header, "kvs": [
                {"key": b64("/xtnl/health/n2"), "value": b64('{"a":1}'), "create_revision": "40",
                 "mod_revision": "41", "lease": "99"}], "count": "1"})
        return httpx.Response(404, text="nope")


def client(gw: Gateway) -> HttpEtcdClient:
    return HttpEtcdClient(["http://e1:2379", "http://e2:2379", "http://e3:2379"],
                          transport=httpx.MockTransport(gw))


def test_grant_keepalive_put_and_failover() -> None:
    gw = Gateway(dead={"e1"})
    c = client(gw)
    g = c.lease_grant(15, timeout_s=2)
    assert (g.lease_id, g.ttl_s, g.cluster_id, g.revision) == (7587, 15, "14841639068965178418", 42)
    assert [r[0] for r in gw.requests] == ["e1", "e2"]          # retried at once on the next endpoint
    assert c.current_endpoint == "http://e2:2379"
    assert c.lease_keepalive(7587, timeout_s=2) == 15
    assert c.put("/k", "v", lease_id=7587, timeout_s=2) == 42
    _, path, body = gw.requests[-1]
    assert path == "/v3/kv/put" and body == {"key": b64("/k"), "value": b64("v"), "lease": "7587"}
    c.lease_revoke(7587, timeout_s=2)
    c.close()


def test_txn_campaign_shape_and_missing_succeeded() -> None:
    gw = Gateway()
    res = client(gw).create_if_absent("/xtnl/leader", "n1", lease_id=5, timeout_s=2)
    assert res.succeeded is False and res.revision == 42
    body = gw.requests[-1][2]
    assert body["compare"] == [{"key": b64("/xtnl/leader"), "target": "CREATE", "result": "EQUAL",
                                "create_revision": "0"}]
    assert body["success"] == [{"request_put": {"key": b64("/xtnl/leader"), "value": b64("n1"), "lease": "5"}}]


def test_range_and_prefix() -> None:
    gw = Gateway()
    c = client(gw)
    kv = c.get("/xtnl/health/n2", timeout_s=2)
    assert kv is not None and kv.value == '{"a":1}' and kv.lease_id == 99 and kv.create_revision == 40
    assert len(c.get_prefix("/xtnl/health/", timeout_s=2)) == 1
    assert gw.requests[-1][2]["range_end"] == base64.b64encode(b"/xtnl/health0").decode()
    assert prefix_range_end("/x") == b"/y"


def test_all_endpoints_down_raises() -> None:
    c = client(Gateway(dead={"e1", "e2", "e3"}))
    with pytest.raises(EtcdError):
        c.lease_keepalive(1, timeout_s=2)


def test_client_errors_are_not_retried() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text="bad")
    c = HttpEtcdClient(["http://e1"], transport=httpx.MockTransport(handler))
    with pytest.raises(EtcdError):
        c.put("/k", "v", timeout_s=1)
    with pytest.raises(ValueError):
        HttpEtcdClient([])
