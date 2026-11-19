"""K5 (operations.md §13): every served / presented certificate has the 30-day expiry alert and the
unreadable-certificate alert; certificates on hosts without an exporter are read by a verifying handshake."""
from __future__ import annotations

import asyncio
import importlib.util
import ssl
import sys
import time
from pathlib import Path
from types import ModuleType

from src.observability.certs import (
    EXPIRY_WARN_S,
    CertReading,
    ReloadingServerTls,
    cert_not_after_s,
    endpoint_not_after_s,
    parse_endpoint,
)
from src.runtime import telemetry as T
from tests.certutil import make_cert

ROOT = Path(__file__).resolve().parents[2]


def _gen_alerts() -> ModuleType:
    spec = importlib.util.spec_from_file_location("xtnl_gen_alerts_k5", ROOT / "deploy/observability/gen_alerts.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_file_cert_readable_and_expiry(tmp_path: Path) -> None:
    long_crt, _ = make_cert(tmp_path, "long", days=400)
    short_crt, _ = make_cert(tmp_path, "short", days=2)
    now = time.time()
    got = {(x.name, x.labels["cert"]): x.value for x in T.cert_samples(
        {"long": str(long_crt), "short": str(short_crt), "gone": str(tmp_path / "gone.crt")}, now)}
    assert got[("xtnl_cert_readable", "long")] == 1 and got[("xtnl_cert_readable", "short")] == 1
    assert got[("xtnl_cert_readable", "gone")] == 0 and ("xtnl_cert_expires_in_seconds", "gone") not in got
    assert got[("xtnl_cert_expires_in_seconds", "long")] > 300 * 86_400
    assert 0 < got[("xtnl_cert_expires_in_seconds", "short")] < EXPIRY_WARN_S


def test_cert_reading_problem() -> None:
    now = 1_000_000.0
    assert CertReading("a", now + 40 * 86_400).problem(now) is None
    assert "expires in 3 day" in str(CertReading("a", now + 3 * 86_400 + 5).problem(now))
    assert "expired" in str(CertReading("a", now - 2 * 86_400).problem(now))
    assert "unreadable" in str(CertReading("a", None).problem(now))


def test_endpoints_from_env_report_malformed_entries() -> None:
    eps = T.cert_endpoints_from_env({"XTNL_METRICS_CERT_ENDPOINTS": "w=10.0.3.10:2380, bad=nohost, r=[::1]:8443",
                                     "XTNL_ETCD_CERT": "/c.crt", "XTNL_ETCD_KEY": "/c.key"})
    assert [(e.name, e.host, e.port, e.client_cert) for e in eps] == [
        ("bad", "", 0, None), ("r", "::1", 8443, ("/c.crt", "/c.key")), ("w", "10.0.3.10", 2380, ("/c.crt", "/c.key"))]
    assert parse_endpoint("h:0") is None and parse_endpoint("h:x") is None


def test_endpoint_trust_follows_the_etcd_ca_or_a_per_entry_ca() -> None:
    """K5 review: the witness's etcd certificates verify against XTNL_ETCD_CA (not only SSL_CERT_FILE); an entry's
    own ';ca=' wins; an endpoint that only the system store covers is warned about at start (names only)."""
    env = {"XTNL_METRICS_CERT_ENDPOINTS": "etcd-witness-server=10.0.3.10:2379,etcd-witness-peer=10.0.3.10:2380;ca=/p.crt,"
                                          "deadman-receiver=d.vpn:8443,dashboard-heartbeat=h.vpn:8444;ca=/int.crt,"
                                          "odd=h.vpn:1;bogus=1",
           "XTNL_ETCD_CA": "/etcd-ca.crt"}
    eps = {e.name: e for e in T.cert_endpoints_from_env(env)}
    assert eps["etcd-witness-server"].cafile == "/etcd-ca.crt" and eps["etcd-witness-server"].port == 2379
    assert eps["etcd-witness-peer"].cafile == "/p.crt" and eps["etcd-witness-peer"].port == 2380
    assert eps["dashboard-heartbeat"].cafile == "/int.crt" and eps["deadman-receiver"].cafile is None
    assert eps["odd"].port == 0                                     # an unknown option is reported, not ignored
    warned = T.endpoint_trust_warnings(list(eps.values()), env)
    assert len(warned) == 1 and "deadman-receiver" in warned[0] and "/" not in warned[0].split(":", 1)[0]
    assert T.endpoint_trust_warnings(list(eps.values()), {**env, "SSL_CERT_FILE": "/bundle.crt"}) == []


async def test_endpoint_cert_read_by_verifying_handshake(tmp_path: Path) -> None:
    crt, key = make_cert(tmp_path, "srv", days=10)
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.load_cert_chain(str(crt), str(key))

    async def handle(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        w.close()
    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=ctx)
    port = server.sockets[0].getsockname()[1]
    try:
        trusted = await asyncio.to_thread(endpoint_not_after_s, "127.0.0.1", port, cafile=str(crt))
        assert trusted is not None and abs(trusted - (cert_not_after_s(str(crt)) or 0)) < 1
        untrusted = await asyncio.to_thread(endpoint_not_after_s, "127.0.0.1", port)       # not in the trust store
        assert untrusted is None
        eps = [T.CertEndpoint("dashboard-heartbeat", "127.0.0.1", port), T.CertEndpoint("bad", "", 0),
               T.CertEndpoint("closed", "127.0.0.1", 1)]

        def read(host: str, port: int, **kw: object) -> float | None:
            return endpoint_not_after_s(host, port, cafile=str(crt), timeout_s=1.0)
        readings = await T.read_endpoint_certs(eps, timeout_s=1.0, read=read)
        assert {r.name: r.readable for r in readings} == {"dashboard-heartbeat": True, "bad": False, "closed": False}
        samples = {(x.name, x.labels["cert"]): x.value for x in T.reading_samples(readings, time.time())}
        assert samples[("xtnl_cert_readable", "dashboard-heartbeat")] == 1
        assert samples[("xtnl_cert_expires_in_seconds", "dashboard-heartbeat")] < EXPIRY_WARN_S
        assert samples[("xtnl_cert_readable", "closed")] == 0
    finally:
        server.close()
        await server.wait_closed()


def test_reloading_server_tls_swaps_the_pair_in_place(tmp_path: Path) -> None:
    """K10: a renewed certificate is loaded into the live context; a broken pair keeps the current one."""
    crt, key = make_cert(tmp_path, "srv", days=5)
    tls = ReloadingServerTls(str(crt), str(key))
    assert not tls.reload_if_changed()
    new_crt, new_key = make_cert(tmp_path, "renewed", days=400)
    crt.write_bytes(new_crt.read_bytes())
    key.write_bytes(new_key.read_bytes())
    assert tls.reload_if_changed() and tls.reloads == 1 and tls.issue is None
    other_crt, _ = make_cert(tmp_path, "other", days=400)
    crt.write_bytes(other_crt.read_bytes() + b"\n")          # a certificate that does not match the key
    assert not tls.reload_if_changed() and tls.issue is not None
    crt.write_bytes(new_crt.read_bytes())                     # fixed: loaded again
    assert tls.reload_if_changed() and tls.issue is None


def test_generated_alerts_cover_every_certificate() -> None:
    G = _gen_alerts()
    by = {a.name: a for a in G.ALERTS}
    assert {"etcd-server", "patroni-etcd-client", "redis-proxy", "etcd-witness-peer", "deadman-receiver",
            "dashboard-heartbeat"} <= set(G.CERTS)
    for name in ("XtnlCertExpiry", "XtnlCertUnreadable"):
        assert len({cw.name for cw in by[name].cloudwatch()}) == len(G.NODES) * len(G.CERTS)
    unread = by["XtnlCertUnreadable"]
    assert "xtnl_cert_readable" in unread.expr and f">= {len(G.CERTS)}" in unread.expr
    assert all(cw.missing == "breaching" for cw in unread.cloudwatch())
    # a down node raises XtnlEngineDown once, not one cert alarm per certificate (composite + ActionsSuppressor)
    assert {(cw.name, cw.suppressed_by) for cw in unread.cloudwatch()} == {
        (f"xtnl-cert-unreadable-{n}-{c}", f"xtnl-engine-down-{n}") for n in G.NODES for c in G.CERTS}
    assert "patroni-ca" in G.FILE_CERTS


def test_engine_env_example_watches_the_files_the_services_serve() -> None:
    """K5 review: every FILE_CERTS name is listed in engine.env.example and points at the file its service loads
    (deploy/etcd/etcd.conf.yml, deploy/patroni/patroni.yml, the Redis TLS proxy unit), not a copy nobody serves."""
    G = _gen_alerts()
    root = ROOT
    env = (root / "deploy" / "env" / "engine.env.example").read_text(encoding="utf-8")
    line = next(x for x in env.splitlines() if x.startswith("XTNL_METRICS_CERTS="))
    listed = T.certs_from_env({"XTNL_METRICS_CERTS": line.split("=", 1)[1]})
    assert set(listed) == set(G.FILE_CERTS)
    etcd = (root / "deploy" / "etcd" / "etcd.conf.yml").read_text(encoding="utf-8")
    patroni = (root / "deploy" / "patroni" / "patroni.yml").read_text(encoding="utf-8")
    proxy = (root / "deploy" / "systemd" / "xtnl-redis-tls-proxy.service").read_text(encoding="utf-8")
    served = {"etcd-server": (etcd, "/etc/etcd/pki/${ETCD_NAME}.crt", "etcd-a"),
              "etcd-peer": (etcd, "/etc/etcd/pki/${ETCD_NAME}-peer.crt", "etcd-a"),
              "patroni-rest": (patroni, "/etc/patroni/pki/${PG_MEMBER_NAME}.crt", "pg-a"),
              "pg-server": (patroni, "/etc/patroni/pki/${PG_MEMBER_NAME}-pg.crt", "pg-a"),
              "patroni-etcd-client": (patroni, "/etc/patroni/pki/${PG_MEMBER_NAME}-etcd-client.crt", "pg-a"),
              "patroni-ca": (patroni, "/etc/patroni/pki/ca.crt", "")}
    for name, (text, template, member) in served.items():
        assert template in text, name
        assert listed[name] == template.replace("${ETCD_NAME}", member).replace("${PG_MEMBER_NAME}", member), name
    for name in ("redis-proxy", "redis-proxy-client-ca"):
        assert f":{listed[name]}" in proxy, name
    assert "XTNL_METRICS_CERTS" in (root / "docs" / "runbooks" / "install-node.md").read_text(encoding="utf-8")
    assert G.SPEC_COVERAGE["certificate expiry"] == ("XtnlCertExpiry", "XtnlCertUnreadable")
