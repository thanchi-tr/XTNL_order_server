"""Metrics registry, exposition and exporter (deploy/observability/metrics.md; IMPLEMENTATION_PLAN 5.6).
No network beyond a loopback socket the test opens itself."""
from __future__ import annotations

import asyncio
import importlib.util
import math
import sys
from pathlib import Path

import pytest

from src.observability.exporter import MetricsExporter, metrics_addr
from src.observability.http import parse_addr
from src.observability.metrics import CONTRACT, CounterVec, MetricsRegistry, Sample, gauge
from src.observability.periodic import PeriodicReader

ROOT = Path(__file__).resolve().parents[2]


def gen_alerts() -> object:
    path = ROOT / "deploy" / "observability" / "gen_alerts.py"
    spec = importlib.util.spec_from_file_location("xtnl_gen_alerts_56", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_the_exporter_contract_is_exactly_metrics_md() -> None:
    """5.6 exports exactly the names, types and labels metrics.md (gen_alerts.DEFINED_METRICS) lists. 7.4 exports
    the last pending row (`xtnl_frames_lost_total`), so the exporter covers every row: whatever the generator still
    lists in PENDING_EXPORT is already exported (no alert uses it until the generator drops it there)."""
    mod = gen_alerts()
    expected = {m.name: (m.type, tuple(m.labels)) for m in mod.DEFINED_METRICS}  # type: ignore[attr-defined]
    ours = {name: (kind, ("node",) + labels) for name, (kind, labels) in CONTRACT.items()}
    assert ours == expected
    assert set(mod.PENDING_EXPORT) <= set(CONTRACT)  # type: ignore[attr-defined]
    assert {m.name for m in mod.METRICS} <= set(CONTRACT)  # type: ignore[attr-defined]
    # 8.2: nothing is pending any more, so the generator's exported set is exactly the exporter's contract.
    assert {m.name for m in mod.METRICS} == set(CONTRACT)  # type: ignore[attr-defined]


def test_frames_lost_is_exported_per_stream_as_a_counter() -> None:
    """7.4: `xtnl_frames_lost_total{stream}` passes the registry's contract check (an unknown name is dropped)."""
    reg = MetricsRegistry("node-a")
    reg.register("term", lambda: [Sample("xtnl_frames_lost_total", 3.0, {"stream": "orders"})])
    text = reg.render()
    assert "# TYPE xtnl_frames_lost_total counter" in text
    assert 'xtnl_frames_lost_total{node="node-a",stream="orders"} 3' in text


def test_exposition_adds_node_and_types_and_exporter_up_is_always_one() -> None:
    reg = MetricsRegistry("node-a")
    reg.register("x", lambda: [Sample("xtnl_bus_queue_depth", 3, {"topic": "orders.internal.dto"}),
                               Sample("xtnl_safe_hold", 0.0)])
    text = reg.render()
    assert 'xtnl_exporter_up{node="node-a"} 1' in text
    assert '# TYPE xtnl_bus_queue_depth gauge' in text
    assert 'xtnl_bus_queue_depth{node="node-a",topic="orders.internal.dto"} 3' in text
    assert 'xtnl_safe_hold{node="node-a"} 0' in text
    assert text.endswith("\n")


def test_contract_breaking_samples_and_failing_collectors_are_dropped_not_exported() -> None:
    reg = MetricsRegistry("n")

    def boom() -> list[Sample]:
        raise RuntimeError("down")
    reg.register("boom", boom)
    reg.register("bad", lambda: [Sample("xtnl_not_in_contract", 1), Sample("xtnl_safe_hold", math.nan),
                                 Sample("xtnl_disk_used_ratio", 0.5), Sample("xtnl_disk_used_ratio", 0.5, {"x": "y"}),
                                 Sample("xtnl_disk_used_ratio", 0.4, {"mount": "/"})])
    names = [(s.name, dict(s.labels)) for s in reg.collect()]
    assert names == [("xtnl_exporter_up", {}), ("xtnl_disk_used_ratio", {"mount": "/"})]
    assert reg.dropped == 5


def test_unknown_values_are_absent_never_a_fake_zero() -> None:
    assert gauge("xtnl_clock_offset_seconds", None) == []
    assert gauge("xtnl_safe_hold", False) == [Sample("xtnl_safe_hold", 0.0, {})]


def test_counter_vec_is_cumulative_and_only_for_contract_counters() -> None:
    c = CounterVec("xtnl_ingest_events_total")
    c.inc(adapter="mock", stream="prices")
    c.inc(2, adapter="mock", stream="prices")
    assert c.value(adapter="mock", stream="prices") == 3
    assert c.samples() == [Sample("xtnl_ingest_events_total", 3.0, {"adapter": "mock", "stream": "prices"})]
    with pytest.raises(ValueError):
        CounterVec("xtnl_bus_queue_depth")
    with pytest.raises(ValueError):
        c.inc(-1, adapter="mock", stream="prices")


async def _http(port: int, request: bytes) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(request)
    await writer.drain()
    data = await reader.read()
    writer.close()
    return data


async def test_exporter_serves_get_metrics_and_refuses_writes() -> None:
    reg = MetricsRegistry("node-b")
    exporter = MetricsExporter(reg, "127.0.0.1", 0)
    port = await exporter.start()
    try:
        ok = await _http(port, b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        assert ok.startswith(b"HTTP/1.1 200") and b"text/plain; version=0.0.4" in ok
        assert b'xtnl_exporter_up{node="node-b"} 1' in ok
        post = await _http(port, b"POST /metrics HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n")
        assert post.startswith(b"HTTP/1.1 405")
        missing = await _http(port, b"GET /nope HTTP/1.1\r\n\r\n")
        assert missing.startswith(b"HTTP/1.1 404")
        bad = await _http(port, b"garbage\r\n\r\n")
        assert bad.startswith(b"HTTP/1.1 400")
        assert exporter.scrapes == 1
    finally:
        await exporter.stop()


def test_metrics_addr_defaults_to_the_contract_port_on_loopback() -> None:
    assert metrics_addr({}) == ("127.0.0.1", 9464)
    assert metrics_addr({"XTNL_METRICS_ADDR": "10.0.1.10:9464"}) == ("10.0.1.10", 9464)
    assert metrics_addr({"XTNL_METRICS_ADDR": "off"}) is None
    assert parse_addr("[fd00::1]:9", 1) == ("fd00::1", 9)
    with pytest.raises(ValueError):
        metrics_addr({"XTNL_METRICS_ADDR": "h:notaport"})


async def test_periodic_reader_values_age_out_after_failures() -> None:
    now = [0.0]
    calls = [0]

    async def read() -> list[Sample]:
        calls[0] += 1
        if calls[0] > 1:
            raise ConnectionError("down")
        return [Sample("xtnl_safe_hold", 1.0)]
    r = PeriodicReader("pg", read, interval_s=5.0, monotonic=lambda: now[0])
    assert r.collect() == []                              # never read: absent
    assert await r.read_once()
    assert r.collect() == [Sample("xtnl_safe_hold", 1.0)]
    now[0] = 10.0
    assert not await r.read_once()
    assert r.collect() == [Sample("xtnl_safe_hold", 1.0)]  # still within 3 x interval
    now[0] = 16.0
    assert r.collect() == []                               # stale: absent, never presented as current


async def test_periodic_reader_bounds_a_hung_read() -> None:
    async def hang() -> list[Sample]:
        await asyncio.Event().wait()
        return []
    r = PeriodicReader("hang", hang, interval_s=1.0, timeout_s=0.05)
    assert not await r.read_once()
    assert r.failures == 1
