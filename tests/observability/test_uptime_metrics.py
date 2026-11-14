"""The uptime / downtime catch-up series (migration 0012) are in the exporter contract exactly as
deploy/observability/metrics.md defines them, and the registry accepts what the components emit."""
from __future__ import annotations

import re
from pathlib import Path

from src.observability.metrics import CONTRACT, CounterVec, MetricsRegistry
from src.runtime.catchup import ROW_KINDS, CatchupMetrics

ROOT = Path(__file__).resolve().parents[2]
EXPECTED = {
    "xtnl_engine_uptime_seconds": ("gauge", ()),
    "xtnl_suspended_gaps_total": ("counter", ()),
    "xtnl_offline_gap_seconds": ("gauge", ()),
    "xtnl_catchup_runs_total": ("counter", ("adapter", "complete")),
    "xtnl_catchup_rows_total": ("counter", ("adapter", "kind")),
}


def test_contract_and_metrics_md_rows() -> None:
    md = (ROOT / "deploy" / "observability" / "metrics.md").read_text(encoding="utf-8")
    for name, (kind, labels) in EXPECTED.items():
        assert CONTRACT[name] == (kind, labels)
        row = next(ln for ln in md.splitlines() if ln.startswith(f"| `{name}` |"))
        assert f"| {kind} |" in row
        assert re.findall(r"`([a-z_]+)`", row.split("|")[3]) == ["node", *labels]
    rules = (ROOT / "deploy" / "observability" / "prometheus" / "xtnl-alerts.rules.yml").read_text(encoding="utf-8")
    assert not [n for n in EXPECTED if n in rules]          # no alert uses them yet (the catch-up pages itself)


def test_catchup_metrics_render_through_the_registry() -> None:
    m = CatchupMetrics()
    m.runs.inc(adapter="stonex", complete="true")
    for kind in ROW_KINDS:
        m.rows.inc(2, adapter="stonex", kind=kind)
    m.offline_gap_s = 5400.0
    reg = MetricsRegistry("pc-1")
    reg.register("c", m.samples)
    text = reg.render()
    assert reg.dropped == 0
    assert "# TYPE xtnl_catchup_rows_total counter" in text
    assert text.count("xtnl_catchup_rows_total{") == len(ROW_KINDS)
    assert 'xtnl_offline_gap_seconds{node="pc-1"} 5400' in text
    assert isinstance(m.runs, CounterVec)


def test_offline_gap_is_absent_before_the_first_catchup() -> None:
    assert not [s for s in CatchupMetrics().samples() if s.name == "xtnl_offline_gap_seconds"]
