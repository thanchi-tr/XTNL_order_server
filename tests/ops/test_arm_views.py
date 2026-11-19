"""The S35 ARM views (plan S35): `arm show`, `arm decisions`, `arm config resolve --position X`, and the registry
check (`arm registry check --against-db` fails on a missing pinned version; the reviewed `PINNED_TOKENS` all load).
The view SQL selects no level, unit, R value or ratio, and output escapes control characters. Synthetic ids only."""
from __future__ import annotations

import io
import json
import re
from typing import Any

from src.armconfig.chain import SeenRows
from src.armconfig.installer import document_json
from src.db.pg import PgConnection
from src.ops import arm_sql as A
from src.ops import cli
from src.ops.arm import ArmContext
from src.ops.arm_registry import (
    PINNED_TOKENS,
    PinTokens,
    missing_tokens,
    pin_tokens,
    pinned_registry_problems,
    token,
)
from src.ops.arm_view import ArmViews, document_tokens
from src.ops.service import OpsService
from src.riskmgr.registry import REGISTRY, ArmRegistry
from src.riskmgr.strategies import BREAK_EVEN_ON_RISK_CLOSE_V1

from tests.armconfig.docs import assignment_row, parsed, raw_doc, route_row, strategy_row
from tests.ops.arm_view_fakes import ArmViewPg, Decision, PositionView
from tests.ops.emulator import NOW


def views(pg: ArmViewPg, registry: ArmRegistry = REGISTRY) -> ArmViews:
    return ArmViews(OpsService(pg, None, clock_ms=lambda: NOW, arm=ArmContext(registry=registry)).arm)


def install(pg: ArmViewPg, raw: dict[str, Any]) -> str:
    doc = parsed(raw)
    seen = SeenRows.build(doc.row_hashes().items())
    pg.a.cache = (doc.doc_hash, doc.schema_version, document_json(doc.schema_version, doc.rows),
                  json.dumps(dict(seen.rows)), seen.head, seen.count, NOW - 3_600_000)
    return doc.doc_hash


# ============================================================================================== the SQL
FORBIDDEN_COLUMNS = ("entry_level", "s0_level", "pinned_units", "proposed_stop", "current_risk_r", "first_bar_ratio",
                     "strategy_state", "strategy_params", "level_", "target_level")


def test_view_sql_selects_no_level_unit_or_r_value() -> None:
    for sql in (A.SQL_POSITIONS_VIEW, A.SQL_DECISIONS_VIEW, A.SQL_PINNED_VERSIONS, A.SQL_DECISION_VERSIONS):
        select = sql.split(" FROM ")[0]
        for col in FORBIDDEN_COLUMNS:
            assert col not in select, (col, sql[:60])
        assert re.search(r"\bpre_basis\b(?! IS NOT NULL)", select) is None
        assert not re.search(r"\b(INSERT|UPDATE|DELETE|MERGE)\b", sql)


# ============================================================================================== arm show / decisions
async def test_arm_show_lists_live_positions_with_escaped_text() -> None:
    pg = ArmViewPg()
    pg.view_positions += [PositionView(state_reason="bad\x1b[31mreason\n"), PositionView(position_id="P2",
                                                                                          state="CLOSED")]
    rep = await views(pg).show(None, include_ended=False, limit=50)
    assert rep.fields["positions"] == 1 and rep.fields["states"] == "TRACKING=1"
    row = rep.rows[0]
    assert row["position"] == "mock:P1" and row["pin"] == "break_even_on_risk_close:1" and row["cursor_hash"]
    assert row["state_reason"] == "bad\\x1b[31mreason\\x0a"
    every = await views(pg).show(None, include_ended=True, limit=50)
    assert every.fields["positions"] == 2
    one = await views(pg).show("mock:P2", include_ended=False, limit=50)      # a named position shows when ended
    assert one.fields["positions"] == 1 and one.rows[0]["state"] == "CLOSED"
    none = await views(pg).show("mock:P9", include_ended=False, limit=50)
    assert none.fields["positions"] == 0 and none.warnings


async def test_arm_decisions_newest_first_without_levels() -> None:
    pg = ArmViewPg()
    pg.decisions += [Decision(decision_id=1, bar_end_ms=NOW - 1_800_000, notice_id=0),
                     Decision(decision_id=2, bar_end_ms=NOW - 900_000, caveats=["delayed"], notice_id=12),
                     Decision(decision_id=3, position_id="P2", bar_end_ms=NOW - 900_000)]
    rep = await views(pg).decisions("mock:P1", from_ms=None, to_ms=None, limit=10)
    assert [r["decision"] for r in rep.rows] == [2, 1]
    assert rep.rows[1]["notice"] == "suppressed" and rep.rows[0]["notice"] == 12
    assert rep.rows[0]["caveats"] == ["delayed"] and rep.rows[0]["envelope"].startswith("v1:")
    ranged = await views(pg).decisions(None, from_ms=NOW - 1_000_000, to_ms=NOW, limit=10)
    assert sorted(r["decision"] for r in ranged.rows) == [2, 3]


# ============================================================================================== config resolve
async def test_config_resolve_names_the_assignment_at_arming_and_now() -> None:
    pg = ArmViewPg()
    raw = raw_doc(strategies=[strategy_row(1), strategy_row(2, version=2)],
                  assignments=[assignment_row(1, effective_from_ms=1_000),
                               assignment_row(2, symbol="SYN.A", version=2, effective_from_ms=NOW - 3_600_000)],
                  route_policy=[route_row(route="*")])
    doc_hash = install(pg, raw)
    pg.view_positions.append(PositionView(config_doc_hash=doc_hash, armed_at_ms=NOW - 86_400_000))
    rep = await views(pg).config_resolve("mock:P1")
    assert rep.outcome == "ok", rep.fields
    at_arming, now = rep.sections["at_arming"][0], rep.sections["now"][0]
    assert at_arming["assignment"] == "assignments:1" and at_arming["matches_pin"] and at_arming["scope"] == "wildcard"
    assert now["assignment"] == "assignments:2" and now["strategy"] == "be_a:v2" and not now["matches_pin"]
    assert now["scope"] == "symbol" and rep.fields["route_policy"] == "default"
    assert any("never changes an armed position" in w for w in rep.warnings)


async def test_config_resolve_refusals() -> None:
    pg = ArmViewPg()
    missing = await views(pg).config_resolve("mock:P1")
    assert missing.outcome == "refused" and missing.fields["refused"] == "not_armed"
    pg.view_positions.append(PositionView())
    uncached = await views(pg).config_resolve("mock:P1")
    assert uncached.fields["refused"] == "no_cached_document"
    install(pg, raw_doc(route_policy=[route_row(route="*")]))
    other = await views(pg).config_resolve("mock:P1")
    assert other.outcome == "ok" and any("not the one this position was armed under" in w for w in other.warnings)


# ============================================================================================== registry check
def test_pinned_tokens_all_load_in_this_registry() -> None:
    assert pinned_registry_problems() == []
    assert PINNED_TOKENS <= set(REGISTRY.registry_tokens())
    bare = ArmRegistry(strategies=[BREAK_EVEN_ON_RISK_CLOSE_V1], guards=[])
    assert pinned_registry_problems(bare) == ["first_bar:1 is pinned for release but not in the registry"]


def test_pin_tokens_read_the_stored_pin_and_fail_closed() -> None:
    p = PositionView()
    got = pin_tokens(p.strategy_kind, p.strategy_impl_version, p.guards_json, p.indicators_json, where="x")
    assert got.tokens == {"break_even_on_risk_close:1", "first_bar:1", "ATR:1"} and not got.unreadable
    bad = pin_tokens("k", 0, "not json", '[{"kind":"ATR"}]', where="row", into=PinTokens())
    assert bad.unreadable == ["row: strategy kind / version", "row: guards", "row: indicator_specs entry"]
    assert missing_tokens({"ATR:1", "ATR:2"}) == ["ATR:2"]


async def test_registry_check_passes_when_every_referenced_version_loads() -> None:
    pg = ArmViewPg()
    pg.view_positions += [PositionView(), PositionView(position_id="P2", state="CLOSED")]
    pg.decisions.append(Decision())
    install(pg, raw_doc(route_policy=[route_row(route="*")]))
    rep = await views(pg).registry_check()
    assert rep.outcome == "ok" and rep.fields["missing"] is None and rep.fields["positions"] == 2
    assert set(rep.fields["referenced"]) == {"ATR:1", "break_even_on_risk_close:1", "first_bar:1"}


async def test_registry_check_fails_on_a_missing_pinned_version() -> None:
    pg = ArmViewPg()
    pg.view_positions.append(PositionView(position_id="P2", state="CLOSED", strategy_impl_version=2))
    rep = await views(pg).registry_check()
    assert rep.outcome == "refused" and rep.fields["refused"] == "registry_missing"
    assert rep.fields["missing"] == ["break_even_on_risk_close:2"]
    assert rep.rows == [{"missing": "break_even_on_risk_close:2", "referenced_by": "position"}]
    guards = ArmViewPg()
    guards.view_positions.append(PositionView(guards_json='[{"kind":"first_bar","params":{},"version":7}]'))
    got = await views(guards).registry_check()
    assert got.fields["missing"] == ["first_bar:7"] and got.rows[0]["referenced_by"] == "live position,position"


async def test_registry_check_covers_decisions_documents_and_unreadable_pins() -> None:
    pg = ArmViewPg()
    pg.decisions.append(Decision(strategy_impl_version=3))
    install(pg, raw_doc(strategies=[strategy_row(impl_version=5)], route_policy=[route_row(route="*")]))
    rep = await views(pg).registry_check()
    assert rep.fields["missing"] == ["break_even_on_risk_close:3", "break_even_on_risk_close:5"]
    by = {r["missing"]: r["referenced_by"] for r in rep.rows}
    assert by == {"break_even_on_risk_close:3": "decision", "break_even_on_risk_close:5": "installed document"}
    bad = ArmViewPg()
    bad.view_positions.append(PositionView(indicators_json="{oops"))
    got = await views(bad).registry_check()
    assert got.outcome == "refused" and got.fields["refused"] == "pin_unreadable"
    assert got.sections["unreadable"][0]["where"].endswith("indicator_specs")


def test_document_tokens_include_required_indicators() -> None:
    doc = parsed(raw_doc(route_policy=[route_row(route="*")]))
    got = document_tokens(document_json(doc.schema_version, doc.rows), REGISTRY)
    assert got.tokens == {"break_even_on_risk_close:1", "first_bar:1", "ATR:1"} and not got.unreadable
    assert document_tokens("not json", REGISTRY).unreadable == ["cached document: not JSON"]


# ============================================================================================== CLI
def test_cli_views_parse_and_exit_codes() -> None:
    pg = ArmViewPg()
    pg.view_positions.append(PositionView(strategy_impl_version=9))

    async def connect(dsn: str) -> tuple[PgConnection, cli.Closer]:
        async def close() -> None:
            return None

        return pg, close

    def run(*argv: str) -> tuple[int, str]:
        out = io.StringIO()
        code = cli.main(list(argv), environ={"ORDER_SERVER_DSN": "postgresql://u@h/db"}, connect=connect,
                        etcd_factory=lambda _e: None, out=out, err=io.StringIO(), arm_context=ArmContext())
        return code, out.getvalue()

    code, out = run("arm", "show")
    assert code == cli.EXIT_OK and "position=mock:P1" in out
    code, out = run("arm", "decisions", "mock:P1", "--limit", "5")
    assert code == cli.EXIT_OK and "decisions: 0" in out
    code, out = run("arm", "registry", "check", "--against-db")
    assert code == cli.EXIT_REFUSED and "break_even_on_risk_close:9" in out
    assert run("arm", "registry", "check")[0] == cli.EXIT_USAGE        # --against-db is required
    assert run("arm", "show", "not-a-target")[0] == cli.EXIT_USAGE
    assert run("arm", "decisions", "--limit", "0")[0] == cli.EXIT_USAGE
    code, out = run("arm", "config", "resolve", "--position", "mock:P1")
    assert code == cli.EXIT_REFUSED and "no_cached_document" in out
    code, out = run("md", "show")
    assert code == cli.EXIT_REFUSED and "no_md_reader" in out


async def test_registry_check_never_prints_a_kind_with_control_characters() -> None:
    assert token("ATR", 1) == "ATR:1" and token("first_bar", 2) == "first_bar:2"
    for bad in ("bad\x1b[2Jkind", "a-b", "a b", "x" * 65, ""):
        assert token(bad, 1) is None
    pg = ArmViewPg()
    pg.decisions.append(Decision(strategy_kind="evil\x1b[2Jkind"))
    pg.view_positions.append(PositionView(guards_json='[{"kind":"g\\u001b]0;x","params":{},"version":1}]'))
    rep = await views(pg).registry_check()
    assert rep.outcome == "refused" and rep.fields["refused"] == "pin_unreadable"
    for name in ("referenced", "live_pins", "decision_versions", "document_versions"):
        assert not any("evil" in t or t.startswith("g") for t in rep.fields[name])   # never a token: unreadable
    shown = [*map(str, rep.fields.values()), *(str(v) for r in rep.rows for v in r.values()),
             *(str(v) for sec in rep.sections.values() for r in sec for v in r.values())]
    assert not any("\x1b" in t for t in shown)                                   # no raw control byte anywhere
    where = [u["where"] for u in rep.sections["unreadable"]]
    assert any("evil" in w for w in where) and any(w.endswith("guards entry") for w in where)
