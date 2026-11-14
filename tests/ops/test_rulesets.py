"""4.9 ops: the CLI validates `rulesets` documents and rows with the engine's own `parse_ruleset` /
`ruleset_json` (validate offline, add inactive, two-person activate, show)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.ops import cli
from src.ops.model import Actor, OpsUsageError
from src.ops.service import OpsService
from src.oversee.ruleset import parse_ruleset, ruleset_json

from tests.ops.conftest import outcomes
from tests.ops.emulator import OpsPg
from tests.ops.test_cli import Harness

GOOD = {"max_position_risk": {"flag_above_pct": "0.8", "halt_above_pct": "1.2"},
        "marks": {"max_clean_spread_pips": "3"}}
BAD = {"max_daily_loss": {"limit_pct_of_sod_equity": "0"}, "nonsense": {}}
ALICE = Actor("alice", "INC-77 tighten position risk")
TWO = Actor("alice", "INC-77 tighten position risk", approver="bob")


def doc(tmp_path: Path, body: object, name: str = "rules.json") -> str:
    p = tmp_path / name
    p.write_text(json.dumps(body), encoding="utf-8")
    return str(p)


@pytest.fixture
def h() -> Harness:
    return Harness()


def test_validate_is_offline_and_uses_the_engine_parser(h: Harness, tmp_path: Path) -> None:
    code, out, err = h.run("ruleset", "validate", doc(tmp_path, GOOD), env={})   # no DSN: no database touched
    assert code == cli.EXIT_OK, err
    assert "valid: True" in out and "flag>0.8% soft>1.2%" in out and h.closed == 0
    effective = json.loads(out.split("effective: ", 1)[1].splitlines()[0])
    assert parse_ruleset(effective, 1) == parse_ruleset(GOOD, 1)
    code, out, _ = h.run("ruleset", "validate", doc(tmp_path, BAD, "bad.json"), env={})
    assert code == cli.EXIT_REFUSED and "refused: invalid_ruleset" in out
    assert "nonsense: unknown section" in out and "limit_pct_of_sod_equity" in out
    code, _, err = h.run("ruleset", "validate", str(tmp_path / "missing.json"), env={})
    assert code == cli.EXIT_USAGE and "cannot read" in err
    assert h.pg.o.audit == []


async def test_add_stores_the_canonical_ruleset_inactive_and_audits(tmp_path: Path) -> None:
    pg = OpsPg()
    pg.seed_ruleset(1, {}, active=True)
    svc = OpsService(pg, None)
    rep = await svc.ruleset_add(json.dumps(GOOD), "rules.json", ALICE)
    assert rep.outcome == "applied" and rep.fields["version"] == 2 and rep.fields["active_version"] == 1
    row = pg.o.rulesets[2]
    assert row["active"] is False and row["created_by"] == "alice"
    assert row["config"] == json.loads(json.dumps(ruleset_json(parse_ruleset(GOOD, 2))))  # complete, explicit
    assert parse_ruleset(row["config"], 2) == parse_ruleset(GOOD, 2)
    assert pg.o.rulesets[1]["active"] is True                         # activation is a separate step
    assert outcomes(pg) == [("ruleset.add", "applied")] and pg.o.audit[0]["target"] == "2"
    assert pg.statements("LOCK TABLE rulesets IN SHARE ROW EXCLUSIVE MODE") == 1


async def test_add_refuses_an_invalid_document_and_stores_nothing() -> None:
    pg = OpsPg()
    rep = await OpsService(pg, None).ruleset_add(json.dumps(BAD), "bad.json", ALICE)
    assert rep.outcome == "refused" and rep.fields["refused"] == "invalid_ruleset"
    assert "nonsense: unknown section" in rep.fields["detail"]
    assert pg.o.rulesets == {} and outcomes(pg) == [("ruleset.add", "refused")]
    rep = await OpsService(pg, None).ruleset_add("{not json", "x.json", ALICE)
    assert rep.fields["refused"] == "invalid_ruleset" and pg.o.rulesets == {}


async def test_activate_is_two_person_and_refuses_a_row_the_engine_would_reject() -> None:
    pg = OpsPg()
    pg.seed_ruleset(1, {}, active=True)
    pg.seed_ruleset(2, BAD)                                           # written behind the CLI's back
    pg.seed_ruleset(3, GOOD)
    svc = OpsService(pg, None)
    with pytest.raises(OpsUsageError):
        await svc.ruleset_activate(3, ALICE)
    rep = await svc.ruleset_activate(2, TWO)
    assert rep.outcome == "refused" and rep.fields["refused"] == "invalid_ruleset"
    assert pg.o.rulesets[1]["active"] and not pg.o.rulesets[2]["active"]        # nothing changed
    rep = await svc.ruleset_activate(3, TWO)
    assert rep.outcome == "applied" and rep.fields["previous_active"] == [1]
    assert [v for v, r in pg.o.rulesets.items() if r["active"]] == [3]
    rep = await svc.ruleset_activate(3, TWO)
    assert rep.fields["refused"] == "already_active"
    rep = await svc.ruleset_activate(9, TWO)
    assert rep.fields["refused"] == "not_found"
    assert outcomes(pg) == [("ruleset.activate", "refused"), ("ruleset.activate", "applied"),
                            ("ruleset.activate", "refused"), ("ruleset.activate", "refused")]
    assert pg.o.audit[1]["approver"] == "bob"


async def test_show_marks_invalid_rows() -> None:
    pg = OpsPg()
    svc = OpsService(pg, None)
    rep = await svc.ruleset_show(None, limit=20)
    assert rep.fields["active_version"] is None and any("base" in w for w in rep.warnings)
    pg.seed_ruleset(1, GOOD)
    pg.seed_ruleset(2, BAD, active=True)
    rep = await svc.ruleset_show(None, limit=20)
    assert [(r["version"], r["active"], r["valid"]) for r in rep.rows] == [(2, True, False), (1, False, True)]
    assert any("invalid" in w for w in rep.warnings)
    one = await svc.ruleset_show(1, limit=20)
    assert one.fields["valid"] is True and parse_ruleset(one.fields["effective"], 1) == parse_ruleset(GOOD, 1)
    with pytest.raises(OpsUsageError):
        await svc.ruleset_show(7, limit=20)


def test_cli_add_then_activate_end_to_end(h: Harness, tmp_path: Path) -> None:
    path = doc(tmp_path, GOOD)
    code, out, err = h.run("ruleset", "add", path, "--operator", "alice", "--reason", "INC-77 tighten risk")
    assert code == cli.EXIT_OK, err
    assert "version: 1" in out and "ruleset activate 1" in out
    code, _, err = h.run("ruleset", "activate", "1", "--operator", "alice", "--reason", "INC-77 tighten risk")
    assert code == cli.EXIT_USAGE and "--approver" in err
    code, out, err = h.run("ruleset", "activate", "1", "--operator", "alice", "--approver", "bob",
                           "--reason", "INC-77 tighten risk")
    assert code == cli.EXIT_OK and "active_version: 1" in out, err
    code, out, _ = h.run("ruleset", "show")
    assert code == cli.EXIT_OK and "active_version: 1" in out
