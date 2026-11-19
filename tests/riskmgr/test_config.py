"""Pure parse of the ARM's local settings (plan S19, RT-M13; design §B6.7, §B14, §C2): defaults, every bound,
problems that name the variable and its bounds but never the value, and no raise on any input."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.riskmgr.config import (
    EDGE_VAR,
    FAIL_GLOBAL_SERIES_N_VAR,
    FAIL_MD_DOWN_VAR,
    FAIL_POOL_DOWN_VAR,
    FAIL_REPING_VAR,
    FAIL_UNRESOLVED_N_VAR,
    MIN_GAP_R_VAR,
    PRE_OFFSET_VAR,
    STOP_READS_VAR,
    ArmSettings,
    FailSettings,
    parse_arm_settings,
)
from src.riskmgr.envelope import EnvelopeParams


def ok(env: dict[str, str]) -> ArmSettings:
    out = parse_arm_settings(env)
    assert isinstance(out, ArmSettings), out
    return out


def problems(env: dict[str, str]) -> list[str]:
    out = parse_arm_settings(env)
    assert isinstance(out, list)
    return out


def test_defaults_are_the_design_values() -> None:
    s = ok({})
    assert s == ArmSettings()
    assert s.envelope == EnvelopeParams(Decimal("0.0")) and s.pre_offset_s == 40 and s.edge_code == "default"
    assert s.stop_reads is False
    assert s.fail == FailSettings(unresolved_n=2, global_series_n=3, md_down_s=900, pool_down_s=900, reping_h=4)


def test_overrides() -> None:
    s = ok({MIN_GAP_R_VAR: "0.25", PRE_OFFSET_VAR: " 45 ", EDGE_VAR: "edge_1", STOP_READS_VAR: "1",
            FAIL_UNRESOLVED_N_VAR: "4", FAIL_GLOBAL_SERIES_N_VAR: "2", FAIL_MD_DOWN_VAR: "300",
            FAIL_POOL_DOWN_VAR: "3600", FAIL_REPING_VAR: "24", "XTNL_ARM_SMTP_PASSWORD": "ignored-here"})
    assert s.envelope.min_gap_r == Decimal("0.25") and s.pre_offset_s == 45 and s.edge_code == "edge_1"
    assert s.stop_reads is True
    assert s.fail == FailSettings(unresolved_n=4, global_series_n=2, md_down_s=300, pool_down_s=3600, reping_h=24)


@pytest.mark.parametrize(("var", "good", "bad"), [
    (MIN_GAP_R_VAR, ("0", "0.5", ".25", " 0.1 "), ("-0.01", "0.51", "NaN", "Infinity", "x", "0.1_0", "1e-1",
                                                 "٠.١")),
    (PRE_OFFSET_VAR, ("30", "45"), ("29", "46", "40.0", "x")),
    (FAIL_UNRESOLVED_N_VAR, ("1", "4"), ("0", "5")),
    (FAIL_GLOBAL_SERIES_N_VAR, ("2", "8"), ("1", "9")),
    (FAIL_MD_DOWN_VAR, ("300", "3600"), ("299", "3601")),
    (FAIL_POOL_DOWN_VAR, ("300", "3600"), ("299", "3601")),
    (FAIL_REPING_VAR, ("1", "24"), ("0", "25", "4h")),
])
def test_bounds(var: str, good: tuple[str, ...], bad: tuple[str, ...]) -> None:
    for value in good:
        ok({var: value})
    for value in bad:
        out = problems({var: value})
        assert len(out) == 1 and out[0].startswith(var), out
        assert value not in out[0].removeprefix(var)


def test_flag_and_edge_forms() -> None:
    assert problems({STOP_READS_VAR: "yes"}) == [f"{STOP_READS_VAR} must be 0 or 1"]
    assert ok({STOP_READS_VAR: "0"}).stop_reads is False
    for bad in ("Edge", "a-b", "x" * 33, "edge one"):
        out = problems({EDGE_VAR: bad})
        assert len(out) == 1 and out[0].startswith(EDGE_VAR)


def test_every_problem_is_reported_and_never_a_value() -> None:
    secret = "0.4999999999999999999999123"
    out = problems({MIN_GAP_R_VAR: "9" + secret, PRE_OFFSET_VAR: "99", FAIL_REPING_VAR: "-1"})
    assert len(out) == 3
    assert not any(secret in p or "99" in p.split(" ", 1)[1] for p in out)


@pytest.mark.parametrize("junk", ["\x00", "1e999999", "0x10", "  ", "٣"])
def test_never_raises(junk: str) -> None:
    for var in (MIN_GAP_R_VAR, PRE_OFFSET_VAR, FAIL_REPING_VAR, EDGE_VAR, STOP_READS_VAR):
        parse_arm_settings({var: junk})
