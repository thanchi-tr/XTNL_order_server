"""Pure parse of the ARM's local settings (design §B6.7, §B11, §B14, §C2 Local flags; plan S19, RT-M13).

`parse_arm_settings(env)` turns a `Mapping[str, str]` handed in by its caller into `ArmSettings` or a list of problems.
It reads no process state itself and never raises: `optional-bootstrap` (S31) passes the process variables in, and a
problem pages and parks the optional services (never a `WiringError`). The enable flag `XTNL_ARM_ENABLED` is parsed
elsewhere (`src/runtime/optional_settings.py`); the ARM channel credentials (S29) and `XTNL_ARM_CONFIG_DSN` (S33) are
secrets read by their own stages. Problems name the variable and its bounds only, **never its value**. These are
local engine settings only, never from Supabase.

| Variable | Default | Bounds | Meaning |
|---|---|---|---|
| `XTNL_ARM_MIN_GAP_R` | 0.0 | 0-0.5 | envelope step (12) gap floor in R (`EnvelopeParams`) |
| `XTNL_ARM_PRE_OFFSET_S` | 40 | the 15m timing row (30-45) | PRE fires at local `T + p50 − pre_offset` (§B5.2) |
| `XTNL_ARM_EDGE` | `default` | `^[a-z0-9_]{1,32}$` | the single Phase 1 edge code (`arm_positions.edge_code`, §B11) |
| `XTNL_ARM_STOP_READS` | 0 | 0 / 1 | PR9 stop-handle reads (only if PR3 shows manual moves are not streamed) |
| `XTNL_ARM_FAIL_UNRESOLVED_N` | 2 | 1-4 | OPEN bars with no final broker bar at their cap before SERIES FAILED |
| `XTNL_ARM_FAIL_GLOBAL_SERIES_N` | 3 | 2-8 | series of one adapter FAILED together before ADAPTER FAILED |
| `XTNL_ARM_FAIL_MD_DOWN_S` | 900 | 300-3600 | `redis-md` unusable this long while a market is OPEN: ARM FAILED |
| `XTNL_ARM_FAIL_POOL_DOWN_S` | 900 | 300-3600 | the ARM pool failing this long while the main pool works: ARM FAILED |
| `XTNL_ARM_FAIL_REPING_H` | 4 | 1-24 | re-ping cadence while FAILED |
| `XTNL_ARM_FAIL_FRONTIER_POSITIONS_N` | 2 | 1-8 | positions stuck together before a stuck frontier is ARM-global (fewer: one POSITION scope each, S38c) |
| `XTNL_ARM_FAIL_RESTARTS_N` | 3 | 2-10 | unclean engine restarts / takeovers inside the window, while positions are armed, before ARM FAILED (S38f) |
| `XTNL_ARM_FAIL_RESTART_WINDOW_S` | 1800 | 300-7200 | the window of `XTNL_ARM_FAIL_RESTARTS_N` (S38f) |

The series `K` of §B14 is the D6 setting `XTNL_MD_MISMATCH_K` (`src/marketdata/config.py`), not repeated here.
"""
from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Final, TypeVar

from src.marketdata.timing import TIMING_TABLE
from src.marketdata.types import M15
from src.riskmgr.envelope import MIN_GAP_R_DEFAULT, MIN_GAP_R_MAX, MIN_GAP_R_MIN, EnvelopeParams

MIN_GAP_R_VAR: Final = "XTNL_ARM_MIN_GAP_R"
PRE_OFFSET_VAR: Final = "XTNL_ARM_PRE_OFFSET_S"
EDGE_VAR: Final = "XTNL_ARM_EDGE"
STOP_READS_VAR: Final = "XTNL_ARM_STOP_READS"
FAIL_UNRESOLVED_N_VAR: Final = "XTNL_ARM_FAIL_UNRESOLVED_N"
FAIL_GLOBAL_SERIES_N_VAR: Final = "XTNL_ARM_FAIL_GLOBAL_SERIES_N"
FAIL_MD_DOWN_VAR: Final = "XTNL_ARM_FAIL_MD_DOWN_S"
FAIL_POOL_DOWN_VAR: Final = "XTNL_ARM_FAIL_POOL_DOWN_S"
FAIL_REPING_VAR: Final = "XTNL_ARM_FAIL_REPING_H"
FAIL_FRONTIER_POSITIONS_VAR: Final = "XTNL_ARM_FAIL_FRONTIER_POSITIONS_N"
FAIL_RESTARTS_N_VAR: Final = "XTNL_ARM_FAIL_RESTARTS_N"
FAIL_RESTART_WINDOW_VAR: Final = "XTNL_ARM_FAIL_RESTART_WINDOW_S"

EDGE_DEFAULT: Final = "default"
_EDGE_RE: Final = re.compile(r"[a-z0-9_]{1,32}")
_INT_RE: Final = re.compile(r"[+-]?[0-9]+")
_DECIMAL_RE: Final = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)")
_ROW: Final = TIMING_TABLE[M15]
T = TypeVar("T", int, Decimal)


@dataclass(frozen=True, kw_only=True)
class FailSettings:
    """The §B14 error-budget thresholds (`XTNL_ARM_FAIL_*`)."""

    unresolved_n: int = 2
    global_series_n: int = 3
    md_down_s: int = 900
    pool_down_s: int = 900
    reping_h: int = 4
    frontier_positions_n: int = 2
    restarts_n: int = 3
    """Unclean engine restarts and takeovers (leader terms ended `lost` / `init_failed`) within `restart_window_s`,
    while positions are armed, before ARM-global FAILED `unclean_restarts` (plan S38f)."""
    restart_window_s: int = 1800


@dataclass(frozen=True, kw_only=True)
class ArmSettings:
    envelope: EnvelopeParams = field(default_factory=EnvelopeParams)
    pre_offset_s: int = _ROW.pre_offset_default_s
    edge_code: str = EDGE_DEFAULT
    stop_reads: bool = False
    fail: FailSettings = field(default_factory=FailSettings)


def _bounded(env: Mapping[str, str], name: str, default: T, lo: T, hi: T, parse: Callable[[str], T],
             problems: list[str]) -> T:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = parse(raw.strip())
    except (ValueError, InvalidOperation):
        problems.append(f"{name} is not a number in [{lo}, {hi}]")
        return default
    if not lo <= value <= hi:
        problems.append(f"{name} is outside [{lo}, {hi}]")
        return default
    return value


def _int(text: str) -> int:
    if not _INT_RE.fullmatch(text):
        raise ValueError("not an integer")
    return int(text)


def _decimal(text: str) -> Decimal:
    if not _DECIMAL_RE.fullmatch(text):   # no underscores, exponent, NaN or non-ASCII digits
        raise ValueError("not a plain decimal")
    value = Decimal(text)
    if not value.is_finite():
        raise ValueError("not finite")
    return value


def _flag(env: Mapping[str, str], name: str, problems: list[str]) -> bool:
    raw = (env.get(name) or "").strip()
    if raw in ("", "0"):
        return False
    if raw == "1":
        return True
    problems.append(f"{name} must be 0 or 1")
    return False


def _edge(env: Mapping[str, str], problems: list[str]) -> str:
    raw = (env.get(EDGE_VAR) or "").strip()
    if not raw:
        return EDGE_DEFAULT
    if not _EDGE_RE.fullmatch(raw):
        problems.append(f"{EDGE_VAR} must match {_EDGE_RE.pattern}")
        return EDGE_DEFAULT
    return raw


def parse_arm_settings(env: Mapping[str, str]) -> ArmSettings | list[str]:
    """`ArmSettings`, or the list of problems (variable names and bounds only). Never raises."""
    problems: list[str] = []
    min_gap = _bounded(env, MIN_GAP_R_VAR, MIN_GAP_R_DEFAULT, MIN_GAP_R_MIN, MIN_GAP_R_MAX, _decimal, problems)
    pre_offset = _bounded(env, PRE_OFFSET_VAR, _ROW.pre_offset_default_s, _ROW.pre_offset_min_s,
                          _ROW.pre_offset_max_s, _int, problems)
    edge = _edge(env, problems)
    stop_reads = _flag(env, STOP_READS_VAR, problems)
    fail = FailSettings(
        unresolved_n=_bounded(env, FAIL_UNRESOLVED_N_VAR, 2, 1, 4, _int, problems),
        global_series_n=_bounded(env, FAIL_GLOBAL_SERIES_N_VAR, 3, 2, 8, _int, problems),
        md_down_s=_bounded(env, FAIL_MD_DOWN_VAR, 900, 300, 3600, _int, problems),
        pool_down_s=_bounded(env, FAIL_POOL_DOWN_VAR, 900, 300, 3600, _int, problems),
        reping_h=_bounded(env, FAIL_REPING_VAR, 4, 1, 24, _int, problems),
        frontier_positions_n=_bounded(env, FAIL_FRONTIER_POSITIONS_VAR, 2, 1, 8, _int, problems),
        restarts_n=_bounded(env, FAIL_RESTARTS_N_VAR, 3, 2, 10, _int, problems),
        restart_window_s=_bounded(env, FAIL_RESTART_WINDOW_VAR, 1800, 300, 7200, _int, problems),
    )
    if problems:
        return problems
    return ArmSettings(envelope=EnvelopeParams(min_gap_r=min_gap), pre_offset_s=pre_offset, edge_code=edge,
                       stop_reads=stop_reads, fail=fail)
