"""Pure parse of the market-data settings (design §C2 Local flags and Secrets; plan S12, RT-M13).

`parse_md_settings(env)` turns a `Mapping[str, str]` handed in by its caller into `MdSettings` or a list of problems.
This module reads no process state itself: `optional-bootstrap` (S31) passes the process variables in, and a problem
pages and parks the optional services. It never raises; the enable flag itself is parsed elsewhere
(`src/runtime/optional_settings.py`). The variables are documented in `deploy/env/engine.env.example` by the stage that
first reads them from the process (S31 / S36).

Problems name the variable and its bounds only, **never its value**, and `MdSettings`' `repr` hides the URLs (they
carry passwords) and the keys (`Keyring`'s repr shows key ids only).

| Variable | Default | Bounds | Meaning |
|---|---|---|---|
| `XTNL_MD_REDIS_URL` | required | `redis://` or `rediss://` | `md_writer` connection (secret) |
| `XTNL_MD_REDIS_READER_URL` | unset | same | `md_reader` connection (secret; the ops CLI) |
| `XTNL_MD_HMAC_KEY` | required | 64 hex digits | content-hash key (secret, §A13) |
| `XTNL_MD_HMAC_KEY_ID` | required | 1-255 | the current key's id; a new key always takes a higher id (§A13) |
| `XTNL_MD_HMAC_KEY_PREV` | unset | 64 hex digits | retained key during a rotation (secret) |
| `XTNL_MD_HMAC_KEY_PREV_ID` | with PREV | 1-254, below the current id | the retained key's id |
| `XTNL_MD_CROSSCHECK_K` | 1.0 | (0, 10] | cross-check tolerance in median clean spreads (§A4) |
| `XTNL_MD_MISMATCH_K` | 3 | 2-10 | consecutive mismatches / invalid buckets that block a series (§A4, D6: one mismatch only flags, so 1 is refused) |
| `XTNL_MD_ROLLBACK_N` | 8 | 2-64 | rollback-set size per series (§A13) |
| `XTNL_MD_DECISION_SYMBOLS` | 8 | 1-32 | decision-grade symbols per adapter (BARS budget, §A6) |
| `XTNL_MD_BROKER_BAR_SETTLE_S` | 10 | 1-120 | two agreeing reads this far apart make a broker bar final (§A4; PR3: StoneX bars final by +5 s) |
| `XTNL_MD_BROKER_BAR_LAG_MS` | 5000 | 0-300000 | PR3 p99 finality lag (StoneX 1m / 15m, measured 5 s): starts the re-read back-off only |
"""
from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Final, TypeVar

from src.marketdata.hashing import KEY_ID_MAX, KEY_ID_MIN, Keyring, KeyringError
from src.marketdata.timing import TIMING_TABLE
from src.marketdata.types import M15

REDIS_URL_VAR: Final = "XTNL_MD_REDIS_URL"
READER_URL_VAR: Final = "XTNL_MD_REDIS_READER_URL"
HMAC_KEY_VAR: Final = "XTNL_MD_HMAC_KEY"
HMAC_KEY_PREV_VAR: Final = "XTNL_MD_HMAC_KEY_PREV"
HMAC_KEY_ID_VAR: Final = "XTNL_MD_HMAC_KEY_ID"
HMAC_KEY_PREV_ID_VAR: Final = "XTNL_MD_HMAC_KEY_PREV_ID"
CROSSCHECK_K_VAR: Final = "XTNL_MD_CROSSCHECK_K"
MISMATCH_K_VAR: Final = "XTNL_MD_MISMATCH_K"
# D6: one mismatch only flags (a caveat and a metric), so the block threshold is at least 2 (design §B14: 2-10).
MISMATCH_K_MIN: Final = 2
MISMATCH_K_MAX: Final = 10
ROLLBACK_N_VAR: Final = "XTNL_MD_ROLLBACK_N"
DECISION_SYMBOLS_VAR: Final = "XTNL_MD_DECISION_SYMBOLS"
BROKER_BAR_SETTLE_VAR: Final = "XTNL_MD_BROKER_BAR_SETTLE_S"
BROKER_BAR_LAG_VAR: Final = "XTNL_MD_BROKER_BAR_LAG_MS"

SECRET_VARS: Final = (REDIS_URL_VAR, READER_URL_VAR, HMAC_KEY_VAR, HMAC_KEY_PREV_VAR)

_URL_RE: Final = re.compile(r"rediss?://\S+")
_KEY_RE: Final = re.compile(r"[0-9a-fA-F]{64}")
_INT_RE: Final = re.compile(r"[+-]?[0-9]+")
T = TypeVar("T", int, Decimal)


@dataclass(frozen=True, kw_only=True)
class MdSettings:
    redis_url: str = field(repr=False)
    reader_url: str | None = field(default=None, repr=False)
    keyring: Keyring
    crosscheck_k: Decimal = Decimal("1.0")
    mismatch_k: int = 3
    rollback_n: int = TIMING_TABLE[M15].rollback_n_default
    decision_symbols_per_adapter: int = 8
    broker_bar_settle_s: int = 10
    broker_bar_lag_ms: int = 5_000


def _bounded(env: Mapping[str, str], name: str, default: T, lo: T, hi: T, parse: Callable[[str], T],
             problems: list[str], *, lo_open: bool = False) -> T:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    bounds = f"{'(' if lo_open else '['}{lo}, {hi}]"
    try:
        value = parse(raw.strip())
    except (ValueError, InvalidOperation):
        problems.append(f"{name} is not a number in {bounds}")
        return default
    below = value <= lo if lo_open else value < lo
    if below or value > hi:
        problems.append(f"{name} is outside {bounds}")
        return default
    return value


def _int(text: str) -> int:
    if not _INT_RE.fullmatch(text):
        raise ValueError("not an integer")
    return int(text)


def _decimal(text: str) -> Decimal:
    value = Decimal(text)
    if not value.is_finite():
        raise ValueError("not finite")
    return value


def _url(env: Mapping[str, str], name: str, problems: list[str], *, required: bool) -> str | None:
    raw = (env.get(name) or "").strip()
    if not raw:
        if required:
            problems.append(f"{name} is not set")
        return None
    if not _URL_RE.fullmatch(raw):
        problems.append(f"{name} is not a redis:// or rediss:// URL")
        return None
    return raw


def _key(env: Mapping[str, str], name: str, problems: list[str], *, required: bool) -> bytes | None:
    raw = (env.get(name) or "").strip()
    if not raw:
        if required:
            problems.append(f"{name} is not set")
        return None
    if not _KEY_RE.fullmatch(raw):
        problems.append(f"{name} is not 64 hex digits (32 bytes)")
        return None
    return bytes.fromhex(raw)


def _key_id(env: Mapping[str, str], name: str, problems: list[str], *, required: bool) -> int | None:
    raw = (env.get(name) or "").strip()
    if not raw:
        if required:
            problems.append(f"{name} is not set")
        return None
    if not _INT_RE.fullmatch(raw) or not KEY_ID_MIN <= int(raw) <= KEY_ID_MAX:
        problems.append(f"{name} is not an integer in [{KEY_ID_MIN}, {KEY_ID_MAX}]")
        return None
    return int(raw)


def parse_md_settings(env: Mapping[str, str]) -> MdSettings | list[str]:
    """`MdSettings`, or the list of problems (variable names and bounds only). Never raises."""
    problems: list[str] = []
    redis_url = _url(env, REDIS_URL_VAR, problems, required=True)
    reader_url = _url(env, READER_URL_VAR, problems, required=False)
    current = _key(env, HMAC_KEY_VAR, problems, required=True)
    current_id = _key_id(env, HMAC_KEY_ID_VAR, problems, required=True)
    previous = _key(env, HMAC_KEY_PREV_VAR, problems, required=False)
    prev_set = bool((env.get(HMAC_KEY_PREV_VAR) or "").strip())
    previous_id = _key_id(env, HMAC_KEY_PREV_ID_VAR, problems, required=prev_set)
    if not prev_set and (env.get(HMAC_KEY_PREV_ID_VAR) or "").strip():
        problems.append(f"{HMAC_KEY_PREV_ID_VAR} is set without {HMAC_KEY_PREV_VAR}")
    retained: tuple[tuple[int, bytes], ...] = ()
    if previous is not None and previous_id is not None:
        retained = ((previous_id, previous),)
    keyring: Keyring | None = None
    if current is not None and current_id is not None and (not prev_set or retained):
        try:
            keyring = Keyring.of(current_id, current, retained)
        except KeyringError as e:
            names = f"{HMAC_KEY_VAR} / {HMAC_KEY_PREV_VAR} ({HMAC_KEY_ID_VAR} / {HMAC_KEY_PREV_ID_VAR})"
            problems.append(f"{names}: {e}")
    crosscheck_k = _bounded(env, CROSSCHECK_K_VAR, Decimal("1.0"), Decimal(0), Decimal(10), _decimal, problems,
                            lo_open=True)
    mismatch_k = _bounded(env, MISMATCH_K_VAR, 3, MISMATCH_K_MIN, MISMATCH_K_MAX, _int, problems)
    rollback_n = _bounded(env, ROLLBACK_N_VAR, TIMING_TABLE[M15].rollback_n_default,
                          TIMING_TABLE[M15].rollback_n_min, 64, _int, problems)
    symbols = _bounded(env, DECISION_SYMBOLS_VAR, 8, 1, 32, _int, problems)
    settle = _bounded(env, BROKER_BAR_SETTLE_VAR, 10, 1, 120, _int, problems)
    lag = _bounded(env, BROKER_BAR_LAG_VAR, 5_000, 0, 300_000, _int, problems)
    if problems or redis_url is None or keyring is None:
        return problems or ["market-data settings are incomplete"]
    return MdSettings(redis_url=redis_url, reader_url=reader_url, keyring=keyring, crosscheck_k=crosscheck_k,
                      mismatch_k=mismatch_k, rollback_n=rollback_n, decision_symbols_per_adapter=symbols,
                      broker_bar_settle_s=settle, broker_bar_lag_ms=lag)
