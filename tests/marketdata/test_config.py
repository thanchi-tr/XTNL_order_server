"""Pure parse of the market-data settings (plan S12; design §C2). Test values only: no real URL or key."""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.marketdata.config import (
    BROKER_BAR_LAG_VAR,
    CROSSCHECK_K_VAR,
    DECISION_SYMBOLS_VAR,
    HMAC_KEY_ID_VAR,
    HMAC_KEY_PREV_ID_VAR,
    HMAC_KEY_PREV_VAR,
    HMAC_KEY_VAR,
    MISMATCH_K_VAR,
    READER_URL_VAR,
    REDIS_URL_VAR,
    ROLLBACK_N_VAR,
    MdSettings,
    parse_md_settings,
)

KEY_HEX = bytes(range(32)).hex()
PREV_HEX = bytes(range(32, 64)).hex()
SECRET_URL = "redis://md_writer:test-secret-pw@127.0.0.1:56382/0"
BASE = {REDIS_URL_VAR: SECRET_URL, HMAC_KEY_VAR: KEY_HEX, HMAC_KEY_ID_VAR: "7"}
ROTATING = {**BASE, HMAC_KEY_PREV_VAR: PREV_HEX, HMAC_KEY_PREV_ID_VAR: "6"}


def ok(env: dict[str, str]) -> MdSettings:
    out = parse_md_settings(env)
    assert isinstance(out, MdSettings), out
    return out


def problems(env: dict[str, str]) -> list[str]:
    out = parse_md_settings(env)
    assert isinstance(out, list)
    return out


def test_defaults() -> None:
    s = ok(BASE)
    assert s.crosscheck_k == Decimal("1.0") and s.mismatch_k == 3 and s.rollback_n == 8
    assert s.decision_symbols_per_adapter == 8 and s.broker_bar_settle_s == 10 and s.broker_bar_lag_ms == 5_000
    assert s.reader_url is None
    assert s.keyring.key_ids == (7,) and s.keyring.current_id == 7


def test_overrides_and_retained_key() -> None:
    s = ok({**ROTATING, READER_URL_VAR: "rediss://md_reader:pw2@10.0.0.5:6382",
            CROSSCHECK_K_VAR: "1.5", MISMATCH_K_VAR: "4", ROLLBACK_N_VAR: "2", DECISION_SYMBOLS_VAR: " 12 ",
            BROKER_BAR_LAG_VAR: "0"})
    assert s.crosscheck_k == Decimal("1.5") and s.mismatch_k == 4 and s.rollback_n == 2
    assert s.decision_symbols_per_adapter == 12 and s.broker_bar_lag_ms == 0
    assert s.keyring.current_id == 7 and s.keyring.key_ids == (7, 6)
    assert s.keyring.key(6) == bytes(range(32, 64))


def test_repr_hides_urls_and_keys() -> None:
    text = repr(ok(ROTATING))
    for secret in ("test-secret-pw", "md_writer", KEY_HEX, PREV_HEX):
        assert secret not in text


def test_missing_required_values() -> None:
    found = problems({})
    assert any(REDIS_URL_VAR in p for p in found) and any(HMAC_KEY_VAR in p for p in found)
    assert any(HMAC_KEY_ID_VAR in p for p in found)


@pytest.mark.parametrize(("name", "value"), [
    (CROSSCHECK_K_VAR, "0"), (CROSSCHECK_K_VAR, "10.5"), (CROSSCHECK_K_VAR, "NaN"), (CROSSCHECK_K_VAR, "abc"),
    (MISMATCH_K_VAR, "0"), (MISMATCH_K_VAR, "1"), (MISMATCH_K_VAR, "11"), (MISMATCH_K_VAR, "2.5"), (ROLLBACK_N_VAR, "1"), (ROLLBACK_N_VAR, "65"),
    (DECISION_SYMBOLS_VAR, "33"), (BROKER_BAR_LAG_VAR, "300001"), (BROKER_BAR_LAG_VAR, "-1"),
    (HMAC_KEY_VAR, "abcd"), (HMAC_KEY_ID_VAR, "0"), (HMAC_KEY_ID_VAR, "256"), (HMAC_KEY_ID_VAR, "seven"),
    (HMAC_KEY_PREV_ID_VAR, "7"),
    (REDIS_URL_VAR, "http://md_writer:test-secret-pw@host"), (READER_URL_VAR, "md_reader@host"),
])
def test_bad_values_are_problems_that_name_the_variable_never_the_value(name: str, value: str) -> None:
    found = problems({**BASE, name: value})
    assert len(found) == 1 and name in found[0]
    if len(value) >= 3:
        assert value not in found[0]
    assert "test-secret-pw" not in found[0]


@pytest.mark.parametrize(("name", "value"), [
    (HMAC_KEY_PREV_VAR, "zz" * 32), (HMAC_KEY_PREV_VAR, KEY_HEX),          # malformed; the current key again
    (HMAC_KEY_PREV_ID_VAR, "7"), (HMAC_KEY_PREV_ID_VAR, "8"),              # not below the current id
    (HMAC_KEY_PREV_ID_VAR, "0"), (HMAC_KEY_PREV_ID_VAR, ""),               # out of range; PREV without its id
])
def test_bad_rotation_values_are_problems_that_name_the_variable(name: str, value: str) -> None:
    found = problems({**ROTATING, name: value})
    assert len(found) == 1 and name in found[0]
    assert KEY_HEX not in found[0] and PREV_HEX not in found[0] and "test-secret-pw" not in found[0]


def test_prev_id_without_prev_key_is_a_problem() -> None:
    found = problems({**BASE, HMAC_KEY_PREV_ID_VAR: "6"})
    assert len(found) == 1 and HMAC_KEY_PREV_ID_VAR in found[0] and HMAC_KEY_PREV_VAR in found[0]


def test_empty_optional_values_take_defaults() -> None:
    s = ok({**BASE, CROSSCHECK_K_VAR: "", READER_URL_VAR: " ", HMAC_KEY_PREV_VAR: ""})
    assert s.crosscheck_k == Decimal("1.0") and s.reader_url is None and len(s.keyring.key_ids) == 1
