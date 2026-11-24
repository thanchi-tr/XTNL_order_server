"""S31 (RT-M13, PR5, §B14): the full parse of the optional services' settings inside `optional-bootstrap`."""
from __future__ import annotations

import pytest

from src.runtime.optional_settings import (
    MAX_TICK_SYMBOLS_PER_ADAPTER,
    TICK_SYMBOLS_VAR,
    OptionalFlags,
    OptionalSettings,
    arm_channel_problems,
    check_tick_symbols,
    parse_optional_settings,
    parse_tick_symbols,
)

KEY = "0123456789abcdef" * 4
MD = {"XTNL_MD_REDIS_URL": "redis://u:secretpw@127.0.0.1:1/0", "XTNL_MD_HMAC_KEY": KEY, "XTNL_MD_HMAC_KEY_ID": "3"}
ADAPTERS = ("mock", "stonex")


def test_tick_symbols_parse() -> None:
    assert parse_tick_symbols(None, ADAPTERS) == {}
    assert parse_tick_symbols("  ", ADAPTERS) == {}
    assert parse_tick_symbols("mock:SYN1, mock:SYN2,stonex:SYN3,mock:SYN1", ADAPTERS) == {
        "mock": ("SYN1", "SYN2"), "stonex": ("SYN3",)}


@pytest.mark.parametrize("raw", ["SYN1", "mock:", ":SYN1", "mock:SYN 1", "other:SYN1"])
def test_bad_tick_symbols_are_problems(raw: str) -> None:
    got = parse_tick_symbols(raw, ADAPTERS)
    assert isinstance(got, list) and got and all(TICK_SYMBOLS_VAR in p for p in got)


def test_at_most_eight_tick_symbols_per_adapter() -> None:
    ok = ",".join(f"mock:S{i}" for i in range(MAX_TICK_SYMBOLS_PER_ADAPTER))
    assert isinstance(parse_tick_symbols(ok, ADAPTERS), dict)
    got = parse_tick_symbols(ok + ",mock:S99", ADAPTERS)
    assert isinstance(got, list) and "at most 8" in got[0]


def test_instrument_check_keeps_unloaded_adapters_and_drops_unknown_symbols() -> None:
    known = {"mock": frozenset({"SYN1"}), "stonex": frozenset()}
    kept, unknown = check_tick_symbols({"mock": ("SYN1", "SYN2"), "stonex": ("SYN3",)}, known.__getitem__)
    assert kept == {"mock": ("SYN1",), "stonex": ("SYN3",)} and unknown == [("mock", "SYN2")]


def test_full_parse_md_only() -> None:
    got = parse_optional_settings({**MD, TICK_SYMBOLS_VAR: "mock:SYN1"}, OptionalFlags(md_enabled=True), ADAPTERS)
    assert isinstance(got, OptionalSettings) and got.arm is None and got.tick_pairs == (("mock", "SYN1"),)
    assert "secretpw" not in repr(got) and KEY not in repr(got)


def test_full_parse_with_the_arm_and_an_out_of_bounds_fail_value() -> None:
    flags = OptionalFlags(md_enabled=True, arm_enabled=True)
    good = parse_optional_settings({**MD}, flags, ADAPTERS)
    assert isinstance(good, OptionalSettings) and good.arm is not None
    bad = parse_optional_settings({**MD, "XTNL_ARM_FAIL_REPING_H": "99"}, flags, ADAPTERS)
    assert isinstance(bad, list) and any("XTNL_ARM_FAIL_REPING_H" in p for p in bad)


def test_problems_never_carry_a_secret() -> None:
    env = {"XTNL_MD_REDIS_URL": "http://u:topsecret@h/0", "XTNL_MD_HMAC_KEY": "zz" + KEY[2:],
           "XTNL_MD_HMAC_KEY_ID": "999", TICK_SYMBOLS_VAR: "nope"}
    got = parse_optional_settings(env, OptionalFlags(md_enabled=True), ADAPTERS)
    assert isinstance(got, list) and len(got) >= 3
    text = " ".join(got)
    assert "topsecret" not in text and KEY[2:] not in text


def test_arm_channels_both_required() -> None:
    both = {"XTNL_ARM_SMTP_HOST": "smtp.example.test", "XTNL_ARM_SMTP_FROM": "arm@example.test",
            "XTNL_ARM_ALERT_EMAIL_TO": "ops@example.test", "XTNL_ARM_PUSH_PROVIDER": "ntfy",
            "XTNL_ARM_PUSH_URL": "https://ntfy.example.test/arm", "XTNL_ARM_PUSH_TOKEN": "tok-secret-1"}
    assert arm_channel_problems(both) == []
    # design PR4: ntfy with a token, or Pushover; a token-less ntfy topic is one push problem, named, no value
    no_token = {k: v for k, v in both.items() if k != "XTNL_ARM_PUSH_TOKEN"}
    got = arm_channel_problems(no_token)
    assert len(got) == 1 and "push" in got[0] and "XTNL_ARM_PUSH_TOKEN" in got[0]
    assert "ntfy.example.test" not in got[0] and "/arm" not in got[0]
    pushover = {**no_token, "XTNL_ARM_PUSH_PROVIDER": "pushover", "XTNL_ARM_PUSH_URL": "",
                "XTNL_ARM_PUSH_TOKEN": "tok-secret-1", "XTNL_ARM_PUSH_USER": "user-1"}
    assert arm_channel_problems(pushover) == []
    assert not any("tok-secret-1" in p for p in arm_channel_problems({**both, "XTNL_ARM_SMTP_HOST": ""}))
    no_push = {k: v for k, v in both.items() if "PUSH" not in k}
    assert [("push" in p) for p in arm_channel_problems(no_push)] == [True]
    assert len(arm_channel_problems({})) == 2
    # the Oversee channels never count for the ARM
    assert len(arm_channel_problems({"XTNL_SMTP_HOST": "smtp.example.test", "XTNL_PUSH_PROVIDER": "ntfy"})) == 2
