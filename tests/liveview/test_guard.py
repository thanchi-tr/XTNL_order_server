"""The privacy guard (docs/design/live-view.md s4.4.5, s6 "privacy guard"): closed allow-lists of slot, id and
payload names, the token deny check, typed slot values ("(not shown)" keeps the item, a bad name drops it), level
slots only in the order / bracket / position kinds, no float in a payload, and always on (raises under -O)."""
from __future__ import annotations

import dataclasses
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import src.dto.entities as entities
from src.liveview.events import (
    DECIMAL_IN_TEXT,
    ID_NAMES,
    KINDS,
    LEVEL_KIND_PREFIXES,
    LEVEL_SLOTS,
    NOT_SHOWN,
    PAYLOAD_KEYS,
    SLOT_TYPES,
    SUFFIXES,
    Event,
    PrivacyError,
    assert_safe,
    check_value,
    guard_payload,
    name_denied,
    render,
)
from src.liveview.stories import DayModel, Series, Story

ROOT = Path(__file__).resolve().parents[2]
T0 = 1_795_000_000_000

SAMPLE: dict[str, Any] = {
    "symbol": "EURUSD", "side": "BUY", "id": "stonex:1039708845", "code": "risk_unknown", "count": 7,
    "time": T0, "duration": 65_000, "level": "1.10203", "operator": "j.smith", "command": "halt.clear",
    "suffixes": [], "bool": True,
}


def ev(kind: str, slots: dict[str, Any] | None = None, ids: dict[str, Any] | None = None, **kw: Any) -> Event:
    return Event(kind=kind, slots=dict(slots or {}), at_ms=T0, clock=kw.pop("clock", "broker"), detected_ms=T0,
                 ids=dict(ids or {}), **kw)


def dto_fields() -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for name in dir(entities):
        obj = getattr(entities, name)
        if isinstance(obj, type) and dataclasses.is_dataclass(obj) and obj.__module__ == entities.__name__:
            out[name] = {f.name for f in dataclasses.fields(obj)}
    return out


ALL_DTO_FIELDS = sorted(set().union(*dto_fields().values()))
#: DTO field names a page item may carry, by where: (slot, id, payload key).
EXPECTED_ALLOWED = {
    "account_id": (False, True, True), "adapter_id": (False, True, False), "broker_order_id": (False, True, False),
    "client_order_id": (False, True, False), "kind": (True, False, True), "linked_position_id": (False, True, False),
    "position_id": (False, True, False), "side": (True, False, True), "status": (True, False, True),
    "symbol": (True, True, True), "take_profit": (True, False, True),
}
MONEY_AND_SIZE = {"balance", "realised_pnl_today", "equity", "margin_used", "free_margin", "unrealised_pnl", "amount",
                  "currency", "units", "ordered_units", "remaining_units", "raw_qty", "accrued_swap", "commission",
                  "bid", "ask", "spread_pips", "entry_price", "contract_size", "point_size", "min_qty",
                  "price_increment", "min_stop_distance", "pnl_ccy", "swap_booked_to_balance"}


def test_the_dto_classes_are_all_seen() -> None:
    assert {"OrderDTO", "PositionDTO", "AccountDTO", "AccountLive", "CashFlowDTO", "PriceDTO",
            "InstrumentSpec"} <= set(dto_fields())


def test_every_money_size_and_market_data_field_is_denied_by_name() -> None:
    assert MONEY_AND_SIZE <= set(ALL_DTO_FIELDS)
    for f in MONEY_AND_SIZE:
        assert name_denied(f), f


def test_the_allowed_dto_fields_are_exactly_the_pinned_set() -> None:
    got = {f: (f in SLOT_TYPES, f in ID_NAMES, f in PAYLOAD_KEYS) for f in ALL_DTO_FIELDS
           if not name_denied(f) and (f in SLOT_TYPES or f in ID_NAMES or f in PAYLOAD_KEYS)}
    assert got == EXPECTED_ALLOWED


@pytest.mark.parametrize("field", ALL_DTO_FIELDS)
def test_each_dto_field_is_rejected_or_accepted_as_listed(field: str) -> None:
    slot_ok, id_ok, payload_ok = EXPECTED_ALLOWED.get(field, (False, False, False))
    # as a slot
    kind = "order.status" if field not in LEVEL_SLOTS else "order.placed"
    value = SAMPLE[SLOT_TYPES[field]] if field in SLOT_TYPES else "x"
    if slot_ok:
        out = assert_safe(ev(kind, {field: value}))
        assert out.slots[field] == value
    else:
        with pytest.raises(PrivacyError):
            assert_safe(ev(kind, {field: value}))
    # as an id
    if id_ok:
        out = assert_safe(ev("order.status", ids={field: "ABC123"}))
        assert out.ids[field] == "ABC123"
    else:
        with pytest.raises(PrivacyError):
            assert_safe(ev("order.status", ids={field: "ABC123"}))
    # as a payload key
    if payload_ok:
        assert guard_payload({field: "BUY"}) == {field: "BUY"} or field in LEVEL_SLOTS
    else:
        with pytest.raises(PrivacyError):
            guard_payload({field: "BUY"})
        with pytest.raises(PrivacyError):
            guard_payload({"state": [{field: "BUY"}]})


def test_whole_token_matching() -> None:
    assert not name_denied("task") and not name_denied("checkpoint") and not name_denied("subject_id")
    assert name_denied("ask") and name_denied("stop_point") and name_denied("PRICE") and name_denied("x_error")
    assert not name_denied("dlq_reason")          # the DLQ field: no token is `error`


# ---- events --------------------------------------------------------------------------------------------------------
def test_a_dlq_event_passes() -> None:
    out = assert_safe(ev("dlq.parked", {"event_type": "ORDER", "id": "stonex:991", "reason": "schema_invalid",
                                        "extra": ["reason"]},
                         ids={"dlq_id": "17", "entity_id": "991", "adapter_id": "stonex"}, clock="server"))
    assert out.title == "ORDER stonex:991 parked in DLQ (schema_invalid)"
    assert out.category == "dlq" and out.severity == "warn"
    assert guard_payload({"dlq": {"QUEUED": 2, "QUARANTINED": 1}, "dlq_status": "QUEUED", "dlq_queued": 2})


@pytest.mark.parametrize(("kind", "slot", "bad"), [
    ("dlq.parked", "id", "1.25"),
    ("order.status", "id", "ord,1,5"),
    ("rule.transition", "reason", "loss_1.5_pct"),
    ("rule.transition", "rule", "r2,5"),
    ("operator.action", "operator", "j1.5x"),
    ("order.status", "symbol", "EUR1.2"),
    ("order.status", "status", "FILLED 1.0"),
    ("halt.recorded", "to_halt", "SOFT;DROP"),
    ("operator.action", "command", "Halt.Clear"),
    ("order.status", "side", "buy"),
])
def test_a_bad_value_becomes_not_shown_and_the_event_is_kept(kind: str, slot: str, bad: str) -> None:
    out = assert_safe(ev(kind, {slot: bad}, clock="server"))
    assert out.slots[slot] == NOT_SHOWN
    assert bad not in out.title
    if "{" + slot + "}" in KINDS[kind].template or slot in ("symbol", "side", "id"):
        assert NOT_SHOWN in out.title


@pytest.mark.parametrize("name", ["position_id", "broker_order_id", "subject_id", "symbol", "adapter_id"])
def test_a_decimal_in_an_id_becomes_not_shown(name: str) -> None:
    out = assert_safe(ev("order.status", ids={name: "12.5"}))
    assert out.ids[name] == NOT_SHOWN


def test_a_non_string_id_becomes_not_shown_except_an_int_epoch() -> None:
    out = assert_safe(ev("engine.leader", {"node": "pc1", "epoch": 7}, ids={"epoch": 7, "term_id": 12},
                         clock="server"))
    assert out.ids == {"epoch": "7", "term_id": NOT_SHOWN}


@pytest.mark.parametrize("slots", [{"foo": "x"}, {"units": 1}, {"entry_price": "1.1"}, {"balance": "1"},
                                   {"price": "1"}, {"error": "x"}, {"detail": "x"}])
def test_an_unknown_or_denied_slot_raises(slots: dict[str, Any]) -> None:
    with pytest.raises(PrivacyError):
        assert_safe(ev("order.status", slots))


@pytest.mark.parametrize("ids", [{"foo": "x"}, {"price": "1"}, {"units": "1"}, {"payload": "x"}, {"order_kind": "x"}])
def test_an_unknown_or_denied_id_raises(ids: dict[str, Any]) -> None:
    with pytest.raises(PrivacyError):
        assert_safe(ev("order.status", ids=ids))


def test_an_unknown_kind_story_key_category_or_clock_raises() -> None:
    with pytest.raises(PrivacyError):
        assert_safe(ev("order.teleported"))
    with pytest.raises(PrivacyError):
        assert_safe(ev("order.status", story="run:abc"))
    with pytest.raises(PrivacyError):
        assert_safe(ev("order.status", parent="pos:a b"))
    with pytest.raises(PrivacyError):
        assert_safe(ev("order.status", category="money"))
    with pytest.raises(PrivacyError):
        assert_safe(ev("order.status", clock="wall"))
    with pytest.raises(PrivacyError):
        render("nope", {})


def test_operator_and_command_slots() -> None:
    out = assert_safe(ev("operator.action", {"operator": "j.smith", "command": "halt.clear", "outcome": "applied"},
                         clock="server"))
    assert out.title == "Operator j.smith: halt.clear (applied)"
    assert check_value("operator", "j.smith") and not check_value("id", "j.smith")
    assert check_value("command", "halt.clear") and not check_value("code", "halt.clear")
    assert check_value("command", "arm.config.accept") and not check_value("command", "a.b.c.d.e")


def test_level_slots_only_in_the_order_bracket_and_position_kinds() -> None:
    out = assert_safe(ev("order.placed", {"kind": "LIMIT", "entry": "1.10203", "symbol": "EURUSD", "side": "SELL",
                                          "id": "stonex:1"}))
    assert out.title == "EURUSD SELL stonex:1: LIMIT order placed at 1.10203"
    for kind in KINDS:
        if kind.startswith(LEVEL_KIND_PREFIXES):
            continue
        for slot in LEVEL_SLOTS:
            with pytest.raises(PrivacyError):
                assert_safe(ev(kind, {slot: "1.1"}, clock="server"))


@pytest.mark.parametrize("bad", [1.1, "1.1.1", "abc", "1e5", True, "NaN", "Infinity", "--1", 7.0])
def test_a_bad_level_becomes_not_shown(bad: Any) -> None:
    out = assert_safe(ev("position.stop_set", {"to_level": bad}))
    assert out.slots["to_level"] == NOT_SHOWN


def test_long_titles_are_shortened_not_dropped() -> None:
    slots = {"symbol": "S" * 40, "side": "SELL", "id": "I" * 64, "leg": "C" * 80, "reviewed": 7, "anomalies": 3,
             "offline": 3_600_000, "target": "T" * 64, "by": "o" * 64, "reason": "R" * 80, "rule": "U" * 80,
             "level": "L" * 80, "account": "A" * 64, "subject": "B" * 64, "hold_count": 4,
             "extra": list(SUFFIXES)}
    out = assert_safe(ev("position.closed_by", slots))
    assert len(out.title) <= 200 and out.title.endswith("…")
    assert out.title.startswith("S" * 40 + " SELL " + "I" * 23 + "…")
    assert out.housekeeping is False


def test_rendered_titles_never_carry_a_decimal_outside_the_level_slots() -> None:
    fields = re.compile(r"\{(\w+)\}")
    for kind, spec in KINDS.items():
        names = set(fields.findall(spec.template)) | {"symbol", "side", "id"}
        for suffix in SUFFIXES.values():
            names |= set(fields.findall(suffix))
        names -= {"p"}
        slots: dict[str, Any] = {}
        for n in names:
            stype = SLOT_TYPES[n]
            if stype == "level" and not kind.startswith(LEVEL_KIND_PREFIXES):
                continue
            slots[n] = SAMPLE[stype]
        slots["extra"] = list(SUFFIXES)
        out = assert_safe(ev(kind, slots, clock="server"))
        assert out.slots == {k: v for k, v in slots.items()}, kind        # every sample value is valid
        title = out.title
        for lvl in LEVEL_SLOTS:
            if lvl in slots:
                title = title.replace(str(slots[lvl]), "")
        assert not DECIMAL_IN_TEXT.search(title), (kind, out.title)
        assert len(out.title) <= 200


def test_every_kind_uses_only_known_slots() -> None:
    fields = re.compile(r"\{(\w+)\}")
    for kind, spec in KINDS.items():
        for n in fields.findall(spec.template):
            assert n == "p" or n in SLOT_TYPES, (kind, n)
            assert not name_denied(n)
    for name in SLOT_TYPES:
        assert not name_denied(name), name


# ---- payloads ------------------------------------------------------------------------------------------------------
def _story() -> dict[str, Any]:
    st = Story(key="pos:stonex:1", kind="trade", state="open", first_ms=T0, last_ms=T0, symbol="EURUSD", side="BUY",
               ids={"position_id": "1"}, counts={"orders": 2}, header="EURUSD BUY 1: open")
    st.levels = {"entry": "1.1", "stop": "1.0", "take_profit": None}
    st.history.append({"at_ms": T0, "entry": "1.1", "stop": "1.0", "take_profit": None})
    st.badges["0f8fad5b-d9cb-469f-a165-70867728950e.12"] = ["delivered"]
    return st.to_json()


def _day() -> dict[str, Any]:
    d = DayModel(T0 - 3_600_000)
    d.streams = {"stonex": {"prices": [{"from_ms": T0, "to_ms": None, "state": "up"}]}}
    return d.to_json(T0)


def _series() -> dict[str, Any]:
    s = Series()
    s.sample(T0, {"sweep_pass_age_s": 1, "alerts_waiting": 0, "dlq_queued": None, "open_breaches": 2})
    return s.to_json()


def _state() -> dict[str, Any]:
    return {"halt": {"level": "NONE", "effective_level": "NONE", "hard_latch": False, "at_ms": T0},
            "engine": {"status": "running", "role": "LEADER", "sweep_age_s": 1},
            "streams": [{"adapter": "stonex", "stream": "prices", "state": "up"}],
            "open_now": [{"kind": "position", "symbol": "EURUSD", "side": "BUY", "entry": "1.1", "stop": None}],
            "alerts": {"waiting": 0, "oldest_ms": None}, "dlq": {"QUEUED": 1}}


PAYLOADS = {"state": _state, "story": _story, "day": _day, "series": _series}


@pytest.mark.parametrize("name", sorted(PAYLOADS))
def test_real_payloads_pass(name: str) -> None:
    p = PAYLOADS[name]()
    assert guard_payload(p) is p


@pytest.mark.parametrize("name", sorted(PAYLOADS))
@pytest.mark.parametrize("key", ["balance", "equity", "units", "pnl", "spread", "free_text", "price", "error_text"])
def test_payloads_with_a_forbidden_key_raise(name: str, key: str) -> None:
    p = PAYLOADS[name]()
    p[key] = "x"
    with pytest.raises(PrivacyError):
        guard_payload(p)


def test_a_nested_forbidden_key_raises() -> None:
    p = _state()
    p["open_now"][0]["ordered_units"] = "1000"
    with pytest.raises(PrivacyError):
        guard_payload(p)
    s = _story()
    s["history"][0]["bid"] = "1.1"
    with pytest.raises(PrivacyError):
        guard_payload(s)
    with pytest.raises(PrivacyError):
        guard_payload({"badges": {"not-a-sid": ["x"]}})
    with pytest.raises(PrivacyError):
        guard_payload({"counts": {"1.5": 1}})
    with pytest.raises(PrivacyError):
        guard_payload({1: "x"})


@pytest.mark.parametrize("payload", [
    {"halt": {"level": 1.5}}, {"sweep_pass_age_s": [[T0, 0.5]]}, {"waiting": 2.0},
    {"dlq": {"QUEUED": 1.0}}, {"streams": {"stonex": {"prices": [{"from_ms": 1.0}]}}}, {"open_now": [[[0.1]]]},
])
def test_a_float_in_a_payload_raises(payload: dict[str, Any]) -> None:
    with pytest.raises(PrivacyError):
        guard_payload(payload)


def test_a_float_under_a_level_key_raises() -> None:
    """No float anywhere (levels are decimal strings): a float under a level key raises like any other float."""
    with pytest.raises(PrivacyError):
        guard_payload({"open_now": [{"entry": 1.1}]})


def test_a_badly_shaped_level_string_is_never_shown() -> None:
    p = guard_payload({"open_now": [{"entry": "1,1", "stop": "1.0", "take_profit": "x"}]})
    assert p == {"open_now": [{"entry": NOT_SHOWN, "stop": "1.0", "take_profit": NOT_SHOWN}]}


# ---- always on -----------------------------------------------------------------------------------------------------
def test_the_guard_raises_under_python_dash_o() -> None:
    code = (
        "import sys\n"
        "from src.liveview.events import Event, PrivacyError, assert_safe, guard_payload\n"
        "assert sys.flags.optimize == 1\n"
        "out = []\n"
        "try:\n"
        "    assert_safe(Event('order.status', {'units': 1}, 1, 'broker', 1))\n"
        "except PrivacyError:\n"
        "    out.append('event')\n"
        "try:\n"
        "    guard_payload({'balance': '1'})\n"
        "except PrivacyError:\n"
        "    out.append('payload')\n"
        "print(','.join(out))\n"
    )
    r = subprocess.run([sys.executable, "-O", "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr[-2000:]
    assert r.stdout.strip() == "event,payload"
    src = (ROOT / "src" / "liveview" / "events.py").read_text(encoding="utf-8")
    assert not re.search(r"^\s*assert\b", src, re.M)
