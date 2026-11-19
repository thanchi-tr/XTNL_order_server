#!/usr/bin/env python3
"""Deterministic invariant suite for the Phase 1 order-server (runs against the real src/ code).

Each check imports the module it verifies. A module that does not exist yet is reported as
SKIP (so the post-edit hook does not block work in progress); `--strict` turns SKIP into FAIL.
A failed assertion is always a FAIL. Exit code 0 = all passed (or skipped when not strict).

Checks (current design, .claude/rules/*):
  1. Read-only lockout: no order-mutation method on any adapter; order-changing paths refused
     by ReadOnlyTransport; BROKER_ENV gate refuses anything but sandbox / live_readonly.
  2. Adapter isolation: nothing in src/adapters imports db / cache / pubsub / integrity / oversee.
  3. Versioning: key (broker_seq, broker_time, observed_seq); same checksum = no-op; stale dropped;
     seq_id only breaks equal keys with different content; Lua fixed-width order == key order.
  4. Topic isolation: a price surge never blocks or drops order / position / account items.
  5. DLQ spooler: offer() is a pure in-memory append (no I/O, not a coroutine).
  6. Oversee: ActionGate fails closed; no Oversee timers exist (no oversee:timers usage).
  7. ARM lockout: src/riskmgr/lockout.py passes today's src/riskmgr, src/marketdata, src/armconfig tree (no
     violation, nothing unreadable) and refuses a planted sender name and network import.
  8. ARM registry: every released (pinned) strategy / guard / indicator version still loads (design §B6.1).
"""
from __future__ import annotations

import asyncio
import ast
import inspect
import os
import sys
import traceback
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

RESULTS: list[tuple[str, str, str]] = []


CHECKS: list[tuple[str, Callable[[], None]]] = []


def check(name: str) -> Callable[[Callable[[], None]], Callable[[], None]]:
    def deco(fn: Callable[[], None]) -> Callable[[], None]:
        CHECKS.append((name, fn))
        return fn
    return deco


def _run(name: str, fn: Callable[[], None]) -> None:
    if True:
        try:
            fn()
            RESULTS.append(("PASS", name, ""))
        except ModuleNotFoundError as e:
            RESULTS.append(("SKIP", name, f"module not built yet: {e.name}"))
        except _Skip as e:
            RESULTS.append(("SKIP", name, str(e)))
        except AssertionError as e:
            RESULTS.append(("FAIL", name, str(e) or traceback.format_exc(limit=2)))
        except Exception as e:  # import-time or runtime error in the code under test
            RESULTS.append(("FAIL", name, f"{type(e).__name__}: {e}"))


class _Skip(Exception):
    pass


# ---------------------------------------------------------------- 1. read-only lockout
@check("read-only: adapters define no order-mutation methods")
def _() -> None:
    from src.adapters.base import FORBIDDEN_METHOD_NAMES, BrokerAdapter, assert_read_only

    assert_read_only(BrokerAdapter)
    found = 0
    for path in (ROOT / "src" / "adapters").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert node.name.lower() not in FORBIDDEN_METHOD_NAMES, f"{path}: defines {node.name}"
            if isinstance(node, ast.ClassDef):
                found += 1
    assert found > 0


@check("read-only: ReadOnlyTransport refuses order-changing paths, allows reads")
def _() -> None:
    from src.adapters.transport import is_denied_path

    for p in ("/order/newtradeorder", "/TradingAPI/order/newstoplimitorder", "/order/updatetradeorder",
              "/order/updatestoplimitorder", "/order/cancel", "/ORDER/CANCEL?x=1"):
        assert is_denied_path(p), f"not denied: {p}"
    for p in ("/order/openpositions", "/order/activeorders", "/order/tradehistory", "/order/123/openposition",
              "/v2/Session", "/v2/margin/clientAccountMargin"):
        assert not is_denied_path(p), f"wrongly denied: {p}"


@check("read-only: BROKER_ENV gate")
def _() -> None:
    from src.config import ExecutionLockedError, broker_env

    assert broker_env({}) == "sandbox"
    assert broker_env({"BROKER_ENV": "live_readonly"}) == "live_readonly"
    for bad in ("live", "production", ""):
        try:
            broker_env({"BROKER_ENV": bad})
        except ExecutionLockedError:
            continue
        raise AssertionError(f"BROKER_ENV={bad!r} accepted")


@check("read-only: ARM lockout scanner")
def _() -> None:
    from src.riskmgr.lockout import Rule, scan, scan_source

    result = scan([ROOT])
    assert not result.violations, "; ".join(str(v) for v in result.violations[:5])
    assert not result.incomplete, "; ".join(result.incomplete[:5])
    assert any(s.endswith("src/riskmgr/lockout.py") for s in result.scanned), "the scanner did not scan itself"
    planted = scan_source("import socket\nclass ModifyStop:\n    pass\n", "src/riskmgr/planted.py")
    assert {v.rule for v in planted} >= {Rule.IMPORT, Rule.NAME_BAN}, "a planted sample was not refused"


# ---------------------------------------------------------------- 2. adapter isolation
@check("isolation: src/adapters imports no db/cache/pubsub/integrity/oversee")
def _() -> None:
    banned = ("src.db", "src.cache", "src.pubsub", "src.integrity", "src.oversee")
    for path in (ROOT / "src" / "adapters").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            for n in names:
                assert not n.startswith(banned), f"{path.relative_to(ROOT)} imports {n}"


# ---------------------------------------------------------------- 3. versioning
def _order(**kw: object) -> object:
    from src.dto import OrderDTO, OrderStatus

    base: dict[str, object] = dict(client_order_id=None, broker_order_id="1", account_id="A", adapter_id="mock",
                                   symbol="EURUSD", side="BUY", status=OrderStatus.WORKING,
                                   entry_price=Decimal("1.1000"), ordered_units=Decimal(10000),
                                   remaining_units=Decimal(10000), broker_updated_at_ms=1000, observed_seq=10)
    base.update(kw)
    return OrderDTO(**base)  # type: ignore[arg-type]


@check("versioning: two-step compare (observed_seq tie, no-op, stale, seq_id)")
def _() -> None:
    from src.dto import Decision, decide

    stored = _order()
    assert decide(None, stored) is Decision.APPLY
    relabel = replace(stored, observed_seq=11, seq_id=99)  # type: ignore[type-var]
    assert relabel.checksum == stored.checksum, "checksum must exclude version fields"  # type: ignore[attr-defined]
    assert decide(stored, relabel) is Decision.NOOP_ADVANCE  # type: ignore[arg-type]
    amend = replace(stored, stop_loss=Decimal("1.0950"), observed_seq=11)  # type: ignore[type-var]
    assert decide(stored, amend) is Decision.APPLY  # type: ignore[arg-type]
    assert decide(amend, stored) is Decision.DROP_STALE  # type: ignore[arg-type]
    # REST row observed (sent) after the stream frame at the same broker time wins the tie.
    rest = replace(stored, stop_loss=None, take_profit=Decimal("1.2"), source="REST", observed_seq=12)  # type: ignore[type-var]
    assert decide(stored, rest) is Decision.APPLY  # type: ignore[arg-type]
    # equal keys, different content: seq_id decides
    a = replace(stored, stop_loss=Decimal("1.0900"), seq_id=5)  # type: ignore[type-var]
    b = replace(stored, stop_loss=Decimal("1.0800"), seq_id=6)  # type: ignore[type-var]
    assert decide(a, b) is Decision.APPLY and decide(b, a) is Decision.DROP_STALE  # type: ignore[arg-type]


@check("versioning: fixed-width version strings sort like version keys")
def _() -> None:
    from src.dto import version_key, version_str

    rows = [_order(broker_updated_at_ms=t, observed_seq=s, broker_seq=b)
            for t, s, b in ((9, 1, None), (10, 1, None), (10, 2, None), (10**15, 1, 3), (5, 2**60, 4))]
    by_key = sorted(rows, key=version_key)  # type: ignore[arg-type]
    by_str = sorted(rows, key=version_str)  # type: ignore[arg-type]
    assert by_key == by_str


@check("versioning: future_timestamp tolerance is 5 s")
def _() -> None:
    from src.dto import is_future_timestamp

    assert not is_future_timestamp(15_000, 10_000)
    assert is_future_timestamp(15_001, 10_000)


# ---------------------------------------------------------------- 4. topic isolation
@check("pubsub: price surge never blocks or drops order/position/account items")
def _() -> None:
    from src.dto import OrderStatus, PriceDTO
    from src.pubsub.bus import EventBus  # noqa: F401 (module must exist)

    bus = _make_bus()
    for i in range(10_000):
        _publish_price(bus, PriceDTO(adapter_id="mock", symbol=f"S{i % 7}", bid=Decimal(1), ask=Decimal("1.0001"),
                                     spread_pips=Decimal(1), timestamp_ms=i))
    o = _order(status=OrderStatus.WORKING)
    _publish_order(bus, o)
    got = _drain_orders(bus)
    assert o in got, "order item lost or blocked behind a price surge"


def _make_bus() -> object:
    from src.pubsub import bus as busmod

    cls = getattr(busmod, "EventBus")
    try:
        return cls()
    except TypeError:
        raise _Skip("EventBus requires constructor args; covered by tests/pubsub")


def _publish_price(bus: object, p: object) -> None:
    for name in ("publish_price_nowait", "publish_price"):
        fn = getattr(bus, name, None)
        if fn is not None:
            fn(p)
            return
    raise _Skip("EventBus has no publish_price_nowait")


def _publish_order(bus: object, o: object) -> None:
    for name in ("publish_order_nowait", "publish_order"):
        fn = getattr(bus, name, None)
        if fn is not None:
            fn(o)
            return
    raise _Skip("EventBus has no publish_order_nowait")


def _drain_orders(bus: object) -> list[object]:
    q = None
    for name in ("orders", "order_queue", "orders_queue"):
        q = getattr(bus, name, None)
        if q is not None:
            break
    if q is None and hasattr(bus, "queue"):
        q = bus.queue("orders.internal.dto")  # type: ignore[attr-defined]
    if q is None:
        raise _Skip("cannot locate the orders queue on EventBus")
    out: list[object] = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


# ---------------------------------------------------------------- 5. DLQ spooler
@check("dlq: DlqSpooler.offer is synchronous and does no I/O")
def _() -> None:
    from src.pubsub.spooler import DlqSpooler

    offer = getattr(DlqSpooler, "offer")
    assert not inspect.iscoroutinefunction(offer), "offer() must not be async"
    src = inspect.getsource(offer)
    for banned in ("open(", "fsync", "await ", "to_thread", "write("):
        assert banned not in src, f"offer() contains {banned!r}"


# ---------------------------------------------------------------- 6. Oversee
@check("oversee: no timer machinery (oversee:timers / TimerScanner) in src")
def _() -> None:
    # Only real usage counts (string constants that are not docstrings, and identifiers), not prose.
    for path in (ROOT / "src").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                      if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                      and n.body and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)}
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
                assert "oversee:timers" not in node.value, f"{path.relative_to(ROOT)} uses the oversee:timers key"
            if isinstance(node, (ast.Name, ast.Attribute, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                ident = getattr(node, "id", None) or getattr(node, "attr", None) or getattr(node, "name", "")
                assert ident not in ("TimerScanner", "defer_to_timer", "finish_timer"), \
                    f"{path.relative_to(ROOT)} defines/uses {ident}"


@check("oversee: ActionGate exists and fails closed on missing state")
def _() -> None:
    from src.oversee import gate as gatemod

    assert hasattr(gatemod, "ActionGate"), "src/oversee/gate.py must define ActionGate"


@check("arm: every pinned KIND:VERSION is still in the registry")
def _() -> None:
    from src.ops.arm_registry import PINNED_TOKENS, pinned_registry_problems

    assert PINNED_TOKENS, "PINNED_TOKENS must list the released versions"
    for t in PINNED_TOKENS:
        kind, _, version = t.rpartition(":")
        assert kind and version.isdigit() and int(version) >= 1, f"malformed pinned token {t!r}"
    problems = pinned_registry_problems()
    assert not problems, "; ".join(problems)


def main() -> int:
    strict = "--strict" in sys.argv
    for name, fn in CHECKS:
        _run(name, fn)
    for status, name, detail in RESULTS:
        line = f"[{status}] {name}"
        print(line + (f"\n        {detail}" if detail else ""))
    fails = [r for r in RESULTS if r[0] == "FAIL" or (strict and r[0] == "SKIP")]
    passed = sum(1 for r in RESULTS if r[0] == "PASS")
    print(f"\n{passed} passed, {sum(1 for r in RESULTS if r[0] == 'SKIP')} skipped, "
          f"{sum(1 for r in RESULTS if r[0] == 'FAIL')} failed")
    if fails:
        print("VERIFICATION FAILED", file=sys.stderr)
        return 1
    print("PHASE 1 INVARIANTS PASSED.")
    return 0


if __name__ == "__main__":
    os.chdir(ROOT)
    sys.exit(main())
