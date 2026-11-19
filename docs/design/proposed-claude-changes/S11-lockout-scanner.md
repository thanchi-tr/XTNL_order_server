# Proposed change (S11): ARM lockout in the hooks, `verify_execution.py`, rules and `CLAUDE.md` (design §B3, §C2)

Status: **proposed, not applied.** Needs the user's approval before anyone edits `.claude/`. No test depends on it:
until the hook is updated, `tests/hooks/test_pre_tool_guard_parity.py` skips its parity tests with "hook update
pending", and the pytest lockout scan of the real tree (`tests/riskmgr/test_lockout.py::test_real_scope_tree_passes`)
gates every stage either way (plan §1).

## Why

S11 builds the read-only lockout scanner `src/riskmgr/lockout.py` before any code exists in `src/riskmgr`,
`src/marketdata` or `src/armconfig`. Design §B3 asks for the same name ban in the PreToolUse hook (so a banned name is
refused at edit time, not only at test time), a `verify_execution.py` check that runs the scanner, the three new
packages (plus `src/runtime/` and `src/notifications/`) in the PostToolUse watched directories, the allow-list copy and
naming convention in new rule files, and the `CLAUDE.md` lines of §C2 Rules. Nothing here narrows anything: the hook's
`FORBIDDEN_PATTERNS` stay repo-wide and unchanged, so `MockBrokerAdapter.set_order` / `set_position` and the StoneX
REST `open_positions` stay legal outside the scope.

The proposed hook below was checked against the parity tests (a scratch copy run through
`tests/hooks/test_pre_tool_guard_parity.py`: all passed, including the post-review cases), and the body of the proposed `verify_execution.py` check was
run against today's tree (passes; the planted sample is refused).

## 1. `.claude/hooks/pre_tool_guard.py` (full new content)

Adds the lockout copy (`LOCKOUT_SCOPE`, `NAME_BAN_FILES`, `NAME_BAN_PATTERN`, `DEF_NAME_RE`, `normalise_name`,
`in_name_ban_scope`) and applies the name ban to files in the scope and `src/runtime/md_ports.py` only. For a
MultiEdit it reads each edit's `new_string` (in the JSON encoding of `edits` a `def` after an escaped newline has no
word boundary), for the name ban and for `FORBIDDEN_PATTERNS` alike: the repo-wide patterns now run over the plain
text of every edit as well as the old JSON form, so a MultiEdit adding `def place_order` anywhere under `src/` is
blocked (review S11; today's hook lets it through). This only widens what is checked; the patterns are unchanged. The
name-ban path match ignores case (NTFS resolves `SRC/RiskMgr` to `src/riskmgr`), and the normaliser splits a letter
from a following digit (`place_order2` becomes `place_order_2`).

```python
#!/usr/bin/env python3
"""PreToolUse guard: blocks outbound order execution in Phase 1 (read-only).

Paths are normalised (Windows backslashes -> '/') before matching, so the guard
applies on every platform.

Two layers:
* FORBIDDEN_PATTERNS apply to every file under src/ (unchanged, repo-wide).
* The ARM read-only lockout name ban (design §B3) applies only to files in
  LOCKOUT_SCOPE and NAME_BAN_FILES. It is a copy of src/riskmgr/lockout.py
  (LOCKOUT_SCOPE, NAME_BAN_FILES, NAME_BAN_PATTERN, DEF_NAME_RE, normalise_name,
  in_name_ban_scope); tests/hooks/test_pre_tool_guard_parity.py keeps them equal.
"""
import json
import re
import sys

# Any of these, written anywhere under src/, is an order-changing capability.
FORBIDDEN_PATTERNS = [
    (r"\bdef\s+(place_order|modify_order|cancel_order|submit_order|close_position|amend_order|update_order)\b",
     "Phase 1 Read-Only Lockout: outbound order execution methods are prohibited."),
    # StoneX order-changing endpoints (adapter-isolation.md §4) and cTrader order messages.
    (r"/(newtradeorder|newstoplimitorder|updatetradeorder|updatestoplimitorder|cancel)\b(?!_)",
     "Phase 1 Read-Only Lockout: order-changing broker endpoints may only appear in the deny-list module."),
    (r"\bProtoOA(NewOrderReq|AmendOrderReq|AmendPositionSLTPReq|ClosePositionReq|CancelOrderReq)\b",
     "Phase 1 Read-Only Lockout: cTrader order-changing messages are prohibited."),
]
# The deny-list itself must name the blocked paths.
DENY_LIST_FILES = ("src/adapters/transport.py",)

# ---- ARM read-only lockout name ban: copy of src/riskmgr/lockout.py (keep equal; the parity test checks it).
LOCKOUT_SCOPE = ("src/riskmgr", "src/marketdata", "src/armconfig")
NAME_BAN_FILES = ("src/runtime/md_ports.py",)
NAME_BAN_PATTERN = re.compile(
    r"(^|_)(place|modify|cancel|submit|close|amend|update|move|set|replace|adjust|trail|send|execute|edit|open"
    r"|create|new)_(order|stop|stop_loss|sl|sltp|position|trade)s?($|_)"
)
DEF_NAME_RE = re.compile(r"\b(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)")
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|(?<=[A-Za-z])(?=[0-9])")
_UNDERSCORE_RUN = re.compile(r"_+")


def normalise_name(name: str) -> str:
    """Leading underscores stripped, CamelCase split into snake_case, runs of `_` collapsed, lower-cased."""
    return _UNDERSCORE_RUN.sub("_", _CAMEL_BOUNDARY.sub("_", name.lstrip("_"))).lower()


def in_name_ban_scope(path: str) -> bool:
    """True for a path inside LOCKOUT_SCOPE or equal to a NAME_BAN_FILES entry (any absolute prefix, either slash,
    any case: NTFS resolves SRC/RiskMgr to src/riskmgr)."""
    p = "/" + path.replace("\\", "/").lower()
    return any(f"/{d}/" in p for d in LOCKOUT_SCOPE) or any(p.endswith(f"/{f}") for f in NAME_BAN_FILES)


def _norm(path: str) -> str:
    return path.replace("\\", "/")


def _new_text(tool_input: dict) -> str:
    """The text a Write / Edit / MultiEdit would add (each edit's new_string, not its JSON encoding)."""
    parts = [tool_input.get("new_string") or "", tool_input.get("content") or ""]
    for edit in tool_input.get("edits", []) or []:
        if isinstance(edit, dict):
            parts.append(edit.get("new_string") or "")
    return "\n".join(p for p in parts if isinstance(p, str))


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    tool_input = payload.get("tool_input", {}) or {}
    file_path = _norm(tool_input.get("file_path", "") or tool_input.get("path", "") or "")
    # Each edit's plain new_string as well as the JSON form: in JSON an escaped newline leaves no \b before `def`.
    content = _new_text(tool_input) + "\n" + json.dumps(tool_input.get("edits", []))

    base = file_path.rsplit("/", 1)[-1]
    if base.startswith(".env"):
        print("BLOCKED: direct modification of .env files is prohibited.", file=sys.stderr)
        return 2

    if "/src/" in f"/{file_path}" and file_path.endswith(".py"):
        for pattern, message in FORBIDDEN_PATTERNS:
            if pattern.startswith("/(") and any(file_path.endswith(f) for f in DENY_LIST_FILES):
                continue
            if re.search(pattern, content):
                print(f"HOOK BLOCKED [{file_path}]: {message}", file=sys.stderr)
                return 2

    if file_path.endswith(".py") and in_name_ban_scope(file_path):
        banned = sorted({m.group(1) for m in DEF_NAME_RE.finditer(_new_text(tool_input))
                         if NAME_BAN_PATTERN.search(normalise_name(m.group(1)))})
        if banned:
            print(f"HOOK BLOCKED [{file_path}]: Phase 1 Read-Only Lockout: the ARM / market-data name ban refuses "
                  f"{', '.join(banned)} (design §B3; naming convention in .claude/rules/riskmgr.md).", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

## 2. `.claude/hooks/post_tool_verify.py`

```diff
--- a/.claude/hooks/post_tool_verify.py
+++ b/.claude/hooks/post_tool_verify.py
@@ -9,7 +9,8 @@
 import subprocess
 import sys
 
-WATCHED_DIRS = ("src/dto/", "src/adapters/", "src/pubsub/", "src/integrity/", "src/oversee/", "src/cache/", "src/db/")
+WATCHED_DIRS = ("src/dto/", "src/adapters/", "src/pubsub/", "src/integrity/", "src/oversee/", "src/cache/", "src/db/",
+                "src/riskmgr/", "src/marketdata/", "src/armconfig/", "src/runtime/", "src/notifications/")
 SCRIPT = os.path.join(".claude", "skills", "live-trading-server", "scripts", "verify_execution.py")
```

## 3. `.claude/skills/live-trading-server/scripts/verify_execution.py`

A new check modelled on the adapters AST walk (`@check("read-only: adapters define no order-mutation methods")`),
placed after the `BROKER_ENV` check; docstring list item 7.

```diff
--- a/.claude/skills/live-trading-server/scripts/verify_execution.py
+++ b/.claude/skills/live-trading-server/scripts/verify_execution.py
@@ -14,6 +14,8 @@ Checks (current design, .claude/rules/*):
   4. Topic isolation: a price surge never blocks or drops order / position / account items.
   5. DLQ spooler: offer() is a pure in-memory append (no I/O, not a coroutine).
   6. Oversee: ActionGate fails closed; no Oversee timers exist (no oversee:timers usage).
+  7. ARM lockout: src/riskmgr/lockout.py passes today's src/riskmgr, src/marketdata, src/armconfig tree (no
+     violation, nothing unreadable) and refuses a planted sender name and network import.
 """
 from __future__ import annotations
 
@@ -106,6 +108,19 @@ def _() -> None:
         raise AssertionError(f"BROKER_ENV={bad!r} accepted")
 
 
+@check("read-only: ARM lockout scanner")
+def _() -> None:
+    from src.riskmgr.lockout import Rule, scan, scan_source
+
+    result = scan([ROOT])
+    assert not result.violations, "; ".join(str(v) for v in result.violations[:5])
+    assert not result.incomplete, "; ".join(result.incomplete[:5])
+    assert any(s.endswith("src/riskmgr/lockout.py") for s in result.scanned), "the scanner did not scan itself"
+    planted = scan_source("import socket\nclass ModifyStop:\n    pass\n", "src/riskmgr/planted.py")
+    assert {v.rule for v in planted} >= {Rule.IMPORT, Rule.NAME_BAN}, "a planted sample was not refused"
+
+
 # ---------------------------------------------------------------- 2. adapter isolation
 @check("isolation: src/adapters imports no db/cache/pubsub/integrity/oversee")
```

## 4. New `.claude/rules/riskmgr.md`

```markdown
---
paths:
  - "src/riskmgr/**"
  - "src/armconfig/**"
  - "src/runtime/md_ports.py"
---

# ARM (Automated Risk Manager) and read-only lockout rules

Design: `docs/design/automated-risk-manager.md` Part B; plan `docs/design/arm-implementation-plan.md`.

1. **Phase 1 sends nothing.** The ARM advises; it never sends, changes or removes a broker order. The hard guarantee is
   `ReadOnlyTransport` (`src/adapters/transport.py`), `FORBIDDEN_METHOD_NAMES` / `assert_read_only`
   (`src/adapters/base.py`) and the scoped packages' inability to import `src.adapters`; none is ever narrowed.
2. **Lockout scanner** (`src/riskmgr/lockout.py`, the review gate). `LOCKOUT_SCOPE` = `src/riskmgr`, `src/marketdata`,
   `src/armconfig` (and any later stop-move package). Direct imports only. Widening any list is a reviewed change made
   in `lockout.py` and here together; `tests/hooks/test_pre_tool_guard_parity.py` keeps the hook copy equal.
   * **Imports, `src/riskmgr` and `src/marketdata`:** `__future__`, `abc`, `asyncio`, `bisect`, `collections`,
     `contextlib`, `dataclasses`, `datetime`, `decimal`, `enum`, `functools`, `hashlib`, `hmac`, `itertools`, `json`,
     `logging`, `math`, `re`, `struct`, `time`, `typing`, `zoneinfo`; `src.dto`, `src.oversee.risk`,
     `src.calendar.sessions` / `schedule` / `machine`, `src.db.arm_ports` (the store Protocols). `src/riskmgr` also
     `src.riskmgr`, `src.marketdata`, `src.armconfig` and `concurrent.futures` (`ThreadPoolExecutor` only);
     `src/marketdata` also `src.marketdata` and `zlib` (`crc32` only), and `redis` only under `src/marketdata/store`.
     `src/marketdata` never imports `src.riskmgr` or `src.armconfig`.
   * **Imports, `src/armconfig`:** the standard-library list above plus `ssl` and `asyncpg`; `src.dto`,
     `src.oversee.risk`, the three calendar modules, `src.db.arm_ports`, `src.armconfig`, and the registries
     `src.riskmgr.registry` and `src.marketdata.indicators.registry` (`parse_params` only).
   * **Always refused, with a named reason:** `os`, `sys`, `builtins`, `importlib`, `gc`, `inspect`, `socket`, `http`,
     `urllib`, `smtplib`, `subprocess`, `ctypes`, `httpx`, `aiohttp`, `requests`, `logging.handlers`,
     `logging.config`, `src.adapters`, `src.runtime`, `src.pubsub`, `src.cache`, `src.notifications`,
     `src.calendar.readiness`, `src.riskmgr.lockout`; `ssl` outside `src/armconfig`; star imports; anything not listed
     (so `uuid`, `secrets` and `random` too).
   * **asyncio:** only `sleep`, `gather`, `wait_for`, `Event`, `Lock`, `Queue`, `create_task`, `timeout`,
     `CancelledError`, `to_thread`, plus `get_running_loop` and `wrap_future` in `src/riskmgr` only. A strategy or
     guard job runs through `loop.run_in_executor(<its version's executor>, ...)`, never `to_thread`.
   * **Event-loop methods, on any receiver** (so the loop from `get_running_loop()` cannot open a socket or a
     process): `create_connection`, `create_server`, `create_datagram_endpoint`, `create_unix_connection`,
     `create_unix_server`, `open_connection`, `open_unix_connection`, `start_server`, `start_unix_server`,
     `create_subprocess_exec`, `create_subprocess_shell`, `subprocess_exec`, `subprocess_shell`, `sock_connect`,
     `sock_sendall`, `sock_sendto`, `sock_sendfile`, `sock_recv`, `sock_recv_into`, `sock_recvfrom`,
     `sock_recvfrom_into`, `sock_accept`, `connect_accepted_socket`, `connect_read_pipe`, `connect_write_pipe`,
     `start_tls`, `getaddrinfo`, `getnameinfo`, `add_reader`, `add_writer`, `sendfile` are refused, also as a
     from-imported name and as a `getattr` literal.
   * **logging:** only `getLogger`, `Logger`, `LoggerAdapter`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (no
     handler classes, `basicConfig` or `root`); `handlers`, `addHandler` and `removeHandler` are refused on any
     receiver but `self` / `cls`.
   * **Module names as attributes:** the refused modules above (`os`, `sys`, `inspect`, `builtins`, `importlib`, `gc`,
     `socket`, `subprocess`, `ctypes`, `http`, `urllib`, `smtplib`, `httpx`, `aiohttp`, `requests`; `ssl` outside
     `src/armconfig`), plus `threading`, `io`, `types`, `weakref`, `traceback`, `warnings`, `multiprocessing`,
     `pickle`, `marshal`, `shutil`, `tempfile`, `pathlib` and the member-limited `asyncio`, `logging`, `concurrent`,
     `zlib`, are refused as an attribute on any receiver and as `from <any module> import <name>`, so `logging.os`,
     `typing.sys.modules` or `from contextlib import sys` cannot reach a refused module through an allowed one.
     Exceptions: `redis.asyncio` (the real client package, store only) and `types` directly under a scoped package
     (`src.marketdata.types`). `copy` is refused the same way except as a direct method call (`d.copy()`). A module
     bound by an import (and `concurrent.futures`) is used only through attribute access, never as a value
     (`lg = logging`), so its member list cannot be skipped.
   * **Reflection:** `getattr` / `setattr` / `hasattr` / `delattr` need a literal name not starting with `_`; `vars` /
     `dir` / `globals` / `locals` only subscripted at once with such a literal key; `dataclasses.asdict` /
     `dataclasses.fields` / `vars` never on a port (a parameter typed as a `...Port`, `Pager`, `PagerWrapper` or a
     local Protocol, or a `self.<...port>` / `self.<...pager>` attribute); `__import__`, `eval`, `exec`, `compile`
     and `__builtins__` refused; the builtins `open`, `breakpoint` and `input` refused (filesystem, debugger, stdin).
     Every builtin these call checks cover may appear only as the direct callee: an alias (`g = getattr`), an argument
     (`functools.partial(getattr)`, `map(getattr, ...)`) or a default value is refused; a function's own parameter or
     local of the same name is not the builtin and passes. `.format` / `.format_map` only on a string literal whose
     fields read no `_` attribute (`'{0.__class__}'` is refused).
   * **Private attributes:** a `_`-prefixed attribute is read only on `self` / `cls` (the first parameter of a method
     defined in a class body, not a staticmethod, never rebound, not shadowed by an inner scope; closures inside the
     method see it) or a zero-argument `super()`; a match pattern never names a `_` attribute (`case
     object(__class__=c)`).
   * **Compiled files** (`.pyc` outside `__pycache__`, `.pyd`, `.so`) are refused in the scope.
   * **The scanner itself** (`SCANNER_SELF`) is exempt by name from the package allow-list, the reflection rules and
     the private-attribute rule (not from dynamic code, the refused builtins, `__builtins__`, the module-name
     attributes or the event-loop methods); its own allow-list is `__future__`, `ast`, `dataclasses`, `enum`, `pathlib`, `re`,
     `sys` (`sys.modules` only), `typing`; the name ban applies to it; no scoped module imports it.
   * **Run time:** `optional-bootstrap` runs `scan()` in a thread before registering any task: a violation parks with
     a page for the rest of the term; an incomplete scan raises and is retried (page after 3 in a row). The scan covers
     the package directories plus the `__file__` of every loaded module of the scanned packages.
3. **Name ban** (scanner and hook, one rule): every `def` / `class` name in the scope and in `src/runtime/md_ports.py`
   is normalised (leading underscores stripped, CamelCase split into snake_case, a letter split from a following
   digit, runs of `_` collapsed, lower-cased)
   and refused when `NAME_BAN_PATTERN` finds a verb (place, modify, cancel, submit, close, amend, update, move, set,
   replace, adjust, trail, send, execute, edit, open, create, new) joined to an object (order, stop, stop_loss, sl,
   sltp, position, trade, optionally plural). **Naming convention:** `held_positions` (not "open positions"),
   `arm_row_update` (not "update position"), `mark_arm_closed` (not "close position"), `close_eligible`,
   `pre_basis`, `StopBasis`, `StopHandleReadPortImpl`. The repo-wide `FORBIDDEN_PATTERNS` are unchanged.
4. **Sinks:** the Phase 1 set is fixed as `ARM_SINKS` = (decision sink, notice sink); `assert_sinks()` refuses any
   other set; a Supabase document can never name a sink.
5. **No market data in PostgreSQL** (design §B3, §B8): ids, bucket times, check names, counts, order-state levels and
   keyed HMAC hashes with their key id only; R values dimensionless (0.01 R); never a price increment, quantum,
   spread, pip size or account value in a notice, page or log record.
6. **Adding a strategy:** a new `(KIND, IMPL_VERSION)` in `src/riskmgr/registry.py` (never edit a pinned version);
   `RESOURCE = STOP_LEVEL` only; every REASON mapped to an engine category; `DECISION_METRICS` typed (`RMultiple` /
   `Ratio`); `parse_params` rejects unknown keys and out-of-bound values; `migrate_state` round-trips from every older
   version or refuses; the registry-wide conformance suite (`tests/riskmgr/conformance.py`: pure and deterministic
   under replay, the envelope property test) passes; names follow §3.
```

## 5. New `.claude/rules/marketdata.md`

```markdown
---
paths:
  - "src/marketdata/**"
---

# Market-data layer rules

Design: `docs/design/automated-risk-manager.md` Part A; plan `docs/design/arm-implementation-plan.md`.

1. **Market data lives only in `redis-md`** (ticks, bars, quotes, spreads, indicator values), derived from the broker;
   it is not a duplicate of PostgreSQL, sits outside SM8 and the integrity worker, and never reaches PostgreSQL, the
   engine Redis, a log record, a page or a notice body. Logs and pages carry series ids, buckets, field names, check
   names and revisions, never values.
2. **Lockout:** `src/marketdata` is in `LOCKOUT_SCOPE`; the import allow-list, asyncio list, reflection rules and name
   ban are in `riskmgr.md` §2-§3 (one copy, kept equal to `src/riskmgr/lockout.py`). In short: `zlib` with `crc32`
   only; `redis` only under `src/marketdata/store`; never `src.riskmgr` or `src.armconfig`; no `uuid`, `secrets` or
   `random` (the writer nonce and jitter come from the runtime through a port).
3. **Never blocks ingestion:** the tick hook is O(1); writes go through bounded queues; a `redis-md` fault parks
   market data (GAPPED) and never stops ingestion or Oversee. Off unless `XTNL_MD_ENABLED=1`.
4. **Adding an indicator:** a new `(KIND, VERSION)` in `src/marketdata/indicators/registry.py` (never edit a pinned
   version); `OUTPUTS` each with a unit (PRICE, RATIO, COUNT) and its quantum pinned per VERSION; `parse_params`
   rejects unknown keys and out-of-bound values; `window()` returns `Count(n)` or `Anchored(anchor_spec, max_bars)`;
   `step()` pure and deterministic under the pinned Decimal context; golden vectors (canonical equals incremental at
   every checkpoint; replay from re-fetched bars gives the identical quantised value); `indicator_id` is the opaque
   hash of the canonical spec, so no id shows the quantum; names follow `riskmgr.md` §3.
```

## 6. `.claude/CLAUDE.md`

```diff
--- a/.claude/CLAUDE.md
+++ b/.claude/CLAUDE.md
@@ Scope & Operational Boundary
-- **Mandatory Skill Invocation:** Whenever modifying files in `src/adapters/`, `src/pubsub/`, `src/dto/`, `src/integrity/`, or `src/oversee/`, explicitly invoke `/live-trading-server` first.
+- **Mandatory Skill Invocation:** Whenever modifying files in `src/adapters/`, `src/pubsub/`, `src/dto/`, `src/integrity/`, `src/oversee/`, `src/marketdata/`, `src/riskmgr/`, or `src/armconfig/`, explicitly invoke `/live-trading-server` first.
+- **Market data only in `redis-md`:** ticks, bars, quotes, spreads and indicator values are derived from the broker and live only in the separate `redis-md` store (not a duplicate of PostgreSQL; outside SM8 and the integrity worker); never in PostgreSQL, the engine Redis, logs or pages. `src/riskmgr`, `src/marketdata` and `src/armconfig` are under the read-only lockout scanner `src/riskmgr/lockout.py` (rules: `.claude/rules/riskmgr.md`, `.claude/rules/marketdata.md`); the market-data and ARM services stay off unless `XTNL_MD_ENABLED` / `XTNL_ARM_ENABLED` are set.
@@ Repository Architecture
+- `src/marketdata/`, `src/riskmgr/`, `src/armconfig/`: Stage 9 market-data layer and ARM (design `docs/design/automated-risk-manager.md`); advice only, nothing is sent to a broker.
```

## After approval

Apply the six changes, then run `.venv/Scripts/python.exe -m pytest -q tests/hooks tests/riskmgr` (the parity tests
run instead of skipping) and `verify_execution.py --strict` (one more check, 12 in all).
