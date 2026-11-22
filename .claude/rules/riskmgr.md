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
     `src/marketdata` also `src.marketdata` and `zlib` (`crc32` only), and `redis` only under `src/marketdata/store`;
     `src/marketdata/indicators/engine.py` alone also `concurrent.futures` (`ThreadPoolExecutor` only) and
     `asyncio.wrap_future` (md-indicators' own bounded executor, plan S38d; `FUTURES_FILES` in `lockout.py`).
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
   spread, pip size or account value in a notice, page or log record. Exception (owner decision 2026-11-21): the
   ARM email's HTML part may show each stop's **distance from the entry in pips**; the relay reads the instrument's
   `point_size` at send time (`src.db.instrument_reads.PgPointSizes`) and never stores it in a notice, prints it or
   logs it. The pip size itself is still never shown.
6. **Adding a strategy:** a new `(KIND, IMPL_VERSION)` in `src/riskmgr/registry.py` (never edit a pinned version);
   `RESOURCE = STOP_LEVEL` only; every REASON mapped to an engine category; `DECISION_METRICS` typed (`RMultiple` /
   `Ratio`); `parse_params` rejects unknown keys and out-of-bound values; `migrate_state` round-trips from every older
   version or refuses; the registry-wide conformance suite (`tests/riskmgr/conformance.py`: pure and deterministic
   under replay, the envelope property test) passes; names follow §3.
7. **FAILED and the error budget** (design §B14; plan S38f):
   - `xtnl_arm` has UPDATE on `arm_failures` only for `counters`, `position_ids`, `last_ping_at_ms`, `ping_count`,
     `last_probe_at_ms`, `last_probe_result` (0020); the BEFORE UPDATE trigger `arm_failures_guard` refuses any change of
     an identity column, a shrinking `position_ids`, any change of a cleared episode and a clear by any role but the table
     owner's. The S31 privilege self-check reads the trigger too.
   - The error budget's counters are durable (`arm_budget_counters`, 0020): written fenced on the ARM pool only on
     change, restored by the FAILED manager at start; a restart is never itself a clean result. `XTNL_ARM_FAIL_RESTARTS_N`
     (3) leader terms ended `lost` / `init_failed` within `XTNL_ARM_FAIL_RESTART_WINDOW_S` (1800 s) while positions are
     armed is ARM-global FAILED `unclean_restarts`.
