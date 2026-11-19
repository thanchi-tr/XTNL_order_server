"""Read-only lockout scanner for the market-data and ARM packages (design §B3; plan S11).

Phase 1 sends nothing. The hard guarantee is elsewhere and is never narrowed: the only REST client the adapters
construct is `ReadOnlyTransport` (`src/adapters/transport.py`, whose deny-list refuses every mutating path), no adapter
has a mutation method (`FORBIDDEN_METHOD_NAMES` / `assert_read_only`, `src/adapters/base.py`), and the scoped packages
cannot import `src.adapters` at all. This scanner is the **review gate** on top: adding a sender, a network client or a
reflective escape to `LOCKOUT_SCOPE` fails the pytest lockout test, the `verify_execution.py` check and (at run time)
the `optional-bootstrap` task, so it can only land as a reviewed widening of the lists below.

What it checks, per file of `LOCKOUT_SCOPE` (direct imports only, so `src.oversee.risk`'s own imports are not
followed, IN-14):

* **Imports** against a per-package allow-list; refused modules carry a named reason. `zlib` only in `src/marketdata`
  and from it only `crc32`; `concurrent.futures` only in `src/riskmgr` and `FUTURES_FILES` (md-indicators' own
  executor, round 13) and from it only `ThreadPoolExecutor`; `redis`
  only in `src/marketdata/store`; `ssl` and `asyncpg` only in `src/armconfig`; `src/marketdata` never imports the ARM;
  no scoped module imports this scanner. Star imports are refused.
* **Module members:** from `asyncio` only the names in `ASYNCIO_ALLOWED` (plus `get_running_loop` and `wrap_future` in
  `src/riskmgr`), so `open_connection`, `create_connection` and `create_subprocess_*` are refused; from `logging` only
  `LOGGING_ALLOWED` (`logging.handlers`, `logging.config`, handler classes and `basicConfig` are refused). A module
  bound by an import is used only through attribute access, never as a value, so its member list cannot be skipped.
* **Names on any receiver:** `REFUSED_ATTRIBUTE_NAMES` (refused and member-limited module names, so `logging.os` or
  `typing.sys.modules` cannot reach a refused module through an allowed one), `LOOP_REFUSED_ATTRIBUTES` (the event
  loop's socket and process methods) and `LOGGER_REFUSED_ATTRIBUTES` (not on `self`) are refused as attributes, as
  from-imported names and as reflection literals.
* **Reflection:** `getattr` / `setattr` / `hasattr` / `delattr` need a literal name that does not start with `_`;
  `vars` / `dir` / `globals` / `locals` are allowed only when subscripted at once with such a literal key;
  `dataclasses.asdict` / `dataclasses.fields` / `vars` are refused on a parameter typed as a port Protocol (ports are
  `__slots__` wrapper objects, so nothing behind them is reachable); `__import__`, `eval`, `exec`, `compile`, the
  `__builtins__` name and the builtins `open`, `breakpoint`, `input` are refused. These builtins may appear only as
  the direct callee (an alias, an argument or a default value is refused); a local of the same name is not the
  builtin. `.format` / `.format_map` only on a string literal whose fields read no `_` attribute.
* **Private attributes:** an attribute whose name starts with `_` may be read only on `self` / `cls` (the first
  parameter of a method defined in a class body, not a staticmethod, never rebound) or a zero-argument `super()`; a
  match pattern never names a `_` attribute.
* **Name ban** (one rule for this scanner and the PreToolUse hook): every `def` / `class` name, normalised by
  `normalise_name` (leading underscores stripped, CamelCase split into snake_case, runs of `_` collapsed,
  lower-cased), is refused when `NAME_BAN_PATTERN` finds a verb-object pair such as place / modify / trail followed by
  order / stop / position. The ban also covers the PR12 wrapper module `src/runtime/md_ports.py` (name ban only).
  Internal names follow the published convention `held_positions`, `arm_row_update`, `mark_arm_closed`.
* **Compiled files** (`.pyc` outside `__pycache__`, `.pyd`, `.so`) inside the scope are refused.

`scan(roots)` covers the package directories under each root plus the `__file__` of every loaded module of the
scanned packages (`sys.modules`), so a module loaded from somewhere else is still checked. It returns a `ScanResult`
with two failure kinds: `violations` (a banned import, member, name or reflection) and `incomplete` (a file it cannot
read or parse, such as a OneDrive placeholder or a locked file). The caller decides: the runtime parks on a violation
and retries an incomplete scan (design §A2).

A file nested too deeply to parse or walk (`MemoryError`, `RecursionError`) is `incomplete`, like an unreadable one.

**The scanner itself** (`SCANNER_SELF`) is exempt by name from the package allow-list, the reflection rules and the
private-attribute rule (not from dynamic code, the refused builtins or the names refused on any receiver), because it parses source files and reads the `__file__` of loaded modules through
`sys.modules`. It is held to its own allow-list `SCANNER_SELF_ALLOWED` (`sys` for `sys.modules` only); the name ban
still applies to it. A copy of it under any other name is refused like every other scoped file.

The scope, `NAME_BAN_PATTERN`, `DEF_NAME_RE` and `normalise_name` are copied into `.claude/hooks/pre_tool_guard.py`;
`tests/hooks/test_pre_tool_guard_parity.py` keeps the copy equal. Widening any list here is a reviewed change and must
be mirrored in `.claude/rules/riskmgr.md`.
"""
from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Final, Iterable, Mapping, Sequence

# ---------------------------------------------------------------------------------------------------- scope
LOCKOUT_SCOPE: Final = ("src/riskmgr", "src/marketdata", "src/armconfig")
"""Directories (relative to the repository root) under the full lockout. A later stop-move package joins here."""

NAME_BAN_FILES: Final = ("src/runtime/md_ports.py",)
"""Files outside the scope that get the name ban only (the PR12 wrapper module, plan S31)."""

SCANNER_SELF: Final = "src/riskmgr/lockout.py"
SCOPE_PACKAGES: Final = ("src.riskmgr", "src.marketdata", "src.armconfig")
NAME_BAN_MODULES: Final = ("src.runtime.md_ports",)

# ---------------------------------------------------------------------------------------------------- name ban
NAME_BAN_PATTERN: Final = re.compile(
    r"(^|_)(place|modify|cancel|submit|close|amend|update|move|set|replace|adjust|trail|send|execute|edit|open"
    r"|create|new)_(order|stop|stop_loss|sl|sltp|position|trade)s?($|_)"
)
DEF_NAME_RE: Final = re.compile(r"\b(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)")
"""Text form used by the hook, which sees edit fragments that may not parse."""

_CAMEL_BOUNDARY: Final = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|(?<=[A-Za-z])(?=[0-9])")
_UNDERSCORE_RUN: Final = re.compile(r"_+")


def normalise_name(name: str) -> str:
    """Leading underscores stripped, CamelCase split into snake_case, runs of `_` collapsed, lower-cased."""
    return _UNDERSCORE_RUN.sub("_", _CAMEL_BOUNDARY.sub("_", name.lstrip("_"))).lower()


def is_banned_name(name: str) -> bool:
    return NAME_BAN_PATTERN.search(normalise_name(name)) is not None


def banned_names_in_text(text: str) -> list[str]:
    """Every `def` / `class` name in `text` that the name ban refuses (the hook's check)."""
    return [m.group(1) for m in DEF_NAME_RE.finditer(text) if is_banned_name(m.group(1))]


def in_name_ban_scope(path: str) -> bool:
    """True for a path inside `LOCKOUT_SCOPE` or equal to a `NAME_BAN_FILES` entry (any absolute prefix, either slash,
    any case: NTFS resolves `SRC/RiskMgr` to `src/riskmgr`)."""
    p = "/" + path.replace("\\", "/").lower()
    return any(f"/{d}/" in p for d in LOCKOUT_SCOPE) or any(p.endswith(f"/{f}") for f in NAME_BAN_FILES)


# ---------------------------------------------------------------------------------------------------- allow-lists
STDLIB_ALLOWED: Final = frozenset({
    "__future__", "abc", "asyncio", "bisect", "collections", "contextlib", "dataclasses", "datetime", "decimal",
    "enum", "functools", "hashlib", "hmac", "itertools", "json", "logging", "math", "re", "struct", "time", "typing",
    "zoneinfo",
})
INTERNAL_ALLOWED: Final = (
    "src.dto",
    "src.oversee.risk",          # pure risk functions
    "src.calendar.sessions",
    "src.calendar.schedule",
    "src.calendar.machine",
    "src.db.arm_ports",          # the src.db store Protocols (plan S24)
)
PACKAGE_EXTRA_ALLOWED: Final[Mapping[str, tuple[str, ...]]] = {
    "riskmgr": ("concurrent.futures", "src.riskmgr", "src.marketdata", "src.armconfig"),
    "marketdata": ("zlib", "src.marketdata"),
    "armconfig": ("ssl", "asyncpg", "src.armconfig", "src.riskmgr.registry", "src.marketdata.indicators.registry"),
}
MARKETDATA_STORE: Final = "src/marketdata/store/"
"""`redis` is allowed only in files under this directory."""

ASYNCIO_ALLOWED: Final = frozenset({
    "sleep", "gather", "wait_for", "Event", "Lock", "Queue", "create_task", "timeout", "CancelledError", "to_thread",
})
ASYNCIO_RISKMGR_EXTRA: Final = frozenset({"get_running_loop", "wrap_future"})
ZLIB_ALLOWED: Final = frozenset({"crc32"})
FUTURES_ALLOWED: Final = frozenset({"ThreadPoolExecutor"})
FUTURES_FILES: Final = frozenset({"src/marketdata/indicators/engine.py"})
"""Files outside `src/riskmgr` that may import `concurrent.futures` (`ThreadPoolExecutor` only) and use `asyncio`'s
`wrap_future`: `md-indicators`' own bounded executor (round 13, plan S38d; never the shared default executor)."""
LOGGING_ALLOWED: Final = frozenset({
    "getLogger", "Logger", "LoggerAdapter", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL",
})
"""Plain `logging` members: no handler classes, `basicConfig`, `root` or `config` (they can open files and sockets)."""

SCANNER_SELF_ALLOWED: Final = frozenset({"__future__", "ast", "dataclasses", "enum", "pathlib", "re", "sys", "typing"})
SCANNER_SELF_SYS_ALLOWED: Final = frozenset({"modules"})

REFUSED_MODULES: Final[Mapping[str, str]] = {
    "os": "process and filesystem access",
    "sys": "interpreter internals",
    "builtins": "reflection through the builtins namespace",
    "importlib": "dynamic import",
    "gc": "object-graph reflection",
    "inspect": "frame and object reflection",
    "socket": "network access",
    "ssl": "TLS network access (allowed only in src/armconfig)",
    "http": "network access (HTTP)",
    "urllib": "network access (HTTP)",
    "smtplib": "network access (mail)",
    "httpx": "network access (HTTP client)",
    "aiohttp": "network access (HTTP client)",
    "requests": "network access (HTTP client)",
    "subprocess": "process execution",
    "ctypes": "native code access",
    "logging.handlers": "logging handlers can open files and sockets",
    "logging.config": "logging config can open files and sockets",
    "src.adapters": "broker adapters: the scoped packages never reach a broker",
    "src.runtime": "runtime and orchestrator: task loops get a small stamp Protocol",
    "src.pubsub": "event bus and orchestrator: the ARM is not a bus subscriber",
    "src.cache": "engine Redis: market data lives only in redis-md",
    "src.notifications": "notification channels: the ARM relay lives in src/notifications",
    "src.calendar.readiness": "calendar readiness dispatcher acts on the engine",
    "src.riskmgr.lockout": "the scanner is imported by no scoped module",
}
EXTRA_REFUSED_MODULE_NAMES: Final = frozenset({
    "threading", "io", "types", "weakref", "copy", "traceback", "warnings", "multiprocessing", "pickle", "marshal",
    "shutil", "tempfile", "pathlib",
})
"""Modules never imported in the scope, refused as attribute names too (see `REFUSED_ATTRIBUTE_NAMES`)."""
LIMITED_MODULE_NAMES: Final = frozenset({"asyncio", "logging", "concurrent", "zlib"})
"""Modules imported only with a member list; reaching one through another module would skip that list."""
REFUSED_ATTRIBUTE_NAMES: Final = frozenset(
    {m for m in REFUSED_MODULES if "." not in m} | (EXTRA_REFUSED_MODULE_NAMES - {"copy"}) | LIMITED_MODULE_NAMES
)
"""Refused as an attribute on any receiver and as `from <anything> import <name>`, so `logging.os`,
`typing.sys.modules` or `from contextlib import sys` cannot reach a refused module through an allowed one. Exceptions:
`ssl` in `src/armconfig`, the real submodules in `REAL_SUBMODULES`, and `types` directly under a scoped package
(`src.marketdata.types`). `copy` is refused the same way except as a direct method call (`d.copy()`)."""
REAL_SUBMODULES: Final = frozenset({"redis.asyncio"})
LOOP_REFUSED_ATTRIBUTES: Final = frozenset({
    "create_connection", "create_server", "create_datagram_endpoint", "create_unix_connection", "create_unix_server",
    "open_connection", "open_unix_connection", "start_server", "start_unix_server", "create_subprocess_exec",
    "create_subprocess_shell", "subprocess_exec", "subprocess_shell", "sock_connect", "sock_sendall", "sock_sendto",
    "sock_sendfile", "sock_recv", "sock_recv_into", "sock_recvfrom", "sock_recvfrom_into", "sock_accept",
    "connect_accepted_socket", "connect_read_pipe", "connect_write_pipe", "start_tls", "getaddrinfo", "getnameinfo",
    "add_reader", "add_writer", "sendfile",
})
"""Event-loop network, socket and process methods, refused on any receiver (`asyncio.get_running_loop()` included)."""
LOGGER_REFUSED_ATTRIBUTES: Final = frozenset({"handlers", "addHandler", "removeHandler"})
"""Refused on any receiver but `self` / `cls`: a logger's handlers can open files and sockets."""
DYNAMIC_CODE_CALLS: Final = frozenset({"__import__", "eval", "exec", "compile"})
REFUSED_BUILTIN_CALLS: Final = frozenset({"open", "breakpoint", "input"})
"""Filesystem access (`os` is refused for the same reason), the debugger and stdin."""
NAMED_REFLECTION_CALLS: Final = frozenset({"getattr", "setattr", "hasattr", "delattr"})
NAMESPACE_REFLECTION_CALLS: Final = frozenset({"vars", "dir", "globals", "locals"})
PORT_TYPE_NAMES: Final = frozenset({"Pager", "PagerWrapper"})
"""Annotation names treated as ports besides any name ending in `Port` and Protocols defined in the same file."""

COMPILED_SUFFIXES: Final = (".pyc", ".pyo", ".pyd", ".so")


# ---------------------------------------------------------------------------------------------------- results
class Rule(Enum):
    IMPORT = "import"
    MEMBER = "member"
    NAME_BAN = "name-ban"
    REFLECTION = "reflection"
    PRIVATE_ATTRIBUTE = "private-attribute"
    PORT_REFLECTION = "port-reflection"
    DYNAMIC_CODE = "dynamic-code"
    REFUSED_BUILTIN = "refused-builtin"
    COMPILED_FILE = "compiled-file"
    LOADED_MODULE = "loaded-module"


@dataclass(frozen=True)
class Violation:
    path: str
    line: int
    rule: Rule
    detail: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: [{self.rule.value}] {self.detail}"


@dataclass(frozen=True)
class ScanResult:
    violations: tuple[Violation, ...]
    incomplete: tuple[str, ...]
    scanned: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.violations and not self.incomplete


class LockoutViolation(Exception):
    """A scan found a banned import, member, name or reflection (the runtime parks with a page)."""


class ScanIncomplete(Exception):
    """A scan could not read or parse a file (the runtime retries with its backoff)."""


def require_clean(result: ScanResult) -> None:
    """Raise `LockoutViolation` on any violation, else `ScanIncomplete` on any unreadable file."""
    if result.violations:
        raise LockoutViolation("; ".join(str(v) for v in result.violations[:20]))
    if result.incomplete:
        raise ScanIncomplete("; ".join(result.incomplete[:20]))


# ---------------------------------------------------------------------------------------------------- sinks
PHASE1_SINKS: Final = ("decision", "notice")
"""The fixed Phase 1 `ARM_SINKS` order (design §B3 Sinks); a Supabase document can never name a sink."""


class SinkSetError(ValueError):
    pass


def assert_sinks(wired: Sequence[str]) -> None:
    """Refuse any wired sink set other than exactly `PHASE1_SINKS`, in that order (used by `optional-bootstrap`)."""
    if tuple(wired) != PHASE1_SINKS:
        raise SinkSetError(f"ARM sinks must be exactly {PHASE1_SINKS} in this order; wired {tuple(wired)}")


# ---------------------------------------------------------------------------------------------------- file context
@dataclass(frozen=True)
class _FileCtx:
    rel: str
    module: str
    is_package: bool
    package: str           # riskmgr | marketdata | armconfig | "" (name ban only)
    is_self: bool
    name_ban_only: bool

    @property
    def in_store(self) -> bool:
        return self.rel.startswith(MARKETDATA_STORE)


def _context(rel: str) -> _FileCtx | None:
    rel = rel.replace("\\", "/").removeprefix("./")
    if not rel.endswith(".py"):
        return None
    module = rel[:-3].replace("/", ".")
    is_package = module.endswith(".__init__")
    if is_package:
        module = module.removesuffix(".__init__")
    if rel in NAME_BAN_FILES:
        return _FileCtx(rel, module, is_package, "", False, True)
    for scope in LOCKOUT_SCOPE:
        if rel.startswith(scope + "/"):
            return _FileCtx(rel, module, is_package, scope.split("/")[1], rel == SCANNER_SELF, False)
    return None


def _under(module: str, prefix: str) -> bool:
    return module == prefix or module.startswith(prefix + ".")


def _member_limits(ctx: _FileCtx) -> dict[str, frozenset[str]]:
    if ctx.is_self:
        return {"sys": SCANNER_SELF_SYS_ALLOWED}
    if ctx.package == "riskmgr":
        asyncio_names = ASYNCIO_ALLOWED | ASYNCIO_RISKMGR_EXTRA
    elif ctx.rel in FUTURES_FILES:
        asyncio_names = ASYNCIO_ALLOWED | frozenset({"wrap_future"})
    else:
        asyncio_names = ASYNCIO_ALLOWED
    return {
        "asyncio": asyncio_names,
        "logging": LOGGING_ALLOWED,
        "zlib": ZLIB_ALLOWED,
        "concurrent": frozenset({"futures"}),
        "concurrent.futures": FUTURES_ALLOWED,
    }


_GENERIC = "not on the import allow-list"


def _refused_module_reason(module: str, ctx: _FileCtx) -> str | None:
    for prefix, reason in REFUSED_MODULES.items():
        if _under(module, prefix):
            if prefix == "ssl" and ctx.package == "armconfig":
                continue
            return reason
    return None


def _import_refusal(module: str, ctx: _FileCtx) -> str | None:
    """None when `module` may be imported by this file, else the named reason."""
    limits = _member_limits(ctx)
    parts = module.split(".")
    if ctx.is_self and parts[0] not in SCANNER_SELF_ALLOWED:
        return f"not on the scanner's own allow-list {sorted(SCANNER_SELF_ALLOWED)}"
    if not ctx.is_self:
        refused = _refused_module_reason(module, ctx)
        if refused is not None:
            return refused
    for i in range(1, len(parts)):
        head = ".".join(parts[:i])
        if head in limits and parts[i] not in limits[head]:
            return f"{module} is not on the {head} allow-list"
    if ctx.is_self:
        return None
    if ctx.package == "marketdata" and (_under(module, "src.riskmgr") or _under(module, "src.armconfig")):
        return "src/marketdata never imports src.riskmgr or src.armconfig"
    if _under(module, "redis") and not ctx.in_store:
        return "redis is allowed only in src/marketdata/store"
    if _under(module, "zlib") and ctx.package != "marketdata":
        return "zlib is allowed only in src/marketdata (crc32 only)"
    if _under(module, "concurrent") and ctx.package != "riskmgr" and ctx.rel not in FUTURES_FILES:
        return "concurrent.futures is allowed only in src/riskmgr and md-indicators' engine (ThreadPoolExecutor only)"
    if _under(module, "asyncpg") and ctx.package != "armconfig":
        return "the database driver is allowed only in src/armconfig"
    if parts[0] in STDLIB_ALLOWED:
        return None
    if _under(module, "redis") and ctx.in_store:
        return None
    if _under(module, "concurrent.futures") and ctx.rel in FUTURES_FILES:
        return None
    for prefix in (*INTERNAL_ALLOWED, *PACKAGE_EXTRA_ALLOWED.get(ctx.package, ())):
        if _under(module, prefix):
            return None
    return _GENERIC


# ---------------------------------------------------------------------------------------------------- AST checker
def _annotation_names(node: ast.AST | None) -> set[str]:
    names: set[str] = set()
    if node is None:
        return names
    for n in ast.walk(node):
        if isinstance(n, ast.Name):
            names.add(n.id)
        elif isinstance(n, ast.Attribute):
            names.add(n.attr)
        elif isinstance(n, ast.Constant) and isinstance(n.value, str):
            try:
                inner = ast.parse(n.value, mode="eval")
            except SyntaxError:
                continue
            names |= _annotation_names(inner.body)
    return names


_PRIVATE_FORMAT_FIELD: Final = re.compile(r"\{[^{}]*\._")
"""A `str.format` field that reads a `_` attribute, such as `{0.__class__}`."""
_FORMAT_METHODS: Final = frozenset({"format", "format_map"})


def _is_static(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    return any((isinstance(d, ast.Name) and d.id == "staticmethod")
               or (isinstance(d, ast.Attribute) and d.attr == "staticmethod") for d in node.decorator_list)


def _bound_names(fn: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda) -> tuple[set[str], set[str]]:
    """(names bound in the body of `fn` other than by an import, names declared global / nonlocal there), without
    entering nested functions, classes or comprehension targets (their own scopes). Defaults, decorators and class
    bases of nested definitions are evaluated in this scope and are walked."""
    bound: set[str] = set()
    declared: set[str] = set()
    stack: list[ast.AST] = [fn.body] if isinstance(fn, ast.Lambda) else list(fn.body)
    while stack:
        n = stack.pop()
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(n.name)
            stack.extend(n.decorator_list)
            if isinstance(n, ast.ClassDef):
                stack.extend(n.bases)
                stack.extend(k.value for k in n.keywords)
            else:
                stack.extend(n.args.defaults)
                stack.extend(d for d in n.args.kw_defaults if d is not None)
            continue
        if isinstance(n, ast.Lambda):
            stack.extend(n.args.defaults)
            stack.extend(d for d in n.args.kw_defaults if d is not None)
            continue
        if isinstance(n, ast.comprehension):
            stack.append(n.iter)
            stack.extend(n.ifs)
            continue
        if isinstance(n, ast.Name) and isinstance(n.ctx, (ast.Store, ast.Del)):
            bound.add(n.id)
        elif isinstance(n, (ast.Global, ast.Nonlocal)):
            declared.update(n.names)
        elif isinstance(n, ast.ExceptHandler) and n.name:
            bound.add(n.name)
        elif isinstance(n, (ast.MatchAs, ast.MatchStar)) and n.name:
            bound.add(n.name)
        elif isinstance(n, ast.MatchMapping) and n.rest:
            bound.add(n.rest)
        stack.extend(ast.iter_child_nodes(n))
    return bound, declared


@dataclass(frozen=True)
class _Scope:
    """A function scope: its local names (parameters and body bindings, not imports, not global / nonlocal) and, for a
    method defined directly in a class body, its first parameter when it is `self` / `cls` and never rebound."""
    names: frozenset[str]
    self_name: str | None


class _Checker(ast.NodeVisitor):
    def __init__(self, tree: ast.Module, ctx: _FileCtx, path: str) -> None:
        self.ctx = ctx
        self.path = path
        self.found: list[Violation] = []
        self.bindings: dict[str, str] = {}
        self.module_bindings: set[str] = set()
        self.parents: dict[int, ast.AST] = {}
        self.protocols: set[str] = set()
        self.port_params: list[frozenset[str]] = [frozenset()]
        self.scopes: list[_Scope] = []
        self.limits = _member_limits(ctx)
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                self.parents[id(child)] = node
            if isinstance(node, ast.Import):
                for alias in node.names:
                    bound = alias.asname or alias.name.split(".")[0]
                    self.bindings[bound] = alias.name if alias.asname else bound
                    self.module_bindings.add(bound)
            elif isinstance(node, ast.ImportFrom):
                base = self._resolve_from(node)
                for alias in node.names:
                    if alias.name != "*":
                        full = f"{base}.{alias.name}" if base else alias.name
                        self.bindings[alias.asname or alias.name] = full
                        if full in self.limits or alias.name in LIMITED_MODULE_NAMES:
                            self.module_bindings.add(alias.asname or alias.name)
            elif isinstance(node, ast.ClassDef):
                for b in node.bases:
                    if (isinstance(b, ast.Name) and b.id == "Protocol") or (
                            isinstance(b, ast.Attribute) and b.attr == "Protocol"):
                        self.protocols.add(node.name)

    # ---------------------------------------------------------------- helpers
    def _add(self, node: ast.AST, rule: Rule, detail: str) -> None:
        self.found.append(Violation(self.path, getattr(node, "lineno", 0), rule, detail))

    def _resolve_from(self, node: ast.ImportFrom) -> str:
        if not node.level:
            return node.module or ""
        base = self.ctx.module if self.ctx.is_package else self.ctx.module.rpartition(".")[0]
        for _ in range(node.level - 1):
            base = base.rpartition(".")[0]
        if node.module:
            return f"{base}.{node.module}" if base else node.module
        return base

    def _dotted(self, node: ast.expr) -> str | None:
        """The module-qualified dotted path of a Name / Attribute chain rooted in an imported name, else None."""
        attrs: list[str] = []
        while isinstance(node, ast.Attribute):
            attrs.append(node.attr)
            node = node.value
        if not isinstance(node, ast.Name) or node.id not in self.bindings:
            return None
        return ".".join([self.bindings[node.id], *reversed(attrs)])

    def _is_direct_call(self, node: ast.expr) -> bool:
        parent = self.parents.get(id(node))
        return isinstance(parent, ast.Call) and parent.func is node

    def _is_local(self, name: str) -> bool:
        return any(name in scope.names for scope in self.scopes)

    def _is_self_receiver(self, node: ast.expr) -> bool:
        """`self` / `cls` as the first parameter of a method defined in a class body (not a staticmethod, never
        rebound, not shadowed by an inner scope), or a zero-argument `super()`."""
        if isinstance(node, ast.Call):
            return (isinstance(node.func, ast.Name) and node.func.id == "super" and not node.args
                    and not node.keywords and "super" not in self.bindings and not self._is_local("super"))
        if not isinstance(node, ast.Name) or node.id not in ("self", "cls"):
            return False
        for scope in reversed(self.scopes):
            if scope.self_name == node.id:
                return True
            if node.id in scope.names:
                return False
        return False

    def _function_scope(self, node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda) -> _Scope:
        a = node.args
        positional = [*a.posonlyargs, *a.args]
        params = {p.arg for p in (*positional, *a.kwonlyargs, *(p for p in (a.vararg, a.kwarg) if p is not None))}
        bound, declared = _bound_names(node)
        self_name: str | None = None
        parent = self.parents.get(id(node))
        if (not isinstance(node, ast.Lambda) and isinstance(parent, ast.ClassDef)
                and any(n is node for n in parent.body) and not _is_static(node) and positional
                and positional[0].arg in ("self", "cls") and positional[0].arg not in bound | declared):
            self_name = positional[0].arg
        return _Scope(frozenset((params | bound) - declared), self_name)

    def _is_port_expr(self, node: ast.expr) -> bool:
        if isinstance(node, ast.Name):
            return any(node.id in params for params in self.port_params)
        if isinstance(node, ast.Attribute) and self._is_self_receiver(node.value):
            tail = node.attr.lstrip("_").lower()
            return tail.endswith("port") or tail.endswith("pager")
        return False

    def _is_port_annotation(self, ann: ast.expr | None) -> bool:
        return any(n.endswith("Port") or n in PORT_TYPE_NAMES or n in self.protocols for n in _annotation_names(ann))

    def _name_ban(self, node: ast.AST, name: str) -> None:
        if is_banned_name(name):
            self._add(node, Rule.NAME_BAN, f"name {name!r} (normalised {normalise_name(name)!r}) matches the name ban")

    def _attribute_refusal(self, name: str, owner: str | None, *, called: bool, on_self: bool) -> str | None:
        """Why the attribute (or from-imported name) `name` is refused whatever it is read on, else None. `owner` is
        the import-rooted dotted path of the receiver when there is one."""
        if name in LOOP_REFUSED_ATTRIBUTES:
            return "event-loop network, socket and process methods are refused on any receiver"
        if name in LOGGER_REFUSED_ATTRIBUTES and not on_self:
            return "logger handlers are refused (they can open files and sockets)"
        if name == "copy":
            return None if called else "names the refused copy module (only a direct .copy() method call passes)"
        if name in REFUSED_ATTRIBUTE_NAMES:
            if name == "ssl" and self.ctx.package == "armconfig":
                return None
            if owner is not None and f"{owner}.{name}" in REAL_SUBMODULES:
                return None
            if name == "types" and owner in SCOPE_PACKAGES:
                return None
            return "names a refused or limited module, which is never reached through another module or object"
        return None

    # ---------------------------------------------------------------- definitions
    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda) -> None:
        # Decorators, defaults and annotations are evaluated outside the function's own scope.
        if not isinstance(node, ast.Lambda):
            for item in (*node.decorator_list, *node.type_params):
                self.visit(item)
            if node.returns is not None:
                self.visit(node.returns)
        self.visit(node.args)
        a = node.args
        params = [*a.posonlyargs, *a.args, *a.kwonlyargs, *(p for p in (a.vararg, a.kwarg) if p is not None)]
        ports = frozenset(p.arg for p in params if self._is_port_annotation(p.annotation))
        self.port_params.append(ports)
        self.scopes.append(self._function_scope(node))
        body: list[ast.AST] = [node.body] if isinstance(node, ast.Lambda) else list(node.body)
        for item in body:
            self.visit(item)
        self.scopes.pop()
        self.port_params.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._name_ban(node, node.name)
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._name_ban(node, node.name)
        self._visit_function(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        self._visit_function(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._name_ban(node, node.name)
        self.generic_visit(node)

    # ---------------------------------------------------------------- imports
    def visit_Import(self, node: ast.Import) -> None:
        if not self.ctx.name_ban_only:
            for alias in node.names:
                reason = _import_refusal(alias.name, self.ctx)
                if reason is not None:
                    self._add(node, Rule.IMPORT, f"import {alias.name}: {reason}")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if not self.ctx.name_ban_only:
            self._check_from(node)
        self.generic_visit(node)

    def _check_from(self, node: ast.ImportFrom) -> None:
        base = self._resolve_from(node)
        reason = _import_refusal(base, self.ctx) if base else _GENERIC
        if reason is not None and reason != _GENERIC:
            self._add(node, Rule.IMPORT, f"from {base} import ...: {reason}")
            return
        for alias in node.names:
            if alias.name == "*":
                self._add(node, Rule.IMPORT, f"from {base} import *: star imports are refused in the lockout scope")
                continue
            full = f"{base}.{alias.name}" if base else alias.name
            named = self._attribute_refusal(alias.name, base or None, called=False, on_self=False)
            if named is not None:
                self._add(node, Rule.MEMBER, f"{full}: {named}")
                continue
            if not self.ctx.is_self:
                refused = _refused_module_reason(full, self.ctx)
                if refused is not None:
                    self._add(node, Rule.IMPORT, f"from {base} import {alias.name}: {refused}")
                    continue
            if base in self.limits:
                if alias.name not in self.limits[base]:
                    self._add(node, Rule.MEMBER, f"{full} is not on the {base} allow-list")
                continue
            sub = _import_refusal(full, self.ctx)
            if reason is None:
                # The module is allowed; only a specific refusal of the member (a refused submodule) counts.
                if sub is not None and sub != _GENERIC:
                    self._add(node, Rule.IMPORT, f"from {base} import {alias.name}: {sub}")
            elif sub is not None:
                self._add(node, Rule.IMPORT, f"from {base} import {alias.name}: {sub}")

    # ---------------------------------------------------------------- attributes and names
    def visit_Attribute(self, node: ast.Attribute) -> None:
        if not self.ctx.name_ban_only:
            self._check_attribute(node)
        self.generic_visit(node)

    def _check_attribute(self, node: ast.Attribute) -> None:
        on_self = self._is_self_receiver(node.value)
        if node.attr.startswith("_") and not self.ctx.is_self and not on_self:
            self._add(node, Rule.PRIVATE_ATTRIBUTE,
                      f"attribute {node.attr!r} is read on something other than self / cls")
        refused = self._attribute_refusal(node.attr, self._dotted(node.value),
                                          called=self._is_direct_call(node), on_self=on_self)
        if refused is not None:
            self._add(node, Rule.MEMBER, f"attribute {node.attr!r}: {refused}")
        if node.attr in _FORMAT_METHODS and not self.ctx.is_self:
            receiver = node.value
            if not (isinstance(receiver, ast.Constant) and isinstance(receiver.value, str)):
                self._add(node, Rule.REFLECTION, f".{node.attr}() on something other than a string literal")
            elif _PRIVATE_FORMAT_FIELD.search(receiver.value):
                self._add(node, Rule.PRIVATE_ATTRIBUTE, f".{node.attr}() template reads a `_` attribute")
        dotted = self._dotted(node)
        if dotted is not None:
            self._check_member_path(node, dotted)

    def _check_member_path(self, node: ast.AST, dotted: str) -> None:
        parts = dotted.split(".")
        for i in range(1, len(parts)):
            head = ".".join(parts[:i])
            if head in self.limits and parts[i] not in self.limits[head]:
                self._add(node, Rule.MEMBER, f"{head}.{parts[i]} is not on the {head} allow-list")
                return
            member = ".".join(parts[: i + 1])
            ref = REFUSED_MODULES.get(member)
            if ref is not None:
                self._add(node, Rule.MEMBER, f"{member}: {ref}")
                return

    def visit_MatchClass(self, node: ast.MatchClass) -> None:
        if not self.ctx.name_ban_only and not self.ctx.is_self:
            for attr in node.kwd_attrs:
                if attr.startswith("_"):
                    self._add(node, Rule.PRIVATE_ATTRIBUTE, f"match pattern reads the attribute {attr!r}")
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if not self.ctx.name_ban_only:
            self._check_name(node)
        self.generic_visit(node)

    def _check_name(self, node: ast.Name) -> None:
        name = node.id
        if name == "__builtins__":
            self._add(node, Rule.DYNAMIC_CODE, "the __builtins__ namespace is refused")
            return
        if not isinstance(node.ctx, ast.Load) or self._is_local(name):
            return
        if name in self.module_bindings:
            parent = self.parents.get(id(node))
            if not (isinstance(parent, ast.Attribute) and parent.value is node):
                self._add(node, Rule.MEMBER,
                          f"module {name!r} used as a value (its members are checked only through attribute access)")
            return
        if name in self.bindings or self._is_direct_call(node):
            return
        # A builtin the call checks cover, used other than as the direct callee (an alias, an argument, a return
        # value), would skip those checks.
        if name in DYNAMIC_CODE_CALLS:
            self._add(node, Rule.DYNAMIC_CODE, f"{name} used as a value is refused (dynamic import or code)")
        elif name in REFUSED_BUILTIN_CALLS:
            self._add(node, Rule.REFUSED_BUILTIN, f"{name} used as a value is refused")
        elif not self.ctx.is_self and name in NAMED_REFLECTION_CALLS | NAMESPACE_REFLECTION_CALLS:
            self._add(node, Rule.REFLECTION, f"{name} used as a value (only a direct call is checked)")

    # ---------------------------------------------------------------- calls
    def visit_Call(self, node: ast.Call) -> None:
        if not self.ctx.name_ban_only:
            self._check_call(node)
        self.generic_visit(node)

    def _check_call(self, node: ast.Call) -> None:
        func = node.func
        name = (func.id if isinstance(func, ast.Name) and func.id not in self.bindings
                and not self._is_local(func.id) else None)
        if name in DYNAMIC_CODE_CALLS:
            self._add(node, Rule.DYNAMIC_CODE, f"{name}() is refused (dynamic import or code)")
            return
        if name in REFUSED_BUILTIN_CALLS:
            self._add(node, Rule.REFUSED_BUILTIN, f"{name}() is refused (filesystem, debugger or stdin)")
            return
        if self.ctx.is_self:
            return
        if name in NAMED_REFLECTION_CALLS:
            arg = node.args[1] if len(node.args) >= 2 and not any(
                isinstance(a, ast.Starred) for a in node.args[:2]) else None
            if not (isinstance(arg, ast.Constant) and isinstance(arg.value, str)):
                self._add(node, Rule.REFLECTION, f"{name}() with a non-literal name")
            elif arg.value.startswith("_"):
                self._add(node, Rule.REFLECTION, f"{name}() with the private name {arg.value!r}")
            elif self._attribute_refusal(arg.value, None, called=False, on_self=False) is not None:
                self._add(node, Rule.REFLECTION, f"{name}() with the refused name {arg.value!r}")
            return
        target: str | None = None
        if name in NAMESPACE_REFLECTION_CALLS:
            parent = self.parents.get(id(node))
            key = parent.slice if isinstance(parent, ast.Subscript) and parent.value is node else None
            if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
                self._add(node, Rule.REFLECTION, f"{name}() used without a literal key")
            elif key.value.startswith("_"):
                self._add(node, Rule.REFLECTION, f"{name}() with the private key {key.value!r}")
            elif key.value in self.module_bindings or self._attribute_refusal(
                    key.value, None, called=False, on_self=False) is not None:
                self._add(node, Rule.REFLECTION, f"{name}() with the module or refused key {key.value!r}")
            if name == "vars":
                target = "vars"
        else:
            dotted = self._dotted(func) if isinstance(func, (ast.Name, ast.Attribute)) else None
            if dotted in ("dataclasses.asdict", "dataclasses.fields"):
                target = dotted
        if target is not None and node.args and self._is_port_expr(node.args[0]):
            self._add(node, Rule.PORT_REFLECTION, f"{target}() applied to a port")


# ---------------------------------------------------------------------------------------------------- scanning
def _check_tree(tree: ast.Module, ctx: _FileCtx, path: str) -> list[Violation]:
    checker = _Checker(tree, ctx, path)
    checker.visit(tree)
    seen: set[tuple[int, Rule, str]] = set()
    out: list[Violation] = []
    for v in checker.found:
        key = (v.line, v.rule, v.detail)
        if key not in seen:
            seen.add(key)
            out.append(v)
    return out


def scan_source(source: str, rel_path: str) -> tuple[Violation, ...]:
    """Check one source text as if it lived at `rel_path` (repository-relative). Raises SyntaxError / ValueError."""
    ctx = _context(rel_path)
    if ctx is None:
        raise ValueError(f"{rel_path} is outside the lockout scope")
    return tuple(_check_tree(ast.parse(source, filename=rel_path), ctx, rel_path))


class _Scan:
    def __init__(self) -> None:
        self.violations: list[Violation] = []
        self.incomplete: list[str] = []
        self.scanned: list[str] = []
        self.seen: set[str] = set()

    def file(self, path: Path, ctx: _FileCtx) -> None:
        key = self._key(path)
        if key in self.seen:
            return
        self.seen.add(key)
        shown = path.as_posix()
        try:
            source = path.read_bytes().decode("utf-8-sig")
            found = _check_tree(ast.parse(source, filename=shown), ctx, shown)
        except SyntaxError as e:
            self.incomplete.append(f"{shown}: SyntaxError at line {e.lineno}")
            return
        except (OSError, UnicodeDecodeError, ValueError, RecursionError, MemoryError) as e:
            # A nesting too deep for the parser or the AST walk is reported like an unreadable file, never raised.
            self.incomplete.append(f"{shown}: {type(e).__name__}")
            return
        self.scanned.append(shown)
        self.violations.extend(found)

    @staticmethod
    def _key(path: Path) -> str:
        try:
            return str(path.resolve()).lower()
        except OSError:
            return str(path.absolute()).lower()

    def tree(self, root: Path) -> None:
        for scope in LOCKOUT_SCOPE:
            base = root / scope
            if not base.exists():
                continue
            try:
                entries = sorted(base.rglob("*"))
            except OSError as e:
                self.incomplete.append(f"{base.as_posix()}: {type(e).__name__}")
                continue
            for p in entries:
                rel = p.relative_to(root).as_posix()
                if "__pycache__" in p.relative_to(root).parts:
                    continue
                if p.suffix in COMPILED_SUFFIXES:
                    self.violations.append(Violation(p.as_posix(), 0, Rule.COMPILED_FILE,
                                                     "compiled module file inside the lockout scope"))
                elif p.suffix == ".py":
                    ctx = _context(rel)
                    if ctx is not None:
                        self.file(p, ctx)
        for name in NAME_BAN_FILES:
            p = root / name
            ctx = _context(name)
            if p.exists() and ctx is not None:
                self.file(p, ctx)

    def loaded(self, modules: Mapping[str, object]) -> None:
        for name, mod in sorted(modules.items()):
            if not (any(_under(name, pkg) for pkg in SCOPE_PACKAGES) or name in NAME_BAN_MODULES):
                continue
            file = getattr(mod, "__file__", None)
            if not isinstance(file, str):
                self.violations.append(Violation(name, 0, Rule.LOADED_MODULE, "loaded module has no source file"))
                continue
            p = Path(file)
            if p.suffix != ".py":
                self.violations.append(Violation(name, 0, Rule.LOADED_MODULE,
                                                 f"loaded module is not Python source ({p.suffix or 'no suffix'})"))
                continue
            rel = name.replace(".", "/") + ("/__init__.py" if p.name == "__init__.py" else ".py")
            ctx = _context(rel)
            if ctx is not None:
                self.file(p, ctx)


def scan(roots: Iterable[Path | str], *, modules: Mapping[str, object] | None = None) -> ScanResult:
    """Scan `LOCKOUT_SCOPE` (and `NAME_BAN_FILES`) under each repository root, plus every loaded module of the
    scanned packages (`modules`, default `sys.modules`). Never raises for a bad file: it is listed in `incomplete`."""
    run = _Scan()
    for root in roots:
        run.tree(Path(root))
    run.loaded(dict(sys.modules) if modules is None else modules)
    return ScanResult(tuple(run.violations), tuple(run.incomplete), tuple(run.scanned))
