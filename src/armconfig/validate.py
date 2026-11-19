"""Row validation of an ARM configuration document (design §B9 Row validation, §B8 Policy, §B6.7; plan S25). Pure.

Zero trust applies to config too: every row is checked before it counts as pending, and a document with any failing
row never becomes a `ValidDocument` (the accepted document stays in force and the installer, S33, pages by row id and
row hash). `validate_document(doc, known, registry, seen=..., at_ms=...)`:

* **Seen rows versus pending rows.** Tables are append-only (a deleted seen row reads as `VANISHED` tamper,
  `src/armconfig/chain.py`), so a superseded or tombstoned row stays in every later document. The full new-pin checks
  apply only to a **pending** row (a ref not in `seen`, or seen with another hash): a deprecated `(kind,
  impl_version)` or guard and a bar spec with no timing row. A row already **seen** with the same hash (`seen` is
  the accepted document's `ref -> row hash`, the `SeenRows` of S33) is parsed with `new_pin=False` (deprecation and
  timing rows are not checked again; design §B6.1: deprecated versions are refused only for new pins).
  **Known ids** (symbol, adapter, account, route) are checked against `KnownIds`, for seen and pending rows alike,
  only while the row is live at `at_ms`: an enabled assignment in force for its key, a route policy in force for its
  route, or any future-dated row (`effective_from_ms > at_ms`). A tombstone or a superseded row naming an id that
  has since been removed, or a pending typo that a later pending row already tombstoned or superseded, never makes a
  document invalid (the tables are append-only, so such a row could otherwise block every later install for good;
  plan S33 review). With `at_ms` None every row counts as live. A **pending** enabled
  assignment that names a strategy row a new pin would refuse (deprecated, or no timing row) is refused with that
  code; a pending tombstone of it is fine, so a deprecated strategy can always be switched off.

* **Every row:** exactly the table's columns (`COLUMNS`: an unknown column is refused, so a text column the schema
  does not define never gets in); an envelope or `pre_offset` key (`LOCAL_ONLY_KEYS`) or a sink key (`SINK_KEYS`)
  anywhere in the row, nested params included, is refused by name; the append-only guard columns `set_at_ms`
  (integer) and `session_user` / `current_user` (role-name form).
* **strategies:** `code` matches `^[a-z0-9_]{1,32}$`; `version` an integer; `(kind, impl_version)` registered and not
  deprecated; guard settings only inside `params.guards: [{kind, version, params}]` (a guard KIND, or a `<kind>.<key>`
  key, among the strategy's own params is refused); `params` pass the registry's `parse_params` (strategy keys, guard
  entries, required guards, bounds, envelope keys) and every bar spec the strategy triggers on has a timing row
  (`TimingRowError`, §B5.2); `(code, version)` unique.
* **assignments:** `edge_code` NULL (Phase 1 single edge, §B11); `symbol`, `adapter` and `account` NULL (wildcard) or
  known through the injected `KnownIds` (an account by id or local alias, stored as the account id); the named
  `(strategy_code, strategy_version)` is a strategy row of the document, whose `RESOURCE` keys the assignment;
  `enabled` a bool (`false` = tombstone); `effective_from_ms` an integer time; two rows with the same key and the
  same `effective_from_ms` are both refused.
* **route_policy:** a known route; `mode` `every_bar` | `actionable`; `pre_channels` a non-empty subset of
  `CHANNELS`; `post_channels` a subset containing `arm_email`; no duplicate channel; `(route, effective_from_ms)`
  unique.

A `ConfigProblem` names the row by `table:id:hash` and a fixed code (`page_text`); its `detail` (keys, kinds, bounds;
never a row value) is escaped with `escape_text` for `arm config show`. `escape_text` is also the one escaper the
show command uses for any Supabase text (control and non-printing characters, backslash).
"""
from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Final, Protocol, TypeVar

from src.armconfig.document import (
    LOCAL_ONLY_KEYS,
    SINK_KEYS,
    ConfigRow,
    Document,
    DocumentProblem,
    int_value,
)
from src.riskmgr.registry import REGISTRY, ArmRegistry, StrategyParams, TimingRowError

CODE_RE: Final = re.compile(r"[a-z0-9_]{1,32}")
KIND_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,31}")
ROLE_RE: Final = re.compile(r"[a-z_][a-z0-9_]{0,62}")
ROW_VERSION_MAX: Final = 2**31 - 1
TIME_MS_MAX: Final = 253_402_300_799_999
"""The last millisecond of the year 9999: the bound of `effective_from_ms` and `set_at_ms`."""

META_COLUMNS: Final = frozenset({"id", "set_at_ms", "session_user", "current_user"})
COLUMNS: Final[Mapping[str, frozenset[str]]] = {
    "strategies": META_COLUMNS | {"code", "version", "kind", "impl_version", "params"},
    "assignments": META_COLUMNS | {"edge_code", "symbol", "account", "adapter", "strategy_code", "strategy_version",
                                   "enabled", "effective_from_ms"},
    "route_policy": META_COLUMNS | {"route", "mode", "pre_channels", "post_channels", "effective_from_ms"},
}
MODES: Final = ("every_bar", "actionable")
CHANNELS: Final = ("arm_email", "arm_push")
REQUIRED_POST_CHANNEL: Final = "arm_email"
DEFAULT_MODE: Final = "every_bar"
DEFAULT_PRE_CHANNELS: Final = ("arm_email",)
DEFAULT_POST_CHANNELS: Final = ("arm_email",)
GUARDS_KEY: Final = "guards"
DETAIL_LIMIT: Final = 200


def escape_text(text: str, limit: int = DETAIL_LIMIT) -> str:
    """`text` with backslash, control and non-printing characters escaped (`\\xNN`, `\\uNNNN`, `\\UNNNNNNNN`),
    cut to `limit` characters (then `...`)."""
    out: list[str] = []
    for ch in text[:limit]:
        if ch == "\\":
            out.append("\\\\")
        elif ch.isprintable():
            out.append(ch)
        else:
            cp = ord(ch)
            out.append(f"\\x{cp:02x}" if cp < 0x100 else f"\\u{cp:04x}" if cp < 0x10000 else f"\\U{cp:08x}")
    if len(text) > limit:
        out.append("...")
    return "".join(out)


# ---------------------------------------------------------------------------------------------------- results
@dataclass(frozen=True)
class KnownIds:
    """The ids a document may name, injected by the caller (S33: `instruments`, configured adapters, accounts and
    their local aliases, notice routes). `accounts` maps an account id or local alias to the account id."""

    symbols: frozenset[str] = frozenset()
    adapters: frozenset[str] = frozenset()
    accounts: Mapping[str, str] = field(default_factory=dict)
    routes: frozenset[str] = frozenset()


@dataclass(frozen=True)
class ConfigProblem:
    """A refused row (or the whole read, `table == ""`): fixed code, row id and hash; `detail` for the show only."""

    code: str
    table: str = ""
    row_id: int | None = None
    row_hash: str = ""
    detail: str = ""

    @property
    def ref(self) -> str:
        if not self.table:
            return "document"
        if self.row_id is None:
            return self.table
        return f"{self.table}:{self.row_id}:{self.row_hash}" if self.row_hash else f"{self.table}:{self.row_id}"

    @property
    def page_text(self) -> str:
        """What a page, log record or audit row may carry: the code and the row's id and hash, never its text."""
        return f"{self.code} {self.ref}"


@dataclass(frozen=True)
class StrategyEntry:
    """A valid `strategies` row: `(code, version)` and its parsed pin (`params`, guard list included)."""

    row_id: int
    code: str
    version: int
    params: StrategyParams
    resource: str

    @property
    def kind(self) -> str:
        return self.params.kind

    @property
    def impl_version(self) -> int:
        return self.params.impl_version


@dataclass(frozen=True)
class Assignment:
    """A valid `assignments` row (NULL = wildcard; `account` is the account id even when the row named an alias)."""

    row_id: int
    edge_code: str | None
    symbol: str | None
    account: str | None
    adapter: str | None
    strategy_code: str
    strategy_version: int
    resource: str
    enabled: bool
    effective_from_ms: int

    @property
    def key(self) -> tuple[str | None, str | None, str | None, str | None, str]:
        """The (scope, RESOURCE) key: one row in force per key (§B9)."""
        return (self.edge_code, self.symbol, self.account, self.adapter, self.resource)

    @property
    def specificity(self) -> tuple[bool, bool, bool, bool]:
        """Precedence edge > symbol > account > adapter, set beats NULL (compared as a tuple)."""
        return (self.edge_code is not None, self.symbol is not None, self.account is not None,
                self.adapter is not None)


@dataclass(frozen=True)
class RoutePolicy:
    """A route's notice policy (§B8). `row_id` 0 = the default policy (no row in force)."""

    route: str
    mode: str = DEFAULT_MODE
    pre_channels: tuple[str, ...] = DEFAULT_PRE_CHANNELS
    post_channels: tuple[str, ...] = DEFAULT_POST_CHANNELS
    effective_from_ms: int = 0
    row_id: int = 0


@dataclass(frozen=True)
class ValidDocument:
    schema_version: int
    doc_hash: str
    strategies: tuple[StrategyEntry, ...]
    assignments: tuple[Assignment, ...]
    route_policies: tuple[RoutePolicy, ...]
    row_hashes: tuple[tuple[str, str], ...]

    def strategy(self, code: str, version: int) -> StrategyEntry | None:
        for s in self.strategies:
            if s.code == code and s.version == version:
                return s
        return None


class ValidationStatus(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"
    SCHEMA_AHEAD = "SCHEMA_AHEAD"


@dataclass(frozen=True)
class ValidationResult:
    status: ValidationStatus
    document: ValidDocument | None = None
    problems: tuple[ConfigProblem, ...] = ()


class _Dated(Protocol):
    @property
    def effective_from_ms(self) -> int: ...

    @property
    def row_id(self) -> int: ...


_D = TypeVar("_D", bound=_Dated)


def latest_per_key(items: Iterable[_D], key: Callable[[_D], Hashable], at_ms: int) -> dict[Hashable, _D]:
    """Per key, the item with the greatest `effective_from_ms <= at_ms` (a tie goes to the higher row id): the rows
    in force at `at_ms`, tombstones included (the resolver drops them)."""
    best: dict[Hashable, _D] = {}
    for item in items:
        if item.effective_from_ms > at_ms:
            continue
        k = key(item)
        held = best.get(k)
        if held is None or (item.effective_from_ms, item.row_id) > (held.effective_from_ms, held.row_id):
            best[k] = item
    return best


# ---------------------------------------------------------------------------------------------------- row checks
class _Refused(Exception):
    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(code)
        self.code = code
        self.detail = detail


def _scan_forbidden_keys(value: object) -> None:
    """An envelope / `pre_offset` or sink key anywhere in the row (the parser bounds the depth)."""
    if isinstance(value, Mapping):
        for key in sorted(k for k in value if isinstance(k, str)):
            if key in LOCAL_ONLY_KEYS:
                raise _Refused("envelope_key", f"{key} is a local engine setting, never from the document")
            if key in SINK_KEYS:
                raise _Refused("sink_key", "a document never names a sink")
        for v in value.values():
            _scan_forbidden_keys(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            _scan_forbidden_keys(v)


def _check_columns(table: str, cols: Mapping[str, object]) -> None:
    unknown = sorted(set(cols) - COLUMNS[table])
    if unknown:
        raise _Refused("unknown_column", ", ".join(escape_text(k, 40) for k in unknown[:5]))
    missing = sorted(COLUMNS[table] - set(cols))
    if missing:
        raise _Refused("missing_column", ", ".join(missing))
    if int_value(cols["set_at_ms"], 0, TIME_MS_MAX) is None:
        raise _Refused("metadata_form", "set_at_ms must be a time in ms")
    for name in ("session_user", "current_user"):
        value = cols[name]
        if not isinstance(value, str) or not ROLE_RE.fullmatch(value):
            raise _Refused("metadata_form", f"{name} must be a role name")


def _text(cols: Mapping[str, object], name: str, pattern: re.Pattern[str], code: str) -> str:
    value = cols[name]
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise _Refused(code, f"{name} must match {pattern.pattern}")
    return value


def _int(cols: Mapping[str, object], name: str, lo: int, hi: int, code: str) -> int:
    value = int_value(cols[name], lo, hi)
    if value is None:
        raise _Refused(code, f"{name} must be an integer in [{lo}, {hi}]")
    return value


def _pin_refusal(registry: ArmRegistry, kind: str, impl_version: int, raw: Mapping[str, object]) -> _Refused | None:
    """Why a new pin of these params would be refused (deprecated strategy or guard, a bar spec with no timing row),
    else None."""
    if registry.strategy_deprecated(kind, impl_version):
        return _Refused("strategy_deprecated", f"strategy {kind} v{impl_version} is deprecated")
    try:
        registry.parse_params(kind, impl_version, raw, new_pin=True)
    except TimingRowError as exc:
        return _Refused("no_timing_row", escape_text(str(exc)))
    except ValueError as exc:   # ParamError: a deprecated guard (the rest passed with new_pin=False)
        return _Refused("params_invalid", escape_text(str(exc)))
    return None


def _strategy(row: ConfigRow, cols: Mapping[str, object], registry: ArmRegistry,
              *, seen: bool) -> tuple[StrategyEntry, _Refused | None]:
    """The entry and, for a seen row, why a new pin of it would be refused (a pending row raises that instead)."""
    code = _text(cols, "code", CODE_RE, "code_form")
    version = _int(cols, "version", 1, ROW_VERSION_MAX, "version_form")
    kind = _text(cols, "kind", KIND_RE, "strategy_unknown")
    impl_version = _int(cols, "impl_version", 1, ROW_VERSION_MAX, "strategy_unknown")
    strategy = registry.strategy(kind, impl_version)
    if strategy is None:
        raise _Refused("strategy_unknown", f"strategy {kind} v{impl_version} is not registered")
    if not seen and registry.strategy_deprecated(kind, impl_version):
        raise _Refused("strategy_deprecated", f"strategy {kind} v{impl_version} is deprecated")
    raw = cols["params"]
    if not isinstance(raw, Mapping):
        raise _Refused("params_invalid", "params must be an object")
    guard_kinds = {k for k, _ in registry.guard_keys()}
    for key in sorted(k for k in raw if isinstance(k, str)):
        if key != GUARDS_KEY and (key in guard_kinds or key.split(".", 1)[0] in guard_kinds):
            raise _Refused("guard_key_outside_guards", "guard settings live only in guards: [{kind, version, params}]")
    try:
        params = registry.parse_params(kind, impl_version, raw, new_pin=not seen)
    except TimingRowError as exc:
        raise _Refused("no_timing_row", escape_text(str(exc))) from None
    except ValueError as exc:   # ParamError: the text names keys, kinds and bounds, never a value
        raise _Refused("params_invalid", escape_text(str(exc))) from None
    entry = StrategyEntry(row_id=row.row_id, code=code, version=version, params=params,
                          resource=str(strategy.RESOURCE.value))
    return entry, (_pin_refusal(registry, kind, impl_version, raw) if seen else None)


class _Deferred:
    """The first known-id refusal of a row (seen or pending), applied only when the row is live (module docstring).
    `seen` still selects the new-pin checks."""

    def __init__(self, seen: bool) -> None:
        self.seen = seen
        self.refused: _Refused | None = None

    def refuse(self, refused: _Refused) -> None:
        if self.refused is None:
            self.refused = refused


def _optional_known(cols: Mapping[str, object], name: str, known: frozenset[str] | Mapping[str, str],
                    code: str, deferred: _Deferred) -> str | None:
    """NULL, or a known id (an account alias mapped to its id). An unknown id keeps the text as written (it only keys
    the row) and the refusal is deferred until liveness is known."""
    value = cols[name]
    if value is None:
        return None
    if not isinstance(value, str):
        raise _Refused(code, f"{name} must be NULL or a known id")
    if value in known:
        return known[value] if isinstance(known, Mapping) else value
    deferred.refuse(_Refused(code, f"{name} must be NULL or a known id"))
    return value


def _assignment(row: ConfigRow, cols: Mapping[str, object], known: KnownIds,
                strategies: Mapping[tuple[str, int], StrategyEntry | None],
                pin_refusals: Mapping[tuple[str, int], _Refused], deferred: _Deferred) -> Assignment:
    if cols["edge_code"] is not None:
        raise _Refused("edge_code_set", "edge_code must be NULL in Phase 1 (single edge)")
    symbol = _optional_known(cols, "symbol", known.symbols, "symbol_unknown", deferred)
    adapter = _optional_known(cols, "adapter", known.adapters, "adapter_unknown", deferred)
    account = _optional_known(cols, "account", known.accounts, "account_unknown", deferred)
    code = _text(cols, "strategy_code", CODE_RE, "code_form")
    version = _int(cols, "strategy_version", 1, ROW_VERSION_MAX, "version_form")
    if (code, version) not in strategies:
        raise _Refused("strategy_ref_unknown", "strategy_code / strategy_version name no strategies row")
    enabled = cols["enabled"]
    if not isinstance(enabled, bool):
        raise _Refused("enabled_form", "enabled must be true or false")
    effective = _int(cols, "effective_from_ms", 0, TIME_MS_MAX, "effective_from_form")
    refusal = pin_refusals.get((code, version))
    if enabled and not deferred.seen and refusal is not None:   # a new pending row would pin a refused strategy
        raise _Refused(refusal.code, refusal.detail)
    entry = strategies[(code, version)]
    return Assignment(row_id=row.row_id, edge_code=None, symbol=symbol, account=account, adapter=adapter,
                      strategy_code=code, strategy_version=version,
                      resource=entry.resource if entry is not None else "", enabled=enabled,
                      effective_from_ms=effective)


def _channels(cols: Mapping[str, object], name: str) -> tuple[str, ...]:
    value = cols[name]
    if (not isinstance(value, (list, tuple)) or not value or not all(isinstance(c, str) for c in value)
            or len(set(value)) != len(value) or not set(value) <= set(CHANNELS)):
        raise _Refused("channels_invalid", f"{name} must be a non-empty list of {', '.join(CHANNELS)} without repeats")
    return tuple(str(c) for c in value)


def _route_policy(row: ConfigRow, cols: Mapping[str, object], known: KnownIds, deferred: _Deferred) -> RoutePolicy:
    route = cols["route"]
    if not isinstance(route, str):
        raise _Refused("route_unknown", "route must be a known route")
    if route not in known.routes:
        deferred.refuse(_Refused("route_unknown", "route must be a known route"))
    mode = cols["mode"]
    if not isinstance(mode, str) or mode not in MODES:
        raise _Refused("mode_invalid", f"mode must be one of {', '.join(MODES)}")
    pre = _channels(cols, "pre_channels")
    post = _channels(cols, "post_channels")
    if REQUIRED_POST_CHANNEL not in post:
        raise _Refused("channels_invalid", f"post_channels must contain {REQUIRED_POST_CHANNEL}")
    effective = _int(cols, "effective_from_ms", 0, TIME_MS_MAX, "effective_from_form")
    return RoutePolicy(route=route, mode=mode, pre_channels=pre, post_channels=post, effective_from_ms=effective,
                       row_id=row.row_id)


def _problem(row: ConfigRow, code: str, detail: str = "") -> ConfigProblem:
    return ConfigProblem(code=code, table=row.table, row_id=row.row_id, row_hash=row.row_hash, detail=detail)


def _checked(row: ConfigRow, problems: list[ConfigProblem]) -> dict[str, object] | None:
    cols = row.columns()
    try:
        _scan_forbidden_keys(cols)
        _check_columns(row.table, cols)
    except _Refused as r:
        problems.append(_problem(row, r.code, r.detail))
        return None
    return cols


def _duplicates(rows: Sequence[tuple[ConfigRow, object]], code: str, detail: str,
                problems: list[ConfigProblem]) -> None:
    groups: dict[object, list[ConfigRow]] = defaultdict(list)
    for row, key in rows:
        groups[key].append(row)
    for group in groups.values():
        if len(group) > 1:
            problems.extend(_problem(r, code, detail) for r in group)


# ---------------------------------------------------------------------------------------------------- validate
def _live(items: Sequence[_D], key: Callable[[_D], Hashable], at_ms: int | None,
          in_force: Callable[[_D], bool]) -> set[int]:
    """Row ids live at `at_ms`: future-dated, or in force for their key (and `in_force` holds); all when None."""
    if at_ms is None:
        return {i.row_id for i in items}
    live = {i.row_id for i in items if i.effective_from_ms > at_ms}
    live.update(i.row_id for i in latest_per_key(items, key, at_ms).values() if in_force(i))
    return live


def _apply_deferred(rows: Mapping[int, ConfigRow], deferred: Mapping[int, _Refused], live: set[int],
                    problems: list[ConfigProblem]) -> None:
    for row_id in sorted(deferred):
        if row_id in live:
            problems.append(_problem(rows[row_id], deferred[row_id].code, deferred[row_id].detail))


def validate_document(doc: Document | DocumentProblem, known: KnownIds, registry: ArmRegistry = REGISTRY, *,
                      seen: Mapping[str, str] | None = None, at_ms: int | None = None) -> ValidationResult:
    """`VALID` with the `ValidDocument`; `INVALID` with every problem (rows by id and hash); `SCHEMA_AHEAD` for a
    newer schema (never validated). `seen` (`ref -> row hash` of the accepted rows) limits the new-pin checks to
    pending rows and `at_ms` (now) the known-id checks to live rows (module docstring). Never raises."""
    if isinstance(doc, DocumentProblem):
        return ValidationResult(ValidationStatus.INVALID, problems=(
            ConfigProblem(code=doc.code, table=doc.table, row_id=doc.row_id, detail=doc.detail),))
    if doc.schema_ahead:
        return ValidationResult(ValidationStatus.SCHEMA_AHEAD,
                                problems=(ConfigProblem(code="schema_ahead"),))
    problems: list[ConfigProblem] = []
    seen_hashes: Mapping[str, str] = seen if seen is not None else {}

    def is_seen(row: ConfigRow) -> bool:
        return seen_hashes.get(row.ref) == row.row_hash

    strategies: dict[tuple[str, int], StrategyEntry | None] = {}
    pin_refusals: dict[tuple[str, int], _Refused] = {}
    strategy_keys: list[tuple[ConfigRow, object]] = []
    valid_strategies: list[StrategyEntry] = []
    for row in doc.table("strategies"):
        cols = _checked(row, problems)
        if cols is None:
            continue
        try:
            entry, pin_refusal = _strategy(row, cols, registry, seen=is_seen(row))
        except _Refused as r:
            problems.append(_problem(row, r.code, r.detail))
            code, version = cols.get("code"), int_value(cols.get("version"), 1, ROW_VERSION_MAX)
            if isinstance(code, str) and CODE_RE.fullmatch(code) and version is not None:
                strategies.setdefault((code, version), None)   # the row exists; it failed on its own
            continue
        strategy_keys.append((row, (entry.code, entry.version)))
        strategies[(entry.code, entry.version)] = entry
        if pin_refusal is not None:
            pin_refusals[(entry.code, entry.version)] = pin_refusal
        valid_strategies.append(entry)
    _duplicates(strategy_keys, "duplicate_strategy", "(code, version) must be unique", problems)

    assignments: list[Assignment] = []
    deferred_ids: dict[int, _Refused] = {}
    for row in doc.table("assignments"):
        cols = _checked(row, problems)
        if cols is None:
            continue
        deferred = _Deferred(is_seen(row))
        try:
            assignments.append(_assignment(row, cols, known, strategies, pin_refusals, deferred))
        except _Refused as r:
            problems.append(_problem(row, r.code, r.detail))
            continue
        if deferred.refused is not None:
            deferred_ids[row.row_id] = deferred.refused
    by_row = {r.row_id: r for r in doc.table("assignments")}
    _apply_deferred(by_row, deferred_ids, _live(assignments, lambda a: a.key, at_ms, lambda a: a.enabled), problems)
    _duplicates([(by_row[a.row_id], (a.key, a.effective_from_ms)) for a in assignments if a.resource],
                "duplicate_effective_from", "two rows with one key and one effective_from_ms", problems)

    policies: list[RoutePolicy] = []
    deferred_routes: dict[int, _Refused] = {}
    for row in doc.table("route_policy"):
        cols = _checked(row, problems)
        if cols is None:
            continue
        deferred = _Deferred(is_seen(row))
        try:
            policies.append(_route_policy(row, cols, known, deferred))
        except _Refused as r:
            problems.append(_problem(row, r.code, r.detail))
            continue
        if deferred.refused is not None:
            deferred_routes[row.row_id] = deferred.refused
    by_route_row = {r.row_id: r for r in doc.table("route_policy")}
    _apply_deferred(by_route_row, deferred_routes, _live(policies, lambda p: p.route, at_ms, lambda p: True),
                    problems)
    _duplicates([(by_route_row[p.row_id], (p.route, p.effective_from_ms)) for p in policies],
                "duplicate_effective_from", "two rows with one route and one effective_from_ms", problems)

    if problems:
        return ValidationResult(ValidationStatus.INVALID, problems=tuple(problems))
    return ValidationResult(ValidationStatus.VALID, document=ValidDocument(
        schema_version=doc.schema_version, doc_hash=doc.doc_hash, strategies=tuple(valid_strategies),
        assignments=tuple(assignments), route_policies=tuple(policies),
        row_hashes=tuple((r.ref, r.row_hash) for r in doc.rows)))


__all__ = [
    "Assignment", "CHANNELS", "COLUMNS", "ConfigProblem", "KnownIds", "MODES", "RoutePolicy", "StrategyEntry",
    "ValidDocument", "ValidationResult", "ValidationStatus", "escape_text", "latest_per_key",
    "validate_document",
]
