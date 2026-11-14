"""Rule configuration from the PostgreSQL `rulesets` table (IMPLEMENTATION_PLAN 4.5; oversee-rules.md §6-§7,
operations.md §12).

* `parse_ruleset(raw, version)`: the row's jsonb `config` laid over the spec defaults (`OverseeConfig()`).
  Unknown sections or keys, wrong types and inconsistent thresholds raise `RulesetError`, listing every
  problem. Missing keys keep the spec value, so `{}` is the spec ruleset.
* `ActiveRuleset`: what the RiskSweep and the price path read. Its `config` (`LiveOverseeConfig`) and
  `registry` (`ReloadableRegistry`) are handed to the RiskSweep once. A new ruleset is **staged** and
  installed at the next sweep boundary, the start of a pass (`RiskSweep.install_ruleset(active)`: every
  RiskSweep.run_pass calls `begin_pass()` before it draws its sweep_id or reads anything, and `end_pass()`
  when it ends; the older `wrap_sweep_ids` wrapper calls `at_boundary()` and is kept for compatibility).
  While a pass runs nothing installs (a direct `install()` is staged), so one pass never mixes two
  rulesets. install_ruleset also makes the NotificationDispatcher's re-page cadence follow the installed
  `sweep.escalate_after_ms`. States of rules a reload removes or disables resolve with `resolved_by = reload`
  (RiskSweep.apply_pass).
* `RulesetReloader`: the LEADER_INIT load and the supervised poll. An invalid active row keeps the last
  good ruleset and pages (once per version); at LEADER_INIT the last good one is the newest valid earlier
  row, else the base config. A reload calls `sweep_now()` for every known account and is audited
  (`ruleset_audit`, fenced).

`sweep.interval_ms` is a deployment setting, not a ruleset value: the gate's freshness window and the
RiskSweep loop are built from it when the term starts, so a ruleset value that differs is ignored, with a
page. The write path (a new versioned row, its activation, the operator audit) is the operator CLI (4.6).
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Final, Literal, Protocol

from src.oversee.config import (
    ADAPTER_WILDCARD,
    BandRuleConfig,
    DailyLossConfig,
    MarksConfig,
    MissingStopConfig,
    OverseeConfig,
    SweepConfig,
)
from src.oversee.rules import RuleRegistry

log = logging.getLogger(__name__)

AuditOutcome = Literal["installed", "fallback", "base", "rejected"]
Pager = Callable[[str, str, str], None]          # (severity, title, body)

SECTIONS: Final[Mapping[str, type]] = MappingProxyType({
    "sweep": SweepConfig, "marks": MarksConfig, "max_position_risk": BandRuleConfig,
    "max_aggregate_open_risk": BandRuleConfig, "missing_stop_loss": MissingStopConfig,
    "max_daily_loss": DailyLossConfig, "margin_usage": BandRuleConfig,
})
BAND_SECTIONS: Final = ("max_position_risk", "max_aggregate_open_risk", "margin_usage")
# Sanity bounds (ms). A confirm or grace longer than these would let a breach stand unconfirmed too long.
MIN_INTERVAL_MS: Final = 100
MAX_CONFIRM_MS: Final = 60_000
MAX_GRACE_MS: Final = 600_000
MAX_DURATION_MS: Final = 86_400_000
DEFAULT_POLL_S: Final = 5.0
STALE_INSTALL_MS: Final = 60_000      # staged but no sweep boundary for this long: install directly
FALLBACK_SCAN: Final = 20             # earlier rows tried at LEADER_INIT when the active row is invalid


class RulesetError(ValueError):
    """The ruleset is unusable; `problems` lists every reason."""

    def __init__(self, version: int | None, problems: Sequence[str]) -> None:
        self.version = version
        self.problems = tuple(problems)
        super().__init__(f"ruleset v{version}: " + "; ".join(self.problems))


# =============================================================================================== parsing
def _load_json(raw: object) -> object:
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        return json.loads(raw, parse_float=Decimal)
    return raw


def _int(path: str, v: object, problems: list[str]) -> int | None:
    if isinstance(v, bool) or not isinstance(v, (int, Decimal)):
        problems.append(f"{path}: expected an integer, got {type(v).__name__}")
        return None
    if isinstance(v, Decimal) and (not v.is_finite() or v != v.to_integral_value()):
        problems.append(f"{path}: expected an integer, got {v}")
        return None
    return int(v)


def _dec(path: str, v: object, problems: list[str]) -> Decimal | None:
    if isinstance(v, bool):
        problems.append(f"{path}: expected a number, got bool")
        return None
    try:
        if isinstance(v, (int, Decimal)):
            out = Decimal(v)
        elif isinstance(v, float):
            out = Decimal(str(v))
        elif isinstance(v, str):
            out = Decimal(v.strip())
        else:
            problems.append(f"{path}: expected a number, got {type(v).__name__}")
            return None
    except InvalidOperation:
        problems.append(f"{path}: not a number: {v!r}")
        return None
    if not out.is_finite():
        problems.append(f"{path}: not a finite number: {v!r}")
        return None
    return out


def _bool(path: str, v: object, problems: list[str]) -> bool | None:
    if not isinstance(v, bool):
        problems.append(f"{path}: expected true / false, got {type(v).__name__}")
        return None
    return v


def _skew_map(path: str, v: object, problems: list[str]) -> Mapping[str, int] | None:
    if not isinstance(v, Mapping):
        problems.append(f"{path}: expected an object {{adapter_id: ms}}")
        return None
    out: dict[str, int] = {}
    for k, ms in v.items():
        n = _int(f"{path}.{k}", ms, problems)
        if n is not None:
            out[str(k)] = n
    return MappingProxyType(out)


def _spread_map(path: str, v: object, problems: list[str]) -> Mapping[tuple[str, str], Decimal] | None:
    """{adapter_id: {symbol | "*": pips}} -> {(adapter_id, symbol): pips}."""
    if not isinstance(v, Mapping):
        problems.append(f"{path}: expected an object {{adapter_id: {{symbol: pips}}}}")
        return None
    out: dict[tuple[str, str], Decimal] = {}
    for adapter, per in v.items():
        if not isinstance(per, Mapping):
            problems.append(f"{path}.{adapter}: expected an object {{symbol: pips}}")
            continue
        for symbol, pips in per.items():
            d = _dec(f"{path}.{adapter}.{symbol}", pips, problems)
            if d is not None:
                out[(str(adapter), str(symbol))] = d
    return MappingProxyType(out)


_MAPPING_FIELDS: Final[Mapping[tuple[str, str], Callable[[str, object, list[str]], object]]] = MappingProxyType({
    ("sweep", "balance_skew_ms"): _skew_map,
    ("marks", "symbol_clean_spread_pips"): _spread_map,
})


def _section(name: str, raw: object, default: Any, problems: list[str]) -> Any:
    if not isinstance(raw, Mapping):
        problems.append(f"{name}: expected an object")
        return default
    fields = {f.name for f in dataclasses.fields(default)}
    changes: dict[str, object] = {}
    for key, value in raw.items():
        path = f"{name}.{key}"
        if key not in fields:
            problems.append(f"{path}: unknown key")
            continue
        current = getattr(default, key)
        parse = _MAPPING_FIELDS.get((name, key))
        parsed: object
        if parse is not None:
            parsed = parse(path, value, problems)
        elif isinstance(current, bool):
            parsed = _bool(path, value, problems)
        elif isinstance(current, int):
            parsed = _int(path, value, problems)
        elif isinstance(current, Decimal):
            parsed = _dec(path, value, problems)
        else:   # pragma: no cover - every OverseeConfig field type is handled above
            problems.append(f"{path}: unsupported field")
            continue
        if parsed is not None:
            changes[key] = parsed
    return replace(default, **changes) if changes else default


def _check_bounds(cfg: OverseeConfig, problems: list[str]) -> None:
    s, m = cfg.sweep, cfg.marks
    for f in dataclasses.fields(s):
        v = getattr(s, f.name)
        if f.name.endswith("_ms") and isinstance(v, int) and not 0 <= v <= MAX_DURATION_MS:
            problems.append(f"sweep.{f.name}: {v} outside [0, {MAX_DURATION_MS}]")
    for adapter, ms in s.balance_skew_ms.items():
        if not 0 <= ms <= MAX_DURATION_MS:
            problems.append(f"sweep.balance_skew_ms.{adapter}: {ms} outside [0, {MAX_DURATION_MS}]")
    if s.interval_ms < MIN_INTERVAL_MS:
        problems.append(f"sweep.interval_ms: {s.interval_ms} below {MIN_INTERVAL_MS}")
    if not Decimal(0) <= s.release_margin < Decimal(1):
        problems.append(f"sweep.release_margin: {s.release_margin} outside [0, 1)")
    if s.gate_freshness_passes < 1:
        problems.append("sweep.gate_freshness_passes: must be >= 1")
    if not 0 < m.stale_mark_ms <= MAX_DURATION_MS:
        problems.append(f"marks.stale_mark_ms: {m.stale_mark_ms} outside (0, {MAX_DURATION_MS}]")
    if not 0 < m.sod_mark_max_age_ms <= MAX_DURATION_MS:
        problems.append(f"marks.sod_mark_max_age_ms: {m.sod_mark_max_age_ms} outside (0, {MAX_DURATION_MS}]")
    if m.max_clean_spread_pips <= 0:
        problems.append("marks.max_clean_spread_pips: must be > 0")
    for (adapter, symbol), pips in m.symbol_clean_spread_pips.items():
        if pips <= 0:
            problems.append(f"marks.symbol_clean_spread_pips.{adapter}.{symbol}: must be > 0")
        if not adapter or not symbol:
            problems.append("marks.symbol_clean_spread_pips: empty adapter or symbol key")
    for name in BAND_SECTIONS:
        b: BandRuleConfig = getattr(cfg, name)
        if b.flag_above_pct <= 0 or b.halt_above_pct <= 0:
            problems.append(f"{name}: thresholds must be > 0")
        if b.flag_above_pct > b.halt_above_pct:
            problems.append(f"{name}: flag_above_pct {b.flag_above_pct} > halt_above_pct {b.halt_above_pct}")
        if not 0 <= b.confirm_ms <= MAX_CONFIRM_MS:
            problems.append(f"{name}.confirm_ms: {b.confirm_ms} outside [0, {MAX_CONFIRM_MS}]")
    ms_cfg = cfg.missing_stop_loss
    if not 0 <= ms_cfg.grace_ms <= MAX_GRACE_MS:
        problems.append(f"missing_stop_loss.grace_ms: {ms_cfg.grace_ms} outside [0, {MAX_GRACE_MS}]")
    if not 0 <= ms_cfg.confirm_ms <= MAX_CONFIRM_MS:
        problems.append(f"missing_stop_loss.confirm_ms: {ms_cfg.confirm_ms} outside [0, {MAX_CONFIRM_MS}]")
    dl = cfg.max_daily_loss
    if not Decimal(0) < dl.limit_pct_of_sod_equity <= Decimal(100):
        problems.append(f"max_daily_loss.limit_pct_of_sod_equity: {dl.limit_pct_of_sod_equity} outside (0, 100]")
    if dl.equity_divergence_pct <= 0:
        problems.append("max_daily_loss.equity_divergence_pct: must be > 0")
    if not 0 <= dl.confirm_ms <= MAX_CONFIRM_MS:
        problems.append(f"max_daily_loss.confirm_ms: {dl.confirm_ms} outside [0, {MAX_CONFIRM_MS}]")


def parse_ruleset(raw: object, version: int) -> OverseeConfig:
    """A `rulesets.config` value (jsonb as a dict, or JSON text) as an OverseeConfig over the spec defaults."""
    problems: list[str] = []
    try:
        doc = _load_json(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise RulesetError(version, [f"not valid JSON: {exc}"]) from exc
    if not isinstance(doc, Mapping):
        raise RulesetError(version, [f"expected a JSON object, got {type(doc).__name__}"])
    base = OverseeConfig()
    sections: dict[str, Any] = {}
    for name, value in doc.items():
        if name not in SECTIONS:
            problems.append(f"{name}: unknown section")
            continue
        sections[name] = _section(name, value, getattr(base, name), problems)
    cfg = replace(base, ruleset_version=version, **sections)
    _check_bounds(cfg, problems)
    if problems:
        raise RulesetError(version, problems)
    return cfg


def ruleset_json(cfg: OverseeConfig) -> dict[str, Any]:
    """The inverse of parse_ruleset (Decimals as strings): what the operator CLI (4.6) writes."""
    out: dict[str, Any] = {}
    for name in SECTIONS:
        sec = getattr(cfg, name)
        body: dict[str, Any] = {}
        for f in dataclasses.fields(sec):
            v = getattr(sec, f.name)
            if (name, f.name) == ("marks", "symbol_clean_spread_pips"):
                nested: dict[str, dict[str, str]] = {}
                for (adapter, symbol), pips in v.items():
                    nested.setdefault(adapter, {})[symbol] = str(pips)
                body[f.name] = nested
            elif isinstance(v, Mapping):
                body[f.name] = dict(v)
            elif isinstance(v, Decimal):
                body[f.name] = str(v)
            else:
                body[f.name] = v
        out[name] = body
    return out


# =============================================================================================== live objects
_CONFIG_FIELDS: Final = frozenset(f.name for f in dataclasses.fields(OverseeConfig))


class LiveOverseeConfig(OverseeConfig):
    """An OverseeConfig whose every field reads the ruleset installed last (`install`).

    The RiskSweep keeps the config object it was built with; this object lets a reload reach it without
    rebuilding the sweep. Only `ActiveRuleset` installs, and only at a sweep boundary."""

    def __init__(self, initial: OverseeConfig) -> None:   # deliberately not the dataclass __init__
        object.__setattr__(self, "_installed", initial)

    def __getattribute__(self, name: str) -> Any:
        if name in _CONFIG_FIELDS:
            return getattr(object.__getattribute__(self, "_installed"), name)
        return object.__getattribute__(self, name)

    @property
    def installed(self) -> OverseeConfig:
        """The plain (immutable) OverseeConfig currently installed."""
        installed: OverseeConfig = object.__getattribute__(self, "_installed")
        return installed

    def install(self, cfg: OverseeConfig) -> None:
        object.__setattr__(self, "_installed", cfg)


class ReloadableRegistry(RuleRegistry):
    """A RuleRegistry whose rules are rebuilt from a new config by `load` (same object, new rules)."""

    def __init__(self, cfg: OverseeConfig) -> None:
        built = RuleRegistry.from_config(cfg)
        super().__init__(built.rules, built.ruleset_version)

    def load(self, cfg: OverseeConfig) -> None:
        built = RuleRegistry.from_config(cfg)
        self.rules = built.rules
        self.ruleset_version = built.ruleset_version


class ActiveRuleset:
    """The installed ruleset plus at most one staged one (installed at the next sweep boundary)."""

    def __init__(self, base: OverseeConfig, *, clock_ms: Callable[[], int],
                 stale_install_ms: int = STALE_INSTALL_MS) -> None:
        self.base = base
        self.config = LiveOverseeConfig(base)
        self.registry = ReloadableRegistry(base)
        self._clock = clock_ms
        self._stale_install_ms = stale_install_ms
        self._pending: OverseeConfig | None = None
        self._pending_since: int | None = None
        self._last_boundary_ms: int | None = None
        self._passes = 0                                  # RiskSweep passes running (begin_pass / end_pass)
        self.installs: list[tuple[int, int]] = []        # (ruleset_version, at_ms), newest last (bounded)

    @property
    def current(self) -> OverseeConfig:
        return self.config.installed

    @property
    def version(self) -> int:
        return self.current.ruleset_version

    @property
    def pending(self) -> OverseeConfig | None:
        return self._pending

    @property
    def pass_running(self) -> bool:
        """A RiskSweep pass is between its `begin_pass()` and `end_pass()`: nothing installs now."""
        return self._passes > 0

    def begin_pass(self) -> bool:
        """RiskSweep: a pass starts. The sweep boundary (`at_boundary()`, which installs a staged ruleset)
        runs first, then the pass counts as running until `end_pass()`, so nothing installs mid-pass: not
        `install_if_sweep_idle()` (however long the pass takes), not a direct `install()` (staged instead),
        not a second boundary call (the `wrap_sweep_ids` wrapper, an overlapping pass). The pass counts as
        running even when the install raises (the pass still runs, on the ruleset installed so far)."""
        try:
            return self.at_boundary()
        finally:
            self._passes += 1

    def end_pass(self) -> None:
        """RiskSweep: the pass started by `begin_pass()` has ended (returned or raised)."""
        self._passes = max(0, self._passes - 1)

    def install(self, cfg: OverseeConfig) -> bool:
        """Install now (LEADER_INIT, an idle sweep). While a RiskSweep pass is running it is staged instead
        and installs at the next sweep boundary, so one pass never mixes two rulesets. True if installed."""
        if isinstance(cfg, LiveOverseeConfig):
            cfg = cfg.installed
        if self.pass_running:
            log.warning("ruleset v%d staged: a RiskSweep pass is running", cfg.ruleset_version)
            self.stage(cfg)
            return False
        self._install(cfg)
        return True

    def _install(self, cfg: OverseeConfig) -> None:
        self.registry.load(cfg)       # builds the rules first: if that raises, neither half has changed
        self.config.install(cfg)
        self.installs = (self.installs + [(cfg.ruleset_version, self._clock())])[-20:]
        log.warning("ruleset v%d installed (%d rules)", cfg.ruleset_version, len(self.registry.rules))

    def stage(self, cfg: OverseeConfig) -> None:
        """Install at the start of the next pass (replaces an earlier staged ruleset)."""
        self._pending = cfg
        self._pending_since = self._clock()

    def at_boundary(self) -> bool:
        """The sweep boundary: install the staged ruleset, if any. Returns True when one was installed.
        An install that raises changes nothing (config and registry stay on the installed ruleset) and keeps
        the ruleset staged, so the next boundary retries it (the RiskSweep pages `ruleset_install_failed`)."""
        self._last_boundary_ms = self._clock()
        if self._pending is None or self.pass_running:     # mid-pass: it stays staged for the next boundary
            return False
        cfg, since = self._pending, self._pending_since
        self._pending, self._pending_since = None, None
        try:
            self._install(cfg)
        except Exception:
            if self._pending is None:            # nothing newer was staged meanwhile: retry this one
                self._pending, self._pending_since = cfg, since
            raise
        return True

    def install_if_sweep_idle(self) -> bool:
        """No sweep boundary since the ruleset was staged, for `stale_install_ms`, and no pass running
        (`begin_pass` without its `end_pass`): the RiskSweep is not running (circuit open, not started), so
        install directly. A pass that is merely slow (stuck on PostgreSQL) keeps it staged."""
        if self._pending is None or self._pending_since is None or self.pass_running:
            return False
        now = self._clock()
        boundary_since = self._last_boundary_ms is not None and self._last_boundary_ms >= self._pending_since
        if boundary_since or now - self._pending_since < self._stale_install_ms:
            return False
        return self.at_boundary()

    def wrap_sweep_ids(self, inner: Callable[[], int]) -> Callable[[], int]:
        """RiskSweep `sweep_ids`: drawn first thing in every pass, so it marks the sweep boundary.
        Superseded by `RiskSweep.install_ruleset(self)`; kept so existing wiring keeps working."""
        def ids() -> int:
            self.at_boundary()
            return inner()
        return ids

    def clean_spread(self, adapter_id: str, symbol: str) -> Decimal | None:
        """TieredStateCommitter `clean_spread`: the installed per-(adapter, symbol) threshold."""
        return self.current.marks.clean_spread_pips(adapter_id, symbol)


# =============================================================================================== reloader
@dataclass(frozen=True)
class RulesetRow:
    """One `rulesets` row."""

    version: int
    config: object
    created_by: str = ""
    reason: str = ""
    created_at_ms: int = 0


class RulesetSource(Protocol):
    """Reads of `rulesets` plus the fenced `ruleset_audit` insert (src/db/ruleset_store.py)."""

    async def active_version(self) -> int | None: ...

    async def active(self) -> RulesetRow | None: ...

    async def earlier(self, below_version: int, limit: int) -> Sequence[RulesetRow]:
        """Rows with a lower version, newest first."""
        ...

    async def audit(self, version: int | None, outcome: AuditOutcome, detail: str) -> None: ...


@dataclass(frozen=True)
class ReloadResult:
    version: int | None
    outcome: AuditOutcome | None      # None: nothing changed
    detail: str = ""


class RulesetReloader:
    """LEADER_INIT load + supervised poll of the active ruleset version."""

    def __init__(self, source: RulesetSource, active: ActiveRuleset, *, pager: Pager | None = None,
                 accounts: Callable[[], Iterable[str]] | None = None,
                 sweep_now: Callable[[str], None] | None = None, poll_s: float = DEFAULT_POLL_S) -> None:
        self._src = source
        self._active = active
        self._pager = pager
        self._accounts = accounts
        self._sweep_now = sweep_now
        self.poll_s = poll_s
        self._seen: tuple[int | None] | None = None      # the active version last acted on (None: never)

    def _page(self, severity: str, title: str, body: str) -> None:
        if self._pager is not None:
            try:
                self._pager(severity, title, body)
            except Exception:  # a page sink must never break a reload
                log.exception("ruleset page failed")

    async def _audit(self, version: int | None, outcome: AuditOutcome, detail: str) -> None:
        try:
            await self._src.audit(version, outcome, detail[:2000])
        except Exception as exc:
            if any(c.__name__ == "EpochMismatch" for c in type(exc).__mro__):
                raise
            log.error("ruleset_audit insert failed (v%s %s): %r", version, outcome, exc)

    def _pin(self, cfg: OverseeConfig) -> OverseeConfig:
        """Keep the term's sweep.interval_ms (a deployment setting; see the module docstring)."""
        running = self._active.base.sweep.interval_ms
        if cfg.sweep.interval_ms == running:
            return cfg
        self._page("info", f"ruleset v{cfg.ruleset_version}: sweep.interval_ms ignored",
                   f"{cfg.sweep.interval_ms} ms requested; the term keeps {running} ms (deployment setting)")
        return replace(cfg, sweep=replace(cfg.sweep, interval_ms=running))

    def _base(self) -> OverseeConfig:
        return self._active.base

    async def load_initial(self) -> ReloadResult:
        """LEADER_INIT: install the active ruleset before the first pass (a read error propagates: the
        step fails like every other LEADER_INIT step; an invalid row never does)."""
        row = await self._src.active()
        if row is None:
            self._active.install(self._base())
            self._seen = (None,)
            await self._audit(None, "base", "no active rulesets row: base config")
            return ReloadResult(None, "base")
        self._seen = (row.version,)
        try:
            cfg = self._pin(parse_ruleset(row.config, row.version))
        except RulesetError as exc:
            await self._audit(row.version, "rejected", str(exc))
            return await self._fallback(row.version, exc)
        self._active.install(cfg)
        await self._audit(row.version, "installed", f"by {row.created_by}: {row.reason}"[:2000])
        return ReloadResult(row.version, "installed")

    async def _fallback(self, bad_version: int, exc: RulesetError) -> ReloadResult:
        for row in await self._src.earlier(bad_version, FALLBACK_SCAN):
            try:
                cfg = self._pin(parse_ruleset(row.config, row.version))
            except RulesetError:
                continue
            self._active.install(cfg)
            detail = f"active v{bad_version} invalid ({'; '.join(exc.problems)}); running v{row.version}"
            self._page("critical", f"ruleset v{bad_version} rejected at LEADER_INIT", detail)
            await self._audit(row.version, "fallback", detail)
            return ReloadResult(row.version, "fallback", detail)
        self._active.install(self._base())
        detail = f"active v{bad_version} invalid ({'; '.join(exc.problems)}); no valid earlier row: base config"
        self._page("critical", f"ruleset v{bad_version} rejected at LEADER_INIT", detail)
        await self._audit(None, "fallback", detail)
        return ReloadResult(None, "fallback", detail)

    async def poll_once(self) -> ReloadResult:
        """One poll: a changed active version is validated and staged (or rejected with a page)."""
        self._active.install_if_sweep_idle()
        version = await self._src.active_version()
        if self._seen is not None and self._seen[0] == version:
            return ReloadResult(version, None)
        if version is None:
            cfg = self._base()
            detail = "no active rulesets row: base config"
        else:
            row = await self._src.active()
            if row is None or row.version != version:      # activation raced the read: next poll
                return ReloadResult(version, None)
            try:
                cfg = self._pin(parse_ruleset(row.config, row.version))
            except RulesetError as exc:
                self._seen = (version,)
                self._page("high", f"ruleset v{version} rejected",
                           f"{'; '.join(exc.problems)}; keeping v{self._active.version}")
                await self._audit(version, "rejected", str(exc))
                return ReloadResult(version, "rejected", str(exc))
            detail = f"by {row.created_by}: {row.reason}"
        self._seen = (version,)
        self._active.stage(cfg)
        await self._audit(version, "installed" if version is not None else "base", detail[:2000])
        self._page("info", f"ruleset v{cfg.ruleset_version} loaded", f"{detail}; installs at the next sweep pass")
        self._wake_accounts()
        return ReloadResult(version, "installed" if version is not None else "base", detail)

    def _wake_accounts(self) -> None:
        if self._accounts is None or self._sweep_now is None:
            return
        for acct in sorted(set(self._accounts())):
            self._sweep_now(acct)

    async def run(self, stop: asyncio.Event, stamp: Callable[[], None] | None = None) -> None:
        """Supervised poll loop. A read error keeps the installed ruleset and retries next poll."""
        while not stop.is_set():
            try:
                await self.poll_once()
            except Exception as exc:
                if any(c.__name__ == "EpochMismatch" for c in type(exc).__mro__):
                    raise
                log.warning("ruleset poll failed: %r", exc)
            if stamp is not None:
                stamp()
            try:
                await asyncio.wait_for(stop.wait(), self.poll_s)
            except TimeoutError:
                pass


__all__ = ["ActiveRuleset", "LiveOverseeConfig", "ReloadableRegistry", "RulesetError", "RulesetReloader",
           "RulesetRow", "RulesetSource", "ReloadResult", "parse_ruleset", "ruleset_json", "ADAPTER_WILDCARD"]
