"""The two local enable flags of the optional market-data and ARM services (design §C2 Local flags; plan §2, RT-M13).

Both are off by default: `XTNL_MD_ENABLED` unset, empty or `0` means no market-data service, and `XTNL_ARM_ENABLED`
likewise. They are read once by `build_engine` and parsed **tolerantly**: a value other than `0` / `1` (or empty)
means off and is reported as a problem the leader pages once per term (`EngineSession.start_optional_services`),
never a `WiringError`, so a typo in a local flag can never stop the engine, ingestion or Oversee. The ARM needs the
market-data layer: `XTNL_ARM_ENABLED=1` without `XTNL_MD_ENABLED=1` leaves the ARM off, with a problem.

`optional_flags_from_env` parses only the flags. The full parse of the other settings (S31, RT-M13) is
`parse_optional_settings`, called inside the `optional-bootstrap` task (`src/runtime/optional.py`), never by
`build_engine`: a bad value there pages and parks the optional services, never a `WiringError`, and never touches
ingestion or Oversee. It combines

* `XTNL_MD_*` (`src.marketdata.config.parse_md_settings`, bounds and secrets),
* `XTNL_ARM_*` / `XTNL_ARM_FAIL_*` (`src.riskmgr.config.parse_arm_settings`, only while the ARM flag is on),
* `XTNL_TICK_SYMBOLS` (design PR5): comma-separated `adapter:SYMBOL` items, at most 8 per adapter, each adapter one of
  this engine's adapters. The instrument check ("checked against `instruments`") needs the adapter's instrument
  map, which SNAPSHOTTING fills after the bootstrap may have run, so it is a separate step
  (`check_tick_symbols`): a symbol the loaded map lacks is dropped and paged by name, an adapter whose map is not
  loaded yet keeps its symbols.

The ARM channels (`XTNL_ARM_` email and push, design §B14: both required before the ARM starts) are checked by
`arm_channel_problems`; a missing channel, or an ntfy push without `XTNL_ARM_PUSH_TOKEN` (design PR4: "ntfy with a
token, or Pushover"), leaves the ARM idle with one page, the market-data layer still runs.
Problems name the variable only, never its value.
"""
from __future__ import annotations

import re
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:   # imported lazily below: with both flags off the engine never loads these packages
    from src.marketdata.config import MdSettings
    from src.riskmgr.config import ArmSettings
    from src.secrets.source import SecretsSource

MD_ENABLED_VAR: Final = "XTNL_MD_ENABLED"
ARM_ENABLED_VAR: Final = "XTNL_ARM_ENABLED"
FLAG_VARS: Final = (MD_ENABLED_VAR, ARM_ENABLED_VAR)
TICK_SYMBOLS_VAR: Final = "XTNL_TICK_SYMBOLS"
MAX_TICK_SYMBOLS_PER_ADAPTER: Final = 8
_SYMBOL_RE: Final = re.compile(r"[A-Za-z0-9._/#-]{1,32}")

_ON: Final = "1"
_OFF: Final = frozenset({"", "0"})


@dataclass(frozen=True)
class OptionalFlags:
    """The parsed enable flags. `problems` are human-readable, value-free notes for one page per term."""

    md_enabled: bool = False
    arm_enabled: bool = False
    problems: tuple[str, ...] = ()

    @property
    def any_enabled(self) -> bool:
        return self.md_enabled or self.arm_enabled


def _flag(environ: Mapping[str, str], name: str, problems: list[str]) -> bool:
    raw = environ.get(name)
    if raw is None:
        return False
    value = raw.strip()
    if value == _ON:
        return True
    if value not in _OFF:
        problems.append(f"{name} is not 0 or 1; treated as 0 (off)")
    return False


def optional_flags_from_env(environ: Mapping[str, str]) -> OptionalFlags:
    """Tolerant parse of `XTNL_MD_ENABLED` / `XTNL_ARM_ENABLED`; never raises."""
    problems: list[str] = []
    md = _flag(environ, MD_ENABLED_VAR, problems)
    arm = _flag(environ, ARM_ENABLED_VAR, problems)
    if arm and not md:
        problems.append(f"{ARM_ENABLED_VAR}=1 needs {MD_ENABLED_VAR}=1; the ARM stays off")
        arm = False
    return OptionalFlags(md_enabled=md, arm_enabled=arm, problems=tuple(problems))


# ---------------------------------------------------------------------------------------------------- full parse (S31)
@dataclass(frozen=True)
class OptionalSettings:
    """Everything `optional-bootstrap` needs from the process environment, parsed and bound-checked."""

    md: MdSettings
    arm: ArmSettings | None = None                  # None: the ARM flag is off
    tick_symbols: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    @property
    def tick_pairs(self) -> tuple[tuple[str, str], ...]:
        """`XTNL_TICK_SYMBOLS` as `(adapter_id, symbol)` pairs (the tracker's pre-warm form)."""
        return tuple((a, s) for a, syms in sorted(self.tick_symbols.items()) for s in syms)


def parse_tick_symbols(raw: str | None, adapter_ids: Collection[str]) -> dict[str, tuple[str, ...]] | list[str]:
    """`XTNL_TICK_SYMBOLS` (`adapter:SYMBOL,...`; empty or unset = none). Problems name the variable and the rule
    broken, never an item (an unknown adapter is named: it is configuration, not a value)."""
    if raw is None or not raw.strip():
        return {}
    problems: list[str] = []
    out: dict[str, list[str]] = {}
    for item in (p.strip() for p in raw.split(",")):
        if not item:
            continue
        adapter, sep, symbol = item.partition(":")
        adapter, symbol = adapter.strip(), symbol.strip()
        if not sep or not adapter or not _SYMBOL_RE.fullmatch(symbol):
            problems.append(f"{TICK_SYMBOLS_VAR}: every item must be adapter:SYMBOL")
            continue
        if adapter not in adapter_ids:
            problems.append(f"{TICK_SYMBOLS_VAR}: adapter {adapter!r} is not configured on this engine")
            continue
        syms = out.setdefault(adapter, [])
        if symbol not in syms:
            syms.append(symbol)
    for adapter, syms in sorted(out.items()):
        if len(syms) > MAX_TICK_SYMBOLS_PER_ADAPTER:
            problems.append(f"{TICK_SYMBOLS_VAR}: at most {MAX_TICK_SYMBOLS_PER_ADAPTER} symbols per adapter "
                            f"({adapter} has {len(syms)})")
    if problems:
        return sorted(set(problems))
    return {a: tuple(s) for a, s in out.items()}


def check_tick_symbols(symbols: Mapping[str, Sequence[str]],
                       instruments: Callable[[str], Collection[str]]) -> tuple[dict[str, tuple[str, ...]],
                                                                               list[tuple[str, str]]]:
    """The instrument check of PR5: `(kept, unknown)`. `instruments(adapter_id)` is the adapter's loaded instrument
    map (empty while SNAPSHOTTING has not loaded it: then every symbol is kept and checked again later)."""
    kept: dict[str, tuple[str, ...]] = {}
    unknown: list[tuple[str, str]] = []
    for adapter, syms in symbols.items():
        known = instruments(adapter)
        if not known:
            kept[adapter] = tuple(syms)
            continue
        kept[adapter] = tuple(s for s in syms if s in known)
        unknown.extend((adapter, s) for s in syms if s not in known)
    return kept, unknown


def resolved_environ(secrets: SecretsSource | None, environ: Mapping[str, str]) -> Mapping[str, str]:
    """The environment with the secrets source's cached secret laid over it (operations.md §13: the `XTNL_MD_*` and
    `XTNL_ARM_*` secrets come from the secrets source on AWS). Never blocks (`resolve_cached`, filled by
    `load_settings`); with the env source (or none) it is `environ` itself, as before."""
    if secrets is None or secrets.kind == "env":
        return environ
    return secrets.resolve_cached(environ)


def arm_channel_problems(environ: Mapping[str, str]) -> list[str]:
    """Both ARM channels must be configured (design §B14, PR4) before the ARM starts: the missing ones by name."""
    from src.notifications.settings import ARM_PREFIX, NotificationSettings

    ch = NotificationSettings.from_env(environ, prefix=ARM_PREFIX)
    problems: list[str] = []
    if not ch.email_enabled:
        problems.append("ARM email channel (XTNL_ARM_ email settings) is not configured")
    if not ch.push_enabled:
        problems.append("ARM push channel (XTNL_ARM_ push settings) is not configured")
    elif ch.push is not None and ch.push.provider == "ntfy" and not ch.push.token:
        # design PR4: the ARM's push is "ntfy with a token, or Pushover" (a token-less topic is anyone's to read)
        problems.append("ARM push channel: ntfy needs XTNL_ARM_PUSH_TOKEN (design PR4)")
    return problems


def parse_optional_settings(environ: Mapping[str, str], flags: OptionalFlags,
                            adapter_ids: Collection[str]) -> OptionalSettings | list[str]:
    """The full parse (RT-M13): `OptionalSettings`, or every problem (names and bounds only). Never raises."""
    from src.marketdata.config import parse_md_settings
    from src.riskmgr.config import parse_arm_settings

    problems: list[str] = []
    md = parse_md_settings(environ)
    if isinstance(md, list):
        problems.extend(md)
    arm: ArmSettings | None = None
    if flags.arm_enabled:
        parsed = parse_arm_settings(environ)
        if isinstance(parsed, list):
            problems.extend(parsed)
        else:
            arm = parsed
    ticks = parse_tick_symbols(environ.get(TICK_SYMBOLS_VAR), adapter_ids)
    if isinstance(ticks, list):
        problems.extend(ticks)
    if problems or isinstance(md, list) or isinstance(ticks, list):
        return problems or ["optional services settings are incomplete"]
    return OptionalSettings(md=md, arm=arm, tick_symbols=ticks)
