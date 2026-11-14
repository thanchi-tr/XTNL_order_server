"""Engine composition (IMPLEMENTATION_PLAN 3.6): `build_engine()` wires the real dependencies.

    environment -> EngineSettings (load_settings; no I/O, secrets never printed)
                -> process-wide: etcd client, LeaseKeeper, HealthBoard, control pool (lazy), Redis client,
                   ClusterControl (cluster_state + etcd SAFE_HOLD mirror), NodeHealthProbe, Patroni REST
                -> NodeController with a session factory: one EngineSession per leader term
                   (src/runtime/session.py) over the PostgreSQL ports of that term (src/runtime/pg_ports.py)

Adapters: StoneX when STONE_X_* are set, Pepperstone (cTrader Open API) when CTRADER_* are set, the
MockBrokerAdapter only when XTNL_USE_MOCK_BROKER=1 (sandbox only). `XTNL_ADAPTERS`, when set, names the
adapters to run explicitly. At least one is required.

Credentials (IMPLEMENTATION_PLAN 4.7): every setting is read from one mapping, the environment with the
AWS Secrets Manager secret laid over it when XTNL_SECRETS_SOURCE=aws-sm (src/secrets). The Patroni REST
credentials (XTNL_PATRONI_USER / XTNL_PATRONI_PASSWORD) and the etcd client TLS (XTNL_ETCD_CA,
XTNL_ETCD_CERT, XTNL_ETCD_KEY) come from it too. With XTNL_TOKEN_KMS_KEY_ID + XTNL_KMS_REGION set, the
Pepperstone adapter gets the KMS-encrypted token store (src/db/token_store.py) and token refresh is on.
A refresh starts only with at least TOKEN_REFRESH_MIN_LEASE_S (8 s) of lease left (`LeaseGuardedTokenStore`,
checked before the 'refresh in flight' row is committed): fence 12 s - renew 1 s leaves at most ~11 s, so the
bound is reachable (operations.md §13, IMPLEMENTATION_PLAN 4.9). A broker AUTH_FAILED asks the secrets
source to re-read the secret (`SecretsSource.invalidate`).
Each new leader term builds its adapters from the latest cached secret, and a StoneX adapter's
credential generation is the secret's version id (`sm:<VersionId>`). Within a term (5.6), a new secret version
makes the orchestrator swap in an adapter built from it (`AdapterRefresher`, synchronous, no I/O), and the
alert channels are rebuilt from it (`settings_from_secrets` -> `RotatingChannels` in the session).

REST buckets and maintenance (6.5): one `RestBudget` per adapter id for the process (`RestBudgets`, shared by
every term's adapter and a rotated replacement); declared broker maintenance windows are read from
`XTNL_MAINTENANCE_FILE` (src/runtime/maintenance.py) by each term's session.

Telemetry (5.6): `Engine` carries the etcd client, the Patroni control, the chrony reader and the raw
environment so `src/main.py` can build the metrics exporter (`src/runtime/telemetry.py`).

Building connects nothing, with one exception: XTNL_SECRETS_SOURCE=aws-sm reads the secret once (before
the event loop starts; `--check-config` therefore proves the secret is readable). The pools are lazy,
the Redis / etcd / HTTP / KMS clients connect on first use and adapters connect only in SM4 on the leader. `check_composition()` (`python -m src.main --check-config`)
builds the whole engine plus one term's session against a placeholder epoch and closes it again.

Phase 1 is read-only: no component wired here sends, modifies or cancels an order.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
import weakref
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final, Literal
from urllib.parse import urlsplit

import httpx
import redis.asyncio as aioredis

from src.adapters.base import BrokerAdapter, TransientBrokerError
from src.adapters.mock import MockBrokerAdapter, build_fx_fixture
from src.adapters.pepperstone.adapter import PepperstoneAdapter
from src.adapters.pepperstone.tokens import TokenPair, TokenStore
from src.adapters.ratelimit import RestBudget
from src.adapters.stonex.adapter import StoneXAdapter
from src.cache.client import create_redis
from src.cache.live_reader import RunIdReader, info_run_id
from src.config import ExecutionLockedError, PepperstoneSettings, StoneXSettings
from src.db.epoch import Epoch
from src.db.pg import ConnectionSource
from src.db.settings import StorageSettings
from src.db.token_store import KmsTokenStore
from src.notifications.rotation import settings_from_secrets
from src.notifications.settings import DeadmanSettings, NotificationSettings
from src.oversee.config import OverseeConfig
from src.db.uptime_store import PgUptimeStore
from src.runtime.catchup import CatchupMetrics, CatchupSettings
from src.runtime.cluster import ClusterControl
from src.runtime.deadman import check_deadman_url
from src.runtime.config import RuntimeConfig
from src.runtime.etcd import EtcdClient, EtcdTls, HttpEtcdClient
from src.runtime.exits import ExitFn, hard_exit
from src.runtime.health import HealthBoard
from src.runtime.lease import LeaseKeeper
from src.runtime.maintenance import MaintenanceSource, maintenance_source_from_env
from src.runtime.node import NodeController
from src.runtime.roles import NodeRole
from src.runtime.patroni import (
    PatroniRestControl,
    asyncpg_standby_check,
    default_endpoints,
    patroni_auth_from_env,
)
from src.runtime.pg_ports import (
    LazyPgSource,
    SpillOnlySink,
    control_application_name,
    pg_leader_ports,
    safe_redact,
)
from src.runtime.ports import LeaderPorts, PortsFactory
from src.runtime.probes import (
    BrokerEndpoint,
    BrokerTlsMonitor,
    ChronyReader,
    clock_reader,
    NodeHealthProbe,
    StandbyRedisFlush,
    TlsConnector,
    tls_handshake,
)
from src.runtime.session import (
    EngineSession,
    SessionContext,
    SessionTimings,
    check_drain_budget,
    default_credential_generation,
)
from src.runtime.spill_standby import NodeSpillDrain
from src.runtime.supervisor import LogPager, TaskSupervisor
from src.runtime.uptime import SuspendEvent, UptimeStore, UptimeTracker, ping_s_from_env
from src.secrets.aws import CredentialProvider, Imdsv2Credentials
from src.secrets.kms import Cipher, KmsCipher, KmsSettings
from src.secrets.source import SOURCE_VAR, SecretsError, SecretsSource, secrets_source_from_env

log = logging.getLogger(__name__)

MOCK_BROKER_VAR: Final = "XTNL_USE_MOCK_BROKER"
# Development only: a machine without chrony (a Windows workstation) reports its clock as synced so the
# node can pass the chrony hard check (operations.md s4). Refused unless BROKER_ENV=sandbox.
DEV_SKIP_CLOCK_VAR: Final = "XTNL_DEV_SKIP_CLOCK_CHECK"
# operations.md §13 (decision of IMPLEMENTATION_PLAN 4.9): a cTrader token refresh needs this much lease left.
TOKEN_REFRESH_MIN_LEASE_S: Final = 8.0
AdapterKind = Literal["stonex", "pepperstone", "mock"]
KNOWN_ADAPTERS: Final[tuple[AdapterKind, ...]] = ("stonex", "pepperstone", "mock")
_STONEX_VARS: Final = ("STONE_X_USERNAME", "STONE_X_PASS", "STONE_X_APP_KEY")
_CTRADER_VARS: Final = ("CTRADER_CLIENT_ID", "CTRADER_CLIENT_SECRET", "CTRADER_ACCESS_TOKEN",
                        "CTRADER_REFRESH_TOKEN", "CTRADER_ACCOUNT_ID")


class WiringError(RuntimeError):
    """The engine cannot be composed from this configuration (the message names variables, never values)."""


NotWiredError = WiringError   # the name src/main.py used before 3.6


def _wall_ms() -> int:
    return time.time_ns() // 1_000_000


# ======================================================================================== settings
@dataclass(frozen=True)
class AdapterPlan:
    kind: AdapterKind
    stonex: StoneXSettings | None = field(default=None, repr=False)
    pepperstone: PepperstoneSettings | None = field(default=None, repr=False)

    @property
    def adapter_id(self) -> str:
        if self.kind == "stonex":
            return StoneXAdapter.adapter_id
        if self.kind == "pepperstone":
            return PepperstoneAdapter.adapter_id
        return "mock"


@dataclass(frozen=True)
class EngineSettings:
    runtime: RuntimeConfig
    storage: StorageSettings = field(repr=False)
    adapters: tuple[AdapterPlan, ...]
    notifications: NotificationSettings
    deadman: DeadmanSettings
    oversee: OverseeConfig = field(default_factory=OverseeConfig)
    warnings: tuple[str, ...] = ()
    heartbeat_copy: DeadmanSettings = field(default_factory=DeadmanSettings)   # 7.4: the dashboard's copy
    patroni_auth: tuple[str, str] | None = field(default=None, repr=False)
    etcd_tls: EtcdTls = field(default_factory=EtcdTls)
    token_kms: KmsSettings | None = None
    secrets_source: str = "env"
    secrets_version: str | None = None

    @property
    def adapter_ids(self) -> tuple[str, ...]:
        return tuple(p.adapter_id for p in self.adapters)

    def describe(self) -> str:
        """One log line, credentials removed."""
        return (f"node {self.runtime.node_id}, BROKER_ENV={self.runtime.broker_env}, adapters "
                f"{', '.join(self.adapter_ids)}, postgres {safe_redact(self.storage.dsn)}, "
                f"redis {safe_redact(self.storage.redis_url)}, secrets {self.secrets_source}, "
                f"token store {'kms' if self.token_kms is not None else 'none'}")


def _present(e: Mapping[str, str], names: Sequence[str]) -> bool:
    return any((e.get(n) or "").strip() for n in names)


def _plan_for(kind: AdapterKind, e: Mapping[str, str]) -> AdapterPlan:
    if kind == "stonex":
        return AdapterPlan("stonex", stonex=StoneXSettings.from_env(e))
    if kind == "pepperstone":
        return AdapterPlan("pepperstone", pepperstone=PepperstoneSettings.from_env(e))
    return AdapterPlan("mock")


def resolve_environment(source: SecretsSource, environ: Mapping[str, str]) -> dict[str, str]:
    """The environment with the secret laid over it. Raises WiringError (names only)."""
    try:
        return source.resolve(environ)
    except SecretsError as exc:
        raise WiringError(f"secrets ({source.kind}): {exc}") from None


def load_settings(config: RuntimeConfig, environ: Mapping[str, str] | None = None, *,
                  secrets: SecretsSource | None = None) -> EngineSettings:
    """Read and validate everything the engine needs. Raises WiringError (names only, no values).

    `secrets` = the source to read through; None selects it from XTNL_SECRETS_SOURCE (and closes it
    again after the one read)."""
    raw: dict[str, str] = dict(os.environ if environ is None else environ)
    if secrets is None:
        try:
            one_shot = secrets_source_from_env(raw)
        except SecretsError as exc:
            raise WiringError(f"secrets: {exc}") from None
        try:
            return load_settings(config, raw, secrets=one_shot)
        finally:
            one_shot.close()
    e = resolve_environment(secrets, raw)
    try:
        storage = StorageSettings.from_env(e)
    except (KeyError, ValueError) as exc:
        raise WiringError(f"storage: {exc.args[0] if exc.args else exc}") from None
    use_mock = (e.get(MOCK_BROKER_VAR) or "").strip() == "1"
    explicit = tuple(a.lower() for a in config.adapters)
    unknown = sorted(set(explicit) - set(KNOWN_ADAPTERS))
    if unknown:
        raise WiringError(f"XTNL_ADAPTERS names unknown adapters: {', '.join(unknown)}")
    wanted: list[AdapterKind] = []
    if explicit:
        wanted = [k for k in KNOWN_ADAPTERS if k in explicit and k != "mock"]
    else:
        if _present(e, _STONEX_VARS):
            wanted.append("stonex")
        if _present(e, _CTRADER_VARS):
            wanted.append("pepperstone")
    if "mock" in explicit and not use_mock:
        raise WiringError(f"XTNL_ADAPTERS names mock but {MOCK_BROKER_VAR} is not 1")
    plans: list[AdapterPlan] = []
    for kind in wanted:
        try:
            plans.append(_plan_for(kind, e))
        except ExecutionLockedError:
            raise
        except (KeyError, ValueError) as exc:
            raise WiringError(f"{kind}: {exc.args[0] if exc.args else exc}") from None
    if use_mock:
        if config.broker_env != "sandbox":
            raise WiringError(f"{MOCK_BROKER_VAR}=1 is refused under BROKER_ENV={config.broker_env}")
        plans.append(AdapterPlan("mock"))
    if not plans:
        raise WiringError("no broker adapter configured: set the STONE_X_* or CTRADER_* variables "
                          f"(or {MOCK_BROKER_VAR}=1 in sandbox)")
    notifications = NotificationSettings.from_env(e)
    deadman_env = DeadmanSettings.from_env(e)
    deadman = DeadmanSettings(config.deadman_url or deadman_env.url, deadman_env.token)
    heartbeat_copy = heartbeat_copy_from_env(e)
    # A bad heartbeat URL is a configuration error at start-up, never a LEADER_INIT failure in every term.
    for var, url in (("XTNL_DEADMAN_URL", deadman.url), (HEARTBEAT_COPY_URL_VAR, heartbeat_copy.url)):
        if url is not None:
            try:
                check_deadman_url(url)
            except ValueError as exc:
                raise WiringError(f"{var}: {exc}") from None
    warnings = list(notifications.issues)
    if heartbeat_copy.url is None:
        warnings.append(f"{HEARTBEAT_COPY_URL_VAR} is not set: the Melbourne dashboard gets no leader heartbeat and "
                        "shows every Redis live value as stale")
    elif heartbeat_copy.token is None:
        warnings.append(f"{HEARTBEAT_COPY_URL_VAR} is set but {HEARTBEAT_COPY_TOKEN_VAR} is not: the dashboard "
                        "listener refuses every heartbeat copy (401) and shows every Redis live value as stale")
    if use_mock:
        warnings.append("MockBrokerAdapter enabled: scripted in-memory broker data, not a real account")
    try:
        patroni_auth = patroni_auth_from_env(e)
        etcd_tls = EtcdTls.from_env(e)
        token_kms = KmsSettings.from_env(e)
    except ValueError as exc:
        raise WiringError(str(exc)) from None
    if patroni_auth is None:
        warnings.append("Patroni REST credentials (XTNL_PATRONI_USER / XTNL_PATRONI_PASSWORD) are not set: "
                        "an authenticated Patroni REST API refuses the SM3 switchover / reinit requests")
    if etcd_tls.cert is not None and not all(ep.startswith("https://") for ep in config.etcd_endpoints):
        warnings.append("XTNL_ETCD_CERT is set but not every XTNL_ETCD_ENDPOINTS entry is https://")
    if any(p.kind == "pepperstone" for p in plans) and token_kms is None:
        warnings.append("pepperstone: no KMS-backed token store is wired (XTNL_TOKEN_KMS_KEY_ID / XTNL_KMS_REGION "
                        "unset); token refresh is disabled and an expired token pair ends in AUTH_FAILED")
    return EngineSettings(config, storage, tuple(plans), notifications, deadman, warnings=tuple(warnings),
                          heartbeat_copy=heartbeat_copy,
                          patroni_auth=patroni_auth, etcd_tls=etcd_tls, token_kms=token_kms,
                          secrets_source=secrets.kind, secrets_version=secrets.version_id)


# ======================================================================================== heartbeat (7.4)
#: The Melbourne dashboard's heartbeat listener (src/dashboard/heartbeat.py) and its bearer token: the leader POSTs
#: a copy of every dead-man heartbeat there, so the dashboard shows Redis live values only when their boot_id
#: epoch is the latest leader heartbeat's (operations.md §9 Dashboard).
HEARTBEAT_COPY_URL_VAR: Final = "XTNL_HEARTBEAT_COPY_URL"
HEARTBEAT_COPY_TOKEN_VAR: Final = "XTNL_HEARTBEAT_COPY_TOKEN"


def heartbeat_copy_from_env(e: Mapping[str, str]) -> DeadmanSettings:
    url = (e.get(HEARTBEAT_COPY_URL_VAR) or "").strip() or None
    token = (e.get(HEARTBEAT_COPY_TOKEN_VAR) or "").strip() or None
    return DeadmanSettings(url, token)


def deadman_token_loader(source: SecretsSource, environ: Mapping[str, str]) -> Callable[[], str | None]:
    """The dead-man bearer token (XTNL_DEADMAN_TOKEN) from the environment with the cached secret laid over it,
    read for every heartbeat (never blocks: `resolve_cached`), so a rotated token needs no restart (§13)."""
    def load() -> str | None:
        return DeadmanSettings.from_env(source.resolve_cached(environ)).token
    return load


def heartbeat_copy_token_loader(source: SecretsSource, environ: Mapping[str, str]) -> Callable[[], str | None]:
    """XTNL_HEARTBEAT_COPY_TOKEN, re-read like `deadman_token_loader`."""
    def load() -> str | None:
        return heartbeat_copy_from_env(source.resolve_cached(environ)).token
    return load


# ======================================================================================== adapters
class LeaseCheck:
    """`pre_connect_check` for the adapters: every connect / LogOn / token refresh first checks the lease
    deadline synchronously (operations.md §2)."""

    def __init__(self, lease_valid: Callable[[], bool]) -> None:
        self._valid = lease_valid

    def __call__(self) -> None:
        if not self._valid():
            raise TransientBrokerError("lease not valid: broker connect refused")


class LeaseGuardedTokenStore:
    """TokenStore proxy: `begin_refresh` (the step that commits the 'refresh in flight' row, right before the
    broker refresh call) first requires `min_lease_s` of lease left, so a refresh never starts on a lease
    about to lapse. `load` / `save` pass through (the save CAS is not epoch-gated: a pair refreshed under the
    lease always lands, operations.md §13)."""

    def __init__(self, inner: TokenStore, lease_valid: Callable[[float], bool],
                 min_lease_s: float = TOKEN_REFRESH_MIN_LEASE_S) -> None:
        self._inner = inner
        self._lease_valid = lease_valid
        self.min_lease_s = min_lease_s

    def __repr__(self) -> str:
        return f"LeaseGuardedTokenStore({self._inner!r}, min_lease_s={self.min_lease_s})"

    async def load(self) -> TokenPair | None:
        return await self._inner.load()

    async def begin_refresh(self, generation: int) -> bool:
        if not self._lease_valid(self.min_lease_s):
            raise TransientBrokerError(
                f"token refresh deferred: less than {self.min_lease_s:.0f} s of lease left")
        return await self._inner.begin_refresh(generation)

    async def save(self, expected_generation: int, pair: TokenPair) -> int | None:
        return await self._inner.save(expected_generation, pair)


class RestBudgets:
    """One REST token bucket per adapter id for the whole process (operations.md §15): every term's adapter and a
    replacement built from a rotated secret share it, so a rotation or a new term never hands out a fresh full
    bucket next to tokens already spent, and `xtnl_rest_requests_total` stays cumulative. The limits come from
    the adapter's settings (`*_REST_RATE_PER_S` / `*_REST_BURST`); a changed limit replaces the bucket."""

    def __init__(self) -> None:
        self._by_id: dict[str, RestBudget] = {}

    def __repr__(self) -> str:
        return f"RestBudgets({sorted(self._by_id)})"

    def get(self, adapter_id: str) -> RestBudget | None:
        return self._by_id.get(adapter_id)

    def for_plan(self, plan: AdapterPlan) -> RestBudget | None:
        """The plan's bucket; None for the (unmetered) mock adapter."""
        cfg: StoneXSettings | PepperstoneSettings | None = (
            plan.stonex if plan.stonex is not None else plan.pepperstone)
        if cfg is None:
            return None
        rate, burst = float(cfg.rest_rate_per_s), int(cfg.rest_burst)
        cur = self._by_id.get(plan.adapter_id)
        if cur is not None and cur.rate_per_s == rate and cur.capacity == burst:
            return cur
        if cur is not None:
            log.warning("%s: REST limits changed (%.3g/s burst %d -> %.3g/s burst %d); new bucket", plan.adapter_id,
                        cur.rate_per_s, cur.capacity, rate, burst)
        fresh = RestBudget(plan.adapter_id, rate_per_s=rate, burst=burst)
        self._by_id[plan.adapter_id] = fresh
        return fresh


def make_adapter(plan: AdapterPlan, pre_connect_check: Callable[[], None], *,
                 clock_ms: Callable[[], int] = _wall_ms, token_store: TokenStore | None = None,
                 budgets: RestBudgets | None = None) -> BrokerAdapter:
    """One adapter. Pepperstone refreshes its token pair only when a (KMS) token store is given, so a
    refreshed pair is always saved before use (operations.md §13). `budgets`: the process-wide REST buckets
    (None: the adapter builds its own)."""
    budget = budgets.for_plan(plan) if budgets is not None else None
    if plan.kind == "stonex":
        assert plan.stonex is not None
        return StoneXAdapter(plan.stonex, pre_connect_check=pre_connect_check, rest_budget=budget,
                             stop_limit_history=plan.stonex.stop_limit_history,
                             history_paging_confirmed=plan.stonex.history_paging_confirmed)
    if plan.kind == "pepperstone":
        assert plan.pepperstone is not None
        return PepperstoneAdapter(plan.pepperstone, pre_connect_check=pre_connect_check, token_store=token_store,
                                  allow_token_refresh=token_store is not None, rest_budget=budget)
    return MockBrokerAdapter.from_fixture(build_fx_fixture(ts_ms=clock_ms()), finite_streams=False)


def adapter_factory(plans: Sequence[AdapterPlan], lease_valid: Callable[[], bool], *,
                    clock_ms: Callable[[], int] = _wall_ms, token_stores: Mapping[str, TokenStore] | None = None,
                    plans_source: Callable[[], Sequence[AdapterPlan]] | None = None,
                    on_built: Callable[[BrokerAdapter], None] | None = None,
                    budgets: RestBudgets | None = None,
                    ) -> Callable[[], Sequence[BrokerAdapter]]:
    """Fresh adapter objects per leader term (a closed adapter is never reused). `plans_source` gives the
    current plans (settings re-read from the latest cached secret); `token_stores` by adapter id."""
    check = LeaseCheck(lease_valid)
    stores = dict(token_stores or {})

    def build() -> Sequence[BrokerAdapter]:
        current = plans_source() if plans_source is not None else plans
        out = [make_adapter(p, check, clock_ms=clock_ms, token_store=stores.get(p.adapter_id), budgets=budgets)
               for p in current]
        if on_built is not None:
            for a in out:
                on_built(a)
        return out

    return build


def rotating_plans(plans: Sequence[AdapterPlan], source: SecretsSource,
                   environ: Mapping[str, str]) -> Callable[[], Sequence[AdapterPlan]]:
    """Plans rebuilt from the cached secret (no I/O, safe on the event loop). A secret that no longer
    yields valid settings keeps the last good plans (logged by variable name only)."""
    last: list[Sequence[AdapterPlan]] = [plans]

    def current() -> Sequence[AdapterPlan]:
        try:
            e = source.resolve_cached(environ)
            fresh = [_plan_for(p.kind, e) for p in plans]
        except ExecutionLockedError:
            raise
        except (SecretsError, KeyError, ValueError) as exc:
            log.warning("secrets: keeping the previous broker settings: %s", exc.args[0] if exc.args else exc)
            return last[0]
        last[0] = fresh
        return fresh

    return current


class CredentialGenerations:
    """`SessionContext.credential_generation`: the cTrader token_generation for Pepperstone, otherwise the
    Secrets Manager version id the adapter was built with (`sm:<VersionId>`), else the default. A rotated
    secret is therefore a new generation, which lets SM4 leave AUTH_FAILED (operations.md §13)."""

    def __init__(self, source: SecretsSource) -> None:
        self._source = source
        self._built: weakref.WeakKeyDictionary[BrokerAdapter, str] = weakref.WeakKeyDictionary()

    def record(self, adapter: BrokerAdapter) -> None:
        version = self._source.version_id
        if version:
            self._built[adapter] = version

    def version_of(self, adapter: BrokerAdapter) -> str | None:
        """The secret version the adapter was built with (None: built without one, e.g. env secrets)."""
        return self._built.get(adapter)

    def __call__(self, adapter: BrokerAdapter) -> str:
        if isinstance(getattr(adapter, "token_generation", None), int):
            return default_credential_generation(adapter)
        version = self._built.get(adapter)
        return f"sm:{version}" if version else default_credential_generation(adapter)


class AdapterRefresher:
    """`refresh_adapter` of the IngestionOrchestrator (credential rotation within a term, operations.md §13).

    Called by SM4 before each authentication and while it sits in AUTH_FAILED. Synchronous and I/O-free: the
    plans come from the cached secret (`rotating_plans`). Returns None while the secret version the current
    adapter was built with is still the current one (or there is no version: env secrets never rotate);
    otherwise a fresh adapter built through `make_adapter` (so its connects keep the lease check), recorded in
    `CredentialGenerations` so it reports the new generation. The mock adapter is never rebuilt."""

    def __init__(self, plans: Callable[[], Sequence[AdapterPlan]], lease_valid: Callable[[], bool],
                 source: SecretsSource, generations: CredentialGenerations, *,
                 clock_ms: Callable[[], int] = _wall_ms, token_stores: Mapping[str, TokenStore] | None = None,
                 budgets: RestBudgets | None = None) -> None:
        self._plans = plans
        self._budgets = budgets
        self._check = LeaseCheck(lease_valid)
        self._source = source
        self._generations = generations
        self._clock = clock_ms
        self._stores = dict(token_stores or {})

    def __call__(self, adapter_id: str, current: BrokerAdapter) -> BrokerAdapter | None:
        version = self._source.version_id
        if not version or self._generations.version_of(current) == version:
            return None
        plan = next((p for p in self._plans() if p.adapter_id == adapter_id), None)
        if plan is None or plan.kind == "mock":
            return None
        fresh = make_adapter(plan, self._check, clock_ms=self._clock, token_store=self._stores.get(adapter_id),
                             budgets=self._budgets)
        self._generations.record(fresh)
        log.warning("%s: secret version changed; a fresh adapter replaces the current one", adapter_id)
        return fresh


#: Deposit / withdrawal history per adapter (tier-persistence.md §4): cTrader reports cash flows with
#: deposits and withdrawals, StoneX exposes none (its accounts' days are marked transfers_unverified).
DEFAULT_TRANSFER_SOURCES: Final[Mapping[str, bool]] = {
    PepperstoneAdapter.adapter_id: True, StoneXAdapter.adapter_id: False, "mock": True}


def broker_endpoints(plans: Sequence[AdapterPlan]) -> list[BrokerEndpoint]:
    out: list[BrokerEndpoint] = []
    for p in plans:
        if p.stonex is not None:
            parts = urlsplit(p.stonex.base_url_v2)
            if parts.hostname:
                out.append(BrokerEndpoint(p.adapter_id, parts.hostname, parts.port or 443))
        elif p.pepperstone is not None:
            out.append(BrokerEndpoint(p.adapter_id, p.pepperstone.host, p.pepperstone.port))
    return out


# ======================================================================================== engine
class ForwardingPager:
    """TaskSupervisor pager: pages go to the current term's session (log + notification outbox), or to
    the log when this node is not leader."""

    def __init__(self) -> None:
        self.controller: NodeController | None = None
        self._log = LogPager()

    def page(self, severity: str, title: str, body: str) -> None:
        session = self.controller.session if self.controller is not None else None
        if isinstance(session, EngineSession):
            session.page(severity, title, body)
        else:
            self._log.page(severity, title, body)


@dataclass
class EngineHooks:
    """Test seams; None = the production implementation."""

    etcd: EtcdClient | None = None
    redis: aioredis.Redis | None = None
    control_source: ConnectionSource | None = None
    ports: PortsFactory | None = None
    run_id_reader: RunIdReader | None = None
    exit_fn: ExitFn | None = None
    chrony: Callable[[], Awaitable[float | None]] | None = None
    tls_connector: TlsConnector | None = None
    secrets: SecretsSource | None = None
    aws_credentials: CredentialProvider | None = None
    token_cipher: Cipher | None = None
    http_client: Callable[[], httpx.AsyncClient] | None = None
    patroni_client: httpx.AsyncClient | None = None
    adapters: Callable[[], Sequence[BrokerAdapter]] | None = None
    timings: SessionTimings | None = None
    clock_ms: Callable[[], int] | None = None
    require_keeper_thread: bool = True
    maintenance: MaintenanceSource | None = None
    uptime_store: UptimeStore | None = None


@dataclass(frozen=True)
class Engine:
    controller: NodeController
    settings: EngineSettings
    context: SessionContext
    keeper: LeaseKeeper
    cluster: ClusterControl
    probe: NodeHealthProbe
    control: ConnectionSource
    closers: tuple[Callable[[], Awaitable[None]], ...] = ()
    spill: NodeSpillDrain | None = None
    # telemetry inputs (src/runtime/telemetry.py, built by src/main.py)
    etcd: EtcdClient | None = None
    patroni: PatroniRestControl | None = None
    chrony: ChronyReader | None = None
    redis: aioredis.Redis | None = None
    environ: Mapping[str, str] = field(default_factory=dict, repr=False)
    rest_budgets: RestBudgets | None = None
    # engine uptime (migration 0012): run by src/main.py run_engine on every node, any role
    uptime: UptimeTracker | None = None

    def new_session(self, epoch: Epoch) -> EngineSession:
        return EngineSession(self.context, epoch)

    async def aclose(self) -> None:
        for close in self.closers:
            try:
                await close()
            except Exception as exc:
                log.warning("engine close step failed: %r", exc)


def build_engine(config: RuntimeConfig, *, environ: Mapping[str, str] | None = None,
                 hooks: EngineHooks | None = None) -> Engine:
    """Compose the NodeController with real dependencies. Connects nothing. Raises WiringError."""
    h = hooks or EngineHooks()
    problems = config.problems()
    if problems:
        raise WiringError("; ".join(problems))
    raw: dict[str, str] = dict(os.environ if environ is None else environ)
    closers: list[Callable[[], Awaitable[None]]] = []
    owned_creds: list[Imdsv2Credentials] = []

    def aws_credentials() -> CredentialProvider:
        """Instance-role credentials shared by Secrets Manager and KMS (created on first need)."""
        if h.aws_credentials is not None:
            return h.aws_credentials
        if not owned_creds:
            owned_creds.append(Imdsv2Credentials())
        return owned_creds[0]

    async def close_creds() -> None:
        for creds in owned_creds:
            creds.close()

    source: SecretsSource
    if h.secrets is not None:
        source = h.secrets
    else:
        try:
            wants_aws = (raw.get(SOURCE_VAR) or "env").strip().lower() == "aws-sm"
            source = secrets_source_from_env(raw, credentials=aws_credentials() if wants_aws else None)
        except SecretsError as exc:
            raise WiringError(f"secrets: {exc}") from None
    try:
        settings = load_settings(config, raw, secrets=source)
    except BaseException:
        source.close()
        for creds in owned_creds:
            creds.close()
        raise

    async def close_source() -> None:
        source.close()
    closers.append(close_source)
    closers.append(close_creds)
    if source.kind != "env":
        source.start()
    for w in settings.warnings:
        log.warning("config: %s", w)
    node = config.node_id
    clock_ms = h.clock_ms or _wall_ms
    exit_fn: ExitFn = h.exit_fn or hard_exit

    etcd: EtcdClient
    if h.etcd is not None:
        etcd = h.etcd
    else:
        try:
            http_etcd = HttpEtcdClient(config.etcd_endpoints, verify=settings.etcd_tls.verify,
                                       cert=settings.etcd_tls.cert)
        except ValueError as exc:
            source.close()
            for creds in owned_creds:
                creds.close()
            raise WiringError(str(exc)) from None
        etcd = http_etcd

        async def close_etcd() -> None:
            http_etcd.close()
        closers.append(close_etcd)
    keeper = LeaseKeeper(etcd, fence=exit_fn)
    board = HealthBoard(etcd, node)
    timings = h.timings or SessionTimings()
    lt = keeper.timings
    problem = check_drain_budget(timings, lt.draining_deadline_s, lt.request_timeout_s)
    if problem is None and not TOKEN_REFRESH_MIN_LEASE_S < lt.fence_after_s - lt.renew_interval_s:
        problem = (f"token refresh needs {TOKEN_REFRESH_MIN_LEASE_S:.0f} s of lease, more than the "
                   f"{lt.fence_after_s - lt.renew_interval_s:.0f} s a renewed lease leaves")
    if problem is None:
        try:
            ping_s = ping_s_from_env(raw)
            catchup_settings = CatchupSettings.from_env(raw, ping_s=ping_s)
        except ValueError as exc:
            problem = str(exc)
    if problem is not None:
        source.close()
        for creds in owned_creds:
            creds.close()
        raise WiringError(problem)

    control: ConnectionSource
    if h.control_source is not None:
        control = h.control_source
    else:
        lazy = LazyPgSource(settings.storage.dsn, application_name=control_application_name(node), max_size=3)
        control = lazy
        closers.append(lazy.close)
    redis = h.redis if h.redis is not None else create_redis(settings.storage.redis_url)
    if h.redis is None:
        closers.append(redis.aclose)
    run_id_reader = h.run_id_reader or info_run_id

    cluster = ClusterControl(control, epoch=lambda: keeper.epoch, etcd=etcd)
    patroni = PatroniRestControl(default_endpoints(settings.storage.dsn), client=h.patroni_client,
                                 auth=settings.patroni_auth, standby_check=asyncpg_standby_check(settings.storage.dsn))
    closers.append(patroni.aclose)
    tls = BrokerTlsMonitor(broker_endpoints(settings.adapters), connector=h.tls_connector or tls_handshake,
                           clock_ms=clock_ms)
    chrony_reader = clock_reader()
    chrony_fn = h.chrony or chrony_reader.offset_ms
    if (raw.get(DEV_SKIP_CLOCK_VAR) or "").strip() == "1" and h.chrony is None:
        if config.broker_env != "sandbox":
            raise WiringError(f"{DEV_SKIP_CLOCK_VAR}=1 is refused under BROKER_ENV={config.broker_env}")
        log.warning("config: %s=1: the chrony hard check is bypassed (development only, sandbox only); "
                    "never set it on an engine node", DEV_SKIP_CLOCK_VAR)

        async def dev_clock_offset_ms() -> float | None:
            return 0.0

        chrony_fn = dev_clock_offset_ms

    async def primary_host() -> str | None:
        return (await patroni.primary()).host

    probe = NodeHealthProbe(node, pg=control, redis=redis, chrony=chrony_fn, tls=tls,
                            patroni_primary_host=primary_host, spill_path=config.spill_path)

    def on_fence_lost(exc: BaseException) -> None:
        log.critical("lost fence: %s", exc)
        exit_fn(f"fence: {exc}")

    async def mirror_safe_hold() -> bool:
        return await cluster.mirror_safe_hold(force=True)

    def default_ports(epoch: Epoch) -> LeaderPorts:
        return pg_leader_ports(epoch, control=control, dsn=settings.storage.dsn, node_id=node, redis=redis,
                               on_fence_lost=on_fence_lost, on_safe_hold=mirror_safe_hold,
                               run_id_reader=run_id_reader)

    token_stores: dict[str, TokenStore] = {}
    if settings.token_kms is not None and any(p.kind == "pepperstone" for p in settings.adapters):
        cipher: Cipher
        if h.token_cipher is not None:
            cipher = h.token_cipher
        else:
            kms = KmsCipher(settings.token_kms.key_id, settings.token_kms.region, credentials=aws_credentials())
            cipher = kms

            async def close_kms() -> None:
                kms.close()
            closers.append(close_kms)
        token_stores[PepperstoneAdapter.adapter_id] = LeaseGuardedTokenStore(
            KmsTokenStore(control, PepperstoneAdapter.adapter_id, cipher, epoch=lambda: keeper.epoch,
                          on_fence_lost=on_fence_lost),
            keeper.lease_valid)

    generations = CredentialGenerations(source)
    pager = ForwardingPager()
    budgets = RestBudgets()
    plans_source = rotating_plans(settings.adapters, source, raw) if source.kind != "env" else None
    adapters = h.adapters or adapter_factory(
        settings.adapters, keeper.lease_valid, clock_ms=clock_ms, token_stores=token_stores,
        plans_source=plans_source, on_built=generations.record, budgets=budgets)
    refresher = None if plans_source is None or h.adapters is not None else AdapterRefresher(
        plans_source, keeper.lease_valid, source, generations, clock_ms=clock_ms, token_stores=token_stores,
        budgets=budgets)
    maintenance = h.maintenance if h.maintenance is not None else maintenance_source_from_env(raw)
    # Engine uptime (every node, any role) and the leader's downtime catch-up (migration 0012). The tracker's
    # coverage / resume hooks are bound to the controller below.
    uptime = UptimeTracker(h.uptime_store or PgUptimeStore(control), node, ping_s=ping_s, clock_ms=clock_ms)
    ctx = SessionContext(
        node_id=node, spill_path=config.spill_path, redis=redis, ports=h.ports or default_ports, adapters=adapters,
        lease=keeper, notifications=settings.notifications, deadman=settings.deadman, oversee=settings.oversee,
        run_id_reader=run_id_reader, http_client=h.http_client or _http_client, patroni=patroni, cluster=cluster,
        tls=tls, chrony_synced=chrony_reader.synced, exit_fn=exit_fn, clock_ms=clock_ms,
        timings=timings, credential_generation=generations, on_auth_failed=lambda _aid: source.invalidate(),
        refresh_adapter=refresher, transfer_sources=DEFAULT_TRANSFER_SOURCES,
        notification_settings=settings_from_secrets(source, raw) if source.kind != "env" else None,
        invalidate_secrets=source.invalidate, on_fence_lost=on_fence_lost, maintenance=maintenance,
        deadman_token=deadman_token_loader(source, raw) if source.kind != "env" else None,
        heartbeat_copy=settings.heartbeat_copy,
        heartbeat_copy_token=heartbeat_copy_token_loader(source, raw) if source.kind != "env" else None,
        catchup=catchup_settings, catchup_metrics=CatchupMetrics(), engine_run_id=lambda: uptime.run_id)

    def session_factory(epoch: Epoch) -> EngineSession:
        return EngineSession(ctx, epoch)

    # Any node drains its own spill file outside a leader term (tier-persistence.md §5): at start-up, and
    # in STANDBY / SAFE_HOLD whenever PostgreSQL accepts the idempotent insert again.
    spill = NodeSpillDrain(SpillOnlySink(control), config.spill_path, node=node, clock_ms=clock_ms, pager=pager)
    controller = NodeController(node, keeper=keeper, board=board, probe=probe, terms=cluster, cluster=cluster,
                                local_redis=StandbyRedisFlush(redis), session_factory=session_factory,
                                pager=pager, ledger=cluster, exit_fn=exit_fn, clock_ms=clock_ms,
                                require_keeper_thread=h.require_keeper_thread, standby_work=spill,
                                standby_delay_after_drain_s=timings.drain_spill_s)

    def leader_streaming() -> Mapping[str, bool]:
        session = controller.session
        return session.streaming_adapters() if isinstance(session, EngineSession) else {}

    def market_open(adapter_id: str) -> bool | None:
        session = controller.session
        if not isinstance(session, EngineSession) or session.calendar.version is None:
            return None
        if adapter_id not in session.calendar.adapters():
            return None
        return session.calendar.market_open(adapter_id)

    def coverage_targets() -> tuple[int, Sequence[str]] | None:
        session = controller.session
        if controller.role is not NodeRole.LEADER or not isinstance(session, EngineSession):
            return None
        return session.coverage_targets()

    def on_suspended(event: SuspendEvent) -> None:
        session = controller.session
        if isinstance(session, EngineSession):
            session.resume_after_sleep(event.wall_gap_ms)

    uptime.coverage = coverage_targets
    uptime.on_suspended = on_suspended
    pager.controller = controller
    probe.leader_streaming = leader_streaming
    probe.market_open = market_open
    return Engine(controller, settings, ctx, keeper, cluster, probe, control, tuple(closers), spill=spill,
                  etcd=etcd, patroni=patroni, chrony=chrony_reader, redis=redis, environ=raw,
                  rest_budgets=budgets, uptime=uptime)


async def drain_spill_at_startup(engine: Engine, timeout_s: float = 5.0) -> int | None:
    """Any node, at start-up: deliver the spill file a crash left into events_dlq (the idempotent
    spill-drain insert, named exception 2). It runs through the SpillDrainService, which samples the
    `.torn` size before the read, so a torn tail moved aside here is paged (spill_torn_tail). None when
    PostgreSQL did not accept it (the standby drain loop, or the leader's DLQ barrier, retries)."""
    spill = engine.spill or NodeSpillDrain(SpillOnlySink(engine.control), engine.settings.runtime.spill_path,
                                           node=engine.settings.runtime.node_id, clock_ms=_wall_ms,
                                           attempt_timeout_s=timeout_s)
    try:
        return await asyncio.wait_for(spill.drain_at_startup(), timeout_s + 1.0)
    except Exception as exc:
        log.warning("start-up spill drain failed: %s", type(exc).__name__)
        return None


def _http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=10.0)


CHECK_EPOCH: Final = Epoch("check-config", 0)


async def check_composition(config: RuntimeConfig, *, environ: Mapping[str, str] | None = None,
                            hooks: EngineHooks | None = None) -> list[str]:
    """`--check-config`: build the engine and one term's session (placeholder epoch, no connection),
    then close them. Returns the problems; an empty list means the composition is complete."""
    problems = list(config.problems())
    if problems:
        return problems
    try:
        engine = build_engine(config, environ=environ, hooks=hooks)
    except (WiringError, ExecutionLockedError) as exc:
        return [str(exc)]
    try:
        session = engine.new_session(CHECK_EPOCH)
        session.init_deps(TaskSupervisor([]))
        for adapter in session.adapters:
            await adapter.close()
        await session.aclose()
    except Exception as exc:
        problems.append(f"leader session composition failed: {type(exc).__name__}: {exc}")
    finally:
        await engine.aclose()
    return problems
