"""redis.asyncio wrappers around the three Lua scripts (tier-persistence.md §1-§2).

* ScriptRunner: SCRIPT LOAD once, EVALSHA, reload + retry on NOSCRIPT (a restarted Redis).
* RedisState: the Redis half of StateCommitter (apply_state.lua, light.lua, cash-flow feed entry,
  halt:state CAS) plus the NO_BOOT queue that SM8 replays as boot-lock holder.
* RedisStreamHealth: the conn:{adapter}:{stream} half of StreamHealthSink.
* trim_feed / flush_local_redis / read_boot_id: feed trimming, the guarded FLUSHDB, boot id helpers.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from importlib import resources
from typing import Any, Final, Literal

import redis.asyncio as aioredis
from redis.exceptions import NoScriptError, RedisError

from src.cache import keys as K
from src.cache.codec import decimal_str, dto_json
from src.cache.digest import entity_hash
from src.dto import AccountDTO, AccountLive, CashFlowDTO, InstrumentSpec, OrderDTO, PositionDTO, PriceDTO
from src.dto import payload_checksum, version_str

log = logging.getLogger(__name__)

ScriptName = Literal["apply_state", "light", "halt_cas"]
SEQ_WIDTH: Final = 20
TS_WIDTH: Final = 16
HALT_ID_WIDTH: Final = 20


def _s(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def pad(value: int, width: int) -> str:
    """Zero-padded fixed-width decimal string (Lua compares these as strings)."""
    if value < 0:
        raise ValueError("fixed-width values must be non-negative")
    out = f"{value:0{width}d}"
    if len(out) != width:
        raise ValueError(f"{value} does not fit in {width} digits")
    return out


def load_script_source(name: ScriptName) -> str:
    return resources.files("src.cache").joinpath("lua", f"{name}.lua").read_text(encoding="utf-8")


class ScriptRunner:
    """SCRIPT LOAD / EVALSHA with a NOSCRIPT fallback (Redis restarted or SCRIPT FLUSH)."""

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis
        self._src: dict[ScriptName, str] = {}
        self._sha: dict[ScriptName, str] = {}

    async def load(self, name: ScriptName) -> str:
        if name not in self._src:
            self._src[name] = load_script_source(name)
        sha = _s(await self._redis.script_load(self._src[name]))
        self._sha[name] = sha
        return sha

    async def run(self, name: ScriptName, keys: Sequence[str], args: Sequence[str]) -> Any:
        sha = self._sha.get(name) or await self.load(name)
        try:
            return await self._redis.evalsha(sha, len(keys), *keys, *args)
        except NoScriptError:
            sha = await self.load(name)
            return await self._redis.evalsha(sha, len(keys), *keys, *args)


class ApplyResult(str, Enum):
    APPLIED = "APPLIED"
    NOOP_ADVANCED = "NOOP_ADVANCED"
    STALE = "STALE"
    NO_BOOT = "NO_BOOT"


@dataclass(frozen=True)
class EntityWrite:
    """One apply_state.lua call: everything is computed from the committed PostgreSQL row."""

    key: str
    version: str
    seq_id: int
    checksum: str
    payload: str
    entity_type: K.EntityType
    entity_id: str
    account_id: str

    @property
    def digest_hash(self) -> str:
        return entity_hash(self.key, self.version, self.checksum)

    def order_key(self) -> tuple[str, str]:
        return (self.version, pad(self.seq_id, SEQ_WIDTH))


def entity_write(dto: OrderDTO | PositionDTO | AccountDTO) -> EntityWrite:
    """EntityWrite for a stateful DTO (key, fixed-width version, checksum, JSON payload)."""
    if isinstance(dto, OrderDTO):
        eid = dto.broker_order_id
    elif isinstance(dto, PositionDTO):
        eid = dto.position_id
    else:
        eid = dto.account_id
    return EntityWrite(
        key=K.entity_key_of(dto), version=version_str(dto), seq_id=dto.seq_id, checksum=dto.checksum,
        payload=dto_json(dto), entity_type=K.entity_type_of(dto), entity_id=eid, account_id=dto.account_id,
    )


def instrument_version(seq_id: int) -> str:
    """Instruments are not broker-versioned: order them by the HLC seq_id of their refresh."""
    return f"{0:020d}{0:016d}{seq_id:020d}"


def instrument_write(spec: InstrumentSpec, seq_id: int) -> EntityWrite:
    return EntityWrite(
        key=K.instrument_key(spec.adapter_id, spec.symbol), version=instrument_version(seq_id), seq_id=seq_id,
        checksum=payload_checksum(spec), payload=dto_json(spec), entity_type="INSTRUMENT",
        entity_id=f"{spec.adapter_id}:{spec.symbol}", account_id="",
    )


class NoBootQueue:
    """Writes refused with NO_BOOT (or lost to a Redis error) after their PostgreSQL commit.

    Kept per key (the newest version wins, the script would drop older ones anyway) until SM8 replays
    them as boot-lock holder, before writing redis:boot_id (tier-persistence.md §2 step 5).
    """

    def __init__(self, on_enqueue: Callable[[], None] | None = None) -> None:
        self._pending: dict[str, EntityWrite] = {}
        self._on_enqueue = on_enqueue

    def add(self, w: EntityWrite) -> None:
        cur = self._pending.get(w.key)
        if cur is None or w.order_key() > cur.order_key():
            self._pending[w.key] = w
        if self._on_enqueue is not None:
            self._on_enqueue()

    def __len__(self) -> int:
        return len(self._pending)

    def snapshot(self) -> list[EntityWrite]:
        return list(self._pending.values())

    def discard(self, w: EntityWrite) -> None:
        if self._pending.get(w.key) == w:
            del self._pending[w.key]


class RedisState:
    """Redis half of the StateCommitter (entity keys, live keys, feed, halt:state)."""

    def __init__(
        self,
        redis: aioredis.Redis,
        expected_boot_id: Callable[[], str],
        *,
        runner: ScriptRunner | None = None,
        no_boot: NoBootQueue | None = None,
        feed_maxlen: int = K.FEED_MAXLEN,
    ) -> None:
        self._redis = redis
        self._runner = runner if runner is not None else ScriptRunner(redis)
        self._expected_boot_id = expected_boot_id
        self.no_boot = no_boot if no_boot is not None else NoBootQueue()
        self._maxlen = feed_maxlen

    # ---- entity keys (apply_state.lua) ---------------------------------------------------------

    async def apply(self, w: EntityWrite, *, lock_token: str | None = None) -> ApplyResult:
        """Run apply_state.lua once. Raises RedisError on transport failure."""
        res = await self._runner.run(
            "apply_state",
            [w.key, K.STATE_DIGEST, K.FEED, K.BOOT_ID, K.BOOT_LOCK],
            [self._expected_boot_id(), lock_token or "", w.version, pad(w.seq_id, SEQ_WIDTH), w.checksum,
             w.digest_hash, w.payload, w.entity_type, w.entity_id, w.account_id, str(w.seq_id),
             str(self._maxlen)],
        )
        return ApplyResult(_s(res))

    async def apply_after_commit(self, w: EntityWrite) -> ApplyResult:
        """Post-commit copy. NO_BOOT or a Redis error queues the write for SM8 (never raises)."""
        try:
            res = await self.apply(w)
        except (RedisError, OSError) as exc:
            log.warning("redis apply failed after commit, queued for SM8: key=%s err=%s", w.key, exc)
            self.no_boot.add(w)
            return ApplyResult.NO_BOOT
        if res is ApplyResult.NO_BOOT:
            self.no_boot.add(w)
        return res

    async def replay_no_boot(self, lock_token: str) -> int:
        """SM8 step 5: re-apply queued writes as boot-lock holder. Returns how many were replayed."""
        done = 0
        for w in self.no_boot.snapshot():
            res = await self.apply(w, lock_token=lock_token)
            if res is ApplyResult.NO_BOOT:
                raise RuntimeError("boot lock not held during NO_BOOT replay")
            self.no_boot.discard(w)
            done += 1
        return done

    # ---- live keys (light.lua) -----------------------------------------------------------------

    async def _light(self, keys: list[str], mode: str, ts_ms: int, ttl_ms: int | None, spread: str,
                     threshold: str, mid: str, force_clean: bool, fields: list[str]) -> tuple[bool, bool]:
        res = await self._runner.run(
            "light", keys,
            [mode, pad(ts_ms, TS_WIDTH), "" if ttl_ms is None else str(ttl_ms), spread, threshold, mid,
             "1" if force_clean else "0", *fields],
        )
        return bool(int(res[0])), bool(int(res[1]))

    async def write_price(self, p: PriceDTO, max_clean_spread_pips: Decimal | None, *,
                          seed: bool = False) -> tuple[bool, bool]:
        """price:{a}:{s} always (latest ts wins); clean_mid:{a}:{s} only when spread <= threshold."""
        fields = ["bid", decimal_str(p.bid), "ask", decimal_str(p.ask), "mid", decimal_str(p.mid),
                  "spread", decimal_str(p.spread_pips), "tradeable", "1" if p.tradeable else "0",
                  "seeded", "1" if seed else "0"]
        return await self._light(
            [K.price_key(p.adapter_id, p.symbol), K.clean_mid_key(p.adapter_id, p.symbol)],
            "seed" if seed else "live", p.timestamp_ms, None, decimal_str(p.spread_pips),
            decimal_str(max_clean_spread_pips), decimal_str(p.mid), False, fields,
        )

    async def seed_clean_mark(self, adapter_id: str, symbol: str, bid: Decimal, ask: Decimal,
                              ts_ms: int, spread_pips: Decimal | None = None) -> tuple[bool, bool]:
        """SM8 step 3: seed price: and clean_mid: from last_clean_marks, only where absent or older."""
        mid = (bid + ask) / 2
        fields = ["bid", decimal_str(bid), "ask", decimal_str(ask), "mid", decimal_str(mid),
                  "spread", decimal_str(spread_pips), "tradeable", "1", "seeded", "1"]
        return await self._light(
            [K.price_key(adapter_id, symbol), K.clean_mid_key(adapter_id, symbol)], "seed", ts_ms, None,
            decimal_str(spread_pips), "", decimal_str(mid), True, fields,
        )

    async def write_account_live(self, live: AccountLive, *, seed: bool = False) -> bool:
        """account_live:{account_id}; a seed (sampled) never overwrites a fresher value."""
        fields = ["adapter_id", live.adapter_id, "currency", live.currency,
                  "equity", decimal_str(live.equity), "margin_used", decimal_str(live.margin_used),
                  "free_margin", decimal_str(live.free_margin), "unrealised_pnl", decimal_str(live.unrealised_pnl),
                  "sampled", "1" if (live.sampled or seed) else "0"]
        wrote, _ = await self._light([K.account_live_key(live.account_id)], "seed" if seed else "live",
                                     live.timestamp_ms, None, "", "", "", False, fields)
        return wrote

    async def write_conn(self, adapter_id: str, stream: K.StreamKind, ts_ms: int) -> bool:
        wrote, _ = await self._light([K.conn_key(adapter_id, stream)], "live", ts_ms, K.CONN_TTL_MS,
                                     "", "", "", False, ["stream", stream])
        return wrote

    async def delete_conn(self, adapter_id: str, stream: K.StreamKind) -> None:
        await self._redis.delete(K.conn_key(adapter_id, stream))

    # ---- feed: cash-flow commits ---------------------------------------------------------------

    async def feed_cash_flow(self, cf: CashFlowDTO) -> str:
        """The second feed writer: one ACCOUNT entry per committed cash flow."""
        entry_id = await self._redis.xadd(
            K.FEED,
            {"entity_type": "ACCOUNT", "entity_id": cf.account_id, "account_id": cf.account_id,
             "seq_id": str(cf.seq_id)},
            maxlen=self._maxlen, approximate=True,
        )
        return _s(entry_id)

    # ---- halt:state (halt_cas.lua) -------------------------------------------------------------

    async def set_halt_state(self, effective_level: str, halt_event_id: int) -> bool:
        res = await self._runner.run("halt_cas", [K.HALT_STATE], [effective_level, pad(halt_event_id, HALT_ID_WIDTH)])
        return _s(res) == "APPLIED"

    async def read_halt_state(self) -> tuple[str, int] | None:
        raw = await self._redis.hgetall(K.HALT_STATE)
        if not raw:
            return None
        d = {_s(k): _s(v) for k, v in raw.items()}
        return d["effective_level"], int(d["halt_event_id"])

    async def delete_halt_state(self) -> None:
        """Integrity 'ahead' case only: DEL then re-apply the latest halt_events row."""
        await self._redis.delete(K.HALT_STATE)


class RedisStreamHealth:
    """conn:{adapter}:{stream} keys: PEXPIRE 30 s, refreshed while SUBSCRIBED, deleted during a gap."""

    def __init__(self, state: RedisState, clock_ms: Callable[[], int]) -> None:
        self._state = state
        self._clock_ms = clock_ms

    async def mark_lost(self, adapter_id: str, stream: K.StreamKind) -> None:
        await self._state.delete_conn(adapter_id, stream)

    async def mark_reconciled(self, adapter_id: str, stream: K.StreamKind) -> None:
        await self._state.write_conn(adapter_id, stream, self._clock_ms())

    async def heartbeat(self, adapter_id: str, stream: K.StreamKind) -> None:
        await self._state.write_conn(adapter_id, stream, self._clock_ms())


# ---- feed trimming (every 10 s) ------------------------------------------------------------------

@dataclass(frozen=True)
class TrimResult:
    min_id: str | None
    length: int
    lag_alert: bool


def _id_tuple(entry_id: str) -> tuple[int, int]:
    ms, _, seq = entry_id.partition("-")
    return int(ms), int(seq or 0)


async def trim_feed(redis: aioredis.Redis, *, maxlen: int = K.FEED_MAXLEN) -> TrimResult:
    """XTRIM oversee:feed MINID <oldest un-acked> across all groups; lag alert at 50 % of the cap."""
    try:
        groups = await redis.xinfo_groups(K.FEED)
    except RedisError:
        return TrimResult(None, 0, False)  # no stream yet
    candidates: list[str] = []
    for g in groups:
        name = _s(g["name"])
        pending = await redis.xpending(K.FEED, name)
        if int(pending["pending"]) > 0 and pending["min"] is not None:
            candidates.append(_s(pending["min"]))
        else:
            candidates.append(_s(g["last-delivered-id"]))
    min_id = min(candidates, key=_id_tuple) if candidates else None
    if min_id is not None and min_id != "0-0":
        await redis.xtrim(K.FEED, minid=min_id, approximate=False)
    length = int(await redis.xlen(K.FEED))
    return TrimResult(min_id, length, length >= maxlen // 2)


# ---- boot id & guarded flush ---------------------------------------------------------------------

async def redis_run_id(redis: aioredis.Redis) -> str:
    info = await redis.info("server")
    return _s(info.get("run_id", ""))


def boot_id_value(run_id: str, epoch_label: str) -> str:
    """redis:boot_id = run_id:epoch, where epoch is `src.db.epoch.Epoch.label` (the fencing token), the same
    value ActionGate compares (`f"{run_id}:{node_epoch}"`, oversee-rules.md §8)."""
    return f"{run_id}:{epoch_label}"


async def read_boot_id(redis: aioredis.Redis) -> str | None:
    raw = await redis.get(K.BOOT_ID)
    return None if raw is None else _s(raw)


FlushReason = Literal["LEADER_INIT", "STANDBY_ENTRY"]


async def flush_local_redis(redis: aioredis.Redis, reason: FlushReason) -> None:
    """FLUSHDB of the engine's Redis database: LEADER_INIT step 3 and STANDBY entry only (operations.md §4).
    The engine never issues FLUSHALL (other databases on the instance are not the engine's).

    Removes redis:boot_id, so the gate stays closed until SM8 rebuilds Redis from PostgreSQL.
    """
    if reason not in ("LEADER_INIT", "STANDBY_ENTRY"):
        raise PermissionError(f"FLUSHDB refused for reason {reason!r}")
    await redis.flushdb()
    if await read_boot_id(redis) is not None:
        raise RuntimeError("redis:boot_id survived FLUSHDB")
