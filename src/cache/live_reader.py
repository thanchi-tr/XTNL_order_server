"""Read-only access to Redis live keys for the RiskSweep snapshot and the ActionGate
(oversee-rules.md §2 Inputs, §8; tier-persistence.md §1).

* `read_quotes`: price:{adapter}:{symbol} and clean_mid:{adapter}:{symbol}, pipelined, with timestamps.
* `read_account_live`: account_live:{account_id}.
* `read_gate_keys`: redis:boot_id, halt:state and INFO run_id.

Values are written by light.lua (`ts` = 16-digit epoch ms) and halt_cas.lua. A malformed hash is
treated as absent, so the sweep falls back to last_clean_marks / UNKNOWN instead of failing.
"""
from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import redis.asyncio as aioredis

from src.cache import keys as K
from src.dto import AccountLive, PriceDTO

log = logging.getLogger(__name__)

SymbolKey = tuple[str, str]


def _s(v: Any) -> str:
    return v.decode("utf-8") if isinstance(v, bytes) else str(v)


def _hash(raw: Any) -> dict[str, str]:
    return {_s(k): _s(v) for k, v in (raw or {}).items()}


def _dec(v: str | None) -> Decimal | None:
    if v is None or v == "":
        return None
    return Decimal(v)


@dataclass(frozen=True)
class LiveQuote:
    latest: PriceDTO | None
    clean_mid: tuple[Decimal, int] | None   # (mid, ts_ms)


def price_from_hash(adapter_id: str, symbol: str, h: Mapping[str, str]) -> PriceDTO | None:
    try:
        bid, ask = _dec(h.get("bid")), _dec(h.get("ask"))
        if bid is None or ask is None or "ts" not in h:
            return None
        return PriceDTO(adapter_id=adapter_id, symbol=symbol, bid=bid, ask=ask, spread_pips=_dec(h.get("spread")),
                        timestamp_ms=int(h["ts"]), tradeable=h.get("tradeable", "1") == "1")
    except (InvalidOperation, ValueError):
        log.warning("malformed price hash %s:%s", adapter_id, symbol)
        return None


def clean_mid_from_hash(h: Mapping[str, str]) -> tuple[Decimal, int] | None:
    try:
        mid = _dec(h.get("mid"))
        if mid is None or "ts" not in h:
            return None
        return mid, int(h["ts"])
    except (InvalidOperation, ValueError):
        return None


def account_live_from_hash(account_id: str, h: Mapping[str, str]) -> AccountLive | None:
    if "ts" not in h:
        return None
    try:
        return AccountLive(account_id=account_id, adapter_id=h.get("adapter_id", ""), currency=h.get("currency", ""),
                           equity=_dec(h.get("equity")), margin_used=_dec(h.get("margin_used")),
                           free_margin=_dec(h.get("free_margin")), unrealised_pnl=_dec(h.get("unrealised_pnl")),
                           timestamp_ms=int(h["ts"]), sampled=h.get("sampled", "0") == "1")
    except (InvalidOperation, ValueError):
        return None


async def read_quotes(redis: aioredis.Redis, symbols: Iterable[SymbolKey]) -> dict[SymbolKey, LiveQuote]:
    """One pipelined round trip; keys with neither a price nor a clean mid are left out."""
    keys = sorted(set(symbols))
    if not keys:
        return {}
    pipe = redis.pipeline(transaction=False)
    for a, s in keys:
        pipe.hgetall(K.price_key(a, s))
        pipe.hgetall(K.clean_mid_key(a, s))
    raw = await pipe.execute()
    out: dict[SymbolKey, LiveQuote] = {}
    for i, (a, s) in enumerate(keys):
        latest = price_from_hash(a, s, _hash(raw[2 * i]))
        clean = clean_mid_from_hash(_hash(raw[2 * i + 1]))
        if latest is not None or clean is not None:
            out[(a, s)] = LiveQuote(latest, clean)
    return out


async def read_account_live(redis: aioredis.Redis, account_id: str) -> AccountLive | None:
    return account_live_from_hash(account_id, _hash(await redis.hgetall(K.account_live_key(account_id))))


@dataclass(frozen=True)
class GateKeys:
    boot_id: str | None
    run_id: str | None
    halt_state: tuple[str, int] | None   # (effective_level, halt_event_id)


RunIdReader = Callable[[aioredis.Redis], Awaitable[str | None]]


async def info_run_id(redis: aioredis.Redis) -> str | None:
    """Redis `INFO server` run_id (changes on every Redis restart, so a persisted boot_id never matches)."""
    info = await redis.info("server")
    run_id = info.get("run_id") if isinstance(info, dict) else None
    return None if not run_id else _s(run_id)


async def read_gate_keys(redis: aioredis.Redis, run_id_reader: RunIdReader = info_run_id) -> GateKeys:
    pipe = redis.pipeline(transaction=False)
    pipe.get(K.BOOT_ID)
    pipe.hgetall(K.HALT_STATE)
    boot_raw, halt_raw = await pipe.execute()
    run_id = await run_id_reader(redis)
    halt = _hash(halt_raw)
    halt_state: tuple[str, int] | None = None
    if "effective_level" in halt and "halt_event_id" in halt:
        try:
            halt_state = (halt["effective_level"], int(halt["halt_event_id"]))
        except ValueError:
            halt_state = None
    return GateKeys(None if boot_raw is None else _s(boot_raw), run_id or None, halt_state)
