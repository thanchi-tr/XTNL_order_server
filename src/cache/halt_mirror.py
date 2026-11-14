"""HaltMirror (src/oversee/halt.py) over Redis: halt:state is written only by halt_cas.lua, which applies
only when the incoming halt_event_id is greater than the stored one (tier-persistence.md §1,
oversee-rules.md §8)."""
from __future__ import annotations

from src.cache.scripts import RedisState
from src.oversee.types import HaltLevel


class RedisHaltMirror:
    def __init__(self, state: RedisState) -> None:
        self._state = state

    async def cas_set(self, level: HaltLevel, event_id: int) -> bool:
        return await self._state.set_halt_state(level.name, event_id)

    async def read(self) -> tuple[HaltLevel, int] | None:
        raw = await self._state.read_halt_state()
        return None if raw is None else (HaltLevel[raw[0]], raw[1])
