"""Recording LEADER_INIT dependencies shared by the leader_init and node tests."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from src.db.epoch import Epoch
from src.runtime.leader_init import FenceOutcome, LeaderInitDeps, SodOutcome


@dataclass
class InitRecorder:
    """Implements every LEADER_INIT protocol; `fail_at` makes that step raise, `hang_at` makes it hang."""

    log: list[str] = field(default_factory=list)
    fail_at: str | None = None
    hang_at: str | None = None
    safe_hold: bool = False
    partial: bool = False
    flags: list[tuple[str, str]] = field(default_factory=list)
    sm4_started: asyncio.Event = field(default_factory=asyncio.Event)
    reseeded: int | None = None

    async def _step(self, name: str) -> None:
        self.log.append(name)
        if self.fail_at == name:
            raise RuntimeError(f"{name} failed")
        if self.hang_at == name:
            await asyncio.Event().wait()

    async def bump_and_fence(self, epoch: Epoch, node_id: str, now_ms: int) -> FenceOutcome:
        await self._step("epoch_bump")
        return FenceOutcome(self.safe_hold, "flap" if self.safe_hold else None, 3, 1)

    async def max_seq_id(self) -> int:
        await self._step("max_seq")
        return 12345

    def reseed(self, max_seq: int) -> None:
        self.log.append("hlc_reseed")
        self.reseeded = max_seq

    async def flushall(self) -> None:      # NodeController STANDBY flush (not a LEADER_INIT step)
        await self._step("flushall")

    async def flushdb(self) -> None:       # LEADER_INIT step 3: the engine's database only
        await self._step("flushdb")

    async def load_ruleset(self, now_ms: int) -> int | None:
        await self._step("ruleset")
        return 3

    async def catch_up(self, now_ms: int) -> SodOutcome:
        await self._step("sod")
        return SodOutcome(1, self.partial)

    async def init_calendar(self, now_ms: int) -> int:
        await self._step("calendar")
        return 7

    async def start_adapters(self) -> None:
        await self._step("sm4")
        self.sm4_started.set()

    async def bootstrap(self) -> None:
        # SM8 runs in parallel with SM4: it only completes after SM4 has started
        await asyncio.wait_for(self.sm4_started.wait(), 1.0)
        await self._step("sm8")

    async def start_risksweep(self) -> None:
        await self._step("risksweep")

    async def startup_barrier(self) -> None:
        await self._step("dlq_barrier")

    async def mark_reached_leader(self, epoch: Epoch, now_ms: int) -> None:
        await self._step("reached_leader")

    def flag(self, code: str, detail: str) -> None:
        self.flags.append((code, detail))

    def deps(self) -> LeaderInitDeps:
        return LeaderInitDeps(self, self, self, self, self, self, self, self, self, self, self, self, rules=self)
