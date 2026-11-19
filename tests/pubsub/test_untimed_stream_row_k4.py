"""K4 (dto-contracts.md §2): a stream row of a broker-time type (ORDER / POSITION on StoneX and cTrader) that
arrives without its broker timestamp has no legal version label (the watermark labels REST rows only, and a local
clock never goes in the broker-time slot). It is refused like a lost frame: never published, its stream's gap
opens (RESYNCING reads the entity again by REST) and it is paged. Local-time types and REST rows are unchanged."""
from __future__ import annotations

from pathlib import Path

import pytest

from src.adapters.base import VersionDecl
from src.dto import AccountDTO
from tests.pubsub.conftest import FakeAdapter, ManualClock, account, order, position
from tests.pubsub.test_orchestrator import make, ready

Page = tuple[str, str, str]


@pytest.fixture(autouse=True)
def _sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BROKER_ENV", "sandbox")


async def test_k4_untimed_stream_order_is_refused_and_opens_the_gap(tmp_path: Path, clock: ManualClock) -> None:
    pages: list[Page] = []
    env = make(tmp_path, clock, pager=lambda s, t, b: pages.append((s, t, b)))
    await ready(env)
    rt = env.orch.runtime("mock")
    env.item("orders", order("O9", t=0))
    assert env.orders() == []                                    # never published with a 0 / local time
    assert rt.subs["orders"].lost and rt.resync_event.is_set()
    assert env.orch.frames_lost.get("orders") == 1
    assert "ingestion frame lost: mock:orders" in [t for _, t, _ in pages]
    assert rt.watermark_ms is None                               # nothing advanced the stream watermark


async def test_k4_untimed_stream_position_is_refused_and_opens_the_gap(tmp_path: Path,
                                                                     clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    rt = env.orch.runtime("mock")
    env.item("orders", position("P9", t=0))                      # positions share the orders stream
    assert env.positions() == []
    assert rt.subs["orders"].lost and rt.resync_event.is_set()


async def test_k4_the_resync_rest_pass_reads_it_again_and_clears_the_gap(tmp_path: Path,
                                                                       clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    rt = env.orch.runtime("mock")
    env.item("orders", order("O9", t=0))
    assert rt.subs["orders"].lost
    env.adapter.open_orders = [order("O9", t=0)]                 # an untimed REST row: watermark-labelled
    rep = await env.orch.run_snapshot("mock")
    assert not rep.dropped_after_watermark
    published = env.orders()
    assert [o.broker_order_id for o in published] == ["O9"]
    assert published[0].broker_time_source == "WATERMARK" and published[0].source == "REST"
    await env.health_ops()
    assert not rt.subs["orders"].lost


async def test_k4_a_timed_stream_row_is_unchanged(tmp_path: Path, clock: ManualClock) -> None:
    env = make(tmp_path, clock)
    await ready(env)
    rt = env.orch.runtime("mock")
    env.item("orders", order("O9", t=clock.ms - 100))
    (o,) = env.orders()
    assert o.broker_time_source == "BROKER" and o.broker_updated_at_ms == clock.ms - 100
    assert not rt.subs["orders"].lost


async def test_k4_local_time_types_stay_in_the_local_clock_domain(tmp_path: Path, clock: ManualClock) -> None:
    """ACCOUNT (and a broker without order time) declares no broker time: an untimed stream row takes the receipt
    time, as before; the K4 refusal applies only where broker time is declared."""
    local = {"ORDER": VersionDecl(False, False), "POSITION": VersionDecl(False, False),
             "ACCOUNT": VersionDecl(False, False)}
    env = make(tmp_path, clock, adapter=FakeAdapter(versioning=local))
    await ready(env)
    rt = env.orch.runtime("mock")
    env.item("account", account("9000"))
    env.item("orders", order("O9", t=0))
    accounts = [a for a in env.account_items() if isinstance(a, AccountDTO)]
    assert [a.broker_updated_at_ms for a in accounts] == [clock.ms]
    (o,) = env.orders()
    assert o.broker_updated_at_ms == clock.ms and o.broker_time_source == "WATERMARK"
    assert not rt.subs["orders"].lost and not rt.subs["account"].lost
