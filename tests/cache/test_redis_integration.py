"""Real Redis integration (skipped unless ORDER_SERVER_TEST_REDIS points at a DISPOSABLE Redis: the test
flushes the selected database)."""
from __future__ import annotations

import os
from decimal import Decimal

import pytest

from src.cache import keys as K
from src.cache.client import create_redis
from src.cache.feed_stream import RedisFeedStream
from src.cache.live_reader import info_run_id, read_gate_keys, read_quotes
from src.cache.scripts import RedisState, boot_id_value, entity_write
from src.dto import OrderDTO, OrderStatus, PriceDTO

URL = os.environ.get("ORDER_SERVER_TEST_REDIS")
pytestmark = [pytest.mark.redis, pytest.mark.skipif(not URL, reason="ORDER_SERVER_TEST_REDIS not set")]


async def test_scripts_feed_and_gate_keys_on_real_redis() -> None:
    assert URL is not None
    r = create_redis(URL)
    try:
        await r.flushdb()
        run_id = await info_run_id(r)
        assert run_id
        boot = boot_id_value(run_id, "5")
        await r.set(K.BOOT_ID, boot)
        state = RedisState(r, lambda: boot)
        o = OrderDTO(client_order_id=None, broker_order_id="R1", account_id="ACC", adapter_id="it", symbol="EURUSD",
                     side="BUY", status=OrderStatus.WORKING, entry_price=Decimal("1.1"), ordered_units=Decimal(1),
                     remaining_units=Decimal(1), broker_updated_at_ms=1, observed_seq=1, seq_id=1)
        assert (await state.apply(entity_write(o))).value == "APPLIED"
        feed = RedisFeedStream(r, "it")
        await feed.ensure_group()
        await state.write_price(PriceDTO(adapter_id="it", symbol="EURUSD", bid=Decimal("1.1"), ask=Decimal("1.1001"),
                                         spread_pips=Decimal("0.1"), timestamp_ms=5), Decimal(1))
        assert (await read_quotes(r, [("it", "EURUSD")]))[("it", "EURUSD")].clean_mid is not None
        assert await state.set_halt_state("SOFT", 1)
        keys = await read_gate_keys(r)
        assert keys.boot_id == boot and keys.run_id == run_id and keys.halt_state == ("SOFT", 1)
    finally:
        await r.flushdb()
        await r.aclose()
