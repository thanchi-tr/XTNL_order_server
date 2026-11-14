"""Read-only cTrader (Pepperstone) connectivity probe: ``python -m src.adapters.pepperstone.probe``.

Reads PepperstoneSettings.from_env() (environment variables only, never .env files), authenticates the
application and the account, and prints the token scope, the account's balance and currency, the counts
of open positions and pending orders, and the first few instruments. It never prints the client secret
or any token, makes no order-changing call, and never refreshes the token pair (a refresh rotates the
pair at the broker, and the probe has no durable store to save it in): an expired token is reported.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from src.adapters.base import BrokerError
from src.adapters.pepperstone.adapter import PepperstoneAdapter
from src.config import PepperstoneSettings


async def run(settings: PepperstoneSettings, *, max_instruments: int = 5,
              adapter: PepperstoneAdapter | None = None) -> int:
    pa = adapter if adapter is not None else PepperstoneAdapter(settings, allow_token_refresh=False)
    try:
        await pa.connect()
        print(f"account {pa.account_id}: scope={pa.permission_scope or '?'} live={pa.is_live_account} "
              f"api_version={await pa.api_version() or '?'}")
        accounts, lives = await pa.fetch_account_snapshot()
        for account, live in zip(accounts, lives, strict=True):
            print(f"balance={account.balance} {account.currency} balance_version={account.broker_seq} "
                  f"margin_used={live.margin_used} (equity not provided by cTrader)")
        instruments = await pa.fetch_instruments()
        positions = await pa.fetch_positions()
        orders = await pa.fetch_open_orders()
        print(f"open positions: {len(positions)}  pending orders: {len(orders)}  instruments: {len(instruments)}")
        for spec in instruments[:max_instruments]:
            print(f"  {spec.symbol:<10} symbolId={spec.broker_symbol} pnl_ccy={spec.pnl_ccy} "
                  f"point={spec.point_size} min_qty={spec.min_qty}")
        return 0
    except BrokerError as exc:
        print(f"cTrader probe failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        await pa.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only cTrader (Pepperstone) connectivity probe")
    parser.add_argument("--instruments", type=int, default=5, help="how many instruments to print")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    settings = PepperstoneSettings.from_env()
    print(repr(settings))   # repr hides the client secret and the tokens
    return asyncio.run(run(settings, max_instruments=args.instruments))


if __name__ == "__main__":
    raise SystemExit(main())
