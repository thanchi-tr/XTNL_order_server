"""Read-only StoneX connectivity probe: ``python -m src.adapters.stonex.probe``.

Reads StoneXSettings.from_env() (environment variables only, never .env files), logs on, and prints
account ids, margin, the counts of open positions and active orders, and the first few instruments.
It never prints the password, app key or session token, and makes no order-changing call.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from src.adapters.base import BrokerError
from src.adapters.stonex.adapter import StoneXAdapter
from src.config import StoneXSettings


async def run(settings: StoneXSettings, *, max_instruments: int = 5, adapter: StoneXAdapter | None = None) -> int:
    sx = adapter if adapter is not None else StoneXAdapter(settings)
    try:
        await sx.connect()
        directory = sx.account_directory
        assert directory is not None
        for acct in directory.accounts:
            print(f"client account {acct.client_account_id} currency={acct.currency or '?'} "
                  f"trading accounts={', '.join(acct.trading_account_ids) or '-'}")
        accounts, lives = await sx.fetch_account_snapshot()
        for account, live in zip(accounts, lives, strict=True):
            print(f"margin {account.account_id}: balance={account.balance} {account.currency} "
                  f"equity={live.equity} margin_used={live.margin_used} free={live.free_margin} "
                  f"unrealised={live.unrealised_pnl}")
        instruments = await sx.fetch_instruments()
        positions = await sx.fetch_positions()
        orders = await sx.fetch_open_orders()
        print(f"open positions: {len(positions)}  active orders: {len(orders)}  instruments: {len(instruments)}")
        for spec in instruments[:max_instruments]:
            print(f"  {spec.symbol:<10} market={spec.broker_symbol} pnl_ccy={spec.pnl_ccy} "
                  f"point={spec.point_size} min_qty={spec.min_qty}")
        return 0
    except BrokerError as exc:
        print(f"StoneX probe failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        await sx.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only StoneX connectivity probe")
    parser.add_argument("--instruments", type=int, default=5, help="how many instruments to print")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    settings = StoneXSettings.from_env()
    print(repr(settings))   # repr hides the password and app key
    return asyncio.run(run(settings, max_instruments=args.instruments))


if __name__ == "__main__":
    raise SystemExit(main())
