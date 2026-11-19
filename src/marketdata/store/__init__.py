"""The `redis-md` store: key names, the Lua write scripts, the script runner and the two clients (design §A2, §A3,
§A7, §A8, §A13; plan S13).

This is the only part of `src/marketdata` that may import `redis` (lockout, design §B3). Every write to `redis-md`
goes through one of the scripts in `scripts.py`: an ordinary write needs the writer token, the exact `md:boot`
sentinel and the connection's `run_id`; a lease write needs the token only; a bootstrap write needs the token and the
boot lock. Nothing here logs a price, a key, a URL or a Redis error message (`safe_redis_error`).
"""
