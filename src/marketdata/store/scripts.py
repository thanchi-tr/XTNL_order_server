"""The `redis-md` Lua write scripts, as `Final` strings, and their argument builders (design §A3, §A7, §A8, §A13;
plan S13, ST-7).

The scripts live here as constants, not as files under `src/cache/lua` (`src.cache` is refused by the lockout and
this package reads no files). Each script checks everything it needs **before its first write**, so a refused or
malformed call changes nothing (a Lua error does not roll back the writes made before it). That includes the stored
state a script compares or increments (`md:meta` `first_bucket`, `last_bucket`, `write_seq`, `quarantined`): pass 1
reads it and refuses `BAD_ARGS meta_state` unless it is absent or digits, and every number in ARGV is bounded
(times and bucket starts at most 13 digits, `rollback_n` at most 3, `write_seq` at most 14, which Lua's `tostring`
still prints as an integer), so pass 2 cannot fail on a value it was given or found.

| Script | Guard | Writes |
|---|---|---|
| `FLUSH` | token, exact `md:boot`, `run_id` in ARGV | ticks (explicit ids, monotonic guard), forming bars, closed buckets (`ZREMRANGEBYSCORE` + `ZADD`, superseded members to `md:bars_rev`), the rollback set `md:rb:{series}` scored by the per-series `write_seq` (`HINCRBY md:meta write_seq`) and trimmed to N, `md:meta`, `XADD md:events MAXLEN ~` |
| `LEASE` | token only | `md:demand:*` (SET PX, or DEL for a release), the registry entry and the lease index `md:registry:leases` (S16) |
| `BOOT_LOCK` / `RENEW` / `RELEASE` | token (lock value for RELEASE) | `md:boot_lock` |
| `BOOT_WRITE` | token and boot lock | bootstrap step 3 / 4: forming cleared, `md:meta` fields, rollback entries restored (`STALE_SEQ` on a conflicting entry, stored or in the same call), registry, expired lease-index entries pruned |
| `FINISH` | token, boot lock, sentinel `run_id` = the connection's (`RUN_ID` in ARGV, else `NO_BOOT`) | `md_reset` event, then `md:boot` (the last data write), then the lock released |
| `QUARANTINE` | token, exact `md:boot`, `run_id` | `md:quarantine:{series}` (check name and record), the member removed from `md:bars` |
| `INDEX` | token only | `md:registry:stored`: the series and tick streams the writer has written (S16; ids only) |
| `RETAIN` | token, exact `md:boot`, `run_id` | retention and memory shedding of one symbol (S15): `XTRIM MINID` then `XTRIM MAXLEN` on `md:ticks:{adapter}:{symbol}`, `ZREMRANGEBYSCORE` below a floor on its TICK bar series (never BROKER: the builder refuses them), `md:meta` `first_bucket` moved to the oldest kept bucket |
| `RETAIN_BROKER` | token, exact `md:boot`, `run_id` | BROKER retention (S38e, design §A5): per stored BROKER series, demanded or not, `ZREMRANGEBYSCORE` below its floor (1 year, or the widest declared backfill), `md:bars_rev` and `md:quarantine` below 7 days, `md:meta` `first_bucket` moved; never the rollback set or `md:events` |
| `IND` | token, exact `md:boot`, `run_id` | one indicator bucket (S18): each output's `md:ind:{id}:{output}` member replaced (`ZREMRANGEBYSCORE s s` + `ZADD`) and trimmed below the value floor, the checkpoint `md:ind_ckpt:{id}` (optional) trimmed below 30 days, the `md:registry:ind` definition (optional), then `indicator_updated` (id and bar end only) on `md:events` |

Refusals are error replies whose first token is a code: `NO_WRITER` (another writer's token: a fenced ex-leader),
`NO_BOOT` (missing, older, foreign sentinel or a `run_id` mismatch), `NO_LOCK`, `BAD_ARGS <field>` (never a value),
`STALE_SEQ`. They are on `errors.REDIS_ERROR_CODES`, so `safe_redis_error` reports them.

**Rollback-set member** = `<write_seq:20>|<bar_end_ms:13>|<bar member>` (the bar member carries bucket, revision,
record, record digest and content hash, `codec`). A revision of an already acknowledged bar is a new member with a new,
higher `write_seq` (R11-M16). An identical re-write (the same member: the idempotent re-write after a `WAITAOF`
timeout) changes nothing and takes no `write_seq`. A silent re-write (`md_rehash`, §A13) replaces the member but adds
no event and no rollback entry; a rollback entry holding the replaced member is rewritten in place (same `write_seq`
and bar end, the new member).

**Superseded member** in `md:bars_rev:{series}` = `<bar member>|<superseded_at_ms:13>`, score = bucket start.
"""
from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Final

from src.marketdata.codec import encode_member, encode_record
from src.marketdata.hashing import record_digest
from src.marketdata.store import keys as K
from src.marketdata.types import Bar, MdGeneration, SeriesId, Source

# ---------------------------------------------------------------------------------------------------- Lua
_PRELUDE: Final = r"""
local function digits(s) return type(s) == 'string' and string.find(s, '^[0-9]+$') ~= nil end
local function uint(s, n) return digits(s) and string.len(s) <= n end
local function stored(v, n) return v == false or v == nil or uint(v, n) end
local function word(s) return type(s) == 'string' and string.find(s, '^[%w_]+$') ~= nil end
local function price(s) return type(s) == 'string' and string.find(s, '^[0-9]+%.?[0-9]*$') ~= nil end
local function hexn(s, n) return type(s) == 'string' and string.len(s) == n and string.find(s, '^[0-9a-f]+$') ~= nil end
local function series_id(s) return type(s) == 'string' and string.find(s, '^[%w%._/#:%-]+$') ~= nil end
local function record(s) return type(s) == 'string' and string.len(s) == 87 and string.find(s, '^[0-9CPDNB]+$') ~= nil end
local function member(s, start)
  if type(s) ~= 'string' or string.len(s) ~= 149 then return false end
  if string.find(s, '^[0-9]+|[0-9CPDNB]+|[0-9a-f]+|[0-9a-f]+$') == nil then return false end
  return tonumber(string.sub(s, 1, 13)) == tonumber(start)
end
local function pad(s, n) s = tostring(s); return string.rep('0', n - string.len(s)) .. s end
local function bad(what) return redis.error_reply('BAD_ARGS ' .. what) end
local function writer_ok(key, token) return token ~= nil and token ~= '' and redis.call('GET', key) == token end
local function boot_ok(key, boot, run_id)
  if type(run_id) ~= 'string' or run_id == '' or type(boot) ~= 'string' then return false end
  if redis.call('GET', key) ~= boot then return false end
  local p = string.find(boot, ':', 1, true)
  return p ~= nil and string.sub(boot, 1, p - 1) == run_id
end
local function lock_ok(key, value) return value ~= nil and value ~= '' and redis.call('GET', key) == value end
"""

FLUSH: Final = _PRELUDE + r"""
-- KEYS[1] md:writer  KEYS[2] md:boot  KEYS[3] md:events  KEYS[4] md:ticks:{adapter}:{symbol}
-- KEYS[5..] per series: bars, bars_rev, forming, meta, rb
-- ARGV: token, boot, run_id, now_ms, events_maxlen, rollback_n, rev_retention_ms,
--       tick_count, ticks (received_ms, bid, ask, broker_ms, ts_source, status) ...,
--       series_count, per series: series, forming_mode (K|D|S), forming_start, forming_record, forming_digest,
--       closed_count, closed (bucket_start, member, bar_end_ms, content_hash, emit E|-) ..., meta_count, pairs ...
-- Returns {ticks_added, backward_steps, outcome_1, write_seq_1, ...}; outcome 0 unchanged, 1 closed, 2 revised,
-- 3 silent re-write.
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if not boot_ok(KEYS[2], ARGV[2], ARGV[3]) then return redis.error_reply('NO_BOOT') end
local now, maxlen, rb_n, rev_keep = ARGV[4], ARGV[5], ARGV[6], ARGV[7]
if not (uint(now, 13) and uint(maxlen, 13) and uint(rb_n, 3) and uint(rev_keep, 13)) then return bad('header') end
if tonumber(rb_n) < 1 then return bad('rollback_n') end
local i = 8
local function take() local v = ARGV[i]; i = i + 1; return v end
local reserved = {write_seq = true, last_write_ms = true, first_bucket = true, last_bucket = true, quarantined = true}

-- pass 1: parse and check everything; nothing is written before this pass ends
local nt = take()
if not digits(nt) then return bad('tick_count') end
local ticks = {}
for t = 1, tonumber(nt) do
  local r, b, a, bt, src, st = take(), take(), take(), take(), take(), take()
  if not (digits(r) and price(b) and price(a) and digits(bt) and word(src) and word(st)) then return bad('tick') end
  if string.len(r) > 15 or tonumber(r) < 1 then return bad('tick') end
  ticks[t] = {r, b, a, bt, src, st}
end
local ng = take()
if not digits(ng) then return bad('series_count') end
ng = tonumber(ng)
if #KEYS ~= 4 + 5 * ng then return bad('keys') end
local groups = {}
for g = 1, ng do
  local grp = {series = take(), fmode = take(), fstart = take(), frec = take(), fdig = take(), closed = {}, meta = {}}
  if not series_id(grp.series) then return bad('series') end
  -- the stored md:meta numbers pass 2 compares and increments: a corrupted field refuses here, not after a write
  local held = redis.call('HMGET', KEYS[4 + 5 * (g - 1) + 4], 'first_bucket', 'last_bucket', 'write_seq')
  if not (stored(held[1], 13) and stored(held[2], 13) and stored(held[3], 14)) then return bad('meta_state') end
  if grp.fmode == 'S' then
    if not (uint(grp.fstart, 13) and record(grp.frec) and hexn(grp.fdig, 8)) then return bad('forming') end
  elseif grp.fmode ~= 'K' and grp.fmode ~= 'D' then
    return bad('forming_mode')
  end
  local nc = take()
  if not digits(nc) then return bad('closed_count') end
  for c = 1, tonumber(nc) do
    local bar = {start = take(), member = take(), bar_end = take(), hash = take(), emit = take()}
    if not (uint(bar.start, 13) and uint(bar.bar_end, 13) and member(bar.member, bar.start) and hexn(bar.hash, 34)) then
      return bad('closed')
    end
    if string.sub(bar.member, -34) ~= bar.hash or (bar.emit ~= 'E' and bar.emit ~= '-') then return bad('closed') end
    grp.closed[c] = bar
  end
  local nm = take()
  if not digits(nm) then return bad('meta_count') end
  for m = 1, tonumber(nm) do
    local f, v = take(), take()
    if not word(f) or reserved[f] or type(v) ~= 'string' then return bad('meta') end
    grp.meta[m] = {f, v}
  end
  groups[g] = grp
end
if i ~= #ARGV + 1 then return bad('argc') end

-- pass 2: writes
local added, backward = 0, 0
if #ticks > 0 then
  local last_ms, last_seq = nil, 0
  if redis.call('EXISTS', KEYS[4]) == 1 then
    local info = redis.call('XINFO', 'STREAM', KEYS[4])
    for j = 1, #info, 2 do
      if info[j] == 'last-generated-id' then
        local id = info[j + 1]
        local d = string.find(id, '-', 1, true)
        last_ms, last_seq = string.sub(id, 1, d - 1), tonumber(string.sub(id, d + 1))
      end
    end
  end
  for _, t in ipairs(ticks) do
    local r = t[1]
    if last_ms == nil or tonumber(r) > tonumber(last_ms) then
      last_ms, last_seq = r, 0
    else
      if tonumber(r) < tonumber(last_ms) then backward = backward + 1 end
      last_seq = last_seq + 1
    end
    redis.call('XADD', KEYS[4], last_ms .. '-' .. tostring(last_seq), 'b', t[2], 'a', t[3], 'bt', t[4],
               'src', t[5], 'st', t[6])
    added = added + 1
  end
end
local out = {added, backward}
local floor_rev = tostring(tonumber(now) - tonumber(rev_keep))
for g, grp in ipairs(groups) do
  local base = 4 + 5 * (g - 1)
  local bars_key, rev_key, forming_key, meta_key, rb_key = KEYS[base + 1], KEYS[base + 2], KEYS[base + 3],
      KEYS[base + 4], KEYS[base + 5]
  if grp.fmode == 'S' then
    redis.call('DEL', forming_key)
    redis.call('HSET', forming_key, 'start', grp.fstart, 'rec', grp.frec, 'digest', grp.fdig, 'at', now)
  elseif grp.fmode == 'D' then
    redis.call('DEL', forming_key)
  end
  for _, bar in ipairs(grp.closed) do
    local existing = redis.call('ZRANGEBYSCORE', bars_key, bar.start, bar.start)
    if #existing == 1 and existing[1] == bar.member then
      table.insert(out, 0); table.insert(out, 0)
    else
      for _, old in ipairs(existing) do redis.call('ZADD', rev_key, bar.start, old .. '|' .. pad(now, 13)) end
      redis.call('ZREMRANGEBYSCORE', bars_key, bar.start, bar.start)
      redis.call('ZADD', bars_key, bar.start, bar.member)
      local first = redis.call('HGET', meta_key, 'first_bucket')
      if not first or tonumber(bar.start) < tonumber(first) then redis.call('HSET', meta_key, 'first_bucket', bar.start) end
      local last = redis.call('HGET', meta_key, 'last_bucket')
      if not last or tonumber(bar.start) > tonumber(last) then redis.call('HSET', meta_key, 'last_bucket', bar.start) end
      if bar.emit == '-' then
        -- a silent re-write (md_rehash): rollback entries holding the replaced member keep their write_seq and bar
        -- end but carry the new member, so no entry is left under a key id that will be retired
        if #existing == 1 then
          local rb = redis.call('ZRANGE', rb_key, 0, -1, 'WITHSCORES')
          for j = 1, #rb, 2 do
            if string.sub(rb[j], 36) == existing[1] then
              redis.call('ZREM', rb_key, rb[j])
              redis.call('ZADD', rb_key, rb[j + 1], string.sub(rb[j], 1, 35) .. bar.member)
            end
          end
        end
        table.insert(out, 3); table.insert(out, 0)
      else
        local kind, outcome = 'bar_closed', 1
        if #existing > 0 then kind, outcome = 'bar_revised', 2 end
        local ws = redis.call('HINCRBY', meta_key, 'write_seq', 1)
        redis.call('ZADD', rb_key, ws, pad(ws, 20) .. '|' .. pad(bar.bar_end, 13) .. '|' .. bar.member)
        redis.call('XADD', KEYS[3], 'MAXLEN', '~', maxlen, '*', 'k', kind, 's', grp.series, 'e', bar.bar_end,
                   'h', bar.hash, 'w', tostring(ws))
        table.insert(out, outcome); table.insert(out, ws)
      end
    end
  end
  redis.call('ZREMRANGEBYRANK', rb_key, 0, -(tonumber(rb_n) + 1))
  redis.call('ZREMRANGEBYSCORE', rev_key, '-inf', '(' .. floor_rev)
  redis.call('HSET', meta_key, 'last_write_ms', now)
  for _, pair in ipairs(grp.meta) do redis.call('HSET', meta_key, pair[1], pair[2]) end
end
return out
"""

LEASE: Final = _PRELUDE + r"""
-- KEYS[1] md:writer  KEYS[2] md:demand:{consumer}:{spec_id}  KEYS[3] registry hash  KEYS[4] md:registry:leases
-- ARGV: token, ttl_ms ('0' = release), lease value, registry field ('' = none), registry value, lease index field
-- Lease writes check only the writer token, never md:boot (design §A7), so a re-assertion lands at once. The lease
-- index (consumer:spec_id) lets a bootstrap find every live lease in the store without KEYS / SCAN (S16).
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if #KEYS ~= 4 or not digits(ARGV[2]) or type(ARGV[3]) ~= 'string' or type(ARGV[4]) ~= 'string'
   or type(ARGV[5]) ~= 'string' or #ARGV ~= 6 then
  return bad('lease')
end
if ARGV[4] ~= '' and not word(ARGV[4]) then return bad('registry') end
if not series_id(ARGV[6]) then return bad('lease_index') end
if tonumber(ARGV[2]) == 0 then
  redis.call('DEL', KEYS[2])
  redis.call('HDEL', KEYS[4], ARGV[6])
  return 0
end
redis.call('SET', KEYS[2], ARGV[3], 'PX', ARGV[2])
redis.call('HSET', KEYS[4], ARGV[6], ARGV[3])
if ARGV[4] ~= '' then redis.call('HSET', KEYS[3], ARGV[4], ARGV[5]) end
return 1
"""

BOOT_LOCK: Final = _PRELUDE + r"""
-- KEYS[1] md:writer  KEYS[2] md:boot_lock ; ARGV: token, lock value, px. Returns 1 (held by this caller) or 0.
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if not word(ARGV[2]) or not digits(ARGV[3]) then return bad('lock') end
local cur = redis.call('GET', KEYS[2])
if cur == ARGV[2] then
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
  return 1
end
if redis.call('SET', KEYS[2], ARGV[2], 'NX', 'PX', ARGV[3]) then return 1 end
return 0
"""

RENEW: Final = _PRELUDE + r"""
-- KEYS[1] md:writer  KEYS[2] md:boot_lock ; ARGV: token, lock value, px.
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if not digits(ARGV[3]) then return bad('lock') end
if not lock_ok(KEYS[2], ARGV[2]) then return redis.error_reply('NO_LOCK') end
redis.call('PEXPIRE', KEYS[2], ARGV[3])
return 1
"""

RELEASE: Final = _PRELUDE + r"""
-- KEYS[1] md:boot_lock ; ARGV: lock value. Returns 1 when this caller's lock was released, else 0.
if lock_ok(KEYS[1], ARGV[1]) then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
"""

BOOT_WRITE: Final = _PRELUDE + r"""
-- Bootstrap steps 3 / 4 (design §A7): under the token and the boot lock, never md:boot.
-- KEYS[1] md:writer  KEYS[2] md:boot_lock  KEYS[3] md:registry:series  KEYS[4] md:registry:leases
-- KEYS[5 .. 4+np] md:demand:* of the lease-index entries to prune ; then per series: forming, meta, rb
-- ARGV: token, lock value, rollback_n, registry_count, pairs ..., prune_count, lease index fields ..., series_count,
--       per series: clear_forming (1|0), meta_count, pairs ..., entry_count, entries (write_seq, member) ...
-- A pruned lease-index field is removed only when its md:demand key is still gone (checked here, not by the caller).
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if not lock_ok(KEYS[2], ARGV[2]) then return redis.error_reply('NO_LOCK') end
if not uint(ARGV[3], 3) or tonumber(ARGV[3]) < 1 then return bad('rollback_n') end
local rb_n = tonumber(ARGV[3])
local i = 4
local function take() local v = ARGV[i]; i = i + 1; return v end
local nr = take()
if not digits(nr) then return bad('registry_count') end
local registry = {}
for r = 1, tonumber(nr) do
  local f, v = take(), take()
  if not series_id(f) or type(v) ~= 'string' then return bad('registry') end
  registry[r] = {f, v}
end
local np = take()
if not digits(np) or string.len(np) > 4 then return bad('prune_count') end
np = tonumber(np)
local prune = {}
for p = 1, np do
  local f = take()
  if not series_id(f) then return bad('prune') end
  prune[p] = f
end
local kb = 4 + np
local ng = take()
if not digits(ng) then return bad('series_count') end
ng = tonumber(ng)
if #KEYS ~= kb + 3 * ng then return bad('keys') end
local numeric = {first_bucket = 13, last_bucket = 13, last_write_ms = 13, quarantined = 14}
local groups = {}
for g = 1, ng do
  local meta_key, rb_key = KEYS[kb + 3 * (g - 1) + 2], KEYS[kb + 3 * (g - 1) + 3]
  if not stored(redis.call('HGET', meta_key, 'write_seq'), 14) then return bad('meta_state') end
  local grp = {clear = take(), meta = {}, entries = {}}
  if grp.clear ~= '1' and grp.clear ~= '0' then return bad('clear_forming') end
  local nm = take()
  if not digits(nm) then return bad('meta_count') end
  for m = 1, tonumber(nm) do
    local f, v = take(), take()
    if not word(f) or f == 'write_seq' or type(v) ~= 'string' then return bad('meta') end
    if numeric[f] and not uint(v, numeric[f]) then return bad('meta') end
    grp.meta[m] = {f, v}
  end
  local ne = take()
  if not digits(ne) then return bad('entry_count') end
  local seen = {}
  for e = 1, tonumber(ne) do
    local ws, m = take(), take()
    if not uint(ws, 14) or tonumber(ws) < 1 or type(m) ~= 'string' or string.len(m) ~= 184
       or string.sub(m, 1, 20) ~= pad(ws, 20) or string.sub(m, 21, 21) ~= '|' or not uint(string.sub(m, 22, 34), 13)
       or string.sub(m, 35, 35) ~= '|' or not member(string.sub(m, 36), string.sub(m, 36, 48)) then
      return bad('entry')
    end
    local n = tonumber(ws)
    if seen[n] ~= nil and seen[n] ~= m then return redis.error_reply('STALE_SEQ') end
    seen[n] = m
    local held = redis.call('ZRANGEBYSCORE', rb_key, ws, ws)
    if #held > 1 or (#held == 1 and held[1] ~= m) then return redis.error_reply('STALE_SEQ') end
    grp.entries[e] = {ws, m}
  end
  groups[g] = grp
end
if i ~= #ARGV + 1 then return bad('argc') end
for _, pair in ipairs(registry) do redis.call('HSET', KEYS[3], pair[1], pair[2]) end
for p, f in ipairs(prune) do
  if redis.call('EXISTS', KEYS[4 + p]) == 0 then redis.call('HDEL', KEYS[4], f) end
end
for g, grp in ipairs(groups) do
  local base = kb + 3 * (g - 1)
  local forming_key, meta_key, rb_key = KEYS[base + 1], KEYS[base + 2], KEYS[base + 3]
  if grp.clear == '1' then redis.call('DEL', forming_key) end
  for _, pair in ipairs(grp.meta) do redis.call('HSET', meta_key, pair[1], pair[2]) end
  local top = tonumber(redis.call('HGET', meta_key, 'write_seq') or '0')
  for _, e in ipairs(grp.entries) do
    redis.call('ZADD', rb_key, e[1], e[2])
    if tonumber(e[1]) > top then top = tonumber(e[1]) end
  end
  redis.call('HSET', meta_key, 'write_seq', tostring(top))
  redis.call('ZREMRANGEBYRANK', rb_key, 0, -(rb_n + 1))
end
return 'OK'
"""

FINISH: Final = _PRELUDE + r"""
-- Bootstrap step 5 (design §A7): md_reset event, then md:boot written last, then the lock released.
-- KEYS[1] md:writer  KEYS[2] md:boot_lock  KEYS[3] md:boot  KEYS[4] md:events
-- ARGV: token, lock value, sentinel, generation, class, events_maxlen, run_id (the connection's, at send time)
-- A sentinel whose run_id is not this connection's (Redis restarted after the call was built) gives NO_BOOT.
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if not lock_ok(KEYS[2], ARGV[2]) then return redis.error_reply('NO_LOCK') end
local s, gen, class, maxlen, run_id = ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7]
if type(s) ~= 'string' or string.find(s, '^[%w]+:[%w%._%-]+:[%w%._%-]+:[0-9]+%.[0-9]+$') == nil then
  return bad('sentinel')
end
if type(gen) ~= 'string' or string.sub(s, -string.len(gen) - 1) ~= ':' .. gen then return bad('generation') end
if not word(class) or not uint(maxlen, 13) or #ARGV ~= 7 then return bad('finish') end
if not word(run_id) or string.sub(s, 1, string.find(s, ':', 1, true) - 1) ~= run_id then
  return redis.error_reply('NO_BOOT')
end
redis.call('XADD', KEYS[4], 'MAXLEN', '~', maxlen, '*', 'k', 'md_reset', 'g', gen, 'c', class)
redis.call('SET', KEYS[3], s)
redis.call('DEL', KEYS[2])
return 'OK'
"""

QUARANTINE: Final = _PRELUDE + r"""
-- KEYS[1] md:writer  KEYS[2] md:boot  KEYS[3] md:bars:{series}  KEYS[4] md:quarantine:{series}  KEYS[5] md:meta:{series}
-- ARGV: token, boot, run_id, now_ms, retention_ms, count, entries (bucket_start, check, record, remove_member) ...
-- Quarantine member = <now:13>|<check>|<bucket_start:13>|<record>, score = bucket start (redis-md only, never PostgreSQL).
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if not boot_ok(KEYS[2], ARGV[2], ARGV[3]) then return redis.error_reply('NO_BOOT') end
local now, keep, n = ARGV[4], ARGV[5], ARGV[6]
if not (uint(now, 13) and uint(keep, 13) and digits(n)) or #KEYS ~= 5 then return bad('header') end
if not stored(redis.call('HGET', KEYS[5], 'quarantined'), 14) then return bad('meta_state') end
n = tonumber(n)
if #ARGV ~= 6 + 4 * n then return bad('argc') end
local entries = {}
for e = 1, n do
  local j = 6 + 4 * (e - 1)
  local start, check, rec, remove = ARGV[j + 1], ARGV[j + 2], ARGV[j + 3], ARGV[j + 4]
  if not (uint(start, 13) and word(check) and record(rec)) then return bad('entry') end
  if remove ~= '' and not member(remove, start) then return bad('member') end
  entries[e] = {start, check, rec, remove}
end
for _, e in ipairs(entries) do
  if e[4] ~= '' then redis.call('ZREM', KEYS[3], e[4]) end
  redis.call('ZADD', KEYS[4], e[1], pad(now, 13) .. '|' .. e[2] .. '|' .. pad(e[1], 13) .. '|' .. e[3])
end
if n > 0 then redis.call('HINCRBY', KEYS[5], 'quarantined', n) end
redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', '(' .. tostring(tonumber(now) - tonumber(keep)))
return n
"""

RETAIN: Final = _PRELUDE + r"""
-- KEYS[1] md:writer  KEYS[2] md:boot  KEYS[3] md:ticks:{adapter}:{symbol}
-- KEYS[4..] per TICK series: bars, meta
-- ARGV: token, boot, run_id, tick_minid_ms, tick_maxlen, series_count, floor (bucket start kept from) ...
-- Returns {ticks_trimmed, bars_removed_1, ...}. Retention and memory shedding only: never a BROKER series (the
-- builder refuses them), never md:bars_rev, md:rb or md:events.
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if not boot_ok(KEYS[2], ARGV[2], ARGV[3]) then return redis.error_reply('NO_BOOT') end
local minid, maxlen, n = ARGV[4], ARGV[5], ARGV[6]
if not (uint(minid, 13) and uint(maxlen, 13) and digits(n)) then return bad('header') end
n = tonumber(n)
if #KEYS ~= 3 + 2 * n then return bad('keys') end
if #ARGV ~= 6 + n then return bad('argc') end
for s = 1, n do
  if not uint(ARGV[6 + s], 13) then return bad('floor') end
  if not stored(redis.call('HGET', KEYS[3 + 2 * s], 'first_bucket'), 13) then return bad('meta_state') end
end
local out = {0}
if redis.call('EXISTS', KEYS[3]) == 1 then
  out[1] = redis.call('XTRIM', KEYS[3], 'MINID', minid) + redis.call('XTRIM', KEYS[3], 'MAXLEN', maxlen)
end
for s = 1, n do
  local bars_key, meta_key = KEYS[2 + 2 * s], KEYS[3 + 2 * s]
  local removed = redis.call('ZREMRANGEBYSCORE', bars_key, '-inf', '(' .. ARGV[6 + s])
  if removed > 0 then
    local first = redis.call('ZRANGE', bars_key, 0, 0, 'WITHSCORES')
    if #first == 2 then redis.call('HSET', meta_key, 'first_bucket', first[2])
    else redis.call('HDEL', meta_key, 'first_bucket') end
  end
  table.insert(out, removed)
end
return out
"""

RETAIN_BROKER: Final = _PRELUDE + r"""
-- KEYS[1] md:writer  KEYS[2] md:boot  KEYS[3..] per BROKER series: bars, bars_rev, quarantine, meta
-- ARGV: token, boot, run_id, now_ms, rev_retention_ms, series_count, floor (bucket start kept from) ...
-- Returns {bars_removed_1, rev_removed_1, quarantine_removed_1, ...}. BROKER retention (design §A5, plan S38e): bars
-- below the floor (1 year, or the widest declared backfill when longer), superseded members and quarantine entries
-- older than rev_retention_ms (by bucket start), for every stored series, demanded or not. Never md:rb or md:events.
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if not boot_ok(KEYS[2], ARGV[2], ARGV[3]) then return redis.error_reply('NO_BOOT') end
local now, keep, n = ARGV[4], ARGV[5], ARGV[6]
if not (uint(now, 13) and uint(keep, 13) and digits(n)) then return bad('header') end
n = tonumber(n)
if #KEYS ~= 2 + 4 * n then return bad('keys') end
if #ARGV ~= 6 + n then return bad('argc') end
for s = 1, n do
  if not uint(ARGV[6 + s], 13) then return bad('floor') end
  if not stored(redis.call('HGET', KEYS[2 + 4 * s], 'first_bucket'), 13) then return bad('meta_state') end
end
local rev_floor = tonumber(now) - tonumber(keep)
local out = {}
for s = 1, n do
  local base = 2 + 4 * (s - 1)
  local bars_key, rev_key, meta_key = KEYS[base + 1], KEYS[base + 2], KEYS[base + 4]
  local removed = redis.call('ZREMRANGEBYSCORE', bars_key, '-inf', '(' .. ARGV[6 + s])
  if removed > 0 then
    local first = redis.call('ZRANGE', bars_key, 0, 0, 'WITHSCORES')
    if #first == 2 then redis.call('HSET', meta_key, 'first_bucket', first[2])
    else redis.call('HDEL', meta_key, 'first_bucket') end
  end
  table.insert(out, removed)
  table.insert(out, redis.call('ZREMRANGEBYSCORE', rev_key, '-inf', '(' .. tostring(rev_floor)))
  table.insert(out, redis.call('ZREMRANGEBYSCORE', KEYS[base + 3], '-inf', '(' .. tostring(rev_floor)))
end
return out
"""

INDEX: Final = _PRELUDE + r"""
-- KEYS[1] md:writer  KEYS[2] md:registry:stored ; ARGV: token, field ... (series ids or adapter:symbol)
-- Token only, like LEASE: an index entry says "this series / tick stream was written", never a value (S16).
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if #KEYS ~= 2 or #ARGV < 2 then return bad('index') end
for j = 2, #ARGV do
  if not series_id(ARGV[j]) then return bad('index') end
end
for j = 2, #ARGV do redis.call('HSET', KEYS[2], ARGV[j], '1') end
return #ARGV - 1
"""

IND: Final = _PRELUDE + r"""
-- KEYS[1] md:writer  KEYS[2] md:boot  KEYS[3] md:events  KEYS[4] md:ind_ckpt:{id}  KEYS[5] md:registry:ind
-- KEYS[6..] md:ind:{id}:{output}, one per output
-- ARGV: token, boot, run_id, events_maxlen, indicator_id, bucket_start, bar_end, value_floor, checkpoint_floor,
--       checkpoint member ('' = none), definition ('' = none), emit (E|-), value members ... (KEYS order)
-- Returns the number of values written. indicator_updated (id and bar end only, never a value) is added after the
-- value writes (design §A6 event order: the bar with bar_closed first, then the value, then indicator_updated).
if not writer_ok(KEYS[1], ARGV[1]) then return redis.error_reply('NO_WRITER') end
if not boot_ok(KEYS[2], ARGV[2], ARGV[3]) then return redis.error_reply('NO_BOOT') end
local maxlen, iid, b, e, vfloor, cfloor = ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9]
local ckpt, def, emit = ARGV[10], ARGV[11], ARGV[12]
if not (uint(maxlen, 13) and hexn(iid, 16) and uint(b, 13) and uint(e, 13) and uint(vfloor, 13)
        and uint(cfloor, 13)) then
  return bad('header')
end
if emit ~= 'E' and emit ~= '-' then return bad('emit') end
local n = #KEYS - 5
if n < 1 or #ARGV ~= 12 + n then return bad('argc') end
local prefix = pad(b, 13) .. '|'
for j = 1, n do
  local m = ARGV[12 + j]
  if type(m) ~= 'string' or string.len(m) > 160 or string.sub(m, 1, 14) ~= prefix
     or string.find(m, '^[0-9]+|[%w%.%-|_]+$') == nil then
    return bad('member')
  end
end
if type(ckpt) ~= 'string' or (ckpt ~= '' and (string.len(ckpt) > 4096 or string.sub(ckpt, 1, 14) ~= prefix)) then
  return bad('checkpoint')
end
if type(def) ~= 'string' or string.len(def) > 1024 then return bad('definition') end
for j = 1, n do
  local key = KEYS[5 + j]
  redis.call('ZREMRANGEBYSCORE', key, b, b)
  redis.call('ZADD', key, b, ARGV[12 + j])
  redis.call('ZREMRANGEBYSCORE', key, '-inf', '(' .. vfloor)
end
if ckpt ~= '' then
  redis.call('ZREMRANGEBYSCORE', KEYS[4], b, b)
  redis.call('ZADD', KEYS[4], b, ckpt)
end
redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', '(' .. cfloor)
if def ~= '' then redis.call('HSET', KEYS[5], iid, def) end
if emit == 'E' then
  redis.call('XADD', KEYS[3], 'MAXLEN', '~', maxlen, '*', 'k', 'indicator_updated', 'i', iid, 'e', e)
end
return n
"""


@dataclass(frozen=True)
class Script:
    """A Lua script and its SHA1 (what `SCRIPT LOAD` returns and `EVALSHA` names)."""

    name: str
    source: str = field(repr=False)

    @property
    def sha(self) -> str:
        return hashlib.sha1(self.source.encode("utf-8")).hexdigest()


SCRIPTS: Final = (
    Script("flush", FLUSH), Script("lease", LEASE), Script("boot_lock", BOOT_LOCK), Script("renew", RENEW),
    Script("release", RELEASE), Script("boot_write", BOOT_WRITE), Script("finish", FINISH),
    Script("quarantine", QUARANTINE), Script("retain", RETAIN), Script("index", INDEX),
    Script("ind", IND), Script("retain_broker", RETAIN_BROKER),
)
BY_NAME: Final = {s.name: s for s in SCRIPTS}

SCRIPT_CODES: Final = frozenset({"NO_BOOT", "NO_WRITER", "NO_LOCK", "BAD_ARGS", "STALE_SEQ"})
"""Refusal codes the scripts return (each on `errors.REDIS_ERROR_CODES`)."""

RUN_ID: Final = "\x00run_id\x00"
"""Placeholder in a call's ARGV that the runner replaces with the connection's current `run_id` at send time."""


@dataclass(frozen=True)
class ScriptCall:
    """One `EVALSHA` call. `args` may hold `RUN_ID`. Hidden from `repr`: the arguments carry prices."""

    script: Script
    keys: tuple[str, ...]
    args: tuple[str, ...] = field(repr=False)


# ---------------------------------------------------------------------------------------------------- argument builders
_WORD_RE: Final = re.compile(r"[A-Za-z0-9_]+")
_RESERVED_META: Final = frozenset({"write_seq", "last_write_ms", "first_bucket", "last_bucket", "quarantined"})
_NUMERIC_META: Final = {"first_bucket": 13, "last_bucket": 13, "last_write_ms": 13, "quarantined": 14}
"""`md:meta` fields a bootstrap may set: digits only, at most this many (the scripts compare or increment them)."""
_PRICE_RE: Final = re.compile(r"[0-9]+\.?[0-9]*")
_INDEX_FIELD_RE: Final = re.compile(r"[A-Za-z0-9._/#-]+(:[A-Za-z0-9._/#-]+){1,4}")
ROLLBACK_N_MAX: Final = 64
WRITE_SEQ_MAX: Final = 10**14 - 1
_MAX_RECEIVED_MS: Final = 10**15 - 1
_MAX_BUCKET_MS: Final = 10**13 - 1


def _rollback_n(n: int) -> int:
    if not 1 <= n <= ROLLBACK_N_MAX:
        raise ValueError(f"rollback_n must be 1..{ROLLBACK_N_MAX}")
    return n


def _word(value: str, what: str) -> str:
    if not _WORD_RE.fullmatch(value):
        raise ValueError(f"{what} must match {_WORD_RE.pattern}")
    return value


def _meta_pairs(pairs: Sequence[tuple[str, str]], reserved: frozenset[str]) -> list[str]:
    out = [str(len(pairs))]
    for name, value in pairs:
        if _word(name, "meta field") in reserved:
            raise ValueError(f"meta field {name!r} is written by the script itself")
        limit = _NUMERIC_META.get(name)
        if limit is not None and not (value.isascii() and value.isdigit() and len(value) <= limit):
            raise ValueError(f"meta field {name!r} must be at most {limit} digits")
        out += [name, value]
    return out


@dataclass(frozen=True)
class TickWrite:
    """One tick for `md:ticks:{adapter}:{symbol}` (prices as canonical decimal text, hidden from `repr`). A tick the
    script would refuse raises `ValueError` here, so the caller drops that one tick instead of the whole flush (with
    its closed bars) being refused on every retry."""

    received_ms: int
    bid: str = field(repr=False)
    ask: str = field(repr=False)
    broker_ms: int
    ts_source: str
    status: str

    def __post_init__(self) -> None:
        if not 1 <= self.received_ms <= _MAX_RECEIVED_MS or self.broker_ms < 0:
            raise ValueError("tick times must be positive")
        if not (_PRICE_RE.fullmatch(self.bid) and _PRICE_RE.fullmatch(self.ask)):
            raise ValueError("tick prices must be plain non-negative decimal text")   # never the value itself
        _word(self.ts_source, "ts_source")
        _word(self.status, "status")


@dataclass(frozen=True)
class FormingWrite:
    """The forming bar of a series (bucket start, encoded record, record digest)."""

    bucket_start_ms: int
    record: str = field(repr=False)
    digest: str


CLEAR_FORMING: Final = "D"
KEEP_FORMING: Final = "K"


@dataclass(frozen=True)
class ClosedBarWrite:
    """A closed bar to store with its content hash. `silent` = an `md_rehash` re-write: no event, no rollback entry."""

    bar: Bar
    content_hash: str
    silent: bool = False

    @property
    def member(self) -> str:
        return encode_member(self.bar, self.content_hash)


@dataclass(frozen=True)
class SeriesFlush:
    """Everything one flush writes for one series. `forming`: a bar to store, `CLEAR_FORMING` or `KEEP_FORMING`."""

    series: SeriesId
    forming: FormingWrite | str = KEEP_FORMING
    closed: tuple[ClosedBarWrite, ...] = ()
    meta: tuple[tuple[str, str], ...] = ()


def forming_write(bar: Bar) -> FormingWrite:
    rec = encode_record(bar)
    return FormingWrite(bar.bucket_start_ms, rec, record_digest(f"{bar.bucket_start_ms}|{rec}"))


def flush_call(token: K.WriterToken, boot: K.BootSentinel | str, adapter_id: str, symbol: str, now_ms: int,
               ticks: Sequence[TickWrite], series: Sequence[SeriesFlush], *, rollback_n: int,
               events_maxlen: int = K.EVENTS_MAXLEN, rev_retention_ms: int = K.REV_RETENTION_MS) -> ScriptCall:
    """The `FLUSH` call for one symbol: its ticks plus every series of that symbol with something to write."""
    _rollback_n(rollback_n)
    keys: list[str] = [K.WRITER, K.BOOT, K.EVENTS, K.ticks(adapter_id, symbol)]
    args: list[str] = [token.text, boot if isinstance(boot, str) else boot.text, RUN_ID, str(now_ms),
                       str(events_maxlen), str(rollback_n), str(rev_retention_ms), str(len(ticks))]
    for t in ticks:
        args += [str(t.received_ms), t.bid, t.ask, str(t.broker_ms), _word(t.ts_source, "ts_source"),
                 _word(t.status, "status")]
    args.append(str(len(series)))
    for s in series:
        if s.series.adapter_id != adapter_id or s.series.symbol != symbol:
            raise ValueError("a flush covers one (adapter, symbol)")
        keys += K.series_keys(s.series)
        if isinstance(s.forming, FormingWrite):
            args += [s.series.canonical, "S", str(s.forming.bucket_start_ms), s.forming.record, s.forming.digest]
        elif s.forming in (CLEAR_FORMING, KEEP_FORMING):
            args += [s.series.canonical, s.forming, "", "", ""]
        else:
            raise ValueError("forming must be a FormingWrite, CLEAR_FORMING or KEEP_FORMING")
        args.append(str(len(s.closed)))
        for c in s.closed:
            if c.bar.series != s.series:
                raise ValueError("a closed bar belongs to its own series")
            args += [str(c.bar.bucket_start_ms), c.member, str(c.bar.bucket_end_ms), c.content_hash,
                     "-" if c.silent else "E"]
        args += _meta_pairs(s.meta, _RESERVED_META)
    return ScriptCall(BY_NAME["flush"], tuple(keys), tuple(args))


# Per closed bar outcome in a FLUSH reply.
UNCHANGED: Final = 0
CLOSED: Final = 1
REVISED: Final = 2
SILENT: Final = 3


@dataclass(frozen=True)
class FlushReply:
    ticks_added: int
    backward_steps: int
    bars: tuple[tuple[int, int], ...]
    """Per closed bar in call order: `(outcome, write_seq)`; `write_seq` is 0 unless an entry was added."""


def parse_flush_reply(reply: object) -> FlushReply:
    if not isinstance(reply, list) or len(reply) < 2 or len(reply) % 2:
        raise ValueError("unexpected FLUSH reply shape")
    values = [int(v) for v in reply]
    pairs = tuple((values[j], values[j + 1]) for j in range(2, len(values), 2))
    return FlushReply(values[0], values[1], pairs)


def lease_call(token: K.WriterToken, consumer: str, spec_id: str, ttl_ms: int, value: str, *,
               registry_key: str = K.REGISTRY_SERIES, registry_field: str = "", registry_value: str = "") -> ScriptCall:
    """A demand lease write (`ttl_ms` > 0) or release (`ttl_ms` = 0); token only, never `md:boot`. The lease is also
    entered in (or removed from) the lease index `md:registry:leases` (S16)."""
    if ttl_ms < 0:
        raise ValueError("ttl_ms must not be negative")
    if registry_field:
        _word(registry_field, "registry field")
    if registry_key not in (K.REGISTRY_SERIES, K.REGISTRY_IND):
        raise ValueError("registry key must be md:registry:series or md:registry:ind")
    return ScriptCall(BY_NAME["lease"], (K.WRITER, K.demand(consumer, spec_id), registry_key, K.LEASE_INDEX),
                      (token.text, str(ttl_ms), value, registry_field, registry_value,
                       K.lease_index_field(consumer, spec_id)))


def index_call(token: K.WriterToken, fields: Sequence[str]) -> ScriptCall:
    """`INDEX`: record series ids (`SeriesId.canonical`) and tick streams (`keys.market_field`) as written."""
    if not fields:
        raise ValueError("an index call names at least one field")
    for f in fields:
        if not _INDEX_FIELD_RE.fullmatch(f):
            raise ValueError("an index field is a series id or adapter:symbol")
    return ScriptCall(BY_NAME["index"], (K.WRITER, K.STORED), (token.text, *fields))


def boot_lock_call(token: K.WriterToken, lock_value: str, px: int = K.BOOT_LOCK_PX) -> ScriptCall:
    return ScriptCall(BY_NAME["boot_lock"], (K.WRITER, K.BOOT_LOCK), (token.text, _word(lock_value, "lock"), str(px)))


def renew_call(token: K.WriterToken, lock_value: str, px: int = K.BOOT_LOCK_PX) -> ScriptCall:
    return ScriptCall(BY_NAME["renew"], (K.WRITER, K.BOOT_LOCK), (token.text, lock_value, str(px)))


def release_call(lock_value: str) -> ScriptCall:
    return ScriptCall(BY_NAME["release"], (K.BOOT_LOCK,), (lock_value,))


@dataclass(frozen=True)
class SeriesBoot:
    """Bootstrap writes for one series: clear its forming bar, set `md:meta` fields, restore rollback entries
    (`(write_seq, rollback member)`, hash-checked by the caller)."""

    series: SeriesId
    clear_forming: bool = True
    meta: tuple[tuple[str, str], ...] = ()
    entries: tuple[tuple[int, str], ...] = field(default=(), repr=False)


def rollback_member(write_seq: int, bar_end_ms: int, bar_member: str) -> str:
    """`<write_seq:20>|<bar_end_ms:13>|<bar member>` (the form `FLUSH` writes)."""
    return f"{write_seq:020d}|{bar_end_ms:013d}|{bar_member}"


def boot_write_call(token: K.WriterToken, lock_value: str, series: Sequence[SeriesBoot], *, rollback_n: int,
                    registry: Sequence[tuple[str, str]] = (), prune: Sequence[tuple[str, str]] = ()) -> ScriptCall:
    """Bootstrap step 3 / 4. Send it alone (`ScriptRunner.run_one` / `run_sequential`), never pipelined with `FINISH`:
    a refused `BOOT_WRITE` must stop the bootstrap before `md:boot` is written. `prune`: `(consumer, spec_id)` of
    lease-index entries whose `md:demand:*` key had expired (removed only if it is still gone)."""
    _rollback_n(rollback_n)
    if len(prune) > 9999:
        raise ValueError("at most 9999 lease-index entries are pruned per call")
    keys: list[str] = [K.WRITER, K.BOOT_LOCK, K.REGISTRY_SERIES, K.LEASE_INDEX]
    args: list[str] = [token.text, lock_value, str(rollback_n), str(len(registry))]
    for name, value in registry:
        args += [name, value]
    args.append(str(len(prune)))
    for consumer, spec_id in prune:
        keys.append(K.demand(consumer, spec_id))
        args.append(K.lease_index_field(consumer, spec_id))
    args.append(str(len(series)))
    for s in series:
        keys += [K.forming(s.series), K.meta(s.series), K.rollback(s.series)]
        args.append("1" if s.clear_forming else "0")
        args += _meta_pairs(s.meta, frozenset({"write_seq"}))
        args.append(str(len(s.entries)))
        for ws, m in s.entries:
            if not 1 <= ws <= WRITE_SEQ_MAX:
                raise ValueError(f"write_seq must be 1..{WRITE_SEQ_MAX}")
            args += [str(ws), m]
    return ScriptCall(BY_NAME["boot_write"], tuple(keys), tuple(args))


def finish_call(token: K.WriterToken, lock_value: str, sentinel: K.BootSentinel, reset_class: str, *,
                events_maxlen: int = K.EVENTS_MAXLEN) -> ScriptCall:
    """Bootstrap step 5: the `md_reset(generation, class)` event, then `md:boot`, then the lock released. The
    connection's `run_id` goes in at send time (`RUN_ID`): a sentinel built before a Redis restart gives `NO_BOOT`
    instead of writing an `md:boot` that every later write would refuse."""
    gen: MdGeneration = sentinel.generation
    return ScriptCall(BY_NAME["finish"], (K.WRITER, K.BOOT_LOCK, K.BOOT, K.EVENTS),
                      (token.text, lock_value, sentinel.text, str(gen), _word(reset_class, "reset class"),
                       str(events_maxlen), RUN_ID))


@dataclass(frozen=True)
class QuarantineEntry:
    """A bar that failed a check: its bucket, the check name, the bar and (optionally) the stored member to remove."""

    bar: Bar
    check: str
    remove_member: str = field(default="", repr=False)


def quarantine_call(token: K.WriterToken, boot: K.BootSentinel | str, series: SeriesId, now_ms: int,
                    entries: Sequence[QuarantineEntry], *, retention_ms: int = K.REV_RETENTION_MS) -> ScriptCall:
    args: list[str] = [token.text, boot if isinstance(boot, str) else boot.text, RUN_ID, str(now_ms),
                       str(retention_ms), str(len(entries))]
    for e in entries:
        if e.bar.series != series:
            raise ValueError("a quarantined bar belongs to its own series")
        args += [str(e.bar.bucket_start_ms), _word(e.check, "check"), encode_record(e.bar), e.remove_member]
    return ScriptCall(BY_NAME["quarantine"], (K.WRITER, K.BOOT, K.bars(series), K.quarantine(series), K.meta(series)),
                      tuple(args))


TICK_MAXLEN: Final = 200_000
"""`md:ticks:*` cap besides the 2 h `MINID` retention (design §A5)."""


@dataclass(frozen=True)
class RetainReply:
    ticks_trimmed: int
    bars_removed: tuple[int, ...]
    """Per series floor in call order."""


def retain_call(token: K.WriterToken, boot: K.BootSentinel | str, adapter_id: str, symbol: str, *,
                tick_minid_ms: int, floors: Sequence[tuple[SeriesId, int]],
                tick_maxlen: int = TICK_MAXLEN) -> ScriptCall:
    """The `RETAIN` call for one symbol: ticks older than `tick_minid_ms` (and beyond `tick_maxlen`) and, per TICK
    series, buckets starting before its floor. A BROKER series is refused: BROKER bars, their indicators and the
    rollback sets are never shed (design §A8)."""
    if not 0 <= tick_minid_ms <= _MAX_BUCKET_MS or not 1 <= tick_maxlen <= _MAX_BUCKET_MS:
        raise ValueError("tick retention bounds out of range")
    keys: list[str] = [K.WRITER, K.BOOT, K.ticks(adapter_id, symbol)]
    args: list[str] = [token.text, boot if isinstance(boot, str) else boot.text, RUN_ID, str(tick_minid_ms),
                       str(tick_maxlen), str(len(floors))]
    for series, floor in floors:
        if series.source is not Source.TICK:
            raise ValueError("retention never trims a BROKER series")
        if series.adapter_id != adapter_id or series.symbol != symbol:
            raise ValueError("a retention call covers one (adapter, symbol)")
        if not 0 <= floor <= _MAX_BUCKET_MS:
            raise ValueError("retention floor out of range")
        keys += [K.bars(series), K.meta(series)]
    args += [str(floor) for _, floor in floors]
    return ScriptCall(BY_NAME["retain"], tuple(keys), tuple(args))


def parse_retain_reply(reply: object) -> RetainReply:
    if not isinstance(reply, list) or not reply:
        raise ValueError("unexpected RETAIN reply shape")
    values = [int(v) for v in reply]
    return RetainReply(values[0], tuple(values[1:]))


@dataclass(frozen=True)
class BrokerRetainReply:
    """Per series in call order: `(bars_removed, superseded_removed, quarantine_removed)` (counts only)."""

    removed: tuple[tuple[int, int, int], ...]


def retain_broker_call(token: K.WriterToken, boot: K.BootSentinel | str, now_ms: int,
                       floors: Sequence[tuple[SeriesId, int]], *,
                       rev_retention_ms: int = K.REV_RETENTION_MS) -> ScriptCall:
    """The `RETAIN_BROKER` call (design §A5, plan S38e): per BROKER series, buckets starting before its floor (the
    1-year retention, or the widest declared backfill when longer), and `md:bars_rev` / `md:quarantine` entries older
    than `rev_retention_ms`. A TICK series is refused (TICK retention is `RETAIN`). Never touches the rollback set."""
    if not 0 <= now_ms <= _MAX_BUCKET_MS or not 0 <= rev_retention_ms <= _MAX_BUCKET_MS:
        raise ValueError("broker retention bounds out of range")
    if not floors:
        raise ValueError("a broker retention call names at least one series")
    keys: list[str] = [K.WRITER, K.BOOT]
    for series, floor in floors:
        if series.source is not Source.BROKER:
            raise ValueError("RETAIN_BROKER trims BROKER series only")
        if not 0 <= floor <= _MAX_BUCKET_MS:
            raise ValueError("retention floor out of range")
        keys += [K.bars(series), K.bars_rev(series), K.quarantine(series), K.meta(series)]
    args: list[str] = [token.text, boot if isinstance(boot, str) else boot.text, RUN_ID, str(now_ms),
                       str(rev_retention_ms), str(len(floors))]
    args += [str(floor) for _, floor in floors]
    return ScriptCall(BY_NAME["retain_broker"], tuple(keys), tuple(args))


def parse_retain_broker_reply(reply: object) -> BrokerRetainReply:
    if not isinstance(reply, list) or not reply or len(reply) % 3:
        raise ValueError("unexpected RETAIN_BROKER reply shape")
    values = [int(v) for v in reply]
    return BrokerRetainReply(tuple((values[i], values[i + 1], values[i + 2]) for i in range(0, len(values), 3)))


_IND_ID_RE: Final = re.compile(r"[0-9a-f]{16}")


@dataclass(frozen=True)
class IndicatorWrite:
    """One indicator bucket for `IND`: a member per output (`(output, member)`, values hidden from `repr`), an
    optional checkpoint member and registry definition, and whether to emit `indicator_updated`."""

    indicator_id: str
    bucket_start_ms: int
    bar_end_ms: int
    members: tuple[tuple[str, str], ...] = field(repr=False)
    checkpoint: str = field(default="", repr=False)
    definition: str = field(default="", repr=False)
    emit: bool = True


def indicator_call(token: K.WriterToken, boot: K.BootSentinel | str, w: IndicatorWrite, *, value_floor_ms: int,
                   checkpoint_floor_ms: int, events_maxlen: int = K.EVENTS_MAXLEN) -> ScriptCall:
    """The `IND` call for one indicator bucket (token, exact `md:boot`, the connection's `run_id` at send time)."""
    if not _IND_ID_RE.fullmatch(w.indicator_id):
        raise ValueError("indicator id must be 16 hex digits")
    if not w.members:
        raise ValueError("an indicator write holds at least one output")
    if not 0 <= w.bucket_start_ms <= _MAX_BUCKET_MS or not 0 <= w.bar_end_ms <= _MAX_BUCKET_MS:
        raise ValueError("indicator bucket out of range")
    keys: list[str] = [K.WRITER, K.BOOT, K.EVENTS, K.indicator_checkpoints(w.indicator_id), K.REGISTRY_IND]
    keys += [K.indicator(w.indicator_id, output) for output, _ in w.members]
    floors = [min(max(v, 0), _MAX_BUCKET_MS) for v in (value_floor_ms, checkpoint_floor_ms)]
    args: list[str] = [token.text, boot if isinstance(boot, str) else boot.text, RUN_ID, str(events_maxlen),
                       w.indicator_id, str(w.bucket_start_ms), str(w.bar_end_ms), str(floors[0]), str(floors[1]),
                       w.checkpoint, w.definition, "E" if w.emit else "-"]
    args += [m for _, m in w.members]
    return ScriptCall(BY_NAME["ind"], tuple(keys), tuple(args))
