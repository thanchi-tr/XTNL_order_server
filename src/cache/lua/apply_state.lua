-- apply_state.lua : the ONLY writer of Redis entity keys (tier-persistence.md §1). Lua 5.1, atomic.
--
-- KEYS[1] entity key (order:/position:/account:/instrument:)
-- KEYS[2] state:digest
-- KEYS[3] oversee:feed
-- KEYS[4] redis:boot_id
-- KEYS[5] redis:boot_lock
-- ARGV[1]  expected boot id (run_id:epoch)
-- ARGV[2]  boot lock token held by the caller ('' = none)
-- ARGV[3]  version, fixed-width digit string (src.dto.version_str)
-- ARGV[4]  seq_id, fixed-width (20 digits) - last tie-breaker
-- ARGV[5]  checksum
-- ARGV[6]  new entity hash, 64 lower-case hex = sha256(key || version || checksum)
-- ARGV[7]  payload JSON
-- ARGV[8]  feed entity_type (ORDER | POSITION | ACCOUNT | INSTRUMENT)
-- ARGV[9]  feed entity_id
-- ARGV[10] account_id
-- ARGV[11] seq_id as written to the feed (plain digits)
-- ARGV[12] feed MAXLEN (approximate trim)
--
-- Returns 'APPLIED' | 'NOOP_ADVANCED' | 'STALE' | 'NO_BOOT'.
-- Versions are compared as fixed-width strings, never with tonumber().

local key, digest_key, feed_key, boot_key, lock_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local version, seq, checksum, new_hash = ARGV[3], ARGV[4], ARGV[5], ARGV[6]

-- 1. Boot check: no boot id (or another term's) means Redis is not trusted yet.
if redis.call('GET', boot_key) ~= ARGV[1] then
    local holder = false
    if ARGV[2] ~= '' then
        holder = (redis.call('GET', lock_key) == ARGV[2])
    end
    if not holder then
        return 'NO_BOOT'
    end
end

if string.len(new_hash) ~= 64 or string.find(new_hash, '[^0-9a-f]') then
    return redis.error_reply('ERR apply_state: entity hash must be 64 lower-case hex')
end
if string.find(version, '[^0-9]') or string.find(seq, '[^0-9]') then
    return redis.error_reply('ERR apply_state: version and seq must be digit strings')
end

-- 2. Version compare (dto-contracts.md §2): key, then checksum, then seq_id.
local cur = redis.call('HMGET', key, 'v', 'seq', 'cs', 'h')
local sv, sseq, scs, sh = cur[1], cur[2], cur[3], cur[4]
local action
if not sv then
    action = 'APPLY'
else
    if string.len(sv) ~= string.len(version) or (sseq and string.len(sseq) ~= string.len(seq)) then
        return redis.error_reply('ERR apply_state: version width mismatch')
    end
    if version > sv then
        if checksum == scs then action = 'ADVANCE' else action = 'APPLY' end
    elseif version < sv then
        return 'STALE'
    elseif checksum == scs then
        return 'NOOP_ADVANCED'       -- identical key and content: nothing to change
    elseif sseq and seq > sseq then
        action = 'APPLY'
    else
        return 'STALE'
    end
end

-- 3. Digest: XOR out the stored hash, XOR in the new one (pure Lua 5.1, no bit library).
local HEX = '0123456789abcdef'
local function nibble_xor(x, y)
    local r, p = 0, 1
    for _ = 1, 4 do
        local a, b = x % 2, y % 2
        if a ~= b then r = r + p end
        x = (x - a) / 2
        y = (y - b) / 2
        p = p * 2
    end
    return r
end
local function xor_hex(a, b)
    local out = {}
    for i = 1, 64 do
        local x = string.find(HEX, string.sub(a, i, i), 1, true) - 1
        local y = string.find(HEX, string.sub(b, i, i), 1, true) - 1
        local z = nibble_xor(x, y) + 1
        out[i] = string.sub(HEX, z, z)
    end
    return table.concat(out)
end
local zero = string.rep('0', 64)
local digest = redis.call('GET', digest_key) or zero
digest = xor_hex(digest, sh or zero)
digest = xor_hex(digest, new_hash)
redis.call('SET', digest_key, digest)

-- 4. Write.
if action == 'ADVANCE' then
    redis.call('HSET', key, 'v', version, 'seq', seq, 'h', new_hash)
    return 'NOOP_ADVANCED'
end

redis.call('HSET', key, 'v', version, 'seq', seq, 'cs', checksum, 'h', new_hash, 'data', ARGV[7],
           'account_id', ARGV[10])

local is_fed = string.sub(key, 1, 6) == 'order:' or string.sub(key, 1, 9) == 'position:'
               or string.sub(key, 1, 8) == 'account:'
if is_fed then
    redis.call('XADD', feed_key, 'MAXLEN', '~', ARGV[12], '*',
               'entity_type', ARGV[8], 'entity_id', ARGV[9], 'account_id', ARGV[10], 'seq_id', ARGV[11])
end
return 'APPLIED'
