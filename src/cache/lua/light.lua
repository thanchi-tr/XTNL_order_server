-- light.lua : live keys price:, clean_mid:, account_live:, conn: (tier-persistence.md §1). Lua 5.1.
-- Latest value wins by timestamp; no boot check, no feed entry, no digest.
--
-- KEYS[1] primary key (price:/account_live:/conn:)
-- KEYS[2] clean_mid key (prices only; optional)
-- ARGV[1] mode: 'live' (write when ts >= stored ts) | 'seed' (write only when absent or stored ts older)
-- ARGV[2] ts, fixed-width 16-digit epoch ms (string compare)
-- ARGV[3] PEXPIRE ms for the primary key ('' = none; conn: keys use 30000)
-- ARGV[4] spread in pips ('' = unknown -> never clean)
-- ARGV[5] max clean spread in pips ('' = none -> never clean)
-- ARGV[6] mid ('' = none)
-- ARGV[7] '1' = clean regardless of spread (a seed from last_clean_marks)
-- ARGV[8..] field, value pairs for the primary key
--
-- Returns {primary_written, clean_written} as 0/1.

local mode, ts = ARGV[1], ARGV[2]
if string.find(ts, '[^0-9]') then
    return redis.error_reply('ERR light: ts must be a digit string')
end

local function may_write(k)
    local cur = redis.call('HGET', k, 'ts')
    if not cur then return true end
    if string.len(cur) ~= string.len(ts) then
        return redis.error_reply('ERR light: ts width mismatch')
    end
    if mode == 'seed' then return ts > cur end
    return ts >= cur
end

local wrote, clean = 0, 0

local ok = may_write(KEYS[1])
if type(ok) == 'table' then return ok end
if ok then
    local args = {'ts', ts}
    for i = 8, #ARGV do args[#args + 1] = ARGV[i] end
    redis.call('HSET', KEYS[1], unpack(args))
    if ARGV[3] ~= '' then redis.call('PEXPIRE', KEYS[1], ARGV[3]) end
    wrote = 1
end

if #KEYS >= 2 and ARGV[6] ~= '' then
    local is_clean = ARGV[7] == '1'
    if not is_clean and ARGV[4] ~= '' and ARGV[5] ~= '' then
        -- spreads are not versions: a numeric compare is correct here
        is_clean = tonumber(ARGV[4]) <= tonumber(ARGV[5])
    end
    if is_clean then
        local ok2 = may_write(KEYS[2])
        if type(ok2) == 'table' then return ok2 end
        if ok2 then
            redis.call('HSET', KEYS[2], 'ts', ts, 'mid', ARGV[6], 'spread', ARGV[4], 'seeded',
                       mode == 'seed' and '1' or '0')
            clean = 1
        end
    end
end

return {wrote, clean}
