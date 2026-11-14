-- halt_cas.lua : the ONLY writer of halt:state (tier-persistence.md §1, oversee-rules.md §8). Lua 5.1.
-- Applies (effective_level, halt_event_id) only when the incoming id is greater than the stored one.
--
-- KEYS[1] halt:state
-- ARGV[1] effective_level (NONE | SOFT | HARD)
-- ARGV[2] halt_event_id, fixed-width 20 digits (string compare, never tonumber)
--
-- Returns 'APPLIED' | 'STALE'.

local level, id = ARGV[1], ARGV[2]
if level ~= 'NONE' and level ~= 'SOFT' and level ~= 'HARD' then
    return redis.error_reply('ERR halt_cas: bad level')
end
if string.find(id, '[^0-9]') then
    return redis.error_reply('ERR halt_cas: id must be a digit string')
end
local cur = redis.call('HGET', KEYS[1], 'halt_event_id')
if cur then
    if string.len(cur) ~= string.len(id) then
        return redis.error_reply('ERR halt_cas: id width mismatch')
    end
    if id <= cur then return 'STALE' end
end
redis.call('HSET', KEYS[1], 'effective_level', level, 'halt_event_id', id)
return 'APPLIED'
