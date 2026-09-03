local marker = KEYS[2]
if redis.call('EXISTS', marker) == 1 then return {2, redis.call('HGET', marker, 'tokens')} end
local consumed = tonumber(redis.call('HGET', KEYS[1], 'consumed') or '0')
local reserved = tonumber(redis.call('HGET', KEYS[1], 'reserved') or '0')
local active = tonumber(redis.call('HGET', KEYS[1], 'active') or '0')
local limit = tonumber(ARGV[1]); local maxActive = tonumber(ARGV[2]); local requested = tonumber(ARGV[3])
if active >= maxActive then return {0, -1} end
if consumed + reserved + requested > limit then return {0, -2} end
redis.call('HINCRBY', KEYS[1], 'reserved', requested); redis.call('HINCRBY', KEYS[1], 'active', 1)
-- ARGV[4] outlives reservation expires_at so a PENDING row after Lua can still be refunded if Worker was down.
redis.call('HSET', marker, 'tokens', requested, 'state', 'RESERVED'); redis.call('PEXPIRE', marker, ARGV[4])
return {1, requested}
