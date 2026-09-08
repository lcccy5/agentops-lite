-- A queryable request no longer occupies a connection slot, but its token hold remains
-- until the authoritative usage is available or the configured deadline expires.
if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
local state = redis.call('HGET', KEYS[2], 'state')
if state == 'AWAITING_USAGE' then return 2 end
if state ~= 'RESERVED' then return 0 end
redis.call('HINCRBY', KEYS[1], 'active', -1)
redis.call('HSET', KEYS[2], 'state', 'AWAITING_USAGE')
redis.call('PEXPIRE', KEYS[2], ARGV[1])
return 1