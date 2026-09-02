if redis.call('EXISTS', KEYS[2]) == 0 then return 0 end
if redis.call('HGET', KEYS[2], 'state') ~= 'RESERVED' then return 0 end
local reserved = tonumber(redis.call('HGET', KEYS[2], 'tokens') or '0')
redis.call('HINCRBY', KEYS[1], 'reserved', -reserved); redis.call('HINCRBY', KEYS[1], 'active', -1)
redis.call('HSET', KEYS[2], 'state', 'COMPENSATED'); redis.call('PEXPIRE', KEYS[2], ARGV[1]); return 1
