-- KEYS[1] quota hash, KEYS[2] reservation stub
-- ARGV[1] stub TTL millis after compensation
-- ARGV[2] reserved_tokens from MySQL (used only when refunding a RESERVED row without a stub)
-- ARGV[3] "1" if MySQL status is RESERVED: Lua success already happened, so refund even when the stub expired
-- PENDING rows omit ARGV[3] or pass "0": a missing stub means Redis never reserved, so do not refund
if redis.call('EXISTS', KEYS[2]) == 1 then
  if redis.call('HGET', KEYS[2], 'state') ~= 'RESERVED' then return 0 end
  local reserved = tonumber(redis.call('HGET', KEYS[2], 'tokens') or '0')
  redis.call('HINCRBY', KEYS[1], 'reserved', -reserved)
  redis.call('HINCRBY', KEYS[1], 'active', -1)
  redis.call('HSET', KEYS[2], 'state', 'COMPENSATED')
  redis.call('PEXPIRE', KEYS[2], ARGV[1])
  return 1
end
if ARGV[3] ~= '1' then return 0 end
-- Claim the stub first so two workers cannot refund the same RESERVED row after the original stub expired.
if redis.call('HSETNX', KEYS[2], 'state', 'COMPENSATED') == 0 then return 0 end
local mysqlTokens = tonumber(ARGV[2] or '0')
redis.call('HSET', KEYS[2], 'tokens', mysqlTokens)
redis.call('HINCRBY', KEYS[1], 'reserved', -mysqlTokens)
redis.call('HINCRBY', KEYS[1], 'active', -1)
redis.call('PEXPIRE', KEYS[2], ARGV[1])
return 1
