-- KEYS[2] is an operation marker, so an adjustment retry cannot apply the same delta twice.
if redis.call('SETNX', KEYS[2], 'APPLIED') == 0 then return 2 end
redis.call('PEXPIRE', KEYS[2], ARGV[2])
redis.call('HINCRBY', KEYS[1], 'consumed', tonumber(ARGV[1]))
return 1
