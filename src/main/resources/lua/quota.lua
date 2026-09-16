-- KEYS[1] = quota key
-- ARGV[1] = tokens to consume, ARGV[2] = limit, ARGV[3] = ttl seconds
-- return: 0 = allowed, 1 = over limit
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local need = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

if used + need > limit then
  return 1
end

redis.call('INCRBY', KEYS[1], need)
redis.call('EXPIRE', KEYS[1], ttl)
return 0
