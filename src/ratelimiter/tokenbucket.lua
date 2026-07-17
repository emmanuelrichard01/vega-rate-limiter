-- Atomic token-bucket check-and-consume.
-- Runs entirely inside Redis via EVALSHA, so concurrent callers from
-- different rate-limiter instances (or threads) can never race each
-- other: Redis executes Lua scripts single-threaded and atomically.
--
-- KEYS[1] = bucket key, e.g. "rl:{clientId}"
-- ARGV[1] = capacity        (max burst size, tokens)
-- ARGV[2] = refill_rate     (tokens added per second)
-- ARGV[3] = cost            (tokens this request consumes, usually 1)
-- ARGV[4] = ttl_seconds     (key expiry so idle clients don't leak memory)
--
-- Returns: { allowed (1|0), tokens_remaining (float, x1000 fixed-point),
--            retry_after_ms (int) }
--
-- Uses Redis's own clock (TIME command) rather than a timestamp passed
-- in by the caller, so clock drift between rate-limiter instances can
-- never cause over- or under-admission.

local key          = KEYS[1]
local capacity      = tonumber(ARGV[1])
local refill_rate   = tonumber(ARGV[2])
local cost          = tonumber(ARGV[3])
local ttl_seconds   = tonumber(ARGV[4])

local t = redis.call('TIME')
local now_ms = (tonumber(t[1]) * 1000) + math.floor(tonumber(t[2]) / 1000)

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local last_ts = tonumber(bucket[2])

if tokens == nil then
  -- first time we've seen this client: start full (burst allowed immediately)
  tokens = capacity
  last_ts = now_ms
end

local elapsed_ms = now_ms - last_ts
if elapsed_ms < 0 then elapsed_ms = 0 end

local refilled = tokens + (elapsed_ms / 1000.0) * refill_rate
if refilled > capacity then refilled = capacity end

local allowed = 0
local retry_after_ms = 0

if refilled >= cost then
  refilled = refilled - cost
  allowed = 1
else
  -- how long until enough tokens accrue for this request
  local deficit = cost - refilled
  retry_after_ms = math.ceil((deficit / refill_rate) * 1000)
end

redis.call('HMSET', key, 'tokens', refilled, 'ts', now_ms)
redis.call('EXPIRE', key, ttl_seconds)

return { allowed, tostring(refilled), retry_after_ms }
