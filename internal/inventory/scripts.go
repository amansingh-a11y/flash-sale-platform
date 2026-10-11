package inventory

import "github.com/redis/go-redis/v9"

// All keys touched by a script share one hash tag ({inv:<sku>:<shard>}), so each
// script runs atomically on a single Redis Cluster slot.
//
// Hold state machine:
//
//	held ──pay──▶ paying ──charge ok──▶ confirmed
//	 │  ◀─declined─┘  │
//	 │                └──reconciler: not charged──▶ released
//	 ├──user cancel──▶ released
//	 └──TTL sweep────▶ expired
//
// Only "held" holds are swept, so a hold can never expire while a charge is in flight.

// KEYS: stock, expiry zset, hold hash
// ARGV: holdID, userID, expiresAtMs
var reserveScript = redis.NewScript(`
local stock = tonumber(redis.call('GET', KEYS[1]) or '0')
if stock <= 0 then return 0 end
redis.call('DECR', KEYS[1])
redis.call('HSET', KEYS[3], 'status', 'held', 'user', ARGV[2], 'expires_at', ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
return 1
`)

// KEYS: hold hash, expiry zset, paying zset
// ARGV: holdID, userID, payDeadlineMs
var beginPaymentScript = redis.NewScript(`
local st = redis.call('HGET', KEYS[1], 'status')
if not st then return 'missing' end
if redis.call('HGET', KEYS[1], 'user') ~= ARGV[2] then return 'not_owner' end
if st == 'paying' then return 'ok' end
if st ~= 'held' then return st end
redis.call('HSET', KEYS[1], 'status', 'paying')
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
return 'ok'
`)

// Returns a paying hold to held (e.g. card declined) so the user can retry
// until the original expiry.
// KEYS: hold hash, expiry zset, paying zset
// ARGV: holdID
var abortPaymentScript = redis.NewScript(`
local st = redis.call('HGET', KEYS[1], 'status')
if st ~= 'paying' then return st or 'missing' end
redis.call('HSET', KEYS[1], 'status', 'held')
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZADD', KEYS[2], redis.call('HGET', KEYS[1], 'expires_at'), ARGV[1])
return 'ok'
`)

// Confirms the hold and appends the order to the shard's outbox stream in the
// same atomic step (transactional outbox).
// KEYS: hold hash, expiry zset, paying zset, outbox stream
// ARGV: holdID, chargeID, amountMinor, terminalTTLms, sku, shard
var confirmScript = redis.NewScript(`
local st = redis.call('HGET', KEYS[1], 'status')
if not st then return 'missing' end
if st == 'confirmed' then return 'already' end
if st ~= 'held' and st ~= 'paying' then return st end
redis.call('HSET', KEYS[1], 'status', 'confirmed', 'charge_id', ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
redis.call('XADD', KEYS[4], '*',
  'order_id', ARGV[1], 'sku', ARGV[5], 'shard', ARGV[6],
  'user', redis.call('HGET', KEYS[1], 'user'),
  'charge_id', ARGV[2], 'amount_minor', ARGV[3])
return 'ok'
`)

// KEYS: hold hash, expiry zset, paying zset, stock
// ARGV: holdID, userID (” skips owner check), allowPaying ('1'/'0'), terminalTTLms
var releaseScript = redis.NewScript(`
local st = redis.call('HGET', KEYS[1], 'status')
if not st then return 'missing' end
if ARGV[2] ~= '' and redis.call('HGET', KEYS[1], 'user') ~= ARGV[2] then return 'not_owner' end
if st == 'paying' and ARGV[3] ~= '1' then return 'paying' end
if st ~= 'held' and st ~= 'paying' then return st end
redis.call('HSET', KEYS[1], 'status', 'released')
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('INCR', KEYS[4])
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return 'ok'
`)

// Hold keys are derived from ARGV[3] rather than declared in KEYS; they share
// the shard's hash tag, so they always live on the same slot.
// KEYS: expiry zset, stock
// ARGV: nowMs, limit, holdKeyPrefix, terminalTTLms
var sweepScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local n = 0
for _, id in ipairs(ids) do
  local hk = ARGV[3] .. id
  if redis.call('HGET', hk, 'status') == 'held' then
    redis.call('HSET', hk, 'status', 'expired')
    redis.call('PEXPIRE', hk, ARGV[4])
    redis.call('INCR', KEYS[2])
    n = n + 1
  end
  redis.call('ZREM', KEYS[1], id)
end
return n
`)
