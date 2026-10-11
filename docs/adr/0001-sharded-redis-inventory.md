# ADR-0001: Sharded Redis counters with Lua scripts as the inventory gate; PostgreSQL as system of record

- Status: Accepted
- Date: 2026-10-11

## Context

Last drop: one `inventory` row per item, decremented in the request
transaction. At 250× load that row's lock serialised every purchase, held DB
connections across the PSP call, and exhausted the pool. Separately, a
check-then-decrement race sold the same seat twice.

The reservation path needs about 10–20K atomic decrements/s on a single SKU
with p99 < 150 ms server-side, and must never oversell.

## Options

1. **Relational row with `SELECT ... FOR UPDATE`.** Simple and strongly consistent, but one hot row caps throughput at roughly 1/lock-time. This is what failed.
2. **Relational, many bucket rows.** Removes the single hot row, but every reservation is still a DB transaction plus connection. Feasible at a few thousand TPS, with no margin.
3. **Redis single key `DECR`.** Fast, but one hot key lives on one Redis node/thread and hold/expiry state isn't atomic with it.
4. **Redis, SKU split into N shard keys, all hold transitions as single-slot Lua scripts.** ✅
5. **Pre-materialised token queue** (`LPOP` from a list of unit IDs). Elegant for seat IDs, but the same hot-key problem unless sharded, and harder to return units.

## Decision

Option 4. Each SKU's stock is split into N keys `{inv:<sku>:<i>}:stock`.
Holds, expiry index, paying index and outbox for shard *i* share the hash tag,
so every transition (reserve, begin payment, abort, confirm, release, sweep) is
one atomic Lua script on one slot. Clients pick a random shard and fall
through to the next if it's empty.

In production use **AWS MemoryDB** (Redis-compatible, multi-AZ transaction log,
acknowledged writes survive failover). PostgreSQL remains the system of record
for orders and money, with `inventory_bucket(sku, shard)` rows carrying
`CHECK (sold <= capacity)` as an independent second guard.

## Consequences

- Throughput scales with N; one SKU uses the whole cluster.
- Oversell is impossible within Redis (atomic scripts) and is caught again in
  Postgres if a failover ever loses writes, which turns a bug into an
  automatic refund and a page.
- Availability is a sum over N keys, so it's cached (250 ms locally, 1 s at the CDN).
- Near sell-out a request may probe up to N shards; mitigated by the sold-out
  bit and by keeping N ≤ 256.
- On plain ElastiCache (async replication) a failover can lose ~1 s of writes;
  the runbook's freeze + rebuild procedure covers that. MemoryDB avoids it.
