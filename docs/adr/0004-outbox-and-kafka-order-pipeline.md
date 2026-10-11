# ADR-0004: Transactional outbox in Redis → Kafka → idempotent order writer

- Status: Accepted
- Date: 2026-10-11

## Context

After a hold is confirmed, the order must reach PostgreSQL, notifications,
analytics and fraud. Writing to Postgres synchronously in the request is what
coupled the hot path to the DB last time. Writing to Redis *and* Kafka from
the service is a dual write: a crash in between loses the event.

## Decision

- The `CONFIRM` Lua script appends the order to the shard's outbox stream
  (`XADD {inv:sku:i}:outbox`) in the same atomic step as the status change.
- A relay reads outbox streams with consumer groups (crashed relays' messages
  are reclaimed with `XAUTOCLAIM`) and produces to Kafka topic
  `orders.confirmed`, key `sku:shard`, `acks=all`, idempotent producer.
- The order writer upserts `orders` (`PK order_id = hold_id`, `UNIQUE charge_id`)
  and increments `inventory_bucket.sold` only when a row was inserted and
  `sold < capacity`. A capacity violation triggers refund + page.
- In the prototype the consumer reads the outbox directly into an in-memory
  store with the same semantics (`internal/orders`).

**Why Kafka over SQS/Kinesis:** replay for reconciliation and backfills, many
independent consumer groups, high partition counts for hot SKUs, and mature
exactly-once producer semantics. SQS FIFO limits per-group throughput and
can't replay.

## Consequences

- The hot path never waits on Postgres; Postgres or Kafka outages increase
  lag, not errors.
- Delivery is at-least-once; effects are exactly-once via business-key
  idempotency.
- Order visibility in "My orders" is eventually consistent (target p99 < 30 s);
  the confirmation screen shows the hold-derived order ID immediately.
