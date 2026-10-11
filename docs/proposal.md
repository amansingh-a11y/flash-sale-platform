# Spike-Resilient Flash Sale & Ticketing Platform: Technical Strategy

> Sections follow the assignment brief. Decisions with real alternatives are
> recorded as ADRs in [`docs/adr/`](adr/). The live-incident runbook is in
> [`docs/runbook.md`](runbook.md). Claims marked **(prototype)** are backed by
> code and tests in this repo.

---

## 0. Executive summary

The last drop failed because every request, whether a bot, a refresh, or a real buyer,
went all the way to one relational row per item and to a synchronous payment
call. The redesign rests on four ideas:

1. **Most traffic should never reach origin.** A CDN serves catalog and
   availability (1 s TTL with request coalescing). A virtual waiting room at the
   edge holds up to 10M users and admits them at the rate the reservation tier
   can safely serve. Bot management drops abusive traffic before it queues.
   Origin sees **about 10–20K writes/s and a bounded read rate**, not 500K rps.
2. **Inventory is a sharded counter with atomic holds, not a row lock.** Each
   hot SKU is split across N Redis shards (64 by default for 50K units). A
   single Lua script reserves a unit and creates a TTL hold. There are no
   distributed locks and no read-modify-write races, so overselling is
   impossible by construction.
3. **Money paths are idempotent end to end.** The PSP idempotency key comes
   from the *hold ID*. Holds have an explicit state machine
   (`held → paying → confirmed`) that a hold cannot expire out of mid-charge. A
   reconciler resolves every unknown outcome. "Charged but no order" becomes a
   transient state that is detected and closed automatically.
4. **Confirmation is atomic with an outbox, persistence is asynchronous.** The
   confirm script writes the order event to an outbox stream in the same atomic
   step. Kafka and an idempotent order writer persist it to PostgreSQL, where a
   per-bucket `CHECK (sold <= capacity)` acts as the last line of defence.

**(prototype)** On a laptop, the Go prototype served 30,000 users competing for
2,000 units in 3.4 s. Exactly 2,000 holds were granted, reserve p99 was 59 ms,
and there were zero oversells. Under an injected 30% transient and 30%
"timeout after charge" PSP failure rate, every charge mapped to exactly one
order and there were no duplicate charges (`internal/api/e2e_test.go`).

---

## 1. Problem understanding & goals

### 1.1 What breaks in a naive architecture at 250× traffic

The naive design is: client → LB → monolith → `UPDATE inventory SET qty = qty - 1 WHERE id = ? AND qty > 0`
→ synchronous PSP call → `INSERT order`, all in one request.

| Failure mode | Mechanism | What we saw last drop |
|---|---|---|
| **Hot row** | Every purchase of the item serialises on one row lock. Throughput collapses to roughly 1/lock-hold-time, about 1–3K TPS at best, and far worse when the lock is held across the PSP call. | p99 300 ms → 25 s |
| **Connection exhaustion** | Requests waiting on that row hold DB connections. The pool (and `max_connections`) fills, so *unrelated* queries fail too. | DB hit connection limits |
| **Retry storms** | Timeouts trigger client, LB, and service retries with no jitter or budget. Each layer multiplies load, typically 3 × 3 = 9× at the moment the system is weakest. | latency spiral |
| **Thundering herd** | 10M users hit "buy" at T+0. Cache entries for the item expire simultaneously, and autoscaling (CPU-based, 2–5 min lag) starts after the spike has peaked. | |
| **Non-atomic multi-step writes** | Charge → crash/timeout → no order. Or check stock → (race) → decrement in two statements. | charged without order; double-sold seat |
| **Non-idempotent retries** | A client retry after a timed-out charge creates a second charge or a second reservation. | |
| **Unbounded queues** | Thread pools and LB queues accept work they will never finish in time, so every request times out instead of some being rejected fast. | |
| **Bots** | Scripted clients take a disproportionate share of inventory and of capacity. | |

### 1.2 Goals

| Goal | Target |
|---|---|
| Peak edge throughput | ≥ 500K rps sustained for 15 min; 1M rps headroom at the CDN/WAF |
| Origin throughput (after waiting room) | ≥ 20K reservation-path rps per region, 2× headroom over admitted rate |
| Reservation-path latency | p99 < 500 ms end-to-end (server p99 < 150 ms) |
| Availability | 99.95% monthly on reserve + pay (≈ 21.9 min/month error budget) |
| Time to scale out | Scheduled: fully pre-warmed at T-30 min. Unscheduled: +2× capacity in < 60 s, +10× in < 5 min; waiting room absorbs the gap |
| Overselling | **Zero**: enforced in two independent layers |
| Double charge | **Zero**: PSP idempotency + one order per charge (`UNIQUE(charge_id)`) |
| Charge without order | Detected in < 2 min, resolved automatically (confirm or refund) in < 10 min |
| RPO / RTO | Orders & payments: RPO 0 within region, < 1 s cross-region; RTO < 1 min (AZ), < 5 min (region). Holds: RPO 0 on MemoryDB (see ADR-0001); a hold lost in failover is released, never double-sold |

### 1.3 Strict vs. degradable guarantees

**Strict (never traded away):**
- No unit sold twice (no oversell, no double-sold seat).
- No customer charged twice for one hold; no charge without either an order or a refund.
- A confirmed order is durable once acknowledged to the user.
- Admission tokens cannot be forged; a user can only act on their own holds.

**Degradable (traded for availability and latency under load):**
- *Freshness of availability counts*: shown up to ~1–3 s stale and labelled approximate.
- *Queue ordering*: the waiting room is approximately FIFO (per edge region, randomised entry for users present before open). Strict global FIFO isn't worth its cost (§5.1).
- *Synchronous confirmation*: may degrade to "payment pending, we'll email you" (HTTP 202) when the PSP is slow.
- *Non-critical features*: recommendations, order history, reviews, and personalised pricing are shed first.
- *Hold duration*: may be shortened (10 → 5 min) during extreme demand to recycle abandoned inventory faster.

---

## 2. Proposed architecture

![HLD](diagrams/hld.png)

Source: [`diagrams/hld.mmd`](diagrams/hld.mmd). Sequence of the reserve/pay path:
[`diagrams/reservation-sequence.png`](diagrams/reservation-sequence.png).

### 2.1 Diagram walkthrough

1. **Edge (blue).** Every request lands on a CDN PoP. WAF and bot management
   (JS challenge, device fingerprint, IP/ASN reputation, per-account velocity)
   drop abusive traffic. Catalog pages, images, and `GET /availability` are
   served from cache (1 s TTL, `stale-while-revalidate`, request coalescing), so
   millions of polls collapse to about one origin request per second per PoP
   per SKU.
2. **Waiting room.** At drop time all users for the event go into a virtual
   waiting room run at the edge (edge KV / Durable Objects, or a managed
   product). Users present before open get a random position; later arrivals
   join FIFO. The room releases users at a configurable rate. That rate is the
   *single most important control knob*, set to the measured safe capacity of
   the reservation tier. Admitted users receive a short-lived HMAC-signed
   **admission token** (`user|event|exp`), and only token holders can reach
   reservation endpoints. Once inventory is gone, the room tells everyone still
   waiting "sold out" without sending them to origin.
3. **Region (yellow).** Active-active across three regions, each over three
   AZs. The API gateway verifies the token cheaply (HMAC, no I/O), applies
   per-user and per-IP token-bucket limits, and caps global concurrency.
4. **Stateless services.** Catalog/availability, the reservation service, and
   the payment orchestrator run on Kubernetes. Each request class has its own
   bulkhead, so a flood of availability calls cannot starve payments.
5. **Hot-path state (orange, failure boundary A).** Inventory lives in Redis
   (AWS MemoryDB in production, for a durable multi-AZ transaction log). Each
   SKU is split into N shard keys spread across cluster slots. All hold
   transitions are single-slot Lua scripts. The idempotency store is a separate
   Redis keyspace.
6. **Payment.** The orchestrator calls the PSP with idempotency key
   `charge:<hold_id>`, behind a circuit breaker, a concurrency limit matching
   the PSP's rate limit, per-attempt timeouts, and a retry budget. PSP webhooks
   are a second confirmation path that reaches the same idempotent `Confirm`.
7. **Reconciler.** Every second it returns expired holds to stock and resolves
   payments whose outcome is unknown by looking up the idempotency key at the
   PSP.
8. **Async pipeline (failure boundary B).** The confirm script appends to a
   per-shard outbox stream atomically. A relay moves entries to Kafka (topic
   `orders.confirmed`, key `sku:shard`). The order writer upserts into
   PostgreSQL; notifications and analytics consume the same topic.
9. **System of record (green, failure boundary C).** Aurora PostgreSQL holds
   orders, payment attempts, and `inventory_bucket` rows that mirror the Redis
   shards with `CHECK (sold <= capacity)`. DynamoDB global tables hold sessions
   and waiting-room state. ClickHouse/S3 hold analytics and reconciliation
   reports.

**Failure boundaries.** If **A** (Redis) fails, reservations for affected
SKUs pause and the waiting room stops admitting; browsing continues from the
CDN; confirmed orders are unaffected. If **B** (Kafka/order writer) fails,
reservations and payments keep working, confirmations are already durable in
the outbox, and persistence lags and catches up. If **C** (Postgres) fails,
the hot path keeps selling, orders queue in Kafka, and order-history reads
degrade. The PSP is an external boundary: its failure degrades to "pay later
within your hold" without losing inventory or charging twice.

### 2.2 Request flow

```
client ─▶ CDN/WAF ─▶ waiting room ─(token)─▶ API gateway ─▶ reservation svc ─▶ Redis shard (Lua)
                                                                │
                                                                ├─▶ payment orchestrator ─▶ PSP
                                                                │          │
                                                                │          └─▶ Redis CONFIRM + outbox (atomic)
                                                                ▼
                                                    outbox relay ─▶ Kafka ─▶ order writer ─▶ Postgres ─▶ notify
```

1. `GET /v1/skus/{sku}/availability`: CDN-cached, approximate.
2. `POST /v1/skus/{sku}/holds` (token + `Idempotency-Key`) → `201 {hold_id, expires_at}` or `409 sold_out`.
3. `POST /v1/holds/{id}/pay` (token + `Idempotency-Key`) → `200 confirmed` | `202 payment_pending` | `402 declined` | `410 hold_expired` | `503 + Retry-After`.
4. Confirmation email or push once the order is persisted (seconds later).

### 2.3 Traffic shaping at the edge

| Mechanism | Purpose | Detail |
|---|---|---|
| **Virtual waiting room** | Converts an unbounded spike into a controlled flow | Release rate per event, adjustable live; drains automatically when sold out; tokens expire in 15 min; queue position visible to users to reduce refresh storms |
| **Bot protection** | Fairness and capacity | Managed bot score + JS/PoW challenge at queue entry; per-account and per-card purchase limits; verified-fan pre-registration for the hottest drops |
| **Rate limiting** | Per-actor fairness and abuse control | Edge: per-IP / per-ASN. Gateway: per-user token bucket (e.g. 5 reserve attempts/min), per-token single active hold per SKU |
| **Request coalescing** | Collapse identical reads | CDN collapsed forwarding for availability; `singleflight` + 250 ms local cache in the service (**prototype**: `AvailableCached`) |
| **Sold-out short-circuit** | Stop wasted writes | The waiting room flips to "sold out" via a pushed flag; the service caches a per-SKU sold-out bit for 500 ms (**prototype**) |

### 2.4 Inventory reservation design (ADR-0001, ADR-0003)

**Data layout (prototype: `internal/inventory`).**

```
{inv:<sku>:<i>}:stock          integer remaining in shard i
{inv:<sku>:<i>}:hold:<id>      hash {status, user, expires_at, charge_id}
{inv:<sku>:<i>}:expiry         zset holdID -> expiresAtMs   (held only)
{inv:<sku>:<i>}:paying         zset holdID -> payDeadlineMs (paying only)
{inv:<sku>:<i>}:outbox         stream of confirmed orders
```

The `{...}` hash tag puts all keys of one shard in one Redis Cluster slot, so
every transition is a single-slot atomic Lua script. Different shards land on
different slots and nodes, so a single hot SKU spreads across the cluster.

- **Atomic decrement + hold creation.** `RESERVE` checks `stock > 0`,
  decrements, writes the hold, and indexes its expiry in one script. There is
  no window between check and decrement.
- **Hot-key handling.** The request picks a random starting shard and falls
  through to the next if empty. 64 shards × ~100K ops/s per Redis node is far
  beyond the admitted write rate, and each shard sees ~1/64 of it. Near
  sell-out, requests walk at most N shards; the sold-out bit stops repeated
  walks.
- **TTL holds and release on timeout.** Holds expire after 10 min. The
  reconciler's `SWEEP` script atomically moves expired `held` holds to
  `expired` and increments stock. Expiry uses an explicit zset rather than key
  TTLs or keyspace notifications, which are lossy.
- **No expiry mid-payment.** `BEGIN_PAYMENT` moves `held → paying` and removes
  the hold from the expiry index before the PSP is called, so the race
  "reservation expired mid-payment" cannot happen. `paying` holds are owned by
  the payment path and, if it dies, by the reconciler.
- **Seat maps (reserved seating).** The same pattern applies with one key per
  seat (`SET seat NX PX ttl`) and per-section shards. Best-available picks a
  random free seat within a section to avoid everyone contending on row A.

### 2.5 Async order pipeline (ADR-0004)

- **Queue choice:** Kafka (MSK). We need replay, multiple independent
  consumers (order writer, notifications, analytics, fraud), retention for
  reconciliation, and high partition counts. SQS FIFO caps per-group
  throughput and lacks replay. Kinesis is viable; Kafka wins on ecosystem and
  consumer flexibility.
- **Outbox.** The confirm script writes to the shard's outbox stream in the
  same atomic step as the state change, so a confirmed hold always has its
  event (transactional outbox, no dual write). A stateless relay forwards to
  Kafka using consumer groups with `XAUTOCLAIM` for crashed relays.
- **Partition key:** `sku:shard`. This spreads a hot SKU over many partitions
  (avoiding one hot partition) while keeping per-hold ordering, since all
  events for a hold come from one shard. We do not need global order across a
  SKU.
- **Idempotency and ordering:** at-least-once delivery everywhere; effects are
  exactly-once because `order_id = hold_id` (`INSERT ... ON CONFLICT DO NOTHING`)
  and `charge_id` is `UNIQUE`. **(prototype: `internal/orders`.)**

### 2.6 Which data belongs where

| Store | Data | Why |
|---|---|---|
| **Edge KV / CDN** | Waiting-room queue state, cached catalog and availability, sold-out flags | Absorbs 10M users without origin round-trips |
| **Redis Cluster / MemoryDB** | Live inventory counters, holds, outbox streams, idempotency records, rate-limit buckets | Sub-ms atomic ops at 100K+ ops/s per shard; Lua for multi-key atomicity; MemoryDB adds a durable multi-AZ log |
| **Aurora PostgreSQL** | Orders, payment attempts, inventory buckets (capacity, sold), events/SKUs master data | Money needs relational integrity: unique constraints, CHECKs, transactions, ad-hoc reconciliation queries |
| **DynamoDB (global tables)** | User sessions, waiting-room membership, per-user purchase counters | Key-value at any scale, multi-region active-active, no hot-partition risk with per-user keys |
| **Kafka** | `orders.confirmed`, `payments.events`, `holds.expired` | Durable, replayable fan-out; decouples hot path from persistence |
| **ClickHouse + S3** | Clickstream, funnel, reconciliation reports, PSP settlement imports | Columnar analytics over billions of events; cheap retention |

### 2.7 Caching strategy

| Layer | What | TTL / policy |
|---|---|---|
| **CDN** | Static assets, event pages, `GET /availability` | Assets: immutable + hash. Pages: 30 s + SWR. Availability: `max-age=1, stale-while-revalidate=2, stale-if-error=30`. Collapsed forwarding |
| **Service local cache** | SKU metadata (shards, price), availability sums | Metadata: immutable per drop, cached forever. Availability: 250 ms + singleflight |
| **Redis** | Source of truth for live inventory: *not a cache* | n/a |
| **Pre-warming** | Event pages and catalog pushed to CDN at T-30 min | Avoids cold-cache stampede at T+0 |

Availability is labelled approximate. Correctness never depends on it, because
the reserve call is the authority.

### 2.8 Pre-warming vs. autoscaling

- **Scheduled drops (most of the load):** the drop calendar drives a
  controller that at **T-30 min** raises replica floors (KEDA cron trigger;
  `deploy/k8s/reservation-api.yaml`), pre-scales Kafka consumers, warms CDN and
  connection pools, raises PSP limits by prior arrangement, and pre-seeds
  inventory. Capacity holds until T+90 min, then scales down slowly.
- **Unscheduled spikes (viral, bot-driven):** (1) the waiting room auto-enables
  when origin admit rate or p99 crosses a threshold, absorbing the spike
  immediately; (2) autoscaling on *leading* signals (request rate,
  waiting-room depth, in-flight requests) with a 0 s scale-up stabilisation
  window and +200%/15 s step; (3) a permanently warm floor sized for 5× normal
  traffic, plus a pool of pre-provisioned nodes (cluster over-provisioning with
  low-priority placeholder pods) so new pods schedule in seconds rather than
  waiting minutes for VMs.

---

## 3. Scalability, performance & fault tolerance

### 3.1 Horizontal scaling and autoscaling signals

All request-path services are stateless; state lives in Redis, Postgres,
DynamoDB, and Kafka. Scaling inputs, in order of precedence:

1. **Waiting-room admit rate**: known *before* the traffic arrives.
2. **Request rate per pod** vs. measured knee (load-tested, target 60% of knee).
3. **In-flight requests / bulkhead saturation**: instant saturation signal.
4. **CPU**: kept only as a backstop.

**Why CPU alone is too slow.** It is a *lagging* signal: it rises only after
pods are already saturated. HPA then needs metric scrape (15–30 s) +
stabilisation + pod scheduling + image pull + JVM/Go warm-up + readiness, and
often node provisioning (2–4 min). A 250× spike lands in 30 s, so CPU-based
scaling reacts after the spike has peaked. It also misses I/O-bound
saturation, such as pods blocked on Redis or the PSP at low CPU.

**Capacity math (per region, peak).** Waiting-room admit rate ~10K users/s ×
~2 requests each = ~20K origin rps. Uncached reads add ~10K rps. At a measured
~800 rps/pod (with TLS and tracing), that is ~40 pods at 100%, so the floor is
**100 pods pre-warmed** (2.5× headroom) with an autoscaling ceiling of 600.
Redis: ~20K script ops/s across 64 shards on a 6-shard-primary cluster is
under 5% of its capacity. PSP: 50K units ÷ 1,500 TPS ≈ 35 s of charging at
the PSP limit, well within 10-min holds.

### 3.2 Sharding and partitioning

| What | Partition key | Hot-partition mitigation |
|---|---|---|
| Inventory (Redis) | `{inv:sku:shard}`, N = clamp(units/500, 8, 256) | Random shard choice; N shards over many slots; per-shard capacity ~ equal |
| Holds | Co-located with their shard (hold ID embeds sku and shard) | Same as inventory |
| Orders (Postgres) | Hash(`order_id`) when it outgrows one writer; `inventory_bucket(sku, shard)` rows | Bucket rows split the "sold" counter so order writes don't serialise on one row |
| Kafka | `sku:shard` | Many partitions per hot SKU |
| Users / sessions (DynamoDB) | `user_id` | Naturally uniform |
| Idempotency | `idem:<op>:<user>:<key>` | Uniform |

**Rebalancing near sell-out.** With random shard selection, shards drain at
about the same rate. When only a few shards still have stock, the fall-through
walk finds them. Optionally, a background job consolidates leftover
units into fewer shards to shorten walks. It isn't needed at N ≤ 256.

### 3.3 Backpressure and load shedding

Reject early, cheaply, and in priority order. Rejection happens at the
outermost layer that can make the decision.

1. **Bots and abusive clients** (WAF/bot score): dropped at the edge.
2. **Unadmitted traffic**: stays in the waiting room; never reaches origin.
3. **Non-essential reads**: recommendations, history, reviews (feature flags off).
4. **Availability polling at origin**: served stale from CDN (`stale-if-error`).
5. **New reservations**: the waiting room slows its release rate; the reserve
   bulkhead sheds with `503 + Retry-After` (jittered).
6. **Payments for existing holds**: *protected last*. They have their own
   bulkhead and are the revenue path. If the PSP is the bottleneck, payment
   requests that were never sent return 503 and the hold stays valid.

Each bulkhead is a non-blocking semaphore (**prototype**:
`resilience.Bulkhead`). When full, the request is rejected in microseconds
instead of queueing into a timeout. Every response carries `Retry-After`. The
gateway enforces a global concurrency limit with an adaptive (AIMD/Gradient)
limiter, so capacity tracks real latency rather than a static number.

### 3.4 Retries without retry storms

- **Exponential backoff with full jitter**: `sleep = rand(0, min(cap, base·2^n))`
  (**prototype**: `resilience.Backoff`).
- **Retry budget**: retries limited to ~10% of first attempts per client
  instance (**prototype**: `resilience.RetryBudget`). During an outage, load
  rises by at most 10%, not 3×.
- **Retry at one layer only**: the payment orchestrator retries the PSP; the
  gateway and LB do not retry non-idempotent requests; SDK clients retry only
  on 503/429 using `Retry-After`.
- **Only idempotent operations are retried**, and every mutating API requires
  an `Idempotency-Key`.
- **Client SDK / web app**: disables the buy button while a request is in
  flight, reuses the same idempotency key on retry, honours `Retry-After`, and
  caps total attempts.

### 3.5 Circuit breakers, bulkheads, timeouts

| Edge | Timeout | Breaker | Bulkhead |
|---|---|---|---|
| Gateway → reservation | 2 s total | Per-upstream outlier ejection | Global concurrency limit |
| Reservation → Redis | 200 ms read/write, 1 s dial | Fail fast on cluster down; shed with 503 | Connection pool 256/pod |
| Reservation → PSP | 3 s per attempt, 2 retries within an 8 s overall budget | Opens after 20 consecutive failures for 5 s, half-open single probe; declines don't count (**prototype**) | Concurrency = PSP rate limit ÷ pod count (**prototype**) |
| Order writer → Postgres | 1 s statement timeout | Pause consumption on errors (lag grows, nothing lost) | Pool 20/consumer |

Timeouts shrink as calls go deeper (deadline propagation), so an inner call
never outlives the request that needs it.

### 3.6 Idempotency and exactly-once effects

| Effect | Key | Guarantee |
|---|---|---|
| Create hold | `Idempotency-Key` header, scoped per user + op, fingerprinted to the request | Retry replays the same `hold_id`; key reuse for a different request → 422 (**prototype**) |
| Charge | PSP key `charge:<hold_id>` (derived server-side, **not** from the client key) | However many pay calls, client keys, retries, or reconciler lookups occur, a hold is charged at most once (**prototype**, tested with two different client keys) |
| Confirm | Lua state check `held/paying → confirmed` | Second confirm is a no-op; outbox written once (**prototype**) |
| Persist order | `order_id = hold_id` PK; `charge_id UNIQUE` | Replayed Kafka messages are no-ops |
| Refund | PSP key `refund:<order_id>` | At most one refund per order |

Exactly-once *delivery* isn't possible across a network; exactly-once *effects*
come from at-least-once delivery plus idempotent state transitions keyed on
business IDs.

### 3.7 Partial failures

| Scenario | Handling |
|---|---|
| **Payment succeeded, order write failed** | Can't lose the order: confirmation and outbox entry are one atomic Redis step. If the service crashes *between* charge and confirm, the hold stays `paying` and the reconciler finds it after the deadline, looks up `charge:<hold_id>` at the PSP, and confirms. If Postgres is down, events wait in Kafka. |
| **PSP timeout (outcome unknown)** | Return `202 payment_pending`; the hold stays `paying` (can't expire). Reconciler looks up the charge: found → confirm; not found after the deadline (≫ PSP timeout) → release unit. A PSP webhook can confirm earlier via the same idempotent path. **(prototype)** |
| **Reservation expired mid-payment** | Prevented: `held → paying` removes the hold from the sweep before charging. If a webhook arrives for an already expired/released hold (async payment methods), try to re-reserve a unit; if none is left, refund automatically and notify. |
| **Card declined** | `paying → held` with original expiry, so the user can retry another card. **(prototype)** |
| **Queue consumer crash** | Unacked messages are reclaimed by `XAUTOCLAIM` (Redis) or by consumer-group rebalance (Kafka); processing is idempotent. **(prototype)** |
| **Poison message** | Logged and dead-lettered after N attempts; alert; never blocks the partition. |
| **Redis primary failover** | MemoryDB: acknowledged writes are durable across AZs (RPO 0). With ElastiCache, writes from the last ~1 s may be lost: the affected SKU is frozen (waiting room pauses), counts are rebuilt as `capacity − confirmed (Postgres) − live holds`, and the Postgres CHECK catches any residual oversell, which is refunded. |
| **Double-sold seat at SoR** | `CHECK (sold <= capacity)` / seat `UNIQUE` rejects → automatic refund + page. Should be unreachable; it exists to turn a bug into an alert, not an incident. |

### 3.8 Multi-AZ / multi-region (ADR-0006)

- **Within a region:** every tier spans 3 AZs (topology spread, PDB 90%);
  Redis/MemoryDB primaries spread across AZs with replicas in others; Aurora
  Multi-AZ; Kafka RF=3, `min.insync.replicas=2`, `acks=all`.
- **Across regions:** stateless tiers and reads are active-active with
  latency routing. **Inventory for a given drop is homed in one region**,
  chosen by the drop's audience. Other regions forward reservation writes to the
  home region (+70–150 ms, still within the 500 ms p99 budget). For truly
  global drops, the allocation is *split* into per-region quotas with a
  rebalancer moving unsold units between regions every few seconds.
  Either way, each unit has exactly one owner at any moment, so overselling
  can't happen across regions.
- **Under partition** (CAP): inventory writes choose **consistency**. If a
  region can't reach a SKU's home, the waiting room pauses that SKU there
  instead of selling against a stale copy. Browsing stays available.
- **Targets:** AZ loss: RTO < 1 min (automatic), RPO 0. Region loss: RTO < 5
  min (inventory home failover with Aurora Global/MemoryDB snapshots + count
  rebuild from Postgres), RPO < 1 s for orders.

### 3.9 Sustaining 99.95%

- **Error budget**: 21.9 min/month. Drops are the riskiest minutes, so they
  get extra protections: a **change freeze** from T-24 h to T+2 h, pre-warmed
  capacity, and a staffed war room.
- **Deployments**: rolling with `maxUnavailable: 0`; readiness fails first and
  connections drain 5 s before shutdown (**prototype**: `main.go`); canary at
  1% → 10% → 50% gated on SLOs; automatic rollback on burn-rate alert.
  Schema changes are expand/contract only.
- **Dependency outages**: PSP down → holds stay valid, pay returns 503 and the
  waiting room can extend hold TTLs; multi-PSP failover for card payments if
  the business has a second acquirer. Redis down → waiting room pauses; no
  data corruption. Kafka/Postgres down → selling continues, persistence lags.
- **Cell architecture** (v2): split the fleet into independent cells (each its
  own Redis, Kafka partitions, and service pods); a drop is pinned to a cell,
  so a bad drop can't take the whole platform down.

---

## 4. Load testing & observability

### 4.1 Load and chaos testing plan

| Test | Profile | Pass criteria |
|---|---|---|
| **Spike** | 2K → 500K rps in 30 s, hold 10 min, drop sharply (`loadtest/k6/spike.js`, `PROFILE=spike`) | Reserve p99 < 500 ms; 0 oversell; error rate (excluding 409 sold-out) < 0.1%; autoscaler reaches target before T+60 s |
| **Stress** | Step up until SLOs break (`PROFILE=stress`) | Know the knee per pod and per Redis shard; graceful 503s past the knee, not timeouts |
| **Soak** | 20K rps for 4 h (`PROFILE=soak`) | No memory/connection leaks; hold expiry and reconciliation keep counts exact |
| **Correctness** | N users ≫ stock with duplicate retries and PSP faults (`cmd/loadgen`, `e2e_test.go`) | holds == stock; charges == orders; 0 refunds needed |
| **Dependency failure** | PSP latency +5 s, 30% errors, total outage; Redis primary kill; Kafka broker loss; Postgres failover; AZ blackhole | Breaker opens; no double charges; hold states resolve; SLOs recover within RTO |

**Traffic generation at scale.** Distributed k6 (k6-operator) or Gatling
across regions, with realistic mixes (98% reads, think time, retries, bot
traffic) replayed from the last drop's logs. A full-scale test runs against a
production-sized staging cell, and quarterly in production during off-peak
hours with synthetic SKUs and a sandbox PSP.

**Game days.** One week before each tier-1 drop: (1) rehearse the drop with
synthetic traffic at 1.5× expected, (2) inject one surprise failure from the
list above, (3) practise the runbook (kill switches, manual scale-up,
communications), and (4) write a blameless review with action items before
the real drop.

### 4.2 Signals that matter

**Golden signals per service:** request rate, error rate by class
(5xx vs. shed 503 vs. business 409/402), latency histograms (p50/p99/p99.9),
and saturation (bulkhead in-flight, Redis CPU and ops/s per shard, connection
pools, Kafka lag).

**Drop-specific business signals:**
- Waiting room: depth, admit rate, time in queue, abandonment.
- Reservation success rate = 201 / (201 + 5xx). Sold-out (409) is tracked separately.
- **Holds by state** (`held`, `paying`, `confirmed`, `expired`, `released`)
  and **oldest `paying` age**: a stuck payment shows up here first.
- **Outbox / Kafka lag: depth *and age* of the oldest unprocessed message.**
  Age is the user-visible metric; depth alone misleads during a spike.
- PSP: latency, error rate, breaker state, retry-budget exhaustion.
- **Error-budget burn rate** for the reservation and payment SLOs.

**Traces:** OpenTelemetry from edge to PSP with `hold_id` and `user_id` as
attributes. Tail-sampling keeps 100% of errors and slow traces and 1% of the
rest during drops (head sampling at 500K rps would be too expensive).

**Logs:** structured JSON, one line per state transition, with `hold_id`,
`charge_id`, and trace ID. They feed reconciliation and support lookups.

### 4.3 SLIs, SLOs and alerting

| SLI | SLO |
|---|---|
| Reservation availability: good = 201/409/402/410, bad = 5xx excluding intentional shed beyond the admitted rate | 99.95% / 30 days |
| Reservation latency: fraction of reserve calls < 500 ms at the edge | 99% |
| Payment completion: holds entering `paying` that reach `confirmed` or `released` within 10 min | 99.99% |
| Order persistence lag: confirmed → in Postgres | p99 < 30 s |
| Correctness | 0 oversells, 0 duplicate charges (any occurrence pages immediately) |

**Avoiding alert fatigue during an intentional spike:**
- Alert on **SLO burn rate** (multi-window: 14.4× over 1 h *and* 5 min → page;
  6× over 6 h → ticket), not on raw RPS, CPU, or queue depth.
- **Drop-aware thresholds:** the drop calendar puts the platform into a "drop
  mode" alert profile, so expected high traffic, high waiting-room depth, and
  planned 503 shedding (when the room is deliberately admitting below
  demand) don't page. Errors, latency, and correctness alerts stay on.
- **Symptom over cause:** page on user-facing SLOs and correctness. Causes
  (Redis CPU, Kafka lag) go to dashboards and tickets unless they predict SLO
  breach.
- Alerts are deduplicated and grouped per drop; one incident channel per drop.

### 4.4 Real-time correctness detection (reconciliation)

| Check | Frequency | How |
|---|---|---|
| **Oversell** | Continuous | Invariant per SKU: `confirmed + held + paying + available == capacity`, checked from Redis every 10 s. Postgres `CHECK` violation in the order writer → page. Kafka consumer counts confirmed per SKU vs. capacity |
| **Duplicate charges** | Every minute during drops, then hourly | `payment_attempts` vs. `orders`: >1 succeeded charge per hold, or per (user, sku) above the purchase limit. PSP settlement report import (daily) vs. orders |
| **Charge without order** | Every minute | Succeeded `payment_attempts` older than 5 min with no order → auto-confirm or refund + alert if > 0 |
| **Stuck reservations** | Continuous | `paying` older than deadline + 5 min (reconciler failing); `held` past expiry not swept; alert on oldest-age |
| **Redis ↔ Postgres drift** | Every 5 min and after any failover | Per bucket: Redis confirmed count vs. Postgres `sold` |

### 4.5 Runbook summary

The full runbook is in [`runbook.md`](runbook.md). Key levers:

- **Kill switches / flags:** pause admissions for an event; pause a single
  SKU; disable non-essential features; force availability to "limited" or
  "sold out"; shorten hold TTL; switch pay to "pending" mode (accept, charge
  async); switch PSP.
- **Manual scale-up:** `kubectl scale` / raise KEDA floor; Redis shard add
  (online resharding); Kafka consumer replicas.
- **Communication:** status page, in-queue banner (the waiting room can show
  a message to everyone instantly), social and support macros; internal
  updates every 15 min.

---

## 5. Trade-off analysis

### 5.1 Fairness (FIFO waiting room) vs. throughput and simplicity

Strict global FIFO needs a single sequencer: a global bottleneck and a
single point of failure across regions. We choose **approximate fairness**:
random order for everyone present at open (removes the advantage of
refreshing faster or being geographically closer), FIFO per edge region for
later arrivals, and fairness enforced *after* admission via one active hold
per user, purchase limits, and bot scoring. That is fairer in practice than
strict FIFO, which bots win by arriving first, and it scales horizontally.
Cost: a user may see their position change slightly; we show coarse ETA
bands instead of exact positions.

### 5.2 Strong consistency of inventory vs. availability and latency

Inventory writes are **CP**: each unit is owned by exactly one shard in one
home region, and a partition pauses sales of that SKU rather than risking an
oversell. A refund and an apology after an oversell costs more, in money,
trust, and regulatory exposure for ticketing, than a 1-minute pause. Reads are
**AP**: availability is eventually consistent and cached. Latency stays low
because the strong path is a single-slot, sub-millisecond Redis operation, not
a distributed transaction. Cost: a region-level failure pauses home-region
SKUs for the failover RTO.

### 5.3 Synchronous confirmation vs. asynchronous processing

**Hybrid.** Users get a synchronous answer for the parts they care about: hold
(sub-100 ms) and payment result (when the PSP answers within ~3 s). Everything
behind confirmation, such as order persistence, email, analytics, and
fulfilment, is async via the outbox. When the PSP is slow we degrade to
`202 payment_pending` and confirm by push/email. Fully async ("we'll let you
know") would scale more simply but is a poor UX for ticketing and produces
support load. Fully sync (DB write in the request) is what failed last time.

### 5.4 Over-provisioning for peak vs. autoscaling lag

For **scheduled** drops we over-provision deliberately: pre-warming 100+ pods
for 2 hours costs a few hundred dollars. A failed drop costs revenue,
reputation, and chargebacks, orders of magnitude more. For **unscheduled**
spikes we keep a modest warm floor (5× normal) plus headroom nodes, and rely
on the **waiting room to absorb autoscaling lag**: it turns "we can't scale in
30 s" into "users wait 60 s in a queue" instead of "everyone gets errors".
Steady-state cost is optimised with spot instances for stateless pods outside
drop windows and savings plans for the floor.

### 5.5 Build vs. buy

| Component | Decision | Reasoning |
|---|---|---|
| **Waiting room** | **Buy** (Cloudflare Waiting Room / Queue-it / Akamai), integrate via signed tokens | Must run at the edge in every PoP with a global queue; vendors have run it at Ticketmaster scale; build cost is high and it isn't our differentiator. The token interface (`internal/admission`) keeps us portable |
| **Bot protection** | **Buy** (Cloudflare Bot Management / Akamai / DataDome) + in-house purchase-limit rules | Adversarial arms race needing global telemetry; our own signals (account age, purchase history) layered on top |
| **Queue / log** | **Buy managed** (MSK or Confluent) | Operating Kafka well is expensive; we need its semantics, not its operations |
| **Inventory / reservation** | **Build** on managed Redis/MemoryDB | Core domain logic, small code surface (~6 Lua scripts), must match our business rules exactly |
| **Payments orchestration** | **Build** thin layer over PSP SDK | Idempotency and state machine are domain-specific; PSP selection may change |

---

## 6. Execution plan

### Phases

| Phase | Scope | Exit criteria |
|---|---|---|
| **v0: Stop the bleeding** (4–6 weeks, before the next drop) | Managed waiting room + bot management in front of the existing stack; CDN caching for catalog/availability; idempotency keys on checkout and PSP calls; DB connection pooler (PgBouncer) and statement timeouts; `CHECK (sold <= capacity)` and unique constraints in the current DB; reconciliation job for charge-without-order; drop-mode alerting | Next drop has **0 oversells, 0 double charges**; DB stays under connection limits because the waiting room caps admission at the measured safe rate |
| **v1: New reservation path** (8–12 weeks) | Redis sharded inventory + holds + payment state machine (this prototype, hardened); payment orchestrator with breaker, bulkheads, retry budget; outbox → Kafka → order writer; KEDA autoscaling on leading signals; pre-warm controller tied to the drop calendar; OTel tracing; full load and chaos suite | Shadow traffic parity for 2 weeks; canary drops pass; full-scale spike test at 1.5× expected peak passes SLOs |
| **v2: Global and cellular** (next 2 quarters) | Multi-region active-active reads; inventory homing / per-region quotas with rebalancer; cell architecture; multi-PSP failover; seat-map reservations on the same engine; automated game days | Region evacuation drill passes RTO < 5 min; a fault injected into one cell leaves the others unaffected |

### Validation before each big drop

1. **Shadow traffic.** Mirror production reservation requests (from v0 traffic)
   into the v1 path against a shadow SKU namespace and sandbox PSP. Compare
   decision parity (would this request have succeeded?), latency, and error
   rates. No user impact.
2. **Dark launch, then canary drops.** Run small, low-risk drops (e.g. 500
   units of merchandise) fully on v1 for 1% → 10% → 50% of admitted users,
   routed by the waiting room via token claims. The waiting room is the
   routing switch, so rollback is instant: stop admitting to v1 and keep
   admitting to v0.
3. **Load tests.** Spike, stress, and soak at 1.5× the forecast for the
   upcoming drop, on production-sized infrastructure, one week before.
4. **Game day.** Rehearse the runbook with injected failures (§4.1).
5. **Go/no-go checklist at T-24 h.** Capacity pre-warm scheduled; PSP limits
   confirmed with the provider; change freeze in effect; on-call and war room
   staffed; kill switches tested in production (flip and revert on a
   synthetic SKU).

### Risks and mitigations

| Risk | Mitigation |
|---|---|
| Redis data loss on failover causes oversell | MemoryDB (durable log); freeze + rebuild procedure; Postgres CHECK as second layer |
| PSP rate limit lower than demand | Negotiate a burst quota per drop; concurrency limiter; long enough holds for the payment queue to drain; second acquirer |
| Waiting-room vendor outage | Fail closed (admissions paused) rather than open; secondary vendor config ready for tier-1 drops |
| Team unfamiliar with the new stack | v0 buys time; runbooks and game days before v1 handles a tier-1 drop |
| Bots adapt | Layered defences; purchase limits and post-sale cancellation of bot orders; verified-fan registration |
