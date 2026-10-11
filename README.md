# Spike-Resilient Flash Sale & Ticketing Platform

A technical strategy and architecture proposal for a checkout platform that
stays **correct, fast and available** when a drop takes traffic from 2K to
500K+ rps in 30 seconds. It comes with a working Go prototype of the
reservation and payment core.

| | |
|---|---|
| **Proposal (main document)** | [`docs/proposal.md`](docs/proposal.md) |
| **HLD diagram** | [`docs/diagrams/hld.png`](docs/diagrams/hld.png) (source: [`hld.mmd`](docs/diagrams/hld.mmd)); walkthrough in [proposal §2.1](docs/proposal.md#21-diagram-walkthrough) |
| **Reserve/pay sequence** | [`docs/diagrams/reservation-sequence.png`](docs/diagrams/reservation-sequence.png) |
| **Decision records** | [`docs/adr/`](docs/adr/) |
| **Incident runbook** | [`docs/runbook.md`](docs/runbook.md) |

![High-level design](docs/diagrams/hld.png)

## Problem summary

During the last drop, p99 latency went from 300 ms to 25 s, the database ran
out of connections, some users were charged without getting an order, and one
seat was sold twice. The root causes were one hot inventory row locked per
purchase, a synchronous payment call inside the DB transaction, retries
without jitter or idempotency, and no admission control in front of origin.

## Key decisions

1. **Edge waiting room + bot management + CDN** keep ~98% of traffic off
   origin. The waiting room admits users at the rate the reservation tier can
   serve and issues signed admission tokens ([ADR-0002](docs/adr/0002-waiting-room-and-admission-tokens.md)).
2. **Inventory as sharded Redis counters with atomic Lua holds.** No hot key
   and no lock. Oversell is impossible by construction, and PostgreSQL
   `CHECK (sold <= capacity)` is a second guard ([ADR-0001](docs/adr/0001-sharded-redis-inventory.md)).
3. **Hold state machine** `held → paying → confirmed`: a hold can't expire
   mid-charge, and a reconciler resolves unknown payment outcomes ([ADR-0003](docs/adr/0003-hold-state-machine.md)).
4. **Idempotency end to end.** The PSP key is derived from the hold ID, so a
   hold is charged at most once regardless of retries ([ADR-0005](docs/adr/0005-idempotency-and-payments.md)).
5. **Transactional outbox → Kafka → idempotent order writer.** The hot path
   never waits on the database ([ADR-0004](docs/adr/0004-outbox-and-kafka-order-pipeline.md)).
6. **Multi-region with inventory homed per drop**: consistent writes,
   available reads ([ADR-0006](docs/adr/0006-multi-region-inventory-homing.md)).
7. **Pre-warm for scheduled drops; autoscale on leading signals**, not CPU ([ADR-0007](docs/adr/0007-prewarm-and-leading-signal-autoscaling.md)).

## Assumptions

From the brief: 10M concurrent users in the waiting room, 50K units for a hot
item, 50:1 read:write, partially predictable spikes, a rate-limited PSP with
occasional timeouts, multi-region users, 99.95% availability, and p99 < 500 ms
on the reservation path.

Added:
- AWS as reference cloud (MemoryDB, Aurora PostgreSQL, MSK, DynamoDB, EKS); every component has a GCP/Azure equivalent.
- The PSP supports idempotency keys and charge lookup by key (Stripe, Adyen and Braintree all do), and can grant a burst quota of ~1–2K TPS per drop.
- The waiting room admits ~10K users/s per region at peak; each admitted user makes ~2 reservation-path calls.
- A hold lasts 10 min; one active hold per user per SKU; per-account purchase limits.
- General-admission units in the prototype. Reserved seating uses the same engine with one key per seat (proposal §2.4).
- Measured ~800 rps per production pod is assumed for capacity math. It is lower than the laptop numbers below, to account for TLS, tracing and noisy neighbours.

## Repository layout

```
cmd/
  reservation-api/   HTTP service (+ reconciler and order consumer in-process for the prototype)
  loadgen/           drop simulator: seeds a SKU, bursts users through reserve -> pay, checks oversell
internal/
  inventory/         sharded Redis inventory, Lua scripts for every hold transition
  admission/         waiting-room token mint/verify (HMAC)
  idempotency/       Idempotency-Key store (pending/done, fingerprinted)
  payment/           PSP interface, fault-injecting fake PSP, resilient client
  resilience/        jittered backoff, retry budget, circuit breaker, bulkhead
  orders/            outbox consumer (XREADGROUP/XAUTOCLAIM), idempotent order store
  reconcile/         expiry sweep + unknown-payment resolution
  api/               HTTP handlers + end-to-end correctness test
deploy/
  k8s/               Deployment, PDB, KEDA ScaledObject (leading signals + pre-warm cron)
  sql/schema.sql     system-of-record schema and reconciliation queries
loadtest/k6/         spike / stress / soak profiles
docs/                proposal, ADRs, runbook, diagrams (.mmd source + .png)
```

## Running it

Requirements: Go 1.26+, and either Docker or a local `redis-server`.

```bash
cp .env.example .env
# replace both placeholders, e.g. with: openssl rand -hex 32

make test                # vet + all tests with -race (uses in-memory Redis, no setup needed)

make up                  # docker compose: redis + reservation-api on :8080
make loadtest            # 30k users vs 2k units, prints latency and the oversell check
make k6                  # optional: k6 spike profile (SCALE=0.01 by default)
make down
```

Without Docker: start `redis-server`, then `make run` in one terminal and
`make loadtest` in another.

### API

| Method & path | Auth | Notes |
|---|---|---|
| `GET /v1/skus/{sku}/availability` | none | CDN-cacheable, approximate |
| `POST /v1/skus/{sku}/holds` | `X-Admission-Token`, `Idempotency-Key` | `201` hold, `409` sold out |
| `GET /v1/holds/{id}` / `DELETE /v1/holds/{id}` | token | owner only |
| `POST /v1/holds/{id}/pay` | token, `Idempotency-Key` | `200` confirmed, `202` pending, `402` declined, `410` expired, `503` retry |
| `POST /admin/skus` / `GET /admin/skus/{sku}/stats` | `X-Admin-Token` | seed a drop / counts |
| `GET /debug/vars` | none | counters (reserve_ok, sold_out, shed_*, pay_*) |

## Results from the prototype

All on one laptop (Apple Silicon, single API process, local Redis):

| Run | Result |
|---|---|
| `TestConcurrentReserveNeverOversells` | 3,000 concurrent reservations, 500 units across 8 shards → exactly 500 holds |
| `TestDropUnderFlakyPSP` | 400 users, 100 units, PSP with 30% transient + 30% "timeout after charge" + 10% declines, two pay attempts per user with *different* client keys → charges == orders, 0 double charges, all unknown outcomes resolved by the reconciler |
| `loadgen` 30k users / 2k units / 300 concurrent | 2,000 holds, 28,000 clean sold-outs, 0 oversell; reserve p50 9.6 ms, p99 59 ms; pay p99 205 ms (80 ms simulated PSP latency); shed payments succeeded on jittered retry |

## Prototype vs. production

The prototype implements the correctness-critical core. These parts are
deliberately simplified:

- **In-memory order store** instead of PostgreSQL. It enforces the same
  idempotency and per-bucket capacity rules as `deploy/sql/schema.sql`.
- **The order consumer reads the Redis outbox directly.** In production a
  relay forwards to Kafka.
- **Fake PSP** with fault injection; no webhooks.
- **The reconciler and consumer run in the API process.** In production they
  are separate deployments, and the reconciler uses leader election.
- **Single Redis node.** Keys already use cluster hash tags, so the scripts
  run unchanged on Redis Cluster or MemoryDB.
- **No waiting room, WAF or CDN.** Admission tokens are minted by `loadgen`
  and the tests using the shared secret.
- **No admission-key rotation, purchase limits or one-hold-per-user rule.**
  They are covered in the proposal.

## Secrets

No real secrets are in this repo or its history. `.env` is git-ignored and
`.env.example` contains placeholders only. Test files use obviously fake
constants. Kubernetes manifests reference a `Secret` populated from an
external secret store.
