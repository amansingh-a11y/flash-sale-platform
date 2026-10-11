# Runbook: live incident during a drop

Roles: **Incident Commander (IC)** decides; **Ops lead** executes levers;
**Comms lead** owns status page, social and support; **Scribe** keeps the
timeline in the incident channel. The IC is named before every tier-1 drop.

## 0. First 2 minutes

1. Open `#inc-drop-<event>`; IC announces themself.
2. Open the drop dashboard. Check in this order:
   - **Correctness panel**: oversell invariant, duplicate charges, charge-without-order count. *Any non-zero → go to §3 immediately.*
   - Reservation success rate and p99; error-budget burn.
   - Waiting room: depth, admit rate.
   - Saturation: bulkhead shed rate per pool, Redis CPU/ops per shard, PSP breaker state and latency, outbox/Kafka **age**.
3. Decide: **slow down** (§1), **degrade** (§2), or **stop** (§3).

## 1. Slow down: reduce admitted load

| Lever | How | Effect |
|---|---|---|
| Lower waiting-room release rate | Waiting-room console / `flags set wr.<event>.rate=<n>` | Origin load drops within seconds; users wait longer instead of getting errors |
| Manual scale-up | `kubectl -n checkout scale deploy/reservation-api --replicas=<n>` or raise the KEDA `minReplicaCount` | Use when saturation is CPU/pod-bound and Redis/PSP have headroom |
| Shorten hold TTL | `flags set holds.ttl=5m` | Recycles abandoned holds faster when sell-through is slow |

## 2. Degrade: shed non-essential work

| Lever | Flag | When |
|---|---|---|
| Disable recommendations / history / reviews | `features.nonessential=off` | Any saturation |
| Availability from CDN only (serve stale) | `availability.origin=off` | Read pool shedding |
| Payment "pending" mode: accept the hold and charge asynchronously with the PSP queue | `pay.mode=async` | PSP latency > 3 s or breaker flapping |
| Switch PSP / acquirer | `psp.primary=<b>` | PSP outage > 2 min (if a second acquirer exists) |
| Extend holds during PSP outage | `holds.extend=+10m` | PSP down, so users don't lose their units |

## 3. Stop: kill switches

| Kill switch | Flag | Use when |
|---|---|---|
| Pause admissions for event | `wr.<event>.paused=true` (room shows banner) | Correctness alert, Redis primary failover, home-region problem |
| Pause one SKU | `sku.<sku>.paused=true` | Problem isolated to one SKU |
| Force sold-out display | `sku.<sku>.display=soldout` | After pausing, to stop retries |

### 3a. Suspected oversell or duplicate charge
1. Pause the SKU (or event). Don't reopen until the counts reconcile.
2. Run the reconciliation queries now (bottom of `deploy/sql/schema.sql`) for the SKU instead of waiting for the next scheduled run.
3. Oversold orders are auto-refunded by the order writer; confirm in the PSP dashboard.
4. Compare Redis per-shard confirmed counts with Postgres `inventory_bucket.sold`. If Redis drifted, rebuild:
   `stock_i = capacity_i − sold_i (Postgres) − live holds_i`.
5. Reopen only after the IC and the payments owner sign off.

### 3b. Redis primary failover during a drop
1. Pause admissions for affected SKUs (automatic if Redis errors > 1% for 30 s).
2. MemoryDB: confirm failover completed; acknowledged writes are durable, so resume.
3. ElastiCache: rebuild counts from Postgres (as above), then resume.

## 4. Communication

- **T+5 min** of user impact: status page "Investigating" + waiting-room banner ("High demand. Your place in line is safe.").
- Every **15 min**: internal update in channel (impact, actions, next update time).
- Support macros: "charged but no ticket" → "Resolved automatically within 10 minutes, either by a confirmed order or a full refund."
- Social: one message per material change; no ETAs we can't keep.
- After resolution: status page "Resolved", then customer email for affected users (refunds, compensation) within 24 h.

## 5. After the incident

Blameless review within 3 business days: timeline, what the dashboards
showed, which levers worked, action items with owners. Update this runbook
and the game-day scenario list.
