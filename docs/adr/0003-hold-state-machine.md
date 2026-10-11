# ADR-0003: TTL holds with an explicit payment state machine

- Status: Accepted
- Date: 2026-10-11

## Context

Users need time to pay (address, 3DS, wallet). Abandoned carts must return
stock. Last drop, a user could be charged while their reservation expired and
was resold.

## Decision

A hold is a hash with an explicit status, mutated only by atomic scripts:

```
held ──pay──▶ paying ──charge ok──▶ confirmed  (+ outbox entry, same script)
 │  ◀─declined / not sent─┘  │
 │                           └─reconciler: charge not found after deadline──▶ released
 ├──user cancel──▶ released
 └──TTL sweep────▶ expired
```

- `RESERVE` creates `held` with `expires_at` and indexes it in a per-shard
  expiry zset; the sweeper returns `held` holds past expiry to stock.
- `BEGIN_PAYMENT` (`held → paying`) runs **before** the PSP call and removes
  the hold from the expiry index, so a hold cannot expire mid-charge.
  `paying` holds are indexed by a payment deadline set well beyond the PSP
  timeout (2 min vs. 3 s × 3 attempts).
- Declines and "never sent" (breaker open, PSP limit) revert to `held` with
  the original expiry, so the user can retry.
- Unknown outcomes stay `paying`; the reconciler looks up `charge:<hold_id>`
  at the PSP after the deadline and confirms or releases.

Expiry uses explicit zsets instead of key TTLs or keyspace notifications,
which are fire-and-forget and lost on disconnect or failover.

## Consequences

- "Reservation expired mid-payment" and "charged without order" are
  eliminated on the synchronous path and reconciled on the asynchronous one.
- A unit can sit in `paying` for up to the payment deadline during a PSP
  outage. This is acceptable; it is also bounded and visible ("oldest
  paying age" metric).
- Every transition is idempotent: repeating it returns the same state.
