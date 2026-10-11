# ADR-0005: End-to-end idempotency; PSP key derived from the hold

- Status: Accepted
- Date: 2026-10-11

## Context

Users were charged twice when retries (client, browser refresh, LB, service)
re-executed a charge whose first attempt had actually succeeded but timed out.

## Decision

1. Every mutating API requires an `Idempotency-Key`. The record is scoped by
   `op:user:key`, stores a request fingerprint (reuse with a different request
   → 422), is `pending` while executing (concurrent duplicate → 409 +
   `Retry-After`) and stores the final response (replayed with
   `Idempotent-Replayed: true`). Retryable failures (503) delete the record so
   the same key can run again.
2. The PSP idempotency key is **derived server-side**: `charge:<hold_id>`.
   It does not depend on the client's key, so a client that sends different
   keys, a reconciler lookup and a webhook all converge on one charge per hold.
3. Refunds use `refund:<order_id>`.
4. PSP calls go through: bulkhead sized to the PSP rate limit → circuit
   breaker (declines don't count as failures) → per-attempt timeout → jittered
   retries limited by a 10% retry budget. Errors are classified as
   *declined* (permanent), *not attempted* (safe to revert the hold) or
   *transient/unknown* (leave `paying`, reconcile).

## Consequences

- Retrying is always safe, at every layer.
- The idempotency store is on the hot path (one `SETNX` + one `SET` per
  mutation); it lives in Redis with 24 h TTL.
- Tested in `internal/api/e2e_test.go`: 30% transient and 30% "timeout after
  charge" injected, two pay attempts per user with different client keys →
  charges == orders, zero refunds.
