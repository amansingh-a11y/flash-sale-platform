# ADR-0002: Edge virtual waiting room (bought) with HMAC admission tokens

- Status: Accepted
- Date: 2026-10-11

## Context

Traffic goes from 2K to 500K+ rps in 30 s; up to 10M users wait for one drop.
No autoscaler reacts in 30 s, and the reservation tier has a real ceiling
(Redis ops, PSP rate limit). We need to convert an unbounded spike into a
controlled flow and protect fairness from bots.

## Options

1. **Autoscale only.** Too slow (minutes) and caps out at downstream limits anyway.
2. **Rate-limit with 429s.** Bounds load but turns into a refresh/retry storm and is unfair (fast clients win).
3. **Virtual waiting room at the edge**, admitting at a configurable rate with signed tokens. ✅
   - 3a. Build on edge compute (Workers/Durable Objects, Lambda@Edge + DynamoDB).
   - 3b. Buy (Cloudflare Waiting Room, Queue-it, Akamai, AWS Virtual Waiting Room solution).

## Decision

Option 3b with a thin, vendor-neutral contract: the room issues a short-lived
token `base64url(user|event|exp).base64url(HMAC-SHA256)`; the gateway and
service verify it with no I/O (`internal/admission`). The user ID is taken
from the token, never from request bodies. Users present at open get random
positions; later arrivals are FIFO per edge region.

## Consequences

- Origin load equals the release rate, which is our main control knob and is
  adjustable live.
- The room absorbs autoscaling lag for unscheduled spikes (auto-enabled on
  latency/admit thresholds).
- Vendor outage: fail *closed* (pause admissions). A secondary configuration
  stays ready for tier-1 drops.
- The HMAC secret is shared between the vendor and us; rotate with a key ID
  and two active keys (not in the prototype).
