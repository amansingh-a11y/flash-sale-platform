# ADR-0006: Multi-region with inventory homed per drop (CP writes, AP reads)

- Status: Accepted
- Date: 2026-10-11

## Context

Users are global; the SLO is 99.95% with p99 < 500 ms on reservation. A
unit of inventory must never be sold in two regions.

## Options

1. **Single global inventory with synchronous cross-region consensus** (Spanner/CockroachDB-style). Strong, but every reservation pays a quorum round-trip (100–200 ms+), and the system is complex to operate at this write rate.
2. **Multi-master Redis (CRDT/active-active).** Counters converge eventually, which allows oversell during partitions. Rejected.
3. **Home region per drop**: inventory lives in one region; other regions forward reservation writes. ✅
4. **Per-region quotas**: split units across regions, rebalance periodically. ✅ for global drops.

## Decision

Default to option 3: each drop's inventory is homed in the region nearest its
audience. Other regions serve reads locally and forward reserve/pay to the
home region over the backbone (+70–150 ms, within budget). For global drops
use option 4: allocate quotas per region from demand forecasts (waiting-room
registrations), and every few seconds move unsold units from regions that are
draining slower to regions that are draining faster. Moves are atomic
take-from-A then give-to-B, so each unit has one owner at any moment.

Under partition, writes choose consistency: a region that can't reach a SKU's
home pauses that SKU in its waiting room. Reads and browsing stay available.

## Consequences

- No cross-region oversell by construction.
- A home-region outage pauses that drop for the failover RTO (< 5 min),
  using MemoryDB snapshots plus count rebuild from Aurora Global.
- Quota mode can leave a few units unsold in one region while another sells
  out; the rebalancer bounds this to seconds.
