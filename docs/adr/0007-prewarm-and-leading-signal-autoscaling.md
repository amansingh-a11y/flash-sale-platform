# ADR-0007: Pre-warm for scheduled drops; autoscale on leading signals

- Status: Accepted
- Date: 2026-10-11

## Context

CPU-based HPA reacted minutes after the last spike peaked. Spikes are
partly scheduled (drops) and partly not (viral, bots).

## Decision

- **Scheduled:** a drop-calendar controller raises replica floors at T-30 min
  (KEDA cron trigger), warms CDN, connection pools and Kafka consumers,
  confirms PSP burst quota, and holds capacity until T+90 min. Scale-down is
  slow (10%/min after a 10-min window).
- **Unscheduled:** KEDA triggers on request rate per pod, waiting-room admit
  rate and in-flight requests, with 0 s scale-up stabilisation and up to
  +200% per 15 s. CPU is kept as a backstop only. A warm floor sized for 5×
  normal traffic plus low-priority placeholder pods keeps spare nodes ready,
  so new pods schedule in seconds.
- The waiting room auto-enables on latency or admit-rate thresholds to cover
  the remaining lag.

## Consequences

- Extra cost of pre-warming is small compared with the cost of a failed drop.
- Requires load-tested per-pod knees to set targets (measured in the stress
  profile, re-measured each release).
- See `deploy/k8s/reservation-api.yaml`.
