// Package payment wraps the third-party PSP. Every charge carries an
// idempotency key derived from the hold ID, so however many times we retry
// (client retries, our retries, reconciler lookups) a hold is charged at most once.
package payment

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amansingh-a11y/flash-sale-platform/internal/resilience"
)

var (
	// ErrDeclined is a business failure: retrying won't help.
	ErrDeclined = errors.New("payment: declined")
	// ErrTransient means the outcome is unknown (timeout, 5xx); the charge may
	// or may not have been applied.
	ErrTransient = errors.New("payment: transient failure")
	// ErrNotAttempted means we never sent the request (breaker open, local
	// rate limit), so it is safe to release the hold back to the user.
	ErrNotAttempted = errors.New("payment: not attempted")
	ErrNotFound     = errors.New("payment: charge not found")
)

func ChargeKey(holdID string) string  { return "charge:" + holdID }
func RefundKey(orderID string) string { return "refund:" + orderID }

type ChargeRequest struct {
	IdempotencyKey string
	UserID         string
	AmountMinor    int64
	Currency       string
}

type Charge struct {
	ID          string
	AmountMinor int64
	Refunded    bool
}

type PSP interface {
	Charge(ctx context.Context, req ChargeRequest) (Charge, error)
	Lookup(ctx context.Context, idempotencyKey string) (Charge, error)
	Refund(ctx context.Context, idempotencyKey, chargeID string) error
}

// FakePSP simulates a provider with latency, transient errors and the nasty
// case of a timeout *after* the charge was applied.
type FakePSP struct {
	Latency       time.Duration
	TransientRate float64 // fail before applying the charge
	AmbiguousRate float64 // apply the charge, then report a timeout
	DeclineRate   float64

	mu      sync.Mutex
	charges map[string]Charge
	seq     atomic.Int64
	Calls   atomic.Int64
}

func NewFakePSP() *FakePSP { return &FakePSP{charges: map[string]Charge{}} }

func (f *FakePSP) sleep(ctx context.Context) error {
	if f.Latency == 0 {
		return nil
	}
	t := time.NewTimer(f.Latency)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", ErrTransient, ctx.Err())
	case <-t.C:
		return nil
	}
}

func (f *FakePSP) Charge(ctx context.Context, req ChargeRequest) (Charge, error) {
	f.Calls.Add(1)
	if err := f.sleep(ctx); err != nil {
		return Charge{}, err
	}
	if rand.Float64() < f.TransientRate {
		return Charge{}, ErrTransient
	}
	f.mu.Lock()
	c, exists := f.charges[req.IdempotencyKey]
	if !exists {
		if rand.Float64() < f.DeclineRate {
			f.mu.Unlock()
			return Charge{}, ErrDeclined
		}
		c = Charge{ID: fmt.Sprintf("ch_%d", f.seq.Add(1)), AmountMinor: req.AmountMinor}
		f.charges[req.IdempotencyKey] = c
	}
	f.mu.Unlock()
	if !exists && rand.Float64() < f.AmbiguousRate {
		return Charge{}, ErrTransient
	}
	return c, nil
}

func (f *FakePSP) Lookup(ctx context.Context, key string) (Charge, error) {
	if err := f.sleep(ctx); err != nil {
		return Charge{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.charges[key]
	if !ok {
		return Charge{}, ErrNotFound
	}
	return c, nil
}

func (f *FakePSP) Refund(ctx context.Context, _ string, chargeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, c := range f.charges {
		if c.ID == chargeID {
			c.Refunded = true
			f.charges[k] = c
			return nil
		}
	}
	return ErrNotFound
}

// Stats returns (charges applied, charges refunded).
func (f *FakePSP) Stats() (charged, refunded int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.charges {
		charged++
		if c.Refunded {
			refunded++
		}
	}
	return
}

// Resilient puts a bulkhead (the PSP's rate limit), a circuit breaker,
// per-attempt timeouts and budgeted jittered retries in front of a PSP.
type Resilient struct {
	PSP            PSP
	Breaker        *resilience.Breaker
	Bulkhead       *resilience.Bulkhead
	Backoff        resilience.Backoff
	Budget         *resilience.RetryBudget
	AttemptTimeout time.Duration
}

func (r *Resilient) call(ctx context.Context, fn func(context.Context) error) error {
	if !r.Bulkhead.TryAcquire() {
		return fmt.Errorf("%w: psp concurrency limit", ErrNotAttempted)
	}
	defer r.Bulkhead.Release()

	attempted := false
	err := resilience.Retry(ctx, r.Backoff, r.Budget, func(ctx context.Context) error {
		if err := r.Breaker.Allow(); err != nil {
			return err
		}
		attempted = true
		actx, cancel := context.WithTimeout(ctx, r.AttemptTimeout)
		defer cancel()
		err := fn(actx)
		if errors.Is(err, ErrDeclined) || errors.Is(err, ErrNotFound) {
			err = resilience.Permanent(err)
		}
		r.Breaker.Record(err)
		return err
	})
	if err != nil && !attempted {
		return fmt.Errorf("%w: %v", ErrNotAttempted, err)
	}
	return err
}

func (r *Resilient) Charge(ctx context.Context, req ChargeRequest) (Charge, error) {
	var c Charge
	err := r.call(ctx, func(ctx context.Context) (err error) {
		c, err = r.PSP.Charge(ctx, req)
		return err
	})
	return c, err
}

func (r *Resilient) Lookup(ctx context.Context, key string) (Charge, error) {
	var c Charge
	err := r.call(ctx, func(ctx context.Context) (err error) {
		c, err = r.PSP.Lookup(ctx, key)
		return err
	})
	return c, err
}

func (r *Resilient) Refund(ctx context.Context, key, chargeID string) error {
	return r.call(ctx, func(ctx context.Context) error { return r.PSP.Refund(ctx, key, chargeID) })
}
