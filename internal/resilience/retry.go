// Package resilience holds the small primitives used between services:
// jittered retries with a retry budget, a circuit breaker, and bulkheads.
package resilience

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// Backoff uses "full jitter": sleep = rand(0, min(Max, Base*2^attempt)).
// Jitter de-synchronises clients so retries don't arrive as a second spike.
type Backoff struct {
	Base        time.Duration
	Max         time.Duration
	MaxAttempts int
}

func (b Backoff) Delay(attempt int) time.Duration {
	d := b.Base << attempt
	if d <= 0 || d > b.Max {
		d = b.Max
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent marks an error as not worth retrying (e.g. card declined, 4xx).
func Permanent(err error) error { return permanentError{err} }

func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

var ErrBudgetExhausted = errors.New("resilience: retry budget exhausted")

// RetryBudget caps retries to a fraction of first attempts (e.g. 10%), so a
// dependency outage can't multiply load by MaxAttempts.
type RetryBudget struct {
	mu     sync.Mutex
	tokens float64
	ratio  float64
	max    float64
}

func NewRetryBudget(ratio float64, maxTokens float64) *RetryBudget {
	return &RetryBudget{ratio: ratio, max: maxTokens, tokens: maxTokens}
}

func (r *RetryBudget) deposit() {
	r.mu.Lock()
	r.tokens = min(r.max, r.tokens+r.ratio)
	r.mu.Unlock()
}

func (r *RetryBudget) withdraw() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tokens < 1 {
		return false
	}
	r.tokens--
	return true
}

// Retry runs fn until it succeeds, returns a Permanent error, attempts run
// out, the budget is exhausted, or ctx is done. Only call it around operations
// that are idempotent.
func Retry(ctx context.Context, b Backoff, budget *RetryBudget, fn func(context.Context) error) error {
	if budget != nil {
		budget.deposit()
	}
	var err error
	for attempt := 0; attempt < b.MaxAttempts; attempt++ {
		if attempt > 0 {
			if budget != nil && !budget.withdraw() {
				return errors.Join(ErrBudgetExhausted, err)
			}
			t := time.NewTimer(b.Delay(attempt))
			select {
			case <-ctx.Done():
				t.Stop()
				return errors.Join(ctx.Err(), err)
			case <-t.C:
			}
		}
		if err = fn(ctx); err == nil || IsPermanent(err) {
			return err
		}
	}
	return err
}
