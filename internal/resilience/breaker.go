package resilience

import (
	"errors"
	"sync"
	"time"
)

var ErrOpen = errors.New("resilience: circuit open")

type breakerState int

const (
	closed breakerState = iota
	open
	halfOpen
)

// Breaker opens after Threshold consecutive failures, fails fast for Cooldown,
// then lets a single probe through (half-open) to test recovery.
type Breaker struct {
	mu        sync.Mutex
	state     breakerState
	failures  int
	openUntil time.Time
	probing   bool

	Threshold int
	Cooldown  time.Duration
	Now       func() time.Time
}

func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{Threshold: threshold, Cooldown: cooldown, Now: time.Now}
}

func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case open:
		if b.Now().Before(b.openUntil) {
			return ErrOpen
		}
		b.state = halfOpen
		b.probing = true
		return nil
	case halfOpen:
		if b.probing {
			return ErrOpen
		}
		b.probing = true
		return nil
	}
	return nil
}

// Record must be called exactly once for every Allow that returned nil.
// Permanent errors (business failures like a declined card) don't count
// against the dependency's health.
func (b *Breaker) Record(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	if err == nil || IsPermanent(err) {
		b.state = closed
		b.failures = 0
		return
	}
	b.failures++
	if b.state == halfOpen || b.failures >= b.Threshold {
		b.state = open
		b.openUntil = b.Now().Add(b.Cooldown)
	}
}

func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return [...]string{"closed", "open", "half-open"}[b.state]
}
