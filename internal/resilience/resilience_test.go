package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

func TestBreakerOpensAndRecovers(t *testing.T) {
	now := time.Now()
	b := NewBreaker(3, time.Second)
	b.Now = func() time.Time { return now }

	for range 3 {
		if err := b.Allow(); err != nil {
			t.Fatal(err)
		}
		b.Record(errBoom)
	}
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatalf("want open, got %v", err)
	}

	now = now.Add(2 * time.Second)
	if err := b.Allow(); err != nil {
		t.Fatalf("probe should be allowed: %v", err)
	}
	if err := b.Allow(); !errors.Is(err, ErrOpen) {
		t.Fatal("only one probe allowed in half-open")
	}
	b.Record(nil)
	if b.State() != "closed" {
		t.Fatalf("state = %s", b.State())
	}
}

func TestBreakerIgnoresPermanentErrors(t *testing.T) {
	b := NewBreaker(1, time.Second)
	_ = b.Allow()
	b.Record(Permanent(errBoom))
	if b.State() != "closed" {
		t.Fatalf("state = %s", b.State())
	}
}

func TestRetryStopsOnPermanentAndBudget(t *testing.T) {
	ctx := context.Background()
	bo := Backoff{Base: time.Microsecond, Max: time.Millisecond, MaxAttempts: 5}

	calls := 0
	err := Retry(ctx, bo, nil, func(context.Context) error { calls++; return Permanent(errBoom) })
	if calls != 1 || !IsPermanent(err) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}

	budget := NewRetryBudget(0.1, 2)
	calls = 0
	err = Retry(ctx, bo, budget, func(context.Context) error { calls++; return errBoom })
	if !errors.Is(err, ErrBudgetExhausted) || calls != 3 {
		t.Fatalf("calls=%d err=%v (budget of 2 retries => 3 calls)", calls, err)
	}
}

func TestBackoffBounded(t *testing.T) {
	bo := Backoff{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond}
	for a := range 40 {
		if d := bo.Delay(a); d < 0 || d > bo.Max {
			t.Fatalf("attempt %d delay %v out of range", a, d)
		}
	}
}

func TestBulkheadSheds(t *testing.T) {
	b := NewBulkhead("x", 1)
	if !b.TryAcquire() {
		t.Fatal("first acquire")
	}
	if b.TryAcquire() {
		t.Fatal("second acquire should shed")
	}
	b.Release()
	if !b.TryAcquire() || b.Shed() != 1 {
		t.Fatalf("shed=%d", b.Shed())
	}
}
