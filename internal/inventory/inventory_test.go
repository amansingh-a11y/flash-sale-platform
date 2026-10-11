package inventory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T, opts Options) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, opts), mr
}

func TestConcurrentReserveNeverOversells(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t, Options{SoldOutCooldown: time.Nanosecond})
	const stock, shards, users = 500, 8, 3000
	if _, err := s.Seed(ctx, "drop1", stock, shards, 1000, "USD"); err != nil {
		t.Fatal(err)
	}

	var ok, soldOut atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64)
	for i := 0; i < users; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			_, err := s.Reserve(ctx, "drop1", "u")
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrSoldOut):
				soldOut.Add(1)
			default:
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if ok.Load() != stock {
		t.Fatalf("reserved %d, want exactly %d", ok.Load(), stock)
	}
	if soldOut.Load() != users-stock {
		t.Fatalf("sold out %d, want %d", soldOut.Load(), users-stock)
	}
	if n, _ := s.Available(ctx, "drop1"); n != 0 {
		t.Fatalf("available = %d, want 0", n)
	}
}

func TestExpiredHoldsReturnToStock(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t, Options{HoldTTL: time.Minute, SoldOutCooldown: time.Nanosecond})
	now := time.Now()
	s.now = func() time.Time { return now }
	if _, err := s.Seed(ctx, "drop2", 2, 2, 1000, "USD"); err != nil {
		t.Fatal(err)
	}
	h1, err := s.Reserve(ctx, "drop2", "alice")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := s.Reserve(ctx, "drop2", "bob")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BeginPayment(ctx, h2.ID, "bob", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	now = now.Add(2 * time.Minute)
	n, err := s.Sweep(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("swept %d, want 1 (paying holds must not expire)", n)
	}
	if got, _ := s.Get(ctx, h1.ID); got.Status != StatusExpired {
		t.Fatalf("h1 status = %s", got.Status)
	}
	if got, _ := s.Get(ctx, h2.ID); got.Status != StatusPaying {
		t.Fatalf("h2 status = %s", got.Status)
	}
	if err := s.Confirm(ctx, h1.ID, "ch_x", 1000); !errors.Is(err, ErrHoldInactive) {
		t.Fatalf("confirm expired hold: %v", err)
	}
	if avail, _ := s.Available(ctx, "drop2"); avail != 1 {
		t.Fatalf("available = %d, want 1", avail)
	}
}

func TestConfirmIsIdempotentAndWritesOutboxOnce(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t, Options{})
	if _, err := s.Seed(ctx, "drop3", 1, 1, 1000, "USD"); err != nil {
		t.Fatal(err)
	}
	h, err := s.Reserve(ctx, "drop3", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BeginPayment(ctx, h.ID, "mallory", time.Now().Add(time.Minute)); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("foreign begin payment: %v", err)
	}
	if err := s.Confirm(ctx, h.ID, "ch_1", 1000); err != nil {
		t.Fatal(err)
	}
	if err := s.Confirm(ctx, h.ID, "ch_1", 1000); !errors.Is(err, ErrAlreadyConfirmed) {
		t.Fatalf("second confirm: %v", err)
	}
	if err := s.Release(ctx, h.ID, "alice", false); !errors.Is(err, ErrAlreadyConfirmed) {
		t.Fatalf("release confirmed: %v", err)
	}
	n, err := s.rdb.XLen(ctx, OutboxKey("drop3", h.Shard)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("outbox len = %d, want 1", n)
	}
}

func TestReleaseRules(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t, Options{})
	if _, err := s.Seed(ctx, "drop4", 1, 1, 1000, "USD"); err != nil {
		t.Fatal(err)
	}
	h, _ := s.Reserve(ctx, "drop4", "alice")
	if err := s.BeginPayment(ctx, h.ID, "alice", time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, h.ID, "alice", false); !errors.Is(err, ErrPaymentInFlight) {
		t.Fatalf("user release while paying: %v", err)
	}
	if err := s.Release(ctx, h.ID, "", true); err != nil {
		t.Fatalf("reconciler release: %v", err)
	}
	if avail, _ := s.Available(ctx, "drop4"); avail != 1 {
		t.Fatalf("available = %d, want 1", avail)
	}
}

func TestSeedRejectsReseed(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t, Options{})
	if _, err := s.Seed(ctx, "drop5", 10, 2, 1, "USD"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Seed(ctx, "drop5", 10, 2, 1, "USD"); !errors.Is(err, ErrAlreadySeeded) {
		t.Fatalf("reseed: %v", err)
	}
}

func TestParseHoldID(t *testing.T) {
	sku, shard, err := ParseHoldID(newHoldID("abc-1", 7))
	if err != nil || sku != "abc-1" || shard != 7 {
		t.Fatalf("got %q %d %v", sku, shard, err)
	}
	for _, bad := range []string{"", "h.x", "h.a b.1.ff", "h.x.-1.ff", "x.sku.1.ff"} {
		if _, _, err := ParseHoldID(bad); err == nil {
			t.Errorf("%q: expected error", bad)
		}
	}
}
