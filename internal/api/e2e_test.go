package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/amansingh/flash-sale-platform/internal/admission"
	"github.com/amansingh/flash-sale-platform/internal/idempotency"
	"github.com/amansingh/flash-sale-platform/internal/inventory"
	"github.com/amansingh/flash-sale-platform/internal/orders"
	"github.com/amansingh/flash-sale-platform/internal/payment"
	"github.com/amansingh/flash-sale-platform/internal/reconcile"
	"github.com/amansingh/flash-sale-platform/internal/resilience"
)

var (
	testSecret = []byte("test-secret-not-for-prod-0123456789")
	testAdmin  = "test-admin-token-0123456789"
)

type harness struct {
	srv   *httptest.Server
	inv   *inventory.Store
	psp   *payment.FakePSP
	store *orders.MemoryStore
	cons  *orders.Consumer
	rec   *reconcile.Reconciler
}

func newHarness(t *testing.T, fake *payment.FakePSP) *harness {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), PoolSize: 64})
	t.Cleanup(func() { _ = rdb.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	inv := inventory.New(rdb, inventory.Options{SoldOutCooldown: time.Nanosecond})
	psp := &payment.Resilient{
		PSP:            fake,
		Breaker:        resilience.NewBreaker(1000, time.Second),
		Bulkhead:       resilience.NewBulkhead("psp", 1000),
		Backoff:        resilience.Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond, MaxAttempts: 2},
		AttemptTimeout: time.Second,
	}
	store := orders.NewMemoryStore()
	s := &Server{
		Inventory:       inv,
		Idempotency:     idempotency.New(rdb, 30*time.Second, time.Hour),
		PSP:             psp,
		Orders:          store,
		AdmissionSecret: testSecret,
		AdminToken:      testAdmin,
		PaymentDeadline: 0, // every unresolved payment is immediately reconcilable
		Log:             log,
		ReadPool:        resilience.NewBulkhead("read", 1000),
		ReservePool:     resilience.NewBulkhead("reserve", 1000),
		PayPool:         resilience.NewBulkhead("pay", 1000),
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return &harness{
		srv: srv, inv: inv, psp: fake, store: store,
		cons: &orders.Consumer{RDB: rdb, Inventory: inv, Store: store, Group: "orders", Name: "t", MinIdle: time.Minute, Batch: 500, Log: log},
		rec:  &reconcile.Reconciler{Inventory: inv, PSP: psp, RetryAfter: time.Second, Log: log},
	}
}

func (h *harness) do(t *testing.T, method, path, user, idemKey string, body any) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, h.srv.URL+path, rdr)
	if user != "" {
		tok, err := admission.Mint(testSecret, admission.Claims{UserID: user, EventID: "e", Expires: time.Now().Add(time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Admission-Token", tok)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	req.Header.Set("X-Admin-Token", testAdmin)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// The core correctness test: a flaky PSP (transient failures, timeouts after
// charging, declines) plus duplicate client retries must never produce an
// oversell, a double charge, or a charge without an order.
func TestDropUnderFlakyPSP(t *testing.T) {
	fake := payment.NewFakePSP()
	fake.TransientRate, fake.AmbiguousRate, fake.DeclineRate = 0.3, 0.3, 0.1
	h := newHarness(t, fake)
	const stock, users = 100, 400

	if code, _ := h.do(t, "POST", "/admin/skus", "", "", map[string]any{
		"sku": "drop", "total": stock, "shards": 8, "price_minor": 4999, "currency": "USD",
	}); code != http.StatusCreated {
		t.Fatalf("seed: %d", code)
	}

	var reserved, soldOut, paid, pending, declined atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 48)
	for i := range users {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			user := fmt.Sprintf("u%d", i)
			code, body := h.do(t, "POST", "/v1/skus/drop/holds", user, "r-"+user, nil)
			// A duplicate retry with the same key must replay, not reserve again.
			code2, body2 := h.do(t, "POST", "/v1/skus/drop/holds", user, "r-"+user, nil)
			if code != code2 || body["hold_id"] != body2["hold_id"] {
				t.Errorf("replay mismatch: %d/%v vs %d/%v", code, body, code2, body2)
			}
			switch code {
			case http.StatusConflict:
				soldOut.Add(1)
				return
			case http.StatusCreated:
				reserved.Add(1)
			default:
				t.Errorf("reserve: %d %v", code, body)
				return
			}
			holdID := body["hold_id"].(string)
			// Two pay attempts with *different* client keys: still one charge.
			for attempt := range 2 {
				code, body := h.do(t, "POST", "/v1/holds/"+holdID+"/pay", user, fmt.Sprintf("p-%s-%d", user, attempt), nil)
				switch code {
				case http.StatusOK:
					paid.Add(1)
					return
				case http.StatusAccepted:
					pending.Add(1)
					return
				case http.StatusPaymentRequired:
					declined.Add(1)
				case http.StatusServiceUnavailable:
				default:
					t.Errorf("pay: %d %v", code, body)
					return
				}
			}
		}()
	}
	wg.Wait()

	if reserved.Load() != stock || soldOut.Load() != users-stock {
		t.Fatalf("reserved=%d soldOut=%d", reserved.Load(), soldOut.Load())
	}

	ctx := context.Background()
	h.psp.TransientRate, h.psp.AmbiguousRate = 0, 0 // PSP recovers
	if err := h.rec.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cons.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}

	charged, refunded := h.psp.Stats()
	nOrders := h.store.Count("drop")
	t.Logf("paid=%d pending=%d declined=%d reconciler=%+v charges=%d orders=%d",
		paid.Load(), pending.Load(), declined.Load(), h.rec.Stats, charged, nOrders)

	if nOrders > stock {
		t.Fatalf("oversold: %d orders for %d units", nOrders, stock)
	}
	if charged != nOrders || refunded != 0 {
		t.Fatalf("charges=%d refunded=%d orders=%d: every charge must map to exactly one order", charged, refunded, nOrders)
	}
	if pending.Load() > 0 && h.rec.Stats.Confirmed+h.rec.Stats.Released == 0 {
		t.Fatal("reconciler did not resolve pending payments")
	}
	stuck, _ := h.inv.StuckPayments(ctx, 1000)
	if len(stuck) != 0 {
		t.Fatalf("%d payments still unresolved", len(stuck))
	}
	avail, _ := h.inv.Available(ctx, "drop")
	if int(avail)+nOrders > stock {
		t.Fatalf("available(%d) + orders(%d) > stock(%d)", avail, nOrders, stock)
	}
}

func TestAuthAndOwnership(t *testing.T) {
	h := newHarness(t, payment.NewFakePSP())
	h.do(t, "POST", "/admin/skus", "", "", map[string]any{"sku": "d", "total": 5, "shards": 1, "price_minor": 1, "currency": "USD"})

	if code, _ := h.do(t, "POST", "/v1/skus/d/holds", "", "k", nil); code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", code)
	}
	if code, _ := h.do(t, "POST", "/v1/skus/d/holds", "alice", "", nil); code != http.StatusBadRequest {
		t.Fatalf("no idempotency key: %d", code)
	}
	_, body := h.do(t, "POST", "/v1/skus/d/holds", "alice", "k1", nil)
	holdID := body["hold_id"].(string)
	if code, _ := h.do(t, "POST", "/v1/holds/"+holdID+"/pay", "mallory", "k2", nil); code != http.StatusNotFound {
		t.Fatalf("foreign pay: %d", code)
	}
	if code, _ := h.do(t, "DELETE", "/v1/holds/"+holdID, "mallory", "", nil); code != http.StatusNotFound {
		t.Fatalf("foreign release: %d", code)
	}
	if code, _ := h.do(t, "DELETE", "/v1/holds/"+holdID, "alice", "", nil); code != http.StatusNoContent {
		t.Fatalf("release: %d", code)
	}
	if code, _ := h.do(t, "POST", "/v1/holds/"+holdID+"/pay", "alice", "k3", nil); code != http.StatusGone {
		t.Fatalf("pay released hold: %d", code)
	}
}
