// Package api is the reservation service's HTTP surface. Handlers are thin:
// admission check, bulkhead, idempotency, then one inventory/payment call.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"expvar"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"github.com/amansingh-a11y/flash-sale-platform/internal/admission"
	"github.com/amansingh-a11y/flash-sale-platform/internal/idempotency"
	"github.com/amansingh-a11y/flash-sale-platform/internal/inventory"
	"github.com/amansingh-a11y/flash-sale-platform/internal/orders"
	"github.com/amansingh-a11y/flash-sale-platform/internal/payment"
	"github.com/amansingh-a11y/flash-sale-platform/internal/resilience"
)

type Server struct {
	Inventory       *inventory.Store
	Idempotency     *idempotency.Store
	PSP             payment.PSP
	Orders          *orders.MemoryStore
	AdmissionSecret []byte
	AdminToken      string
	PaymentDeadline time.Duration
	Log             *slog.Logger

	ReadPool    *resilience.Bulkhead
	ReservePool *resilience.Bulkhead
	PayPool     *resilience.Bulkhead

	// Ready is flipped to false at shutdown so the LB drains us before we stop.
	Ready func() bool
	Ping  func(context.Context) error
}

var metrics = expvar.NewMap("reservation")

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /debug/vars", expvar.Handler())

	mux.Handle("GET /v1/skus/{sku}/availability", s.shed(s.ReadPool, http.HandlerFunc(s.availability)))
	mux.Handle("POST /v1/skus/{sku}/holds", s.shed(s.ReservePool, s.admitted(s.reserve)))
	mux.Handle("GET /v1/holds/{id}", s.shed(s.ReadPool, s.admitted(s.getHold)))
	mux.Handle("DELETE /v1/holds/{id}", s.shed(s.ReservePool, s.admitted(s.releaseHold)))
	mux.Handle("POST /v1/holds/{id}/pay", s.shed(s.PayPool, s.admitted(s.pay)))

	mux.HandleFunc("POST /admin/skus", s.admin(s.seed))
	mux.HandleFunc("GET /admin/skus/{sku}/stats", s.admin(s.stats))
	return mux
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if (s.Ready != nil && !s.Ready()) || (s.Ping != nil && s.Ping(r.Context()) != nil) {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type apiError struct {
	Code    string `json:"error"`
	Message string `json:"message,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// retryAfter is jittered so shed clients don't come back in lockstep.
func retryAfter(w http.ResponseWriter, base int) {
	w.Header().Set("Retry-After", strconv.Itoa(base+rand.IntN(base+1)))
}

// shed rejects immediately when the pool is full. Cheap rejection keeps
// latency flat for admitted work instead of queueing everyone into timeouts.
func (s *Server) shed(pool *resilience.Bulkhead, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !pool.TryAcquire() {
			metrics.Add("shed_"+pool.Name, 1)
			retryAfter(w, 1)
			writeJSON(w, http.StatusServiceUnavailable, apiError{Code: "overloaded"})
			return
		}
		defer pool.Release()
		next.ServeHTTP(w, r)
	})
}

type ctxKey struct{}

func userFrom(r *http.Request) string { return r.Context().Value(ctxKey{}).(string) }

// admitted requires a valid waiting-room token. The user ID comes from the
// token, never from the request body, so it can't be spoofed.
func (s *Server) admitted(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := admission.Verify(s.AdmissionSecret, r.Header.Get("X-Admission-Token"), time.Now())
		if err != nil {
			metrics.Add("admission_rejected", 1)
			writeJSON(w, http.StatusUnauthorized, apiError{Code: "not_admitted", Message: err.Error()})
			return
		}
		h(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, c.UserID)))
	})
}

func (s *Server) admin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Token")), []byte(s.AdminToken)) != 1 {
			writeJSON(w, http.StatusUnauthorized, apiError{Code: "unauthorized"})
			return
		}
		h(w, r)
	}
}

type result struct {
	status    int
	body      any
	retryable bool // don't persist; let the client retry with the same key
}

// idempotent replays the stored response for a repeated Idempotency-Key and
// otherwise runs fn once. Keys are scoped per user and operation.
func (s *Server) idempotent(w http.ResponseWriter, r *http.Request, op, fingerprint string, fn func() result) {
	k := r.Header.Get("Idempotency-Key")
	if k == "" || len(k) > 128 {
		writeJSON(w, http.StatusBadRequest, apiError{Code: "idempotency_key_required"})
		return
	}
	scoped := op + ":" + userFrom(r) + ":" + k
	rec, err := s.Idempotency.Begin(r.Context(), scoped, fingerprint)
	switch {
	case errors.Is(err, idempotency.ErrInProgress):
		retryAfter(w, 1)
		writeJSON(w, http.StatusConflict, apiError{Code: "request_in_progress"})
		return
	case errors.Is(err, idempotency.ErrMismatch):
		writeJSON(w, http.StatusUnprocessableEntity, apiError{Code: "idempotency_key_reused"})
		return
	case err != nil:
		s.Log.Error("idempotency begin", "err", err)
		retryAfter(w, 1)
		writeJSON(w, http.StatusServiceUnavailable, apiError{Code: "unavailable"})
		return
	case rec != nil:
		metrics.Add("idempotent_replay", 1)
		w.Header().Set("Idempotent-Replayed", "true")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.Status)
		_, _ = w.Write(rec.Body)
		return
	}

	res := fn()
	body, _ := json.Marshal(res.body)
	if res.retryable {
		_ = s.Idempotency.Abandon(r.Context(), scoped)
	} else if err := s.Idempotency.Complete(r.Context(), scoped, fingerprint, res.status, body); err != nil {
		s.Log.Error("idempotency complete", "err", err)
	}
	if res.status == http.StatusServiceUnavailable {
		retryAfter(w, 2)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(res.status)
	_, _ = w.Write(append(body, '\n'))
}

func (s *Server) availability(w http.ResponseWriter, r *http.Request) {
	sku := r.PathValue("sku")
	n, err := s.Inventory.AvailableCached(r.Context(), sku)
	switch {
	case errors.Is(err, inventory.ErrUnknownSKU):
		writeJSON(w, http.StatusNotFound, apiError{Code: "unknown_sku"})
		return
	case err != nil:
		writeJSON(w, http.StatusServiceUnavailable, apiError{Code: "unavailable"})
		return
	}
	// Lets the CDN coalesce millions of polls into ~1 origin request/sec/PoP.
	w.Header().Set("Cache-Control", "public, max-age=1, stale-while-revalidate=2, stale-if-error=30")
	writeJSON(w, http.StatusOK, map[string]any{"sku": sku, "available": n, "approximate": true})
}

func (s *Server) reserve(w http.ResponseWriter, r *http.Request) {
	sku := r.PathValue("sku")
	s.idempotent(w, r, "reserve", idempotency.Fingerprint(sku), func() result {
		h, err := s.Inventory.Reserve(r.Context(), sku, userFrom(r))
		switch {
		case err == nil:
			metrics.Add("reserve_ok", 1)
			return result{status: http.StatusCreated, body: h}
		case errors.Is(err, inventory.ErrSoldOut):
			metrics.Add("reserve_sold_out", 1)
			return result{status: http.StatusConflict, body: apiError{Code: "sold_out"}}
		case errors.Is(err, inventory.ErrUnknownSKU), errors.Is(err, inventory.ErrInvalidSKU):
			return result{status: http.StatusNotFound, body: apiError{Code: "unknown_sku"}}
		default:
			metrics.Add("reserve_error", 1)
			s.Log.Error("reserve", "err", err)
			return result{status: http.StatusServiceUnavailable, body: apiError{Code: "unavailable"}, retryable: true}
		}
	})
}

func (s *Server) ownedHold(w http.ResponseWriter, r *http.Request) (inventory.Hold, bool) {
	h, err := s.Inventory.Get(r.Context(), r.PathValue("id"))
	if err == nil && h.UserID != userFrom(r) {
		err = inventory.ErrHoldNotFound
	}
	switch {
	case err == nil:
		return h, true
	case errors.Is(err, inventory.ErrHoldNotFound), errors.Is(err, inventory.ErrInvalidHoldID):
		writeJSON(w, http.StatusNotFound, apiError{Code: "hold_not_found"})
	default:
		writeJSON(w, http.StatusServiceUnavailable, apiError{Code: "unavailable"})
	}
	return inventory.Hold{}, false
}

func (s *Server) getHold(w http.ResponseWriter, r *http.Request) {
	if h, ok := s.ownedHold(w, r); ok {
		writeJSON(w, http.StatusOK, h)
	}
}

func (s *Server) releaseHold(w http.ResponseWriter, r *http.Request) {
	err := s.Inventory.Release(r.Context(), r.PathValue("id"), userFrom(r), false)
	switch {
	case err == nil, errors.Is(err, inventory.ErrHoldInactive):
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, inventory.ErrHoldNotFound), errors.Is(err, inventory.ErrNotOwner), errors.Is(err, inventory.ErrInvalidHoldID):
		writeJSON(w, http.StatusNotFound, apiError{Code: "hold_not_found"})
	case errors.Is(err, inventory.ErrPaymentInFlight), errors.Is(err, inventory.ErrAlreadyConfirmed):
		writeJSON(w, http.StatusConflict, apiError{Code: "hold_not_releasable"})
	default:
		writeJSON(w, http.StatusServiceUnavailable, apiError{Code: "unavailable"})
	}
}

type orderResponse struct {
	OrderID  string `json:"order_id"`
	Status   string `json:"status"`
	ChargeID string `json:"charge_id,omitempty"`
}

// pay charges the PSP with an idempotency key derived from the hold, not from
// the client's key, so even a client that changes keys can't be charged twice
// for one hold.
func (s *Server) pay(w http.ResponseWriter, r *http.Request) {
	holdID := r.PathValue("id")
	hold, ok := s.ownedHold(w, r)
	if !ok {
		return
	}
	s.idempotent(w, r, "pay", idempotency.Fingerprint(holdID), func() result {
		return s.doPay(r.Context(), hold)
	})
}

func (s *Server) doPay(ctx context.Context, hold inventory.Hold) result {
	unavailable := result{status: http.StatusServiceUnavailable, body: apiError{Code: "unavailable"}, retryable: true}

	err := s.Inventory.BeginPayment(ctx, hold.ID, hold.UserID, time.Now().Add(s.PaymentDeadline))
	switch {
	case errors.Is(err, inventory.ErrAlreadyConfirmed):
		h, _ := s.Inventory.Get(ctx, hold.ID)
		return result{status: http.StatusOK, body: orderResponse{OrderID: hold.ID, Status: "confirmed", ChargeID: h.ChargeID}}
	case errors.Is(err, inventory.ErrHoldInactive):
		return result{status: http.StatusGone, body: apiError{Code: "hold_expired"}}
	case err != nil:
		s.Log.Error("begin payment", "err", err)
		return unavailable
	}

	meta, err := s.Inventory.Meta(ctx, hold.SKU)
	if err != nil {
		_ = s.Inventory.AbortPayment(ctx, hold.ID)
		return unavailable
	}
	charge, err := s.PSP.Charge(ctx, payment.ChargeRequest{
		IdempotencyKey: payment.ChargeKey(hold.ID), UserID: hold.UserID,
		AmountMinor: meta.PriceMinor, Currency: meta.Currency,
	})
	switch {
	case err == nil:
		if err := s.Inventory.Confirm(ctx, hold.ID, charge.ID, charge.AmountMinor); err != nil &&
			!errors.Is(err, inventory.ErrAlreadyConfirmed) {
			// Charged but not confirmed: the hold stays "paying" and the
			// reconciler confirms it from the PSP record.
			s.Log.Error("confirm after charge", "hold", hold.ID, "err", err)
			metrics.Add("pay_pending", 1)
			return result{status: http.StatusAccepted, body: orderResponse{OrderID: hold.ID, Status: "payment_pending"}}
		}
		metrics.Add("pay_ok", 1)
		return result{status: http.StatusOK, body: orderResponse{OrderID: hold.ID, Status: "confirmed", ChargeID: charge.ID}}
	case errors.Is(err, payment.ErrDeclined):
		_ = s.Inventory.AbortPayment(ctx, hold.ID)
		metrics.Add("pay_declined", 1)
		return result{status: http.StatusPaymentRequired, body: apiError{Code: "payment_declined"}}
	case errors.Is(err, payment.ErrNotAttempted):
		_ = s.Inventory.AbortPayment(ctx, hold.ID)
		metrics.Add("pay_not_attempted", 1)
		return unavailable
	default:
		// Outcome unknown. Leave the hold in "paying" (it can't expire) and
		// let the reconciler resolve it against the PSP.
		metrics.Add("pay_pending", 1)
		return result{status: http.StatusAccepted, body: orderResponse{OrderID: hold.ID, Status: "payment_pending"}}
	}
}

type seedRequest struct {
	SKU        string `json:"sku"`
	Total      int    `json:"total"`
	Shards     int    `json:"shards"`
	PriceMinor int64  `json:"price_minor"`
	Currency   string `json:"currency"`
}

func (s *Server) seed(w http.ResponseWriter, r *http.Request) {
	var req seedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, apiError{Code: "bad_request", Message: err.Error()})
		return
	}
	m, err := s.Inventory.Seed(r.Context(), req.SKU, req.Total, req.Shards, req.PriceMinor, req.Currency)
	switch {
	case errors.Is(err, inventory.ErrAlreadySeeded):
		writeJSON(w, http.StatusConflict, apiError{Code: "already_seeded"})
		return
	case err != nil:
		writeJSON(w, http.StatusBadRequest, apiError{Code: "bad_request", Message: err.Error()})
		return
	}
	for i, c := range m.Capacities {
		s.Orders.SetCapacity(req.SKU, i, c)
	}
	writeJSON(w, http.StatusCreated, map[string]any{"sku": req.SKU, "shards": m.Shards, "capacities": m.Capacities})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	sku := r.PathValue("sku")
	m, err := s.Inventory.Meta(r.Context(), sku)
	if err != nil {
		writeJSON(w, http.StatusNotFound, apiError{Code: "unknown_sku"})
		return
	}
	avail, err := s.Inventory.Available(r.Context(), sku)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, apiError{Code: "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sku": sku, "total": m.TotalUnits, "available": avail, "orders": s.Orders.Count(sku),
	})
}
