// Command reservation-api runs the reservation service plus, for the
// prototype, the reconciler and order consumer in the same process. In
// production those run as separate deployments (see docs/proposal.md).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/amansingh-a11y/flash-sale-platform/internal/api"
	"github.com/amansingh-a11y/flash-sale-platform/internal/config"
	"github.com/amansingh-a11y/flash-sale-platform/internal/idempotency"
	"github.com/amansingh-a11y/flash-sale-platform/internal/inventory"
	"github.com/amansingh-a11y/flash-sale-platform/internal/orders"
	"github.com/amansingh-a11y/flash-sale-platform/internal/payment"
	"github.com/amansingh-a11y/flash-sale-platform/internal/reconcile"
	"github.com/amansingh-a11y/flash-sale-platform/internal/resilience"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr, Password: cfg.RedisPassword,
		PoolSize: 256, DialTimeout: time.Second, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond,
	})
	defer rdb.Close()

	inv := inventory.New(rdb, inventory.Options{HoldTTL: cfg.HoldTTL})
	fake := payment.NewFakePSP()
	fake.Latency, fake.TransientRate, fake.AmbiguousRate, fake.DeclineRate =
		cfg.PSPLatency, cfg.PSPTransientRate, cfg.PSPAmbiguousRate, cfg.PSPDeclineRate
	psp := &payment.Resilient{
		PSP:            fake,
		Breaker:        resilience.NewBreaker(20, 5*time.Second),
		Bulkhead:       resilience.NewBulkhead("psp", cfg.PSPMaxConcurrency),
		Backoff:        resilience.Backoff{Base: 50 * time.Millisecond, Max: time.Second, MaxAttempts: 3},
		Budget:         resilience.NewRetryBudget(0.1, 50),
		AttemptTimeout: cfg.PSPAttemptTimeout,
	}
	store := orders.NewMemoryStore()

	var ready atomic.Bool
	ready.Store(true)
	srv := &api.Server{
		Inventory:       inv,
		Idempotency:     idempotency.New(rdb, 30*time.Second, 24*time.Hour),
		PSP:             psp,
		Orders:          store,
		AdmissionSecret: cfg.AdmissionSecret,
		AdminToken:      cfg.AdminToken,
		PaymentDeadline: cfg.PaymentDeadline,
		Log:             log,
		ReadPool:        resilience.NewBulkhead("read", cfg.MaxInflightRead),
		ReservePool:     resilience.NewBulkhead("reserve", cfg.MaxInflightReserve),
		PayPool:         resilience.NewBulkhead("pay", cfg.MaxInflightPay),
		Ready:           ready.Load,
		Ping:            func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	host, _ := os.Hostname()
	consumer := &orders.Consumer{
		RDB: rdb, Inventory: inv, Store: store, Group: "order-writer", Name: host,
		MinIdle: 30 * time.Second, Batch: 200, Log: log,
		OnOversell: func(ctx context.Context, o orders.Order) {
			log.Error("OVERSELL detected, refunding", "order", o.OrderID, "sku", o.SKU)
			if err := psp.Refund(ctx, payment.RefundKey(o.OrderID), o.ChargeID); err != nil {
				log.Error("refund failed", "order", o.OrderID, "err", err)
			}
		},
	}
	rec := &reconcile.Reconciler{Inventory: inv, PSP: psp, RetryAfter: 30 * time.Second, Log: log}
	go consumer.Run(ctx, 200*time.Millisecond)
	go rec.Run(ctx, cfg.ReconcileInterval)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           http.TimeoutHandler(srv.Handler(), 10*time.Second, `{"error":"timeout"}`),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	// Fail readiness first so the load balancer stops routing, then drain.
	ready.Store(false)
	time.Sleep(5 * time.Second)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	log.Info("stopped")
}
