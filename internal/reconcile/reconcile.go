// Package reconcile closes the gaps the synchronous path can leave behind:
// expired holds that still hold stock, and holds whose charge outcome is
// unknown (PSP timed out). It is what turns "charged but no order" into either
// a confirmed order or a released hold, never both and never neither.
package reconcile

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/amansingh-a11y/flash-sale-platform/internal/inventory"
	"github.com/amansingh-a11y/flash-sale-platform/internal/payment"
)

type Reconciler struct {
	Inventory *inventory.Store
	PSP       payment.PSP
	// RetryAfter pushes the deadline out when the PSP can't tell us yet.
	RetryAfter time.Duration
	Log        *slog.Logger

	Stats struct {
		Expired, Confirmed, Released, Deferred int
	}
}

func (r *Reconciler) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			r.Log.Error("reconcile", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (r *Reconciler) RunOnce(ctx context.Context) error {
	n, err := r.Inventory.Sweep(ctx, 500)
	r.Stats.Expired += n
	if err != nil {
		return err
	}
	stuck, err := r.Inventory.StuckPayments(ctx, 200)
	if err != nil {
		return err
	}
	for _, id := range stuck {
		r.resolve(ctx, id)
	}
	return nil
}

// The payment deadline is set well beyond the PSP's own timeout, so by the
// time a hold is "stuck" a NotFound from the PSP means the charge never landed.
func (r *Reconciler) resolve(ctx context.Context, holdID string) {
	charge, err := r.PSP.Lookup(ctx, payment.ChargeKey(holdID))
	switch {
	case err == nil:
		if err := r.Inventory.Confirm(ctx, holdID, charge.ID, charge.AmountMinor); err != nil &&
			!errors.Is(err, inventory.ErrAlreadyConfirmed) {
			r.Log.Error("reconcile confirm", "hold", holdID, "err", err)
			return
		}
		r.Stats.Confirmed++
	case errors.Is(err, payment.ErrNotFound):
		if err := r.Inventory.Release(ctx, holdID, "", true); err != nil &&
			!errors.Is(err, inventory.ErrHoldInactive) {
			r.Log.Error("reconcile release", "hold", holdID, "err", err)
			return
		}
		r.Stats.Released++
	default:
		_ = r.Inventory.ExtendPayment(ctx, holdID, time.Now().Add(r.RetryAfter))
		r.Stats.Deferred++
	}
}
