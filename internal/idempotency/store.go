// Package idempotency stores the first response for a client-supplied
// Idempotency-Key so retries (client, LB, or mobile network) replay the same
// result instead of re-executing the side effect.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrInProgress = errors.New("idempotency: request with this key is in progress")
	ErrMismatch   = errors.New("idempotency: key reused with a different request")
)

type Record struct {
	State       string `json:"state"` // pending | done
	Fingerprint string `json:"fp"`
	Status      int    `json:"status,omitempty"`
	Body        []byte `json:"body,omitempty"`
}

type Store struct {
	rdb        redis.UniversalClient
	pendingTTL time.Duration
	doneTTL    time.Duration
}

func New(rdb redis.UniversalClient, pendingTTL, doneTTL time.Duration) *Store {
	return &Store{rdb: rdb, pendingTTL: pendingTTL, doneTTL: doneTTL}
}

func Fingerprint(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func key(k string) string { return "idem:" + k }

// Begin claims the key. If it returns a non-nil Record, the caller must replay
// it instead of executing. A crash after Begin leaves a pending record that
// expires after pendingTTL, after which the client may retry.
func (s *Store) Begin(ctx context.Context, k, fingerprint string) (*Record, error) {
	b, _ := json.Marshal(Record{State: "pending", Fingerprint: fingerprint})
	ok, err := s.rdb.SetNX(ctx, key(k), b, s.pendingTTL).Result()
	if err != nil {
		return nil, err
	}
	if ok {
		return nil, nil
	}
	raw, err := s.rdb.Get(ctx, key(k)).Bytes()
	if errors.Is(err, redis.Nil) {
		return s.Begin(ctx, k, fingerprint)
	}
	if err != nil {
		return nil, err
	}
	var r Record
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	if r.Fingerprint != fingerprint {
		return nil, ErrMismatch
	}
	if r.State != "done" {
		return nil, ErrInProgress
	}
	return &r, nil
}

func (s *Store) Complete(ctx context.Context, k, fingerprint string, status int, body []byte) error {
	b, _ := json.Marshal(Record{State: "done", Fingerprint: fingerprint, Status: status, Body: body})
	return s.rdb.Set(ctx, key(k), b, s.doneTTL).Err()
}

// Abandon frees the key after a retryable failure (e.g. load shed) so the
// client's retry with the same key executes again.
func (s *Store) Abandon(ctx context.Context, k string) error {
	return s.rdb.Del(ctx, key(k)).Err()
}
