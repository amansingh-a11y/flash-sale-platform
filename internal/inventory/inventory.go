// Package inventory implements the hot-path reservation store: per-SKU stock is
// split across N Redis shards so a single popular item never becomes one hot key,
// and every state transition is a single atomic Lua script.
package inventory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

var (
	ErrSoldOut          = errors.New("inventory: sold out")
	ErrUnknownSKU       = errors.New("inventory: unknown sku")
	ErrAlreadySeeded    = errors.New("inventory: sku already seeded")
	ErrInvalidSKU       = errors.New("inventory: invalid sku")
	ErrInvalidHoldID    = errors.New("inventory: invalid hold id")
	ErrHoldNotFound     = errors.New("inventory: hold not found")
	ErrNotOwner         = errors.New("inventory: hold belongs to another user")
	ErrHoldInactive     = errors.New("inventory: hold expired or released")
	ErrAlreadyConfirmed = errors.New("inventory: hold already confirmed")
	ErrPaymentInFlight  = errors.New("inventory: payment in flight")
)

const (
	StatusHeld      = "held"
	StatusPaying    = "paying"
	StatusConfirmed = "confirmed"
	StatusReleased  = "released"
	StatusExpired   = "expired"
)

var skuPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type Hold struct {
	ID        string    `json:"hold_id"`
	SKU       string    `json:"sku"`
	Shard     int       `json:"-"`
	UserID    string    `json:"-"`
	Status    string    `json:"status"`
	ExpiresAt time.Time `json:"expires_at"`
	ChargeID  string    `json:"charge_id,omitempty"`
}

type SKUMeta struct {
	Shards     int
	PriceMinor int64
	Currency   string
	Capacities []int
	TotalUnits int
}

type ShardRef struct {
	SKU   string
	Shard int
}

type Options struct {
	HoldTTL         time.Duration
	TerminalTTL     time.Duration
	SoldOutCooldown time.Duration
	AvailabilityTTL time.Duration
}

type Store struct {
	rdb  redis.UniversalClient
	opts Options
	now  func() time.Time

	meta    sync.Map // sku -> SKUMeta (immutable once seeded)
	soldOut sync.Map // sku -> time.Time until which reserve short-circuits

	availGroup singleflight.Group
	availMu    sync.Mutex
	availCache map[string]availEntry
}

type availEntry struct {
	value int64
	at    time.Time
}

func New(rdb redis.UniversalClient, opts Options) *Store {
	if opts.HoldTTL == 0 {
		opts.HoldTTL = 10 * time.Minute
	}
	if opts.TerminalTTL == 0 {
		opts.TerminalTTL = 24 * time.Hour
	}
	if opts.SoldOutCooldown == 0 {
		opts.SoldOutCooldown = 500 * time.Millisecond
	}
	if opts.AvailabilityTTL == 0 {
		opts.AvailabilityTTL = 250 * time.Millisecond
	}
	return &Store{rdb: rdb, opts: opts, now: time.Now, availCache: map[string]availEntry{}}
}

func tag(sku string, shard int) string { return fmt.Sprintf("{inv:%s:%d}", sku, shard) }

func stockKey(sku string, shard int) string   { return tag(sku, shard) + ":stock" }
func expiryKey(sku string, shard int) string  { return tag(sku, shard) + ":expiry" }
func payingKey(sku string, shard int) string  { return tag(sku, shard) + ":paying" }
func holdPrefix(sku string, shard int) string { return tag(sku, shard) + ":hold:" }
func OutboxKey(sku string, shard int) string  { return tag(sku, shard) + ":outbox" }
func metaKey(sku string) string               { return "inv:meta:" + sku }

const skuSetKey = "inv:skus"

func holdKey(id string, sku string, shard int) string { return holdPrefix(sku, shard) + id }

// Hold IDs embed the SKU and shard so any hold operation is a single-slot call.
func newHoldID(sku string, shard int) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("h.%s.%d.%s", sku, shard, hex.EncodeToString(b[:]))
}

func ParseHoldID(id string) (sku string, shard int, err error) {
	parts := strings.Split(id, ".")
	if len(parts) != 4 || parts[0] != "h" || !skuPattern.MatchString(parts[1]) {
		return "", 0, ErrInvalidHoldID
	}
	shard, err = strconv.Atoi(parts[2])
	if err != nil || shard < 0 {
		return "", 0, ErrInvalidHoldID
	}
	return parts[1], shard, nil
}

// Seed distributes total units evenly across shards. Re-seeding a live SKU is
// refused so a stray admin call cannot reset counts mid-drop.
func (s *Store) Seed(ctx context.Context, sku string, total, shards int, priceMinor int64, currency string) (SKUMeta, error) {
	if !skuPattern.MatchString(sku) {
		return SKUMeta{}, ErrInvalidSKU
	}
	if total <= 0 || shards <= 0 || shards > total {
		return SKUMeta{}, fmt.Errorf("inventory: need 0 < shards <= total, got shards=%d total=%d", shards, total)
	}
	ok, err := s.rdb.HSetNX(ctx, metaKey(sku), "shards", shards).Result()
	if err != nil {
		return SKUMeta{}, err
	}
	if !ok {
		return SKUMeta{}, ErrAlreadySeeded
	}
	caps := make([]int, shards)
	for i := range caps {
		caps[i] = total / shards
		if i < total%shards {
			caps[i]++
		}
	}
	pipe := s.rdb.Pipeline()
	for i, c := range caps {
		pipe.Set(ctx, stockKey(sku, i), c, 0)
	}
	pipe.HSet(ctx, metaKey(sku), "price_minor", priceMinor, "currency", currency, "total", total)
	pipe.SAdd(ctx, skuSetKey, sku)
	if _, err := pipe.Exec(ctx); err != nil {
		return SKUMeta{}, err
	}
	m := SKUMeta{Shards: shards, PriceMinor: priceMinor, Currency: currency, Capacities: caps, TotalUnits: total}
	s.meta.Store(sku, m)
	return m, nil
}

func (s *Store) Meta(ctx context.Context, sku string) (SKUMeta, error) {
	if v, ok := s.meta.Load(sku); ok {
		return v.(SKUMeta), nil
	}
	vals, err := s.rdb.HGetAll(ctx, metaKey(sku)).Result()
	if err != nil {
		return SKUMeta{}, err
	}
	shards, _ := strconv.Atoi(vals["shards"])
	if shards == 0 {
		return SKUMeta{}, ErrUnknownSKU
	}
	price, _ := strconv.ParseInt(vals["price_minor"], 10, 64)
	total, _ := strconv.Atoi(vals["total"])
	m := SKUMeta{Shards: shards, PriceMinor: price, Currency: vals["currency"], TotalUnits: total}
	s.meta.Store(sku, m)
	return m, nil
}

// Reserve takes one unit, starting at a random shard so load spreads evenly and
// falling through to the others when a shard is empty.
func (s *Store) Reserve(ctx context.Context, sku, userID string) (Hold, error) {
	if until, ok := s.soldOut.Load(sku); ok && s.now().Before(until.(time.Time)) {
		return Hold{}, ErrSoldOut
	}
	m, err := s.Meta(ctx, sku)
	if err != nil {
		return Hold{}, err
	}
	expires := s.now().Add(s.opts.HoldTTL)
	start := mrand.IntN(m.Shards)
	for k := 0; k < m.Shards; k++ {
		shard := (start + k) % m.Shards
		id := newHoldID(sku, shard)
		got, err := reserveScript.Run(ctx, s.rdb,
			[]string{stockKey(sku, shard), expiryKey(sku, shard), holdKey(id, sku, shard)},
			id, userID, expires.UnixMilli()).Int()
		if err != nil {
			return Hold{}, err
		}
		if got == 1 {
			return Hold{ID: id, SKU: sku, Shard: shard, UserID: userID, Status: StatusHeld, ExpiresAt: expires}, nil
		}
	}
	// Units can return via expiry/cancel, so this is a short cooldown, not a permanent flag.
	s.soldOut.Store(sku, s.now().Add(s.opts.SoldOutCooldown))
	return Hold{}, ErrSoldOut
}

func (s *Store) Get(ctx context.Context, holdID string) (Hold, error) {
	sku, shard, err := ParseHoldID(holdID)
	if err != nil {
		return Hold{}, err
	}
	vals, err := s.rdb.HGetAll(ctx, holdKey(holdID, sku, shard)).Result()
	if err != nil {
		return Hold{}, err
	}
	if len(vals) == 0 {
		return Hold{}, ErrHoldNotFound
	}
	ms, _ := strconv.ParseInt(vals["expires_at"], 10, 64)
	return Hold{
		ID: holdID, SKU: sku, Shard: shard, UserID: vals["user"], Status: vals["status"],
		ExpiresAt: time.UnixMilli(ms), ChargeID: vals["charge_id"],
	}, nil
}

func mapResult(res string) error {
	switch res {
	case "ok":
		return nil
	case "missing":
		return ErrHoldNotFound
	case "not_owner":
		return ErrNotOwner
	case "already", StatusConfirmed:
		return ErrAlreadyConfirmed
	case StatusPaying:
		return ErrPaymentInFlight
	case StatusReleased, StatusExpired:
		return ErrHoldInactive
	default:
		return fmt.Errorf("inventory: unexpected script result %q", res)
	}
}

// BeginPayment moves held -> paying, which removes the hold from the expiry
// sweep. It is idempotent for a hold that is already paying.
func (s *Store) BeginPayment(ctx context.Context, holdID, userID string, deadline time.Time) error {
	sku, shard, err := ParseHoldID(holdID)
	if err != nil {
		return err
	}
	res, err := beginPaymentScript.Run(ctx, s.rdb,
		[]string{holdKey(holdID, sku, shard), expiryKey(sku, shard), payingKey(sku, shard)},
		holdID, userID, deadline.UnixMilli()).Text()
	if err != nil {
		return err
	}
	return mapResult(res)
}

func (s *Store) AbortPayment(ctx context.Context, holdID string) error {
	sku, shard, err := ParseHoldID(holdID)
	if err != nil {
		return err
	}
	res, err := abortPaymentScript.Run(ctx, s.rdb,
		[]string{holdKey(holdID, sku, shard), expiryKey(sku, shard), payingKey(sku, shard)},
		holdID).Text()
	if err != nil {
		return err
	}
	if res == StatusHeld {
		return nil
	}
	return mapResult(res)
}

// Confirm returns ErrAlreadyConfirmed (not a failure) on repeat calls.
func (s *Store) Confirm(ctx context.Context, holdID, chargeID string, amountMinor int64) error {
	sku, shard, err := ParseHoldID(holdID)
	if err != nil {
		return err
	}
	res, err := confirmScript.Run(ctx, s.rdb,
		[]string{holdKey(holdID, sku, shard), expiryKey(sku, shard), payingKey(sku, shard), OutboxKey(sku, shard)},
		holdID, chargeID, amountMinor, s.opts.TerminalTTL.Milliseconds(), sku, shard).Text()
	if err != nil {
		return err
	}
	return mapResult(res)
}

// Release returns the unit to stock. userID == "" skips the owner check
// (reconciler); allowPaying lets the reconciler release a hold whose charge
// was confirmed not to exist.
func (s *Store) Release(ctx context.Context, holdID, userID string, allowPaying bool) error {
	sku, shard, err := ParseHoldID(holdID)
	if err != nil {
		return err
	}
	allow := "0"
	if allowPaying {
		allow = "1"
	}
	res, err := releaseScript.Run(ctx, s.rdb,
		[]string{holdKey(holdID, sku, shard), expiryKey(sku, shard), payingKey(sku, shard), stockKey(sku, shard)},
		holdID, userID, allow, s.opts.TerminalTTL.Milliseconds()).Text()
	if err != nil {
		return err
	}
	if res == "ok" {
		s.soldOut.Delete(sku)
	}
	return mapResult(res)
}

func (s *Store) Shards(ctx context.Context) ([]ShardRef, error) {
	skus, err := s.rdb.SMembers(ctx, skuSetKey).Result()
	if err != nil {
		return nil, err
	}
	var refs []ShardRef
	for _, sku := range skus {
		m, err := s.Meta(ctx, sku)
		if err != nil {
			return nil, err
		}
		for i := 0; i < m.Shards; i++ {
			refs = append(refs, ShardRef{SKU: sku, Shard: i})
		}
	}
	return refs, nil
}

// Sweep expires holds past their TTL and returns the units to stock.
func (s *Store) Sweep(ctx context.Context, limitPerShard int) (int, error) {
	refs, err := s.Shards(ctx)
	if err != nil {
		return 0, err
	}
	now := s.now().UnixMilli()
	total := 0
	for _, r := range refs {
		n, err := sweepScript.Run(ctx, s.rdb,
			[]string{expiryKey(r.SKU, r.Shard), stockKey(r.SKU, r.Shard)},
			now, limitPerShard, holdPrefix(r.SKU, r.Shard), s.opts.TerminalTTL.Milliseconds()).Int()
		if err != nil {
			return total, err
		}
		if n > 0 {
			s.soldOut.Delete(r.SKU)
		}
		total += n
	}
	return total, nil
}

// StuckPayments lists holds whose payment deadline has passed without a
// confirm/abort, i.e. the charge outcome is unknown and must be reconciled.
func (s *Store) StuckPayments(ctx context.Context, limitPerShard int) ([]string, error) {
	refs, err := s.Shards(ctx)
	if err != nil {
		return nil, err
	}
	now := strconv.FormatInt(s.now().UnixMilli(), 10)
	var ids []string
	for _, r := range refs {
		got, err := s.rdb.ZRangeByScore(ctx, payingKey(r.SKU, r.Shard), &redis.ZRangeBy{
			Min: "-inf", Max: now, Count: int64(limitPerShard),
		}).Result()
		if err != nil {
			return nil, err
		}
		ids = append(ids, got...)
	}
	return ids, nil
}

func (s *Store) ExtendPayment(ctx context.Context, holdID string, deadline time.Time) error {
	sku, shard, err := ParseHoldID(holdID)
	if err != nil {
		return err
	}
	return s.rdb.ZAddXX(ctx, payingKey(sku, shard), redis.Z{Score: float64(deadline.UnixMilli()), Member: holdID}).Err()
}

// Available is exact at read time; AvailableCached is the read-path variant
// (stale by up to AvailabilityTTL, one Redis round-trip per SKU per instance).
func (s *Store) Available(ctx context.Context, sku string) (int64, error) {
	m, err := s.Meta(ctx, sku)
	if err != nil {
		return 0, err
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, m.Shards)
	for i := range cmds {
		cmds[i] = pipe.Get(ctx, stockKey(sku, i))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return 0, err
	}
	var sum int64
	for _, c := range cmds {
		n, _ := c.Int64()
		sum += n
	}
	return sum, nil
}

func (s *Store) AvailableCached(ctx context.Context, sku string) (int64, error) {
	s.availMu.Lock()
	e, ok := s.availCache[sku]
	s.availMu.Unlock()
	if ok && s.now().Sub(e.at) < s.opts.AvailabilityTTL {
		return e.value, nil
	}
	v, err, _ := s.availGroup.Do(sku, func() (any, error) {
		n, err := s.Available(ctx, sku)
		if err != nil {
			return int64(0), err
		}
		s.availMu.Lock()
		s.availCache[sku] = availEntry{value: n, at: s.now()}
		s.availMu.Unlock()
		return n, nil
	})
	return v.(int64), err
}
