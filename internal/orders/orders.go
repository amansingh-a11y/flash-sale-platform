// Package orders drains the per-shard outbox streams written atomically by
// inventory.Confirm and persists orders into the system of record.
//
// Delivery is at-least-once (consumer groups + XAUTOCLAIM for crashed
// consumers); exactly-once *effects* come from Store.Upsert being idempotent on
// order_id. The store also enforces per-shard capacity as the last line of
// defence against oversell (e.g. after a Redis failover lost writes).
package orders

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/amansingh-a11y/flash-sale-platform/internal/inventory"
)

var ErrCapacityExceeded = errors.New("orders: shard capacity exceeded")

type Order struct {
	OrderID     string    `json:"order_id"`
	SKU         string    `json:"sku"`
	Shard       int       `json:"-"`
	UserID      string    `json:"user_id"`
	ChargeID    string    `json:"charge_id"`
	AmountMinor int64     `json:"amount_minor"`
	CreatedAt   time.Time `json:"created_at"`
}

// Store mirrors deploy/sql/schema.sql: INSERT ... ON CONFLICT (order_id) DO
// NOTHING plus UPDATE inventory_bucket SET sold = sold + 1 WHERE sold < capacity,
// in one transaction.
type Store interface {
	Upsert(ctx context.Context, o Order) (created bool, err error)
}

type shardKey struct {
	sku   string
	shard int
}

type MemoryStore struct {
	mu       sync.Mutex
	orders   map[string]Order
	sold     map[shardKey]int
	capacity map[shardKey]int
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{orders: map[string]Order{}, sold: map[shardKey]int{}, capacity: map[shardKey]int{}}
}

func (m *MemoryStore) SetCapacity(sku string, shard, capacity int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.capacity[shardKey{sku, shard}] = capacity
}

func (m *MemoryStore) Upsert(_ context.Context, o Order) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.orders[o.OrderID]; ok {
		return false, nil
	}
	k := shardKey{o.SKU, o.Shard}
	if c, ok := m.capacity[k]; ok && m.sold[k] >= c {
		return false, ErrCapacityExceeded
	}
	m.sold[k]++
	m.orders[o.OrderID] = o
	return true, nil
}

func (m *MemoryStore) Count(sku string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, o := range m.orders {
		if o.SKU == sku {
			n++
		}
	}
	return n
}

func (m *MemoryStore) Get(id string) (Order, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[id]
	return o, ok
}

type Consumer struct {
	RDB       redis.UniversalClient
	Inventory *inventory.Store
	Store     Store
	Group     string
	Name      string
	MinIdle   time.Duration
	Batch     int64
	// OnOversell is invoked when the system of record rejects an order the
	// reservation layer confirmed. It must refund and alert.
	OnOversell func(ctx context.Context, o Order)
	Log        *slog.Logger

	mu     sync.Mutex
	groups map[string]bool
}

func (c *Consumer) ensureGroup(ctx context.Context, stream string) error {
	c.mu.Lock()
	done := c.groups[stream]
	c.mu.Unlock()
	if done {
		return nil
	}
	err := c.RDB.XGroupCreateMkStream(ctx, stream, c.Group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	c.mu.Lock()
	if c.groups == nil {
		c.groups = map[string]bool{}
	}
	c.groups[stream] = true
	c.mu.Unlock()
	return nil
}

func (c *Consumer) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if _, err := c.PollOnce(ctx); err != nil && ctx.Err() == nil {
			c.Log.Error("order consumer poll", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// PollOnce reclaims messages stuck with dead consumers, then reads new ones,
// across every shard outbox. Returns the number of messages processed.
func (c *Consumer) PollOnce(ctx context.Context) (int, error) {
	refs, err := c.Inventory.Shards(ctx)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, r := range refs {
		stream := inventory.OutboxKey(r.SKU, r.Shard)
		if err := c.ensureGroup(ctx, stream); err != nil {
			return total, err
		}
		claimed, _, err := c.RDB.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: stream, Group: c.Group, Consumer: c.Name, MinIdle: c.MinIdle, Start: "0-0", Count: c.Batch,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return total, err
		}
		msgs := claimed
		res, err := c.RDB.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: c.Group, Consumer: c.Name, Streams: []string{stream, ">"}, Count: c.Batch, Block: -1,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return total, err
		}
		for _, s := range res {
			msgs = append(msgs, s.Messages...)
		}
		for _, m := range msgs {
			if err := c.handle(ctx, m); err != nil {
				// Not acked: redelivered via XAUTOCLAIM after MinIdle.
				c.Log.Error("order handle", "id", m.ID, "err", err)
				continue
			}
			if err := c.RDB.XAck(ctx, stream, c.Group, m.ID).Err(); err != nil {
				return total, err
			}
			total++
		}
	}
	return total, nil
}

func (c *Consumer) handle(ctx context.Context, m redis.XMessage) error {
	o, err := parse(m)
	if err != nil {
		c.Log.Error("poison outbox message, dropping", "id", m.ID, "err", err)
		return nil
	}
	_, err = c.Store.Upsert(ctx, o)
	if errors.Is(err, ErrCapacityExceeded) {
		if c.OnOversell != nil {
			c.OnOversell(ctx, o)
		}
		return nil
	}
	return err
}

func parse(m redis.XMessage) (Order, error) {
	str := func(k string) string { s, _ := m.Values[k].(string); return s }
	shard, err := strconv.Atoi(str("shard"))
	if err != nil {
		return Order{}, fmt.Errorf("shard: %w", err)
	}
	amount, err := strconv.ParseInt(str("amount_minor"), 10, 64)
	if err != nil {
		return Order{}, fmt.Errorf("amount: %w", err)
	}
	o := Order{
		OrderID: str("order_id"), SKU: str("sku"), Shard: shard, UserID: str("user"),
		ChargeID: str("charge_id"), AmountMinor: amount, CreatedAt: time.Now(),
	}
	if o.OrderID == "" || o.SKU == "" {
		return Order{}, errors.New("missing order_id/sku")
	}
	return o, nil
}
