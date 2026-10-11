-- System of record (PostgreSQL / Aurora). Redis gates the hot path; this
-- schema is the authority for money and for the final "no oversell" check.

-- One row per (sku, shard), mirroring the Redis shards. Spreading capacity
-- across rows means order writes for a hot SKU don't serialise on one row lock.
CREATE TABLE inventory_bucket (
    sku       text    NOT NULL,
    shard     int     NOT NULL,
    capacity  int     NOT NULL CHECK (capacity >= 0),
    sold      int     NOT NULL DEFAULT 0,
    PRIMARY KEY (sku, shard),
    CONSTRAINT no_oversell CHECK (sold <= capacity)
);

CREATE TABLE orders (
    order_id      text        PRIMARY KEY,          -- = hold id, so replays are no-ops
    sku           text        NOT NULL,
    shard         int         NOT NULL,
    user_id       text        NOT NULL,
    charge_id     text        NOT NULL UNIQUE,      -- one order per charge
    amount_minor  bigint      NOT NULL,
    currency      char(3)     NOT NULL,
    status        text        NOT NULL DEFAULT 'confirmed',
    created_at    timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (sku, shard) REFERENCES inventory_bucket (sku, shard)
);
CREATE INDEX orders_user_idx ON orders (user_id, created_at DESC);

-- Every PSP interaction, keyed by our idempotency key; reconciliation joins
-- this against orders and the PSP's settlement report.
CREATE TABLE payment_attempts (
    idempotency_key text        PRIMARY KEY,         -- charge:<hold_id> / refund:<order_id>
    hold_id         text        NOT NULL,
    psp_charge_id   text,
    status          text        NOT NULL,            -- pending|succeeded|failed|refunded|unknown
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Order writer, one transaction per outbox message (batched in practice):
--
--   BEGIN;
--   INSERT INTO orders (...) VALUES (...) ON CONFLICT (order_id) DO NOTHING;
--   -- only if a row was inserted:
--   UPDATE inventory_bucket SET sold = sold + 1
--    WHERE sku = $1 AND shard = $2 AND sold < capacity;
--   -- 0 rows updated => oversell: ROLLBACK, refund, page on-call.
--   COMMIT;

-- Reconciliation queries (run every minute during a drop):
-- charges without an order
--   SELECT p.* FROM payment_attempts p LEFT JOIN orders o ON o.order_id = p.hold_id
--    WHERE p.status = 'succeeded' AND o.order_id IS NULL AND p.updated_at < now() - interval '5 min';
-- buckets over capacity (should be impossible given the CHECK)
--   SELECT sku, shard, sold, capacity FROM inventory_bucket WHERE sold > capacity;
-- duplicate charges for one user/sku
--   SELECT user_id, sku, count(*) FROM orders GROUP BY 1, 2 HAVING count(*) > 1;
