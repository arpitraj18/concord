-- Orders, as any shop has them. payment_version guards against applying an older event after a newer one.
CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    gateway         TEXT NOT NULL,
    payment_id      TEXT NOT NULL,
    amount_minor    BIGINT NOT NULL,
    currency        TEXT NOT NULL,
    status          TEXT NOT NULL,                 -- AWAITING_PAYMENT | PAID | PAYMENT_FAILED | REFUNDED | CANCELLED
    payment_state   TEXT,                          -- last canonical payment state applied
    payment_version BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS orders_by_payment ON orders (gateway, payment_id);
-- Serves the "orders changed since" feed (keyset pagination on (updated_at, order_id)) and the checkout timeout job.
CREATE INDEX IF NOT EXISTS orders_by_change ON orders (updated_at, order_id);
CREATE INDEX IF NOT EXISTS orders_awaiting ON orders (created_at) WHERE status = 'AWAITING_PAYMENT';

-- One row per Concord event ever received: the primary key IS the deduplication.
CREATE TABLE IF NOT EXISTS processed_events (
    idempotency_key TEXT PRIMARY KEY,
    order_id        TEXT,
    outcome         TEXT NOT NULL,                 -- APPLIED | STALE | UNKNOWN_ORDER
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Business side effects (ship the goods, refund the customer...). The thing that must never happen twice.
CREATE TABLE IF NOT EXISTS effects (
    id              BIGSERIAL PRIMARY KEY,
    order_id        TEXT NOT NULL,
    effect          TEXT NOT NULL,                 -- FULFIL | STOP_FULFILMENT | NOTIFY_PAYMENT_FAILED | RECORD_REFUND
    idempotency_key TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every delivery attempt that reached us, duplicates included (to measure how many we absorbed).
CREATE TABLE IF NOT EXISTS deliveries (
    id              BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    duplicate       BOOLEAN NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
