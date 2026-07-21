-- executions: the authoritative account-scoped fill log (#023).
--
-- Each committed match produces TWO legs (maker + taker) sharing one
-- `execution_id`, distinguished by `liquidity`. Every leg is an authoritative
-- `ExecutionRecord` (src/models.rs). This table is the durable second backend
-- behind the SAME `ExecutionsStore` contract (src/exchange/stores.rs, #008): the
-- in-memory store and this table serve identical reads.
--
-- Cents are BIGINT (i64) — LOSSLESS because the venue-owned `MAX_PRICE_CENTS`
-- (1e12) bounds every price/fee/theo well inside i64 (docs/05 §4.1,
-- governance-precedence §2.1). Money is integer cents, never a float.
--
-- `id BIGSERIAL` is the durable insertion surrogate — the SQL home of the
-- in-memory store's monotonic `ord`, so `ORDER BY id` yields the identical
-- journal-ordered listing on either backend. The `(execution_id, liquidity)`
-- UNIQUE constraint backs the idempotent `ON CONFLICT DO UPDATE` upsert
-- (`record`) which preserves the row's `id` on a re-record, keeping list order
-- identical to the in-memory backend.
--
-- `executed_at_ms` is the VENUE-CLOCK millisecond stamp (`EventTimestamp`) — a
-- logical venue time (which under a stepped/replay clock is virtual), NOT a wall
-- clock, so it is a BIGINT domain value. `recorded_at TIMESTAMPTZ` is the genuine
-- wall-clock durability audit column.
--
-- NOTE (scope): the durable command journal + journal-backed recovery is v0.3
-- (#029) and is NOT built here. This log persists fills, but book/fold state is
-- NOT recovered on restart — a restart without an admin snapshot is a fresh venue.

CREATE TABLE executions (
    id                  BIGSERIAL   PRIMARY KEY,
    execution_id        TEXT        NOT NULL,
    liquidity           TEXT        NOT NULL,
    order_id            TEXT        NOT NULL,
    account             TEXT        NOT NULL,
    symbol              TEXT        NOT NULL,
    instrument          TEXT        NOT NULL,
    side                TEXT        NOT NULL,
    quantity            BIGINT      NOT NULL,
    price_cents         BIGINT      NOT NULL,
    fee_cents           BIGINT      NOT NULL,
    theo_value_cents    BIGINT      NOT NULL,
    edge_cents          BIGINT      NOT NULL,
    underlying_sequence BIGINT      NOT NULL,
    latency_us          BIGINT      NOT NULL,
    executed_at_ms      BIGINT      NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT executions_leg_uniq UNIQUE (execution_id, liquidity),
    CONSTRAINT executions_side_chk CHECK (side IN ('buy', 'sell')),
    CONSTRAINT executions_liquidity_chk CHECK (liquidity IN ('maker', 'taker')),
    CONSTRAINT executions_quantity_chk CHECK (quantity >= 0),
    CONSTRAINT executions_price_chk CHECK (price_cents >= 0),
    CONSTRAINT executions_theo_chk CHECK (theo_value_cents >= 0),
    CONSTRAINT executions_seq_chk CHECK (underlying_sequence >= 0),
    CONSTRAINT executions_latency_chk CHECK (latency_us >= 0),
    CONSTRAINT executions_executed_at_chk CHECK (executed_at_ms >= 0)
);

-- Account-scoped listing in journal (`id`) order, with an optional underlying
-- filter — the two indexes cover the `list`/`get` read shapes.
CREATE INDEX executions_account_idx ON executions (account, id);
CREATE INDEX executions_account_symbol_idx ON executions (account, symbol, id);
