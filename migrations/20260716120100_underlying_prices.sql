-- underlying_prices: the latest per-underlying price (kept from the Backend,
-- docs/06 §6). Schema skeleton for #023 — the read/write code lands with the
-- prices surface that owns it; this issue supplies only the durable table.
--
-- Money is integer cents (BIGINT, lossless — never a float). `venue_ts_ms` is the
-- VENUE-CLOCK millisecond stamp (a logical time, not wall clock); `updated_at
-- TIMESTAMPTZ` is the wall-clock durability audit column.

CREATE TABLE underlying_prices (
    symbol      TEXT        PRIMARY KEY,
    price_cents BIGINT      NOT NULL,
    bid_cents   BIGINT,
    ask_cents   BIGINT,
    volume      BIGINT,
    venue_ts_ms BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT underlying_prices_price_chk CHECK (price_cents >= 0),
    CONSTRAINT underlying_prices_bid_chk CHECK (bid_cents IS NULL OR bid_cents >= 0),
    CONSTRAINT underlying_prices_ask_chk CHECK (ask_cents IS NULL OR ask_cents >= 0),
    CONSTRAINT underlying_prices_volume_chk CHECK (volume IS NULL OR volume >= 0),
    CONSTRAINT underlying_prices_ts_chk CHECK (venue_ts_ms >= 0)
);
