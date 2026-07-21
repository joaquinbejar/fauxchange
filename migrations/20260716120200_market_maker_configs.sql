-- market_maker_configs: persisted market-maker persona configuration (kept from
-- the Backend, docs/06 §6). Schema skeleton for #023 — the read/write code lands
-- with the market-maker persistence that owns it; this issue supplies only the
-- durable table.
--
-- The persona knobs are carried as JSONB (opaque to this issue). Any monetary
-- knob inside the JSON payload is integer cents by the venue-wide rule, never a
-- float.

CREATE TABLE market_maker_configs (
    id         TEXT        PRIMARY KEY,
    config     JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
