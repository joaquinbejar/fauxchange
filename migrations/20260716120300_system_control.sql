-- system_control: the kill-switch / venue control-plane state (kept from the
-- Backend, docs/06 §6). Schema skeleton for #023 — the read/write code lands with
-- the control plane that owns it; this issue supplies only the durable table.
--
-- Each row is one named control flag with an optional JSONB detail payload.

CREATE TABLE system_control (
    key        TEXT        PRIMARY KEY,
    enabled    BOOLEAN     NOT NULL,
    detail     JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
