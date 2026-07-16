-- accounts: the venue account registry, persisted (NEW in fauxchange, docs/06 §6,
-- §8). Grounds every column in the #012 `Account` / `Credentials` model
-- (src/auth.rs): the account is in-memory by default and PostgreSQL when
-- `DATABASE_URL` is set, re-seeded from the manifest on a DB-less restart.
--
-- Schema skeleton for #023 — the provisioning-from-manifest read/write logic is
-- #024; this issue supplies only the durable table it persists into.
--
-- SECURITY: `password_hash` is the FIX password as an **Argon2id PHC string**
-- ($argon2id$...) at the pinned OWASP parameters — NEVER plaintext, never a
-- pepper, never logged (src/auth.rs, docs/08 §7). It is NULL for a REST/WS-only
-- account (no stored credential). The JWT path stores no secret at all: a JWT is
-- verified by the RS256 public key and its `sub` IS `id`, so there is no JWT
-- column here.

CREATE TABLE accounts (
    -- The account identity (the JWT `sub`).
    id                 TEXT        PRIMARY KEY,
    -- The STP / mass-cancel owner hash the matching engine keys on (Hash32 = 32
    -- bytes).
    owner              BYTEA       NOT NULL,
    -- The registered permission set — a subset of {'read','trade','admin'}
    -- (`Admin` implies `Read` + `Trade`, enforced in the auth layer).
    permissions        TEXT[]      NOT NULL,
    -- The FIX password as an Argon2id PHC string — NEVER plaintext. NULL for a
    -- REST/WS-only account.
    password_hash      TEXT,
    -- The FIX `Username (553)` that indexes this account for a FIX logon; NULL
    -- when the account may not log in over FIX.
    fix_username       TEXT        UNIQUE,
    -- The immutable FIX `(SenderCompID, TargetCompID)` binding (ADR-0010),
    -- declared now, enforced from v0.4.
    fix_sender_comp_id TEXT,
    fix_target_comp_id TEXT,
    -- Bumped by a revoke; a token/logon minted below it is refused per-request.
    revocation_epoch   BIGINT      NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT accounts_owner_len_chk CHECK (octet_length(owner) = 32),
    CONSTRAINT accounts_revocation_chk CHECK (revocation_epoch >= 0),
    CONSTRAINT accounts_permissions_nonempty_chk CHECK (cardinality(permissions) > 0),
    -- Every permission token must be in the fixed vocabulary — the lowercase
    -- serde values of `models::Permission` (#004: 'read' / 'trade' / 'admin', where
    -- `Admin` implies `Read` + `Trade`, grounded in #012's `Account.permissions`).
    -- Subset containment (`<@`) rejects a provisioning bug that would otherwise
    -- store an out-of-vocabulary token that later fails to deserialize. Migrations
    -- are immutable once merged, so the vocabulary is pinned in the schema now.
    CONSTRAINT accounts_permissions_vocab_chk CHECK (
        permissions <@ ARRAY['read', 'trade', 'admin']::text[]
    ),
    -- The comp-id binding is a pair: both present or both absent.
    CONSTRAINT accounts_fix_comp_ids_chk CHECK (
        (fix_sender_comp_id IS NULL) = (fix_target_comp_id IS NULL)
    )
);

CREATE INDEX accounts_fix_username_idx ON accounts (fix_username);
