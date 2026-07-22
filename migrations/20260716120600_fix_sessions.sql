-- FIX session store: the durable, account-keyed home of the acceptor's per-session
-- sequence counters, its outbound-frame resend log, and its `SequenceReset`
-- session-event audit trail (#095, #038, docs/adr/0010 §5,
-- docs/03 §5.2). This is the DURABLE store that swaps in behind the SAME
-- `FixSessionStore` trait as the in-memory `InMemoryFixSessionStore`
-- (src/gateway/fix/store.rs) when `DATABASE_URL` is set, exactly as the durable
-- venue journal (#029) does. It is TRANSPORT session state, NOT the per-underlying
-- `VenueEvent` journal: a sequence reset is a transport-level fact, never a book
-- mutation (docs/adr/0010 §5), so it lives in its own tables here and never on the
-- sequenced determinism path.
--
-- Every table is keyed on the immutable FIX session identity
-- `(account_id, sender_comp_id, target_comp_id)` — the AUTHENTICATED account plus
-- its bound `(SenderCompID 49, TargetCompID 56)` tuple (docs/adr/0010 rule 2).
-- Keying on the account triple (never the CompID tuple alone) is what makes a
-- re-pointed or reused CompID unable to inherit, address, or resend another
-- account's messages.
--
-- Three tables:
--
--   fix_session_counters  — ONE row per session key = the in-memory `Slot`'s
--                           counters. `(next_sender_seq, next_target_seq)` are the
--                           pair a reconnect / process-restart resumes from. This
--                           table is ALSO the authoritative key REGISTRY: a row
--                           here (created on first touch with the `1`-based
--                           defaults) means the key is "known", so the
--                           `MAX_SESSION_KEYS` keyspace bound is enforced by a
--                           count of this table (mirrors the in-memory unified-map
--                           bound). Counters are checked, non-wrapping `1`-based
--                           `MsgSeqNum` (FIX 34), so the CHECK pins `>= 1`.
--
--   fix_session_outbound  — the bounded outbound resend log: MANY rows per key, one
--                           per retained framed message, held for a possible
--                           `ResendRequest (2)` replay. `frame` is the complete
--                           pre-framed FIX bytes exactly as first sent (a resend
--                           replays the ORIGINAL bytes at the ORIGINAL MsgSeqNum).
--                           `id BIGSERIAL` is the durable insertion/eviction order:
--                           the in-memory store appends to a `Vec` and evicts the
--                           OLDEST (front) past the count / byte bounds, so eviction
--                           here is `ORDER BY id ASC` (oldest first) and a range
--                           read is `ORDER BY seq ASC, id ASC` (stable, matching the
--                           in-memory stable `sort_by_key(seq)`). There is
--                           deliberately NO unique key on `seq`: the in-memory log
--                           APPENDS (a post-reset re-send at a re-used seq yields two
--                           entries), and the durable store is faithful to that.
--
--   fix_session_resets    — the bounded, append-only `SequenceReset` audit ring:
--                           MANY rows per key, oldest first (`ORDER BY id ASC`),
--                           each recording one durable reset of the session's
--                           sequence state (docs/adr/0010 §5). `at_ms` is the
--                           INJECTED VENUE CLOCK instant the caller supplied — never
--                           a wall-clock read — so the audit trail replays
--                           deterministically. `trigger` is the two-value reset
--                           vocabulary pinned by a CHECK.
--
-- The per-key resend-log (count + bytes) and reset-audit (count) bounds are the
-- in-memory store's DoS controls (docs/08 §5); the durable store re-applies them by
-- evicting the oldest rows after each append. `recorded_at` / `updated_at`
-- TIMESTAMPTZ are the genuine wall-clock durability audit columns; the venue-clock
-- `at_ms` inside `fix_session_resets` is the logical instant, never a wall clock.
--
-- The `frame` bytes are NEVER logged (they may carry secrets); they are stored for
-- resend and read back verbatim. Parameterised `sqlx` only
-- (src/gateway/fix/pg_store.rs) — no value or identifier is ever interpolated
-- (rules SQL & Persistence).

CREATE TABLE fix_session_counters (
    account_id          TEXT        NOT NULL,
    sender_comp_id      TEXT        NOT NULL,
    target_comp_id      TEXT        NOT NULL,
    next_sender_seq     BIGINT      NOT NULL,
    next_target_seq     BIGINT      NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fix_session_counters_pkey
        PRIMARY KEY (account_id, sender_comp_id, target_comp_id),
    CONSTRAINT fix_session_counters_sender_chk CHECK (next_sender_seq >= 1),
    CONSTRAINT fix_session_counters_target_chk CHECK (next_target_seq >= 1)
);

CREATE TABLE fix_session_outbound (
    id                  BIGSERIAL   PRIMARY KEY,
    account_id          TEXT        NOT NULL,
    sender_comp_id      TEXT        NOT NULL,
    target_comp_id      TEXT        NOT NULL,
    seq                 BIGINT      NOT NULL,
    frame               BYTEA       NOT NULL,
    stored_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fix_session_outbound_seq_chk CHECK (seq >= 1)
);

-- The resend range-read + eviction shape: every frame for one session key in
-- (seq, id) order (`outbound_range`), and the oldest-first (`id`) eviction scan.
CREATE INDEX fix_session_outbound_range_idx
    ON fix_session_outbound (account_id, sender_comp_id, target_comp_id, seq, id);

CREATE TABLE fix_session_resets (
    id                  BIGSERIAL   PRIMARY KEY,
    account_id          TEXT        NOT NULL,
    sender_comp_id      TEXT        NOT NULL,
    target_comp_id      TEXT        NOT NULL,
    at_ms               BIGINT      NOT NULL,
    trigger             TEXT        NOT NULL,
    old_next_sender_seq BIGINT      NOT NULL,
    old_next_target_seq BIGINT      NOT NULL,
    new_next_sender_seq BIGINT      NOT NULL,
    new_next_target_seq BIGINT      NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fix_session_resets_trigger_chk
        CHECK (trigger IN ('logon_reset', 'sequence_reset')),
    CONSTRAINT fix_session_resets_at_chk CHECK (at_ms >= 0),
    CONSTRAINT fix_session_resets_sender_chk
        CHECK (old_next_sender_seq >= 0 AND new_next_sender_seq >= 0),
    CONSTRAINT fix_session_resets_target_chk
        CHECK (old_next_target_seq >= 0 AND new_next_target_seq >= 0)
);

-- The reset-audit read + eviction shape: every reset for one session key in
-- insertion (`id`) order, oldest first (`reset_events` + the ring eviction).
CREATE INDEX fix_session_resets_key_idx
    ON fix_session_resets (account_id, sender_comp_id, target_comp_id, id);
