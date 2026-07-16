-- venue command journal: the durable home of the write-ahead `VenueCommand` /
-- `VenueEvent` envelope stream the per-underlying single-writer actor writes
-- (#029, docs/adr/0006 §3, docs/02 §6). This is the DURABLE store that swaps in
-- behind the SAME `OptionChainJournal`-shaped `VenueJournal` trait as the
-- in-memory `InMemoryVenueJournal`; the receipt / recovery / durability contract
-- is UNCHANGED from v0.1, only the store is swapped.
--
-- Two tables:
--
--   journal_headers   — ONE row per underlying stream, recording the run's
--                       `lineage_id` and the envelope `schema_version`. Recovery
--                       reads it FIRST to rehydrate `lineage_id` (so re-derived
--                       ids match) and to REFUSE a journal whose schema is newer
--                       than the binary (a typed `JournalError::SchemaTooNew`,
--                       never a mis-parse).
--
--   journal_records   — the append-only paired-record stream. Each
--                       `underlying_sequence N` carries TWO records — a `command`
--                       (step 1, before execute) and the paired `event` (step 4,
--                       after capture) — plus, at a snapshot restore, an `epoch`
--                       marker. `(underlying, underlying_sequence, kind)` is the
--                       UNIQUE key, so a command is never appended twice and an
--                       idempotent re-append (the ambiguous-result recovery path)
--                       is a NO-OP, never a gap or a duplicate.
--
-- `id BIGSERIAL` is the durable insertion surrogate = the actor's turn/append
-- order (the actor is the sole writer per underlying), so `ORDER BY id` yields
-- the journaled total order for `read_from`. `payload` stores the EXACT
-- `serde_json` bytes of the `venue.v1` `JournalRecord` envelope as TEXT — the
-- journal is a durable, versioned, venue-owned WIRE contract, so the envelope is
-- persisted verbatim (an upstream/JSONB key-reorder can never silently mutate a
-- `venue.v1` record); the projected `kind` / `underlying_sequence` columns are the
-- routing + unique-key index, not a second source of truth.
--
-- `recorded_at TIMESTAMPTZ` is the genuine wall-clock durability audit column; the
-- envelope's own `venue_ts` (a logical, possibly-virtual venue clock) lives inside
-- `payload`, never a wall clock.
--
-- Parameterised `sqlx` only (src/db/journal.rs) — no value or identifier is ever
-- interpolated (rules SQL & Persistence).

CREATE TABLE journal_headers (
    underlying          TEXT        PRIMARY KEY,
    lineage_id          TEXT        NOT NULL,
    schema_version      TEXT        NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE journal_records (
    id                  BIGSERIAL   PRIMARY KEY,
    underlying          TEXT        NOT NULL,
    underlying_sequence BIGINT      NOT NULL,
    kind                TEXT        NOT NULL,
    payload             TEXT        NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT journal_records_key_uniq UNIQUE (underlying, underlying_sequence, kind),
    CONSTRAINT journal_records_kind_chk CHECK (kind IN ('command', 'event', 'epoch')),
    CONSTRAINT journal_records_seq_chk CHECK (underlying_sequence >= 0)
);

-- The stream read shape: every record at or after `from`, in append (`id`) order,
-- scoped to one underlying (`read_from` / `last_sequence` / the tail read-back).
CREATE INDEX journal_records_stream_idx ON journal_records (underlying, underlying_sequence, id);
