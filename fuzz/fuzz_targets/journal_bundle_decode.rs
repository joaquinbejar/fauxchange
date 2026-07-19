//! The journal-record / scenario-bundle deserialiser fuzz target (#052) — a
//! **secondary** fuzz target
//! ([docs/08 §6](../../docs/08-threat-model.md#6-fuzzing-and-adversarial-testing));
//! the FIX tag-value parser (`fix_decode.rs`, #042) is the primary target.
//! This folds the #034 adversarial-fixture surface (`tests/adversarial.rs`,
//! whose committed corpus seeds this target) into the coverage-guided
//! `cargo fuzz` set, so `cargo fuzz` now covers FIX + REST + WS + journal.
//!
//! Drives arbitrary UTF-8 text through the **exact two REAL deserialisers**
//! the durable read path and the on-disk replay/seed-bundle load path use —
//! not a reimplementation:
//!
//! 1. [`fauxchange::exchange::decode_journal_record`] — the bounded
//!    per-record deserialiser the durable store's read path calls for every
//!    row (`src/exchange/journal.rs`): one write-ahead `venue.v1`
//!    `Command`/`Event`/`Epoch` record → a typed [`fauxchange::exchange::JournalRecord`]
//!    or a typed [`fauxchange::exchange::JournalError`].
//! 2. [`fauxchange::simulation::ScenarioBundle::from_json`] — the bounded
//!    on-disk bundle deserialiser the replay/record-export path uses
//!    (`src/simulation/replay.rs`): a full scenario bundle (a `manifest` plus
//!    one `JournalStream` per underlying, each nesting the SAME record shape
//!    stage 1 decodes) → a typed [`fauxchange::simulation::ScenarioBundle`]
//!    or a typed [`fauxchange::simulation::ReplayError`].
//!
//! Both functions are **self-bounding** — each enforces its own byte
//! ceiling ([`fauxchange::exchange::MAX_JOURNAL_RECORD_BYTES`] /
//! [`fauxchange::simulation::MAX_BUNDLE_BYTES`]) BEFORE the `serde_json`
//! parse, returning a typed `ResourceLimit` reject for an over-ceiling input
//! rather than buffering it — so, unlike `rest_json_decode` / `ws_frame_decode`
//! (whose ceiling is enforced by a DIFFERENT layer the transport applies
//! before the decoder runs), this harness needs no size pre-check of its
//! own: the ceiling IS part of the real decode path here
//! ([08 §4](../../docs/08-threat-model.md#4-untrusted-input-hardening), #034).
//!
//! Neither stage may ever panic or allocate unboundedly; a malformed /
//! hostile input must always reject cleanly to its typed error, never a
//! panic and never a silent accept — and, per the replay driver's
//! all-or-nothing contract, never a **partial apply**
//! ([ADR-0006](../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//! This target does not re-assert WHICH typed reject a given input produces
//! (that is `tests/adversarial.rs`'s fixture matrix, which seeds this exact
//! corpus); it only proves the decode path itself never crashes or hangs on
//! adversarial input. `#![no_main]` is the standard libFuzzer harness
//! exception (isolated to this fuzz-only crate, never the venue's
//! `#![forbid(unsafe_code)]` source).

#![no_main]

use fauxchange::exchange::decode_journal_record;
use fauxchange::simulation::ScenarioBundle;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    // Stage 1: one write-ahead journal record.
    let _ = decode_journal_record(text);
    // Stage 2: a full scenario/replay bundle — nests the same record shape
    // one level deeper, inside each stream.
    let _ = ScenarioBundle::from_json(text);
});
