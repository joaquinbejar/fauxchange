//! Pins the duplicate-top-level-JSON-key expectation for
//! `fauxchange::gateway::ws::parse_frame` (review finding #52-2 on
//! `fuzz/fuzz_targets/ws_frame_decode.rs`).
//!
//! The fix that makes `parse_frame` REJECT a frame carrying a duplicate
//! top-level key (e.g. two `"action"` fields) lands on a LOWER stack branch
//! (#14, the WS production-parser hardening) and is **not yet rebased into
//! this working tree**. Asserting "duplicate key ⇒ Reject" unconditionally
//! inside the `ws_frame_decode` libFuzzer target body would fail on every
//! run on THIS tree, before #14 ever lands — so that assertion lives here
//! instead, in an ORDINARY Cargo integration test (`fuzz/tests/`, not one of
//! the `#![no_main]` libFuzzer binaries in `fuzz_targets/` — none of those
//! are built or run by `cargo test`; see `test = false` on every `[[bin]]`
//! in `fuzz/Cargo.toml`), gated by [`DUPLICATE_KEY_REJECTION_LANDED`]:
//!
//! - **today** (`false`) it exercises and documents the CURRENT, pre-#14
//!   behaviour: `parse_frame` resolves the duplicate `"action"` key
//!   deterministically via `serde_json::Value`'s own last-key-wins insertion
//!   semantics and never panics — either an `Action` or a `Reject` is a
//!   valid outcome, so this branch accepts both;
//! - **once #14 rebases in**, flip the constant to `true` and this SAME test
//!   starts pinning the FIXED behaviour (a typed, non-terminal `Reject`)
//!   instead — the assertion is already written below, not left as a
//!   follow-up TODO.
//!
//! Runs under plain `cargo test --manifest-path fuzz/Cargo.toml` (this file
//! needs no `#[cfg(test)]` wrapper — every file under `tests/` is already
//! test-only by Cargo convention).

use fauxchange::gateway::ws::{FrameOutcome, parse_frame};

/// Flip to `true` once stack branch #14 (the WS duplicate-top-level-key
/// rejection fix) is rebased into this tree.
const DUPLICATE_KEY_REJECTION_LANDED: bool = false;

/// The committed corpus seed for this scenario
/// (`fuzz/corpus/ws_frame_decode/duplicate_action_field.json`) — two
/// top-level `"action"` keys. `"action"` is the tag field that selects which
/// `ClientAction` variant `parse_frame` dispatches to, so a duplicate on
/// exactly this field (as opposed to some dispatch-irrelevant field, like the
/// pre-existing `duplicate_field.json` seed's duplicate `"channel"`) is the
/// highest-value case to pin.
const DUPLICATE_ACTION_SEED: &str =
    include_str!("../corpus/ws_frame_decode/duplicate_action_field.json");

#[test]
fn duplicate_top_level_action_key() {
    let outcome = parse_frame(DUPLICATE_ACTION_SEED);
    if DUPLICATE_KEY_REJECTION_LANDED {
        match outcome {
            FrameOutcome::Reject(err) => {
                assert!(
                    !err.terminal,
                    "a duplicate-top-level-key rejection must stay non-terminal, like every \
                     other decode/validation reject `parse_frame` returns"
                );
            }
            other => panic!(
                "a duplicate top-level `action` key must be rejected once #14 lands, got \
                 {other:?}"
            ),
        }
    } else {
        // Pre-#14: last-key-wins resolution must never panic, but is NOT yet
        // required to reject — either outcome is valid today.
        match outcome {
            FrameOutcome::Action(..) => {}
            FrameOutcome::Reject(err) => assert!(!err.terminal),
        }
    }
}
