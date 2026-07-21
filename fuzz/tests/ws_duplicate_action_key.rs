//! Pins the duplicate-top-level-JSON-key expectation for
//! `fauxchange::gateway::ws::parse_frame` (review finding #52-2 on
//! `fuzz/fuzz_targets/ws_frame_decode.rs`).
//!
//! The fix that makes `parse_frame` REJECT a frame carrying a duplicate
//! top-level key (e.g. two `"action"` fields) landed on a LOWER stack branch
//! (#14, the WS production-parser hardening) and is **now rebased into this
//! tree**. The assertion lives here — in an ORDINARY Cargo integration test
//! (`fuzz/tests/`, not one of the `#![no_main]` libFuzzer binaries in
//! `fuzz_targets/` — none of those are built or run by `cargo test`; see
//! `test = false` on every `[[bin]]` in `fuzz/Cargo.toml`) — rather than
//! inside the `ws_frame_decode` libFuzzer body, which processes arbitrary
//! bytes and cannot pin a specific seed's expected outcome. It is gated by
//! [`DUPLICATE_KEY_REJECTION_LANDED`]:
//!
//! - now that #14 is present (`true`) it pins the FIXED behaviour: a
//!   duplicate `"action"` key is a typed, **non-terminal** `Reject`;
//! - before #14 landed (`false`) it accepted either an `Action` or a
//!   `Reject`, documenting the pre-fix `serde_json::Value` last-key-wins
//!   resolution (kept below so the pre-#14 contract stays on record).
//!
//! Runs under plain `cargo test --manifest-path fuzz/Cargo.toml` (this file
//! needs no `#[cfg(test)]` wrapper — every file under `tests/` is already
//! test-only by Cargo convention).

use fauxchange::gateway::ws::{FrameOutcome, parse_frame};

/// `true` now that stack branch #14 (the WS duplicate-top-level-key rejection
/// fix) is rebased into this tree — the test pins the typed-`Reject` behaviour.
const DUPLICATE_KEY_REJECTION_LANDED: bool = true;

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
