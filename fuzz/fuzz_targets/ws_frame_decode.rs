//! The WebSocket client-frame decoder fuzz target (#052) — a **secondary**
//! fuzz target ([docs/08 §6](../../docs/08-threat-model.md#6-fuzzing-and-adversarial-testing));
//! the FIX tag-value parser (`fix_decode.rs`, #042) is the primary target.
//!
//! Drives arbitrary bytes through the **exact same function** the WS socket
//! loop calls on every inbound text frame
//! (`src/gateway/ws/mod.rs::handle_socket`,
//! `Some(Ok(Message::Text(text))) => parse_frame(text.as_str())`):
//! [`fauxchange::gateway::ws::parse_frame`], turning one frame into a typed
//! [`fauxchange::gateway::ws::ClientAction`] or a typed, non-terminal
//! [`fauxchange::WsError`] via [`fauxchange::gateway::ws::FrameOutcome`].
//!
//! Two gates mirror what the real transport guarantees BEFORE `parse_frame`
//! ever runs, rather than re-deriving them inside the decoder itself:
//!
//! - **Frame-size ceiling.** `ws_handler` bounds the inbound frame/message
//!   size explicitly (`.max_frame_size(MAX_WS_FRAME_BYTES)` /
//!   `.max_message_size(MAX_WS_FRAME_BYTES)`, axum's own `WebSocketUpgrade`
//!   builder) — a frame over [`MAX_WS_FRAME_BYTES`] never reaches
//!   `handle_socket` in production, so the harness skips any larger input
//!   rather than reimplementing axum's WS framing limiter.
//! - **UTF-8 validity.** A WebSocket `Text` frame's payload is guaranteed
//!   valid UTF-8 by the protocol layer itself (RFC 6455 §5.6; `tungstenite`
//!   — which axum's `ws` feature wraps — rejects an invalid-UTF-8 text frame
//!   at the codec level, surfacing as `Some(Err(_))` and closing the socket
//!   BEFORE `handle_socket`'s `Message::Text(_)` match arm is ever reached).
//!   `parse_frame` therefore never receives non-UTF-8 bytes in production;
//!   the harness mirrors that by skipping any input that is not valid UTF-8
//!   rather than reimplementing the WebSocket codec's own UTF-8 validation.
//!
//! Neither gate hides a validation gap in `parse_frame` itself — both are
//! enforced by a DIFFERENT, already-fuzzed-elsewhere layer (axum's
//! `WebSocketUpgrade` limiter; `tungstenite`'s RFC-6455 codec), so the
//! harness mirrors rather than re-derives them, exactly like `fix_decode`'s
//! `MAX_FRAME_BYTES` mirror of the acceptor's own cap. `parse_frame` itself
//! may never panic or allocate unboundedly on ANY UTF-8 string under the
//! ceiling; a malformed frame must always reject to the typed, non-terminal
//! [`fauxchange::WsError`] envelope, never a panic and never a silent
//! accept — and the [`FrameOutcome`] this target gets back is no longer
//! discarded: it is matched and asserted on every call.
//!
//! - **`FrameOutcome::Reject`** must ALWAYS carry a non-terminal
//!   [`fauxchange::WsError`] (`terminal: false` — a frame-decode/validation
//!   reject never closes the connection, only an auth failure elsewhere in
//!   the gateway does) tagged with the current wire schema
//!   ([`fauxchange::WS_ERROR_SCHEMA`]).
//! - **`FrameOutcome::Action`** must be the result of a DETERMINISTIC parse:
//!   feeding the SAME text through `parse_frame` a second time must resolve
//!   to the SAME `ClientAction` variant. This holds on ANY base — today,
//!   because `serde_json`'s `Value` construction is non-hash-order-dependent
//!   and resolves a duplicate top-level key (e.g. two `"action"` fields) the
//!   same way on every call; after the parser fix, because a rejected input
//!   rejects identically on every call too — so it is a re-check the
//!   duplicate-key rejection fix (stack branch #14, not yet in this tree,
//!   [docs/adr](../../docs/adr)) cannot break either way. `fauxchange`'s
//!   determinism guarantee for the sequenced order path
//!   ([CLAUDE.md](../../CLAUDE.md) "Determinism via a single-writer actor")
//!   depends on `parse_frame` itself already being a pure function of its
//!   input, so this is a real invariant, not busywork.
//!
//! This target still does not assert WHICH typed reject a given INVALID
//! input produces (that is `tests/security.rs` / `src/gateway/ws/protocol.rs`'s
//! own unit tests, which this corpus overlaps); nor does it assert
//! "duplicate top-level key ⇒ Reject" — that expectation is not yet true on
//! THIS tree (the fix lives on stack branch #14) and is pinned instead by
//! the ordinary Cargo integration test
//! `fuzz/tests/ws_duplicate_action_key.rs`, gated so it documents today's
//! behaviour and starts asserting the fixed behaviour once #14 rebases in,
//! without ever failing on THIS tree in the meantime. `#![no_main]` is the
//! standard libFuzzer harness exception (isolated to this fuzz-only crate,
//! never the venue's `#![forbid(unsafe_code)]` source).

#![no_main]

use fauxchange::gateway::ws::{FrameOutcome, MAX_WS_FRAME_BYTES, parse_frame};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_WS_FRAME_BYTES {
        return;
    }
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };

    match parse_frame(text) {
        FrameOutcome::Action(action, _request_id) => {
            // A pure-function re-check: parsing the IDENTICAL text again must
            // resolve to the SAME `ClientAction` variant. This is the
            // invariant that stays true regardless of how (or whether) a
            // duplicate top-level key is resolved — see the module docs.
            let second = match parse_frame(text) {
                FrameOutcome::Action(second, _) => second,
                FrameOutcome::Reject(err) => panic!(
                    "parse_frame accepted then rejected the SAME text on a second call \
                     (non-deterministic decode): {err:?}"
                ),
            };
            assert_eq!(
                std::mem::discriminant(&action),
                std::mem::discriminant(&second),
                "parse_frame picked a different ClientAction variant on a second call with \
                 identical input text — a non-deterministic decode"
            );
        }
        FrameOutcome::Reject(err) => {
            assert!(
                !err.terminal,
                "a WS frame-decode/validation rejection must be non-terminal (the connection \
                 stays open) — got a terminal WsError for: {err:?}"
            );
            assert_eq!(
                err.schema,
                fauxchange::WS_ERROR_SCHEMA,
                "every WsError parse_frame returns must carry the current wire schema tag"
            );
        }
    }
});
