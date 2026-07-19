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
//! accept. This target does not assert WHICH typed reject a given input
//! produces (that is `tests/security.rs` / `src/gateway/ws/protocol.rs`'s
//! own unit tests, which this corpus overlaps); it only proves the decode
//! path itself never crashes or hangs on adversarial input. `#![no_main]` is
//! the standard libFuzzer harness exception (isolated to this fuzz-only
//! crate, never the venue's `#![forbid(unsafe_code)]` source).

#![no_main]

use fauxchange::gateway::ws::{MAX_WS_FRAME_BYTES, parse_frame};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_WS_FRAME_BYTES {
        return;
    }
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = parse_frame(text);
    }
});
