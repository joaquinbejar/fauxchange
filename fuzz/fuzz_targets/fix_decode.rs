//! The FIX tag-value parser fuzz target (#042) — the **primary** fuzz target
//! named by [docs/08-threat-model.md §4/§6](../../docs/08-threat-model.md#4-untrusted-input-hardening).
//!
//! This drives arbitrary bytes through the **exact same two-stage decode path**
//! the live acceptor drives on every inbound TCP read
//! (`src/gateway/fix/acceptor.rs::run_session`), not a reimplementation:
//!
//! 1. [`BoundedFrameDecoder::decode`] — the framing layer, which pre-checks the
//!    two hostile-arithmetic fields (`BodyLength (9)`, `CheckSum (10)`) before
//!    delegating to `ironfix_transport::FixCodec`, splitting the byte stream
//!    into complete frames.
//! 2. [`fauxchange::gateway::fix::decode`] — the tag-value decoder, turning one
//!    complete frame into a typed [`DecodedMessage`] or a typed
//!    [`FixDecodeError`].
//!
//! Neither stage may ever panic, allocate unboundedly, or silently accept a
//! malformed frame as valid — every failure must surface as a typed error
//! (`FrameError` / `FixDecodeError`). This target does not assert *which*
//! typed error a given input produces (that is the adversarial-fixture matrix
//! in `tests/fix_adversarial.rs`, which shares this exact corpus); it only
//! proves the decode path itself never crashes or hangs on adversarial input.
//! `#![no_main]` + the raw-pointer libFuzzer FFI entrypoint `fuzz_target!`
//! expands to is the standard, documented `unsafe` exception for a libFuzzer
//! harness — isolated to this fuzz-only crate, never the venue's
//! `#![forbid(unsafe_code)]` source (`src/lib.rs`).

#![no_main]

use bytes::BytesMut;
use fauxchange::gateway::fix::{BoundedFrameDecoder, decode};
use libfuzzer_sys::fuzz_target;

/// Mirrors the venue's own `[fix] max_frame_bytes` default
/// (`fauxchange::config::DEFAULT_FIX_MAX_FRAME_BYTES`), duplicated as a
/// literal because the fuzz crate does not otherwise depend on `config`'s
/// wider (config-file-parsing) surface — keeping the fuzz crate's dependency
/// graph to exactly what the decode path itself needs. Re-verify against
/// `src/config.rs::DEFAULT_FIX_MAX_FRAME_BYTES` if that default ever changes.
const MAX_FRAME_BYTES: usize = 256 * 1024;

/// A harness-only safety cap on how many frames one fuzz iteration drains from
/// a single input. This is NOT a production behaviour — the real acceptor
/// drains every buffered frame per read — it only bounds how much work one
/// libFuzzer iteration can do so a corpus entry that decodes into very many
/// tiny frames still returns promptly (a genuine hang is still caught by
/// libFuzzer's own `-timeout` regardless of this cap).
const MAX_FRAMES_PER_INPUT: usize = 4096;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    let mut decoder = BoundedFrameDecoder::new(MAX_FRAME_BYTES);
    let mut frames_seen = 0usize;

    loop {
        match decoder.decode(&mut buf) {
            Ok(Some(frame)) => {
                // Stage 2: the SAME tag-value decode path the acceptor's
                // `dispatch` calls (src/gateway/fix/acceptor.rs `dispatch`:
                // `super::decode(frame)`). The result is intentionally
                // discarded — both `Ok` and `Err` are valid outcomes; a panic
                // is the only failure this target detects.
                let _ = decode(&frame);
                frames_seen += 1;
                if frames_seen >= MAX_FRAMES_PER_INPUT {
                    break;
                }
            }
            // Incomplete (buffer for more bytes) or a framing-layer reject —
            // both match the real acceptor's behaviour (loop back to read more
            // / close the session) and both end this iteration cleanly.
            Ok(None) | Err(_) => break,
        }
    }
});
