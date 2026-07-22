//! **Adversarial fixture corpus** for the FIX tag-value decode path — the
//! v0.4 **security gate** (#042) for the untrusted-network (network-attacker)
//! decode surface
//! ([08 §4](../docs/08-threat-model.md#4-untrusted-input-hardening),
//! [08 §6](../docs/08-threat-model.md#6-fuzzing-and-adversarial-testing),
//! [TESTING.md §14](../docs/TESTING.md#14-security-testing)).
//!
//! ## What this proves
//!
//! Each committed hostile fixture under `fuzz/corpus/fix_decode/` is fed to the
//! **real** two-stage decode path the acceptor drives on every inbound TCP read
//! (`BoundedFrameDecoder::decode` — the framing layer — then
//! [`fauxchange::gateway::fix::decode`] — the tag-value layer;
//! `src/gateway/fix/acceptor.rs::dispatch`) and MUST produce the **correct
//! typed reject** — asserted by the **specific** `CodecError` (framing layer) /
//! [`FixDecodeError`] variant and its [`FixRejectRoute`] classification, never
//! a blanket `is_err()` — with:
//!
//! - **no panic** (every fixture runs the production decode path, both stages);
//! - **no silent accept** (a fixture designed to be hostile never decodes as a
//!   valid message).
//!
//! This is the SAME corpus the `cargo-fuzz` target
//! (`fuzz/fuzz_targets/fix_decode.rs`) uses as its seed corpus — one committed
//! set of files serves both the coverage-guided fuzzer and this fixed
//! typed-reject assertion suite, so they can never drift apart.
//!
//! ## Scope: parser-level rejects only
//!
//! Every fixture here is a **decode-layer** (parser) attack — oversized frames,
//! truncated messages, tag injection, duplicate/missing required tags,
//! out-of-range economic fields, malformed symbols, repeating-group
//! delimiter/order violations. A decode-layer failure can only ever route to a
//! session `Reject (3)` (a [`FixRejectRoute::SessionReject`], structural/format
//! failure) or a `BusinessMessageReject (j)` (a recognised-but-unhandled
//! application `MsgType`) — `8 Rejected` / `OrderCancelReject (9)` /
//! `MarketDataRequestReject (Y)` require a message that decoded successfully
//! and then failed a downstream ORDER-PATH business rule (an unknown order, a
//! missing permission, an unsupported entry type), which is out of scope for a
//! parser fixture by construction. Those reply types are already proven correct
//! end-to-end over a live TCP session in `tests/fix_session.rs`
//! (`test_malformed_frame_is_a_session_reject_3`,
//! `test_unsupported_application_message_is_business_message_reject_j`,
//! `test_cancel_of_unknown_order_is_order_cancel_reject_9`,
//! `test_market_data_trade_only_request_is_a_market_data_reject`,
//! `test_conflicting_clordid_reuse_is_rejected_duplicate_order`) — this suite
//! does not re-prove the wire rendering, only the decode-layer classification
//! that feeds it.
//!
//! ## Regenerating the corpus
//!
//! The committed files are (re)generated from the real wire-frame builders with
//! `UPDATE_CORPUS=1 cargo test --test fix_adversarial` (mirroring the
//! `tests/adversarial.rs` convention), then committed. The default run **reads
//! the committed files** and asserts the typed reject — it never regenerates,
//! so a drift is a test failure.
//!
//! ## A real fuzzer-found regression: `mid_body_checksum_overflow.fix`
//!
//! `cargo fuzz run fix_decode` once found a genuine crash within ~90s of mutating
//! `group_count_mismatch_delimiter_violation.fix` below: a mid-body injected
//! `CheckSum (10) = 624` (before the real trailing `10=130`) overflowed
//! `ironfix-tagvalue` 0.3.0's `parse_checksum` `u8` fold (`d0*100 + d1*10 + d2`;
//! `6*100` overflows). It was first closed by a venue `reject_malformed_checksum`
//! pre-decode guard; as of `ironfix-tagvalue` 0.3.1 (#140) the fix lives UPSTREAM
//! — `Decoder::decode` folds the FIRST `CheckSum (10)` it reaches with a CHECKED
//! `parse_checksum` (u16, range-checked to `0..=255`), returning
//! `DecodeError::InvalidFieldValue{tag:10}` (→ `FixDecodeError::Framing`, a typed
//! session reject) instead of overflowing — so the venue guard was retired and
//! this fixture now proves the UPSTREAM checked decoder rejects it with no panic.
//! `mid_body_checksum_overflow.fix` is the ACTUAL minimized crashing input
//! (committed byte-for-byte, not reconstructed through [`frame_with_body`]) as a
//! permanent regression seed.

use std::fs;
use std::path::PathBuf;

use bytes::BytesMut;
use fauxchange::config::DEFAULT_FIX_MAX_FRAME_BYTES;
use fauxchange::gateway::fix::limits::{MAX_FIELDS_PER_MESSAGE, MAX_GROUP_ENTRIES};
use fauxchange::gateway::fix::{
    BoundedFrameDecoder, FixDecodeError, FixRejectRoute, SessionRejectReason, decode,
};
// #140: the framing layer's own hostile-arithmetic prechecks were retired; every
// framing reject now surfaces as `ironfix_transport::CodecError` straight from the
// checked `FixCodec`.
use ironfix_transport::CodecError;

// ============================================================================
// Corpus file plumbing (read committed files; regenerate under UPDATE_CORPUS)
// ============================================================================

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/fix_decode")
}

/// Returns the committed corpus bytes for `name`, (re)writing them from
/// `produce` first when `UPDATE_CORPUS` is set. The assertion always feeds the
/// **on-disk** bytes to the decode path, so a committed file that drifts from
/// its producer is caught.
fn corpus(name: &str, produce: impl FnOnce() -> Vec<u8>) -> Vec<u8> {
    let path = corpus_dir().join(name);
    if std::env::var_os("UPDATE_CORPUS").is_some() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create corpus dir");
        }
        fs::write(&path, produce()).expect("write corpus file");
    }
    fs::read(&path).unwrap_or_else(|e| panic!("read corpus file {}: {e}", path.display()))
}

// ============================================================================
// Wire-frame helpers
// ============================================================================

/// Frames `body` on an arbitrary (possibly hostile) `begin_string`, computing a
/// REAL `BodyLength (9)` and `CheckSum (10)` over the actual bytes — so a
/// fixture fails for the SPECIFIC reason it targets, never an incidental
/// checksum/length mismatch.
fn frame(begin_string: &str, body: &[u8]) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.extend_from_slice(format!("8={begin_string}\x01").as_bytes());
    msg.extend_from_slice(format!("9={}\x01", body.len()).as_bytes());
    msg.extend_from_slice(body);
    let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
    msg.extend_from_slice(format!("10={:03}\x01", (sum % 256) as u8).as_bytes());
    msg
}

/// [`frame`] on the pinned `FIX.4.4` begin string.
fn frame_with_body(body: &[u8]) -> Vec<u8> {
    frame("FIX.4.4", body)
}

// ============================================================================
// Fixtures — framing layer (BoundedFrameDecoder)
// ============================================================================

/// Oversized frame: the declared `BodyLength (9)` alone exceeds the configured
/// cap — rejected before the full declared body arrives, no unbounded allocation.
/// A single body field (`35=0`) follows the length field so the buffer clears
/// `FixCodec`'s 20-byte minimum and the codec parses the (over-cap) declared
/// length, firing `MessageTooLarge` *before* its completeness check.
fn oversized_frame_declared_body_length_over_cap() -> Vec<u8> {
    let mut msg = Vec::new();
    msg.extend_from_slice(b"8=FIX.4.4\x01");
    msg.extend_from_slice(format!("9={}\x01", DEFAULT_FIX_MAX_FRAME_BYTES + 1).as_bytes());
    msg.extend_from_slice(b"35=0\x01");
    msg
}

/// Truncated message: a valid `BeginString`/`BodyLength` header followed by
/// only PART of the declared body — no `CheckSum` trailer at all (a TCP read
/// landing mid-frame). The framing layer must buffer for more bytes, not
/// reject and not panic.
fn truncated_incomplete_frame_no_trailer() -> Vec<u8> {
    let full_body = b"35=0\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01";
    let mut msg = Vec::new();
    msg.extend_from_slice(b"8=FIX.4.4\x01");
    msg.extend_from_slice(format!("9={}\x01", full_body.len()).as_bytes());
    msg.extend_from_slice(&full_body[..full_body.len() / 2]);
    msg
}

/// A complete, correctly-length-declared frame whose trailing `CheckSum (10)`
/// digits are `999` — a value `> 255`, never a real `sum % 256` checksum, and the
/// exact shape that once overflowed `ironfix-tagvalue`'s `u8` fold (now rejected
/// by the checked `parse_checksum` in 0.3.1 → `CodecError::InvalidBodyLength`).
fn malformed_checksum_value_out_of_range() -> Vec<u8> {
    let body = b"35=0\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01";
    let mut msg = Vec::new();
    msg.extend_from_slice(b"8=FIX.4.4\x01");
    msg.extend_from_slice(format!("9={}\x01", body.len()).as_bytes());
    msg.extend_from_slice(body);
    msg.extend_from_slice(b"10=999\x01");
    msg
}

// ============================================================================
// Fixtures — tag-value decode layer (fix::decode)
// ============================================================================

/// `BeginString (8)` is not the pinned `FIX.4.4`.
fn begin_string_mismatch() -> Vec<u8> {
    let body = b"35=0\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01";
    frame("FIX.4.2", body)
}

/// A structurally unknown `MsgType (35)` — not a tag `ironfix_core::MsgType`
/// recognises at all.
fn unsupported_structural_msg_type() -> Vec<u8> {
    let body = b"35=ZZ\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01";
    frame_with_body(body)
}

/// A well-formed application message with an unsupported `MsgType` (`R`,
/// QuoteRequest — recognised by FIX 4.4, unhandled by the venue dialect).
fn unsupported_application_msg_type() -> Vec<u8> {
    let body = b"35=R\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01";
    frame_with_body(body)
}

/// A `NewOrderSingle (D)` missing the required `Side (54)`.
fn missing_required_tag_side() -> Vec<u8> {
    let body = b"35=D\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x0111=nodside\x0155=BTC-20240329-50000-C\x0160=20240329-12:00:00.000\x0140=2\x0144=500.00\x0138=1\x0159=1\x01";
    frame_with_body(body)
}

/// A duplicate `MsgSeqNum (34)` outside any repeating group.
fn duplicate_scalar_tag_seqnum() -> Vec<u8> {
    let body = b"35=0\x0149=CLIENT\x0156=VENUE\x0134=1\x0134=2\x0152=20240329-12:00:00.000\x01";
    frame_with_body(body)
}

/// Tag injection: a `NewOrderSingle (D)` carrying an injected `NoRelatedSym
/// (146)` group-count tag — a field that is NEVER legitimate on this message
/// type — positioned between two `Symbol (55)` fields, attempting to fake-open
/// a group span so the duplicate `Symbol` looks like a legitimate repeat. The
/// message-type-keyed duplicate check (P2.3,
/// [`codec::FieldBag::reject_duplicate_scalar_tags`](fauxchange::gateway::fix))
/// closes this regardless of any in-stream marker, proven here end-to-end
/// through the wire decoder (not just the internal `FieldBag` unit test).
fn tag_injection_fake_group_count_reopens_symbol() -> Vec<u8> {
    let body = b"35=D\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x0155=BTC-20240329-50000-C\x01146=1\x0155=BTC-20240329-50000-C\x01";
    frame_with_body(body)
}

/// An out-of-range economic field: `Price (44)` whose integer-cents value
/// exceeds the representable `u64` range.
fn out_of_range_price() -> Vec<u8> {
    let body = b"35=D\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x0140=2\x0144=99999999999999999999999999999999.99\x0111=cl-1\x0155=BTC-20240329-50000-C\x0154=1\x0160=20240329-12:00:00.000\x0138=1\x01";
    frame_with_body(body)
}

/// A `Symbol (55)` that does not parse as `UNDERLYING-YYYYMMDD-STRIKE-STYLE`.
fn malformed_symbol_wrong_grammar() -> Vec<u8> {
    let body = b"35=D\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x0140=2\x0144=500.00\x0111=cl-1\x0155=NOT-A-VALID-SYMBOL\x0154=1\x0160=20240329-12:00:00.000\x0138=1\x01";
    frame_with_body(body)
}

/// A repeating-group delimiter/order violation: `MarketDataRequest (V)`
/// declares `NoRelatedSym (146) = 2` but supplies only ONE `Symbol (55)` group
/// entry.
fn group_count_mismatch_delimiter_violation() -> Vec<u8> {
    let body = b"35=V\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01262=MDR-1\x01263=1\x01264=0\x01267=1\x01269=0\x01146=2\x0155=BTC-20240329-50000-C\x01";
    frame_with_body(body)
}

/// A repeating group declaring far more entries than the decode-layer ceiling
/// — rejected cheaply, before any per-entry work (a DoS-shaped input).
fn group_entries_over_ceiling() -> Vec<u8> {
    let body = format!(
        "35=V\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01262=MDR-1\x01263=1\x01264=0\x01267={over}\x01269=0\x01146=1\x0155=BTC-20240329-50000-C\x01",
        over = MAX_GROUP_ENTRIES + 10,
    );
    frame_with_body(body.as_bytes())
}

/// A message carrying more fields than the decode-layer ceiling — rejected
/// cheaply, before any per-field work (a DoS-shaped input; the largest
/// committed fixture, deliberately so it also exercises the framing layer's
/// own byte-length behaviour on a large-but-legitimately-declared frame).
fn fields_over_ceiling() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(
        b"35=0\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01",
    );
    for _ in 0..(MAX_FIELDS_PER_MESSAGE + 10) {
        // MDEntryType (269) is a pure group-member tag (limits.rs
        // PURE_GROUP_MEMBER_TAGS) and so is always legitimately repeatable —
        // the duplicate-scalar-tag check never fires first here, isolating
        // the field-count ceiling.
        body.extend_from_slice(b"269=0\x01");
    }
    frame_with_body(&body)
}

/// The ACTUAL minimized `cargo fuzz run fix_decode` crashing input (141 bytes,
/// committed byte-for-byte — see the module doc "A real fuzzer-found
/// regression"): a `MarketDataRequest` carrying an injected mid-body `CheckSum
/// (10) = 624` BEFORE the real trailing `CheckSum (10) = 130`. `624 > 255` once
/// overflowed `ironfix-tagvalue`'s `parse_checksum` u8 fold; `Decoder::decode`
/// folds the FIRST tag-10 occurrence it reaches, so the mid-body one crashed the
/// pre-0.3.1 unchecked fold. As of 0.3.1 (#140) that fold is checked, so this is
/// now rejected as a typed `Framing` reject upstream. Deliberately NOT
/// reconstructed through [`frame_with_body`] (which appends only one trailer) —
/// this is the literal regression bytes.
fn mid_body_checksum_overflow() -> Vec<u8> {
    b"8=FIX.4.4\x019=118\x0135=V\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01262=MDR-1\x01263=1\x01264=0\x01267=1\x01269=\x0110=624\x0155=BTC-20240329-50000-C\x0110=130\x01".to_vec()
}

// ============================================================================
// Tests — framing layer
// ============================================================================

#[test]
fn test_oversized_frame_is_rejected_at_the_framing_layer_before_a_frame_completes() {
    // #140: the by-policy byte cap `BoundedFrameDecoder` sets on `FixCodec`
    // (`max_message_size = max_frame_bytes`) rejects an over-cap declared length as
    // `MessageTooLarge` at the framing boundary — before the full body arrives and
    // with no unbounded allocation. The checked frame-length add means no panic.
    let bytes = corpus(
        "oversized_frame_declared_body_length_over_cap.fix",
        oversized_frame_declared_body_length_over_cap,
    );
    let mut buf = BytesMut::from(bytes.as_slice());
    let mut decoder = BoundedFrameDecoder::new(DEFAULT_FIX_MAX_FRAME_BYTES);
    match decoder.decode(&mut buf) {
        Err(CodecError::MessageTooLarge { size, max_size }) => {
            assert!(size > DEFAULT_FIX_MAX_FRAME_BYTES);
            assert_eq!(max_size, DEFAULT_FIX_MAX_FRAME_BYTES);
        }
        other => panic!("expected MessageTooLarge, got {other:?}"),
    }
}

#[test]
fn test_truncated_message_is_buffered_not_rejected_and_never_panics() {
    let bytes = corpus(
        "truncated_incomplete_frame_no_trailer.fix",
        truncated_incomplete_frame_no_trailer,
    );
    let mut buf = BytesMut::from(bytes.as_slice());
    let mut decoder = BoundedFrameDecoder::new(DEFAULT_FIX_MAX_FRAME_BYTES);
    match decoder.decode(&mut buf) {
        Ok(None) => {}
        other => panic!("a genuinely incomplete frame must be buffered (Ok(None)), got {other:?}"),
    }
}

#[test]
fn test_malformed_checksum_value_is_rejected_at_the_framing_layer() {
    // #140: a trailing `CheckSum (10) = 999` (> 255) makes `FixCodec`'s checked
    // `parse_checksum` return None, which the codec maps to `InvalidBodyLength` — a
    // typed framing reject, no u8-fold overflow and no panic.
    let bytes = corpus(
        "malformed_checksum_value_out_of_range.fix",
        malformed_checksum_value_out_of_range,
    );
    let mut buf = BytesMut::from(bytes.as_slice());
    let mut decoder = BoundedFrameDecoder::new(DEFAULT_FIX_MAX_FRAME_BYTES);
    match decoder.decode(&mut buf) {
        Err(CodecError::InvalidBodyLength) => {}
        other => panic!("expected InvalidBodyLength, got {other:?}"),
    }
}

// ============================================================================
// Tests — tag-value decode layer
// ============================================================================

#[test]
fn test_begin_string_mismatch_is_a_typed_session_reject() {
    let bytes = corpus("begin_string_mismatch.fix", begin_string_mismatch);
    match decode(&bytes) {
        Err(err @ FixDecodeError::BeginStringMismatch { .. }) => {
            assert!(matches!(
                err.reject_route(),
                FixRejectRoute::SessionReject {
                    ref_tag: Some(8),
                    ..
                }
            ));
        }
        other => panic!("expected BeginStringMismatch, got {other:?}"),
    }
}

#[test]
fn test_unsupported_structural_msg_type_is_a_typed_session_reject() {
    let bytes = corpus(
        "unsupported_structural_msg_type.fix",
        unsupported_structural_msg_type,
    );
    match decode(&bytes) {
        Err(err @ FixDecodeError::UnsupportedMsgType { .. }) => {
            assert!(matches!(
                err.reject_route(),
                FixRejectRoute::SessionReject {
                    ref_tag: Some(35),
                    ..
                }
            ));
        }
        other => panic!("expected UnsupportedMsgType, got {other:?}"),
    }
}

#[test]
fn test_unsupported_application_msg_type_routes_to_business_message_reject() {
    let bytes = corpus(
        "unsupported_application_msg_type.fix",
        unsupported_application_msg_type,
    );
    match decode(&bytes) {
        Err(ref err @ FixDecodeError::UnsupportedApplicationMsgType { ref msg_type }) => {
            assert_eq!(msg_type, "R");
            assert_eq!(err.reject_route(), FixRejectRoute::BusinessMessageReject);
        }
        other => panic!("expected UnsupportedApplicationMsgType, got {other:?}"),
    }
}

#[test]
fn test_missing_required_side_is_a_typed_session_reject() {
    let bytes = corpus("missing_required_tag_side.fix", missing_required_tag_side);
    match decode(&bytes) {
        Err(FixDecodeError::MissingRequiredField { tag: 54 }) => {}
        other => panic!("expected MissingRequiredField(54), got {other:?}"),
    }
}

#[test]
fn test_duplicate_scalar_seqnum_is_a_typed_session_reject() {
    let bytes = corpus(
        "duplicate_scalar_tag_seqnum.fix",
        duplicate_scalar_tag_seqnum,
    );
    match decode(&bytes) {
        Err(FixDecodeError::DuplicateTag { tag: 34 }) => {}
        other => panic!("expected DuplicateTag(34), got {other:?}"),
    }
}

#[test]
fn test_tag_injection_cannot_smuggle_a_duplicate_symbol() {
    let bytes = corpus(
        "tag_injection_fake_group_count_reopens_symbol.fix",
        tag_injection_fake_group_count_reopens_symbol,
    );
    match decode(&bytes) {
        Err(FixDecodeError::DuplicateTag { tag: 55 }) => {}
        other => panic!(
            "expected the injected group-count tag to NOT re-exempt the duplicate \
             Symbol(55), got {other:?}"
        ),
    }
}

#[test]
fn test_out_of_range_price_is_a_typed_price_seam_reject() {
    let bytes = corpus("out_of_range_price.fix", out_of_range_price);
    match decode(&bytes) {
        Err(FixDecodeError::Price { tag: 44, .. }) => {}
        other => panic!("expected Price(44) seam error, got {other:?}"),
    }
}

#[test]
fn test_malformed_symbol_is_a_typed_session_reject() {
    let bytes = corpus(
        "malformed_symbol_wrong_grammar.fix",
        malformed_symbol_wrong_grammar,
    );
    match decode(&bytes) {
        Err(err @ FixDecodeError::MalformedSymbol { .. }) => {
            assert!(matches!(
                err.reject_route(),
                FixRejectRoute::SessionReject {
                    ref_tag: Some(55),
                    ..
                }
            ));
        }
        other => panic!("expected MalformedSymbol, got {other:?}"),
    }
}

#[test]
fn test_group_count_mismatch_is_a_typed_session_reject() {
    let bytes = corpus(
        "group_count_mismatch_delimiter_violation.fix",
        group_count_mismatch_delimiter_violation,
    );
    match decode(&bytes) {
        Err(FixDecodeError::GroupCountMismatch {
            count_tag: 146,
            declared: 2,
            decoded: 1,
        }) => {}
        other => panic!("expected GroupCountMismatch(146, 2, 1), got {other:?}"),
    }
}

#[test]
fn test_group_entries_over_ceiling_is_rejected_cheaply() {
    let bytes = corpus("group_entries_over_ceiling.fix", group_entries_over_ceiling);
    match decode(&bytes) {
        Err(FixDecodeError::TooManyGroupEntries {
            count_tag: 267,
            max,
            ..
        }) => {
            assert_eq!(max, MAX_GROUP_ENTRIES);
        }
        other => panic!("expected TooManyGroupEntries(267), got {other:?}"),
    }
}

#[test]
fn test_fields_over_ceiling_is_rejected_cheaply() {
    let bytes = corpus("fields_over_ceiling.fix", fields_over_ceiling);
    match decode(&bytes) {
        Err(FixDecodeError::TooManyFields { count, max }) => {
            assert!(count > max);
            assert_eq!(max, MAX_FIELDS_PER_MESSAGE);
        }
        other => panic!("expected TooManyFields, got {other:?}"),
    }
}

#[test]
fn test_fuzzer_found_mid_body_checksum_overflow_is_a_typed_reject_not_a_panic() {
    // Regression for the #042 fuzzer-discovered crash (see the module doc "A real
    // fuzzer-found regression"): a mid-body injected CheckSum(10)=624 (> 255) used
    // to overflow `ironfix-tagvalue`'s u8 checksum fold inside `Decoder::decode`.
    // As of ironfix-tagvalue 0.3.1 (#140) `Decoder::decode` folds the FIRST tag-10
    // it reaches with the CHECKED `parse_checksum` (u16, range-checked), returning
    // `DecodeError::InvalidFieldValue{tag:10}` → `FixDecodeError::Framing` — a typed
    // session reject, no panic in debug OR release. The reject CLASS matches the
    // retired guard's (`SessionReject` / `IncorrectDataFormat`); the checked decoder
    // does not carry the optional `RefTagID (371) = 10` hint the guard did.
    let bytes = corpus("mid_body_checksum_overflow.fix", mid_body_checksum_overflow);
    match decode(&bytes) {
        Err(err @ FixDecodeError::Framing(_)) => {
            assert!(matches!(
                err.reject_route(),
                FixRejectRoute::SessionReject {
                    reason: SessionRejectReason::IncorrectDataFormat,
                    ..
                }
            ));
        }
        other => panic!("expected a Framing reject, got {other:?}"),
    }
}

// ============================================================================
// Blanket sweep — every committed fixture, through the REAL two-stage pipeline
// ============================================================================

/// Feeds every file committed under `fuzz/corpus/fix_decode/` through the SAME
/// `BoundedFrameDecoder` → `decode` pipeline the fuzz target and the acceptor
/// use, asserting only the one property every fixture shares regardless of its
/// specific typed reject: **no panic**. New fixtures dropped into the corpus
/// directory are automatically covered by this sweep even before a specific
/// per-fixture assertion is written for them.
#[test]
fn test_every_committed_corpus_fixture_never_panics_through_the_real_pipeline() {
    let dir = corpus_dir();
    let mut checked = 0usize;
    for entry in
        fs::read_dir(&dir).unwrap_or_else(|e| panic!("read corpus dir {}: {e}", dir.display()))
    {
        let entry = entry.expect("dir entry");
        if !entry.file_type().expect("file type").is_file() {
            continue;
        }
        let bytes = fs::read(entry.path()).expect("read fixture");
        let mut buf = BytesMut::from(bytes.as_slice());
        let mut decoder = BoundedFrameDecoder::new(DEFAULT_FIX_MAX_FRAME_BYTES);
        while let Ok(Some(frame)) = decoder.decode(&mut buf) {
            let _ = decode(&frame);
        }
        checked += 1;
    }
    assert!(
        checked >= 15,
        "expected at least 15 committed adversarial fixtures, found {checked} \
         (the corpus directory shrank — the fuzz job and this suite lose coverage)"
    );
}
