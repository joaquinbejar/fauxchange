//! Transport layer: FIX 4.4 gateway — an acceptor built on IronFix
//! primitives. Order-entry, execution reports, and market data, tier T2
//! (v0.4).
//!
//! This module is the **typed message vocabulary** (#036): a typed struct per
//! supported message on the pinned `fauxchange.fix44.v1` dialect
//! ([docs/specs/fix-dialect.md](../../../docs/specs/fix-dialect.md)), with
//! hand-written encode/decode over `ironfix-tagvalue`'s `Decoder`/`Encoder`
//! (`ironfix-derive` is `todo!()`, so the structs are hand-written by design),
//! requiredness/group/enum validation, and the checked decimal-`Price` ↔
//! integer-`Cents` [`price`] seam. The TCP [`acceptor`] (#037) frames inbound
//! bytes and decodes them through this vocabulary at a dispatch seam; the session
//! FSM and durable sequence store (#038), the order-path routing (#039), and the
//! market-data wiring (#040) plug into that seam. The acceptor is spawned by
//! `main.rs` when `[fix] enabled` is set (disabled by default until #038 lands).
//!
//! Governed by `docs/03-protocol-surfaces.md`.

use ironfix_core::message::MsgType;
use ironfix_dictionary::Version;
use ironfix_tagvalue::Decoder;

pub mod acceptor;
pub mod codec;
pub mod enums;
pub mod error;
pub mod execution;
pub mod fsm;
pub mod header;
pub mod limits;
pub mod marketdata;
pub mod md_projection;
pub mod order;
pub mod order_flow;
pub mod pg_store;
pub mod price;
pub mod session;
pub mod store;

use codec::FieldBag;
use limits::{MAX_FIELDS_PER_MESSAGE, truncate_untrusted};

pub use acceptor::{
    BoundedFrameDecoder, FixAcceptor, FixAcceptorConfig, FixSession, FixSessionFactory,
    OutboundBusy, SessionControl, SessionOutbound, StubSession, StubSessionFactory,
    message_type_str,
};
pub use enums::{
    CommType, CxlRejResponseTo, ExecType, LastLiquidityInd, MassCancelRequestType,
    MassCancelResponse, MdEntryType, MdUpdateAction, OrdStatus, OrdType, OrderSide,
    SubscriptionRequestType, TimeInForce,
};
pub use error::{FixDecodeError, FixRejectRoute, SessionRejectReason};
pub use fsm::{
    SessionConfig, SessionError, SessionFsm, SessionPhase, VenueFixSession, VenueFixSessionFactory,
};
pub use header::{StandardHeader, UtcTimestamp};
pub use pg_store::{PgFixSessionStore, select_fix_session_store};
pub use price::{
    CENTS_SCALE, PriceScale, PriceSeamError, parse_decimal_to_cents, parse_signed_decimal_to_cents,
    render_cents_to_decimal, render_signed_cents_to_decimal,
};
pub use store::{
    FixSessionStore, InMemoryFixSessionStore, ResetTrigger, SequenceResetEvent, SessionCounters,
    SessionKey, SessionStoreError, StoredOutbound,
};

/// The pinned dialect version this gateway speaks — versioned with the wire
/// surface ([fix-dialect §1](../../../docs/specs/fix-dialect.md), [SEMVER.md](../../../docs/SEMVER.md)).
///
/// A dialect change (a field, a requiredness, an enum value) bumps this constant,
/// the affected goldens, and the round-trip tests in one commit.
pub const FIX_DIALECT: &str = "fauxchange.fix44.v1";

/// The FIX `BeginString (8)` value, fixed to `FIX.4.4` via
/// [`ironfix_dictionary::Version::Fix44`] ([fix-dialect §1](../../../docs/specs/fix-dialect.md)).
pub const BEGIN_STRING: &str = Version::Fix44.begin_string();

/// A typed FIX message: the standard-header carrier that encodes to wire bytes
/// and decodes back from a message's fields.
///
/// Every supported message implements this so the vocabulary shares one
/// encode/decode contract and the round-trip property (`decode(encode(m)) == m`)
/// is uniform.
pub trait FixBody: Sized {
    /// The `MsgType (35)` value (`A`, `D`, `8`, …).
    const MSG_TYPE: &'static str;

    /// The standard header carried on this message.
    fn header(&self) -> &StandardHeader;

    /// Decodes the message body from its fields, given the already-decoded
    /// standard header.
    ///
    /// # Errors
    ///
    /// A [`FixDecodeError`] for any missing/mis-conditioned/malformed field.
    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError>;

    /// Encodes the message to complete FIX wire bytes (header/trailer framed).
    fn encode(&self) -> Vec<u8>;
}

/// A decoded FIX message of any type the dialect supports.
///
/// The vocabulary decodes both directions so the round-trip property holds per
/// message; the acceptor (#037) routes only the inbound subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedMessage {
    /// `Logon (A)`.
    Logon(session::Logon),
    /// `Logout (5)`.
    Logout(session::Logout),
    /// `Heartbeat (0)`.
    Heartbeat(session::Heartbeat),
    /// `TestRequest (1)`.
    TestRequest(session::TestRequest),
    /// `ResendRequest (2)`.
    ResendRequest(session::ResendRequest),
    /// `SequenceReset (4)`.
    SequenceReset(session::SequenceReset),
    /// `Reject (3)`.
    Reject(session::Reject),
    /// `NewOrderSingle (D)`.
    NewOrderSingle(order::NewOrderSingle),
    /// `OrderCancelRequest (F)`.
    OrderCancelRequest(order::OrderCancelRequest),
    /// `OrderCancelReplaceRequest (G)`.
    OrderCancelReplaceRequest(order::OrderCancelReplaceRequest),
    /// `OrderMassCancelRequest (q)`.
    OrderMassCancelRequest(order::OrderMassCancelRequest),
    /// `OrderStatusRequest (H)`.
    OrderStatusRequest(order::OrderStatusRequest),
    /// `ExecutionReport (8)`.
    ExecutionReport(execution::ExecutionReport),
    /// `OrderCancelReject (9)`.
    OrderCancelReject(execution::OrderCancelReject),
    /// `OrderMassCancelReport (r)`.
    OrderMassCancelReport(execution::OrderMassCancelReport),
    /// `BusinessMessageReject (j)`.
    BusinessMessageReject(execution::BusinessMessageReject),
    /// `MarketDataRequest (V)`.
    MarketDataRequest(marketdata::MarketDataRequest),
    /// `MarketDataSnapshotFullRefresh (W)`.
    MarketDataSnapshotFullRefresh(marketdata::MarketDataSnapshotFullRefresh),
    /// `MarketDataIncrementalRefresh (X)`.
    MarketDataIncrementalRefresh(marketdata::MarketDataIncrementalRefresh),
    /// `MarketDataRequestReject (Y)`.
    MarketDataRequestReject(marketdata::MarketDataRequestReject),
}

impl DecodedMessage {
    /// Re-encodes the message to complete FIX wire bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Logon(m) => m.encode(),
            Self::Logout(m) => m.encode(),
            Self::Heartbeat(m) => m.encode(),
            Self::TestRequest(m) => m.encode(),
            Self::ResendRequest(m) => m.encode(),
            Self::SequenceReset(m) => m.encode(),
            Self::Reject(m) => m.encode(),
            Self::NewOrderSingle(m) => m.encode(),
            Self::OrderCancelRequest(m) => m.encode(),
            Self::OrderCancelReplaceRequest(m) => m.encode(),
            Self::OrderMassCancelRequest(m) => m.encode(),
            Self::OrderStatusRequest(m) => m.encode(),
            Self::ExecutionReport(m) => m.encode(),
            Self::OrderCancelReject(m) => m.encode(),
            Self::OrderMassCancelReport(m) => m.encode(),
            Self::BusinessMessageReject(m) => m.encode(),
            Self::MarketDataRequest(m) => m.encode(),
            Self::MarketDataSnapshotFullRefresh(m) => m.encode(),
            Self::MarketDataIncrementalRefresh(m) => m.encode(),
            Self::MarketDataRequestReject(m) => m.encode(),
        }
    }
}

/// Decodes a complete FIX frame into a typed [`DecodedMessage`].
///
/// The frame is validated by the `ironfix` codec (checksum, `BodyLength (9)`,
/// `MsgType (35)`, framing), the `BeginString (8)` is checked against the pinned
/// `FIX.4.4`, and the standard header + body are decoded into the matching typed
/// struct. An application `MsgType` the venue understands but has no struct for
/// is classified as [`FixDecodeError::UnsupportedApplicationMsgType`] (routing to
/// `BusinessMessageReject (j)`, #039); a structurally unknown `MsgType` is a
/// session-level [`FixDecodeError::UnsupportedMsgType`]. **No `.unwrap()` runs on
/// the caller bytes.**
///
/// # Errors
///
/// A [`FixDecodeError`] for any framing, header, requiredness, enum, group, or
/// price-seam failure.
pub fn decode(bytes: &[u8]) -> Result<DecodedMessage, FixDecodeError> {
    // `BodyLength (9)` and `CheckSum (10)` are the two attacker-controlled numeric
    // fields the decoder folds. As of ironfix-tagvalue 0.3.1 both are folded with
    // CHECKED, non-wrapping arithmetic inside `Decoder::decode`: the frame-length
    // add is a `checked_add` chain plus an exact declared-vs-actual body-length
    // match (→ `DecodeError::InvalidBodyLength`), and `parse_checksum` folds the
    // three digits in `u16` and range-checks to `0..=255` (→ the FIRST tag-10 it
    // reaches yields `DecodeError::InvalidFieldValue`, covering a duplicate /
    // mid-body `10=`), with numeric tag folding so a zero-padded `009=` / `010=`
    // cannot bypass it. Both surface through `FixDecodeError::Framing` as a session
    // `Reject (3)` with `IncorrectDataFormat` — no panic, in a debug or a release
    // build. The venue's own pre-decode guards were therefore retired in #140 (the
    // decoder now owns both checks); `BoundedFrameDecoder` stays only as the
    // framing-layer byte-cap DoS ceiling.
    let raw = Decoder::new(bytes).decode()?;

    let begin_string = raw.begin_string();
    if begin_string != BEGIN_STRING {
        return Err(FixDecodeError::BeginStringMismatch {
            expected: BEGIN_STRING,
            actual: truncate_untrusted(begin_string),
        });
    }

    // Bound the field count and reject a duplicate scalar tag (no silent
    // first-wins) before dispatching to a message decoder.
    if raw.field_count() > MAX_FIELDS_PER_MESSAGE {
        return Err(FixDecodeError::TooManyFields {
            count: raw.field_count(),
            max: MAX_FIELDS_PER_MESSAGE,
        });
    }
    let fields = FieldBag::collect(&raw);
    // `Symbol (55)` is a repeating-group member only in MarketDataRequest (V) /
    // IncrementalRefresh (X); in every other message it is a scalar and a
    // duplicate is a session violation. Keying on `MsgType (35)` (not on
    // in-stream field position) is what makes the duplicate check unspoofable —
    // a bogus `NoRelatedSym` count injected into a NewOrderSingle cannot make a
    // duplicate Symbol look legitimate.
    let symbol_repeatable = limits::symbol_repeats_in_msg_type(raw.msg_type());
    fields.reject_duplicate_scalar_tags(symbol_repeatable)?;
    let header = StandardHeader::decode(&fields)?;

    match raw.msg_type() {
        MsgType::Logon => Ok(DecodedMessage::Logon(session::Logon::decode_body(
            header, &fields,
        )?)),
        MsgType::Logout => Ok(DecodedMessage::Logout(session::Logout::decode_body(
            header, &fields,
        )?)),
        MsgType::Heartbeat => Ok(DecodedMessage::Heartbeat(session::Heartbeat::decode_body(
            header, &fields,
        )?)),
        MsgType::TestRequest => Ok(DecodedMessage::TestRequest(
            session::TestRequest::decode_body(header, &fields)?,
        )),
        MsgType::ResendRequest => Ok(DecodedMessage::ResendRequest(
            session::ResendRequest::decode_body(header, &fields)?,
        )),
        MsgType::SequenceReset => Ok(DecodedMessage::SequenceReset(
            session::SequenceReset::decode_body(header, &fields)?,
        )),
        MsgType::Reject => Ok(DecodedMessage::Reject(session::Reject::decode_body(
            header, &fields,
        )?)),
        MsgType::NewOrderSingle => Ok(DecodedMessage::NewOrderSingle(
            order::NewOrderSingle::decode_body(header, &fields)?,
        )),
        MsgType::OrderCancelRequest => Ok(DecodedMessage::OrderCancelRequest(
            order::OrderCancelRequest::decode_body(header, &fields)?,
        )),
        MsgType::OrderCancelReplaceRequest => Ok(DecodedMessage::OrderCancelReplaceRequest(
            order::OrderCancelReplaceRequest::decode_body(header, &fields)?,
        )),
        MsgType::OrderMassCancelRequest => Ok(DecodedMessage::OrderMassCancelRequest(
            order::OrderMassCancelRequest::decode_body(header, &fields)?,
        )),
        MsgType::OrderStatusRequest => Ok(DecodedMessage::OrderStatusRequest(
            order::OrderStatusRequest::decode_body(header, &fields)?,
        )),
        MsgType::ExecutionReport => Ok(DecodedMessage::ExecutionReport(
            execution::ExecutionReport::decode_body(header, &fields)?,
        )),
        MsgType::OrderCancelReject => Ok(DecodedMessage::OrderCancelReject(
            execution::OrderCancelReject::decode_body(header, &fields)?,
        )),
        MsgType::OrderMassCancelReport => Ok(DecodedMessage::OrderMassCancelReport(
            execution::OrderMassCancelReport::decode_body(header, &fields)?,
        )),
        MsgType::BusinessMessageReject => Ok(DecodedMessage::BusinessMessageReject(
            execution::BusinessMessageReject::decode_body(header, &fields)?,
        )),
        MsgType::MarketDataRequest => Ok(DecodedMessage::MarketDataRequest(
            marketdata::MarketDataRequest::decode_body(header, &fields)?,
        )),
        MsgType::MarketDataSnapshotFullRefresh => {
            Ok(DecodedMessage::MarketDataSnapshotFullRefresh(
                marketdata::MarketDataSnapshotFullRefresh::decode_body(header, &fields)?,
            ))
        }
        MsgType::MarketDataIncrementalRefresh => Ok(DecodedMessage::MarketDataIncrementalRefresh(
            marketdata::MarketDataIncrementalRefresh::decode_body(header, &fields)?,
        )),
        MsgType::MarketDataRequestReject => Ok(DecodedMessage::MarketDataRequestReject(
            marketdata::MarketDataRequestReject::decode_body(header, &fields)?,
        )),
        // A structurally unknown `MsgType` (an unmapped tag character) → session
        // `Reject (3)` with `InvalidMsgType`.
        MsgType::Custom(raw_type) => Err(FixDecodeError::UnsupportedMsgType {
            msg_type: truncate_untrusted(raw_type),
        }),
        // A recognised application `MsgType` the venue has no handler for →
        // `BusinessMessageReject (j)` (the seam hook; routing is #039).
        other => Err(FixDecodeError::UnsupportedApplicationMsgType {
            msg_type: truncate_untrusted(other.as_str()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironfix_core::types::{CompId, SeqNum};

    #[test]
    fn test_begin_string_is_fix44_from_dictionary() {
        assert_eq!(BEGIN_STRING, "FIX.4.4");
        assert_eq!(BEGIN_STRING, Version::Fix44.begin_string());
    }

    #[test]
    fn test_dialect_version_is_pinned() {
        assert_eq!(FIX_DIALECT, "fauxchange.fix44.v1");
    }

    /// Builds a complete frame from a body (the fields after `9=<len>SOH`, using
    /// real SOH), computing a **valid** BodyLength and CheckSum.
    fn frame_with_body(body: &[u8]) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(b"8=FIX.4.4\x01");
        msg.extend_from_slice(format!("9={}\x01", body.len()).as_bytes());
        msg.extend_from_slice(body);
        let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
        msg.extend_from_slice(format!("10={:03}\x01", (sum % 256) as u8).as_bytes());
        msg
    }

    /// Builds a frame with an ARBITRARY declared BodyLength(9) and a valid
    /// CheckSum over the actual bytes (the shape a BodyLength attack takes).
    fn frame_with_declared_body_length(declared: &str, body: &[u8]) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(b"8=FIX.4.4\x01");
        msg.extend_from_slice(format!("9={declared}\x01").as_bytes());
        msg.extend_from_slice(body);
        let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
        msg.extend_from_slice(format!("10={:03}\x01", (sum % 256) as u8).as_bytes());
        msg
    }

    /// Builds a frame with arbitrary literal tag LABELS for `BodyLength(9)` and
    /// `CheckSum(10)` — the numeric-tag-fold bypass shape. `bl_label`/`cs_label`
    /// are the on-the-wire tag strings (`"9"`/`"009"`, `"10"`/`"010"`); ironfix
    /// folds them numerically, so the guard must too. The CheckSum value stays
    /// valid over the preceding bytes.
    fn frame_with_tag_labels(
        bl_label: &str,
        declared: &str,
        cs_label: &str,
        body: &[u8],
    ) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.extend_from_slice(b"8=FIX.4.4\x01");
        msg.extend_from_slice(format!("{bl_label}={declared}\x01").as_bytes());
        msg.extend_from_slice(body);
        let sum: u32 = msg.iter().map(|&b| u32::from(b)).sum();
        msg.extend_from_slice(format!("{cs_label}={:03}\x01", (sum % 256) as u8).as_bytes());
        msg
    }

    /// A minimal valid Heartbeat body.
    const HEARTBEAT_BODY: &[u8] =
        b"35=0\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01";

    /// The reject class every framing/body-length/checksum failure now routes to,
    /// via `FixDecodeError::Framing` — a session `Reject (3)` with
    /// `IncorrectDataFormat`. The pre-#140 guards additionally pinned `ref_tag`
    /// (9 / 10); the checked decoder does not, but the reject CLASS is preserved
    /// (`RefTagID` is optional in FIX), which is the parity contract that matters.
    fn assert_framing_incorrect_data_format(err: &FixDecodeError) {
        assert!(
            matches!(err, FixDecodeError::Framing(_)),
            "expected a Framing reject, got {err:?}"
        );
        assert!(
            matches!(
                err.reject_route(),
                FixRejectRoute::SessionReject {
                    reason: SessionRejectReason::IncorrectDataFormat,
                    ..
                }
            ),
            "expected SessionReject/IncorrectDataFormat, got {:?}",
            err.reject_route()
        );
    }

    #[test]
    fn test_decode_rejects_oversized_body_length_without_panic() {
        // #140 regression: a valid-checksum frame whose declared BodyLength is
        // u64::MAX. ironfix-tagvalue 0.3.1's `Decoder` folds `body_start +
        // body_length` with `checked_add`, so the overflow is a typed
        // `DecodeError::InvalidBodyLength` (→ Framing) — no panic, in debug OR
        // release. This is the equivalent reject the retired `validate_body_length`
        // guard produced (same class, minus the `ref_tag: Some(9)` hint).
        let hostile = frame_with_declared_body_length("18446744073709551615", HEARTBEAT_BODY);
        match decode(&hostile) {
            Err(err) => assert_framing_incorrect_data_format(&err),
            Ok(msg) => panic!("expected a Framing reject, decoded {msg:?}"),
        }
    }

    #[test]
    fn test_decode_rejects_wrong_small_body_length() {
        // A small-but-wrong declared length is equally malformed — the decoder's
        // exact declared-vs-actual body-length match rejects it (→ Framing).
        let wrong = frame_with_declared_body_length("5", HEARTBEAT_BODY);
        match decode(&wrong) {
            Err(err) => assert_framing_incorrect_data_format(&err),
            Ok(msg) => panic!("expected a Framing reject, decoded {msg:?}"),
        }
    }

    #[test]
    fn test_decode_rejects_non_numeric_body_length() {
        // A non-numeric BodyLength fails the decoder's own parse (→ Framing).
        let bad = frame_with_declared_body_length("notanumber", HEARTBEAT_BODY);
        match decode(&bad) {
            Err(err) => assert_framing_incorrect_data_format(&err),
            Ok(msg) => panic!("expected a Framing reject, decoded {msg:?}"),
        }
    }

    #[test]
    fn test_decode_rejects_leading_zero_body_length_tag_without_panic() {
        // Bypass PoC (was a leading-zero panic path): `009=` folds to tag 9. The
        // 0.3.1 decoder folds the tag numerically AND uses `checked_add` on the
        // declared length, so `009=<u64::MAX>` is a typed reject (→ Framing), never
        // a panic — the zero-padded bypass is closed upstream.
        for label in ["009", "0009", "00009"] {
            let hostile =
                frame_with_tag_labels(label, "18446744073709551615", "10", HEARTBEAT_BODY);
            match decode(&hostile) {
                Err(err) => assert_framing_incorrect_data_format(&err),
                Ok(msg) => panic!("expected a Framing reject for `{label}=`, decoded {msg:?}"),
            }
        }
    }

    #[test]
    fn test_decode_rejects_leading_zero_checksum_tag_without_panic() {
        // The trailer locator's identical blind spot: `010=` folds to 10. The 0.3.1
        // decoder tracks the first tag-10 by folded tag, so a huge declared
        // BodyLength with a `010=` checksum is still a typed reject (→ Framing).
        let hostile = frame_with_tag_labels("9", "18446744073709551615", "010", HEARTBEAT_BODY);
        match decode(&hostile) {
            Err(err) => assert_framing_incorrect_data_format(&err),
            Ok(msg) => panic!("expected a Framing reject for `010=` checksum, decoded {msg:?}"),
        }
    }

    #[test]
    fn test_decode_does_not_false_reject_leading_zero_tags_with_correct_length() {
        // No false-reject: leading-zero tags with a CORRECT BodyLength and a valid
        // checksum decode cleanly (the decoder folds `009=`/`010=` numerically).
        let ok = frame_with_tag_labels(
            "009",
            &HEARTBEAT_BODY.len().to_string(),
            "010",
            HEARTBEAT_BODY,
        );
        match decode(&ok) {
            Ok(DecodedMessage::Heartbeat(_)) => {}
            other => panic!("expected Heartbeat (no false reject), got {other:?}"),
        }
    }

    #[test]
    fn test_decode_accepts_frame_with_correct_body_length() {
        // A correctly-framed heartbeat decodes.
        let bytes = frame_with_body(HEARTBEAT_BODY);
        match decode(&bytes) {
            Ok(DecodedMessage::Heartbeat(_)) => {}
            other => panic!("expected Heartbeat, got {other:?}"),
        }
    }

    #[test]
    fn test_encoder_produced_frame_round_trips_through_decode() {
        // An encoder-produced frame's BodyLength + CheckSum always match, so it
        // decodes cleanly — no false reject from the checked decoder.
        let header = StandardHeader::new(
            CompId::new("CLIENT").expect("comp"),
            CompId::new("VENUE").expect("comp"),
            SeqNum::new(1),
            UtcTimestamp::parse(52, "20240329-12:00:00.000").expect("ts"),
        );
        let bytes = DecodedMessage::Heartbeat(session::Heartbeat {
            header,
            test_req_id: Some("TR-1".to_string()),
        })
        .encode();
        assert!(decode(&bytes).is_ok());
    }

    #[test]
    fn test_decode_rejects_mid_body_checksum_injection_without_panic() {
        // #140 regression (the fuzzer-found P1): a MarketDataRequest carrying an
        // injected mid-body `10=624` BEFORE the real trailing checksum. The 0.3.1
        // decoder folds the FIRST tag-10 it reaches (`624`) through the CHECKED
        // `parse_checksum` (u16 fold, range-checked to 0..=255), which returns None
        // → `DecodeError::InvalidFieldValue{tag:10}` (→ Framing) — no u8-fold
        // overflow and no panic. Equivalent to the retired `reject_malformed_checksum`
        // guard's reject (same class, minus the `ref_tag: Some(10)` hint).
        let body = b"35=V\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01262=MDR-1\x01263=1\x01264=0\x01267=1\x01269=0\x0110=624\x0155=BTC-20240329-50000-C\x01";
        let frame = frame_with_body(body);
        match decode(&frame) {
            Err(err) => assert_framing_incorrect_data_format(&err),
            Ok(msg) => panic!("expected a Framing reject, decoded {msg:?}"),
        }
    }

    #[test]
    fn test_decode_rejects_trailing_checksum_over_255_without_panic() {
        // The positionally-trailing overflow vector, likewise owned by the checked
        // decoder: a correctly-length-declared frame whose trailing CheckSum(10) is
        // `624` (> 255) makes `parse_checksum` return None → Framing, no panic.
        let mut frame = Vec::new();
        frame.extend_from_slice(b"8=FIX.4.4\x01");
        frame.extend_from_slice(format!("9={}\x01", HEARTBEAT_BODY.len()).as_bytes());
        frame.extend_from_slice(HEARTBEAT_BODY);
        frame.extend_from_slice(b"10=624\x01"); // trailing checksum > 255
        match decode(&frame) {
            Err(err) => assert_framing_incorrect_data_format(&err),
            Ok(msg) => panic!("expected a Framing reject, decoded {msg:?}"),
        }
    }

    #[test]
    fn test_decode_accepts_single_trailing_checksum_in_domain() {
        // The parallel positive case: a well-formed frame with exactly one trailing
        // checksum in `000..=255` decodes to its typed struct (no false reject).
        let bytes = frame_with_body(HEARTBEAT_BODY);
        match decode(&bytes) {
            Ok(DecodedMessage::Heartbeat(_)) => {}
            other => panic!("expected Heartbeat, got {other:?}"),
        }
    }

    #[test]
    fn test_decode_never_panics_across_mid_body_checksum_space() {
        // Exhaustive over the full 3-digit input space `000..=999` in the MID-BODY
        // position (the position the decoder folds first), driven through the REAL
        // `decode` — the retired guard's magnitude sweep, moved to prove the
        // upstream checked fold. A mid-body `10=` always truncates the frame early,
        // so EVERY value is a typed reject (→ Framing): `> 255` via `parse_checksum`
        // returning None, `<= 255` via the resulting checksum/body-length mismatch.
        // The loop completing IS the no-panic assertion, in a debug OR release build.
        for value in 0u16..=999 {
            let body = format!(
                "35=V\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x0110={value:03}\x0155=BTC-20240329-50000-C\x01"
            );
            let frame = frame_with_body(body.as_bytes());
            match decode(&frame) {
                Err(FixDecodeError::Framing(_)) => {}
                other => panic!("mid-body `10={value:03}` must be a Framing reject, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_decode_rejects_duplicate_scalar_tag() {
        // Two MsgSeqNum(34) fields — a duplicate of a non-repeatable tag.
        let body = b"35=0\x0149=CLIENT\x0156=VENUE\x0134=1\x0134=2\x0152=20240329-12:00:00.000\x01";
        let frame = frame_with_body(body);
        match decode(&frame) {
            Err(FixDecodeError::DuplicateTag { tag }) => assert_eq!(tag, 34),
            other => panic!("expected DuplicateTag(34), got {other:?}"),
        }
    }

    #[test]
    fn test_decode_rejects_group_over_ceiling() {
        // A MarketDataRequest declaring more group entries than the ceiling is
        // rejected cheaply, before per-entry work.
        let body = b"35=V\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01262=MDR\x01263=1\x01264=0\x01267=2000\x01269=0\x01146=1\x0155=BTC-20240329-50000-C\x01";
        let frame = frame_with_body(body);
        match decode(&frame) {
            Err(FixDecodeError::TooManyGroupEntries {
                count_tag,
                declared,
                max,
            }) => {
                assert_eq!(count_tag, 267);
                assert_eq!(declared, 2000);
                assert_eq!(max, limits::MAX_GROUP_ENTRIES);
            }
            other => panic!("expected TooManyGroupEntries(267), got {other:?}"),
        }
    }

    #[test]
    fn test_decode_rejects_too_many_fields() {
        // A frame with more fields than the ceiling is rejected. Uses a repeatable
        // tag (269) so the duplicate check does not fire first.
        let mut body = Vec::new();
        body.extend_from_slice(
            b"35=0\x0149=CLIENT\x0156=VENUE\x0134=1\x0152=20240329-12:00:00.000\x01",
        );
        for _ in 0..(MAX_FIELDS_PER_MESSAGE + 10) {
            body.extend_from_slice(b"269=0\x01");
        }
        let frame = frame_with_body(&body);
        match decode(&frame) {
            Err(FixDecodeError::TooManyFields { count, max }) => {
                assert!(count > max);
                assert_eq!(max, MAX_FIELDS_PER_MESSAGE);
            }
            other => panic!("expected TooManyFields, got {other:?}"),
        }
    }
}
