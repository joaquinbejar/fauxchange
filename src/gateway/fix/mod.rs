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
//! integer-`Cents` [`price`] seam. The TCP acceptor (#037), the session FSM and
//! durable sequence store (#038), the order-path routing (#039), and the
//! market-data wiring (#040) build on this layer; it compiles standalone with
//! tests and is not yet referenced by a live gateway.
//!
//! Governed by `docs/03-protocol-surfaces.md`.

use ironfix_core::message::MsgType;
use ironfix_dictionary::Version;
use ironfix_tagvalue::Decoder;

pub mod codec;
pub mod enums;
pub mod error;
pub mod execution;
pub mod header;
pub mod limits;
pub mod marketdata;
pub mod order;
pub mod price;
pub mod session;

use codec::FieldBag;
use limits::{MAX_FIELDS_PER_MESSAGE, truncate_untrusted};

pub use enums::{
    CommType, CxlRejResponseTo, ExecType, LastLiquidityInd, MassCancelRequestType,
    MassCancelResponse, MdEntryType, MdUpdateAction, OrdStatus, OrdType, OrderSide,
    SubscriptionRequestType, TimeInForce,
};
pub use error::{FixDecodeError, FixRejectRoute, SessionRejectReason};
pub use header::{StandardHeader, UtcTimestamp};
pub use price::{
    CENTS_SCALE, PriceScale, PriceSeamError, parse_decimal_to_cents, parse_signed_decimal_to_cents,
    render_cents_to_decimal, render_signed_cents_to_decimal,
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
    // Guard BodyLength(9) BEFORE the ironfix codec: ironfix-tagvalue 0.3.0
    // computes `body_start + body_length` UNCHECKED on the attacker-controlled
    // BodyLength, which panics on an oversized declared value. Validating exact
    // match here both closes that panic and is correct FIX (a BodyLength that
    // disagrees with the real frame is malformed).
    validate_body_length(bytes)?;

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

/// Validates `BodyLength (9)` against the frame's actual body length, before the
/// `ironfix-tagvalue` codec runs — the belt for its unchecked `body_start +
/// body_length` add (which panics on an oversized declared value).
///
/// The check is deliberately narrow: it only acts when it can positively locate
/// `BeginString (8)`, `BodyLength (9)`, and the trailing `CheckSum (10)` field.
/// In every other case it defers to the ironfix codec's own framing errors —
/// and those cases cannot reach the unchecked add, because ironfix returns
/// `Incomplete` / `MissingBodyLength` / `ChecksumMismatch` first. When the
/// structure IS locatable (the only path that reaches the panic — a
/// valid-checksum frame), a declared length that differs from the actual body
/// length is rejected.
///
/// Actual body length is, per the FIX spec, the number of bytes after the
/// `9=<value>SOH` field up to and including the `SOH` immediately before the
/// `10=` checksum field.
/// Splits one `SOH`-delimited `tag=value` field into `(numeric tag, value)`.
///
/// The tag is the digit run before the first `=`, folded **numerically** —
/// exactly as `ironfix-tagvalue`'s own `parse_tag` folds it — so a
/// leading-zero encoding (`009`) yields the same tag `9` as the canonical
/// `9`. This is what makes the [`validate_body_length`] guard complete: a
/// literal `starts_with(b"9=")` locator misses `009=`, which ironfix still
/// accepts and drives straight into its unchecked `BodyLength` addition
/// (decoder.rs:145) — the leading-zero panic bypass. Folding the digits the
/// way ironfix does means the guard recognises EXACTLY the tags ironfix will,
/// with no open-ended parsing tolerance to fall out of sync with: a FIX tag is
/// always a digit run before `=`, and `SOH` cannot appear inside a value (it
/// is the field delimiter), so this fold is exhaustive for tag identification.
/// Do NOT "simplify" this back to a byte-prefix compare — that reopens the
/// bypass.
///
/// Returns `None` when there is no `=`, the tag has a non-digit byte, or the
/// tag folds past `u64` (never a real `8`/`9`/`10`).
fn split_field(field: &[u8]) -> Option<(u64, &[u8])> {
    let eq = field.iter().position(|&b| b == b'=')?;
    let (tag_bytes, rest) = field.split_at(eq);
    if tag_bytes.is_empty() {
        return None;
    }
    let mut tag: u64 = 0;
    for &b in tag_bytes {
        if !b.is_ascii_digit() {
            return None;
        }
        tag = tag.checked_mul(10)?.checked_add(u64::from(b - b'0'))?;
    }
    Some((tag, &rest[1..])) // skip the '='
}

fn validate_body_length(bytes: &[u8]) -> Result<(), FixDecodeError> {
    /// SOH field delimiter.
    const SOH: u8 = 0x01;
    /// The FIX header/trailer tags this guard identifies, folded numerically.
    const BEGIN_STRING_TAG: u64 = 8;
    const BODY_LENGTH_TAG: u64 = 9;
    const CHECKSUM_TAG: u64 = 10;

    let find_soh = |from: usize| {
        bytes
            .get(from..)
            .and_then(|s| s.iter().position(|&b| b == SOH))
            .map(|p| from + p)
    };

    // Field 1 must be BeginString(8) — matched by NUMERIC tag fold, so `08=…`
    // is recognised too (see `split_field`).
    let Some(soh1) = find_soh(0) else {
        return Ok(()); // ironfix → Incomplete
    };
    if !matches!(split_field(&bytes[0..soh1]), Some((BEGIN_STRING_TAG, _))) {
        return Ok(()); // ironfix → InvalidBeginString
    }

    // Field 2 must be BodyLength(9).
    let bl_start = soh1 + 1;
    let Some(soh2) = find_soh(bl_start) else {
        return Ok(());
    };
    let digits = match split_field(&bytes[bl_start..soh2]) {
        Some((BODY_LENGTH_TAG, value)) => value,
        _ => return Ok(()), // ironfix → MissingBodyLength
    };
    // Parse the declared length; a non-usize (non-digit or overflow) is malformed.
    let declared = match std::str::from_utf8(digits)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
    {
        Some(value) => value,
        None => {
            return Err(FixDecodeError::InvalidBodyLength {
                declared: truncate_untrusted(&String::from_utf8_lossy(digits)),
                actual: 0,
            });
        }
    };
    let body_start = soh2 + 1;

    // The last field must be CheckSum(10) — again by numeric fold, so `010=…`
    // is recognised (the trailer locator has the identical blind spot).
    if bytes.last() != Some(&SOH) {
        return Ok(()); // ironfix → Incomplete
    }
    let trailing_soh = bytes.len() - 1;
    let Some(prev_soh) = bytes[..trailing_soh].iter().rposition(|&b| b == SOH) else {
        return Ok(());
    };
    let checksum_tag_start = prev_soh + 1;
    if !matches!(
        split_field(&bytes[checksum_tag_start..trailing_soh]),
        Some((CHECKSUM_TAG, _))
    ) {
        return Ok(()); // not the checksum field where expected; let ironfix handle
    }

    // actual body length = [body_start, checksum_tag_start).
    let actual = match checksum_tag_start.checked_sub(body_start) {
        Some(value) => value,
        None => {
            return Err(FixDecodeError::InvalidBodyLength {
                declared: truncate_untrusted(&String::from_utf8_lossy(digits)),
                actual: 0,
            });
        }
    };
    if declared != actual {
        return Err(FixDecodeError::InvalidBodyLength {
            declared: truncate_untrusted(&String::from_utf8_lossy(digits)),
            actual,
        });
    }
    Ok(())
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

    #[test]
    fn test_decode_rejects_oversized_body_length_without_panic() {
        // PoC: a valid-checksum frame whose declared BodyLength is u64::MAX. The
        // ironfix codec's unchecked `body_start + body_length` would panic; our
        // guard rejects it first with a typed error (no panic).
        let hostile = frame_with_declared_body_length("18446744073709551615", HEARTBEAT_BODY);
        match decode(&hostile) {
            Err(FixDecodeError::InvalidBodyLength { actual, .. }) => {
                assert_eq!(actual, HEARTBEAT_BODY.len());
            }
            other => panic!("expected InvalidBodyLength, got {other:?}"),
        }
    }

    #[test]
    fn test_decode_rejects_wrong_small_body_length() {
        // A small-but-wrong declared length is equally malformed.
        let wrong = frame_with_declared_body_length("5", HEARTBEAT_BODY);
        assert!(matches!(
            decode(&wrong),
            Err(FixDecodeError::InvalidBodyLength { .. })
        ));
    }

    #[test]
    fn test_decode_rejects_non_numeric_body_length() {
        let bad = frame_with_declared_body_length("notanumber", HEARTBEAT_BODY);
        assert!(matches!(
            decode(&bad),
            Err(FixDecodeError::InvalidBodyLength { .. })
        ));
    }

    #[test]
    fn test_decode_rejects_leading_zero_body_length_tag_without_panic() {
        // Bypass PoC: `009=` folds to tag 9 for ironfix but is not `9=` to a
        // literal-prefix guard, so a byte-prefix guard would defer and ironfix
        // would panic on the unchecked add. The numeric-fold guard rejects it.
        for label in ["009", "0009", "00009"] {
            let hostile =
                frame_with_tag_labels(label, "18446744073709551615", "10", HEARTBEAT_BODY);
            match decode(&hostile) {
                Err(FixDecodeError::InvalidBodyLength { .. }) => {}
                other => panic!("expected InvalidBodyLength for `{label}=`, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_decode_rejects_leading_zero_checksum_tag_without_panic() {
        // The trailer locator has the identical blind spot: `010=` folds to 10.
        // With a huge declared BodyLength, a `010=` checksum tag must still be
        // located (numeric fold) so declared != actual is caught before ironfix.
        let hostile = frame_with_tag_labels("9", "18446744073709551615", "010", HEARTBEAT_BODY);
        match decode(&hostile) {
            Err(FixDecodeError::InvalidBodyLength { .. }) => {}
            other => panic!("expected InvalidBodyLength for `010=` checksum, got {other:?}"),
        }
    }

    #[test]
    fn test_decode_does_not_false_reject_leading_zero_tags_with_correct_length() {
        // No false-reject: leading-zero tags with a CORRECT BodyLength must not
        // trip the guard (declared == actual). ironfix's own downstream decode
        // is its business; the guard just must not raise InvalidBodyLength.
        let ok = frame_with_tag_labels(
            "009",
            &HEARTBEAT_BODY.len().to_string(),
            "010",
            HEARTBEAT_BODY,
        );
        assert!(!matches!(
            decode(&ok),
            Err(FixDecodeError::InvalidBodyLength { .. })
        ));
    }

    #[test]
    fn test_decode_accepts_frame_with_correct_body_length() {
        // A correctly-framed heartbeat passes the guard and decodes.
        let bytes = frame_with_body(HEARTBEAT_BODY);
        match decode(&bytes) {
            Ok(DecodedMessage::Heartbeat(_)) => {}
            other => panic!("expected Heartbeat, got {other:?}"),
        }
    }

    #[test]
    fn test_encoder_produced_frame_passes_the_body_length_guard() {
        // An encoder-produced frame's BodyLength always matches the guard's
        // computed actual length — no false reject.
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
