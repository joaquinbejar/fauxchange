//! The typed FIX decode/validation error and its reject classification.
//!
//! Every inbound-byte path in the FIX vocabulary returns a [`FixDecodeError`] —
//! there is **no `.unwrap()` on caller bytes** anywhere in the module. A missing
//! required (`R`) tag, a mis-conditioned conditional (`C`) tag, an unknown
//! closed-set enum value, a repeating-group delimiter/order violation, a
//! malformed `Symbol`, or an unsupported application `MsgType` is a **typed
//! error, never a silent default** ([fix-dialect §5](../../../docs/specs/fix-dialect.md#5-validation-and-reject-behaviour)).
//!
//! This module also carries the **typed classification** ([`FixDecodeError::reject_route`],
//! [`FixRejectRoute`]) the session layer (#038) and order-path routing (#039)
//! turn into an actual reject frame: a structural failure resolves to a
//! session-level `Reject (3)` with a [`SessionRejectReason (373)`](SessionRejectReason)
//! and a `RefTagID (371)`; an unsupported application `MsgType` resolves toward
//! `BusinessMessageReject (j)`. Building the wire reject frame is #038/#039 —
//! this layer only classifies.

use ironfix_core::error::{DecodeError, EncodeError};

use crate::exchange::SymbolError;

use super::limits::truncate_untrusted;
use super::price::PriceSeamError;

/// A venue-side FIX **encode** failure (ironfix 0.4). The `ironfix-tagvalue`
/// encoder **defers** field-write errors — an over-long value, a non-finite/invalid
/// field, or a missing `MsgType (35)` — to [`Encoder::finish`](ironfix_tagvalue::Encoder::finish),
/// so building an **outbound** frame can fail. This is a venue-side invariant
/// violation (the venue constructs every outbound frame from already-validated
/// data), NOT untrusted caller input, so it is a distinct type from the inbound
/// [`FixDecodeError`]. `Display` is **redacted** — it never carries the offending
/// field bytes into a log.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FixEncodeError {
    /// The `ironfix-tagvalue` encoder rejected an outbound frame at finish (a
    /// deferred field-write error). The upstream detail is kept as the error source
    /// for programmatic inspection but is deliberately **not** rendered in `Display`.
    #[error("outbound FIX frame could not be encoded")]
    Encode(#[from] EncodeError),
}

/// A FIX session-level reject reason (`SessionRejectReason (373)`).
///
/// Only the standard values the venue emits are named; any other decoded value
/// is preserved losslessly as [`Other`](Self::Other) so a round-trip of a
/// received `Reject (3)` is exact. The numeric wire values are the FIX 4.4
/// `SessionRejectReason` code points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionRejectReason {
    /// `1` — a required tag was missing.
    RequiredTagMissing,
    /// `5` — a field carried a value outside its allowed set/range (an unknown
    /// closed-set enum value, an off-domain economic field).
    ValueIsIncorrect,
    /// `6` — a field value was not in the required data format.
    IncorrectDataFormat,
    /// `9` — a `SenderCompID`/`TargetCompID` problem.
    CompIdProblem,
    /// `10` — the `SendingTime (52)` was outside the accuracy window / malformed.
    SendingTimeAccuracy,
    /// `11` — the `MsgType (35)` is invalid or unsupported at the session level.
    InvalidMsgType,
    /// `13` — a tag appeared more than once outside a repeating group.
    TagAppearsMoreThanOnce,
    /// `16` — the `NoXXX` count did not match the number of decoded group
    /// entries.
    IncorrectNumInGroupCount,
    /// Any other `SessionRejectReason` value, preserved verbatim.
    Other(u16),
}

impl SessionRejectReason {
    /// Returns the FIX 4.4 `SessionRejectReason (373)` numeric code.
    #[must_use]
    #[inline]
    pub const fn to_fix(self) -> u16 {
        match self {
            Self::RequiredTagMissing => 1,
            Self::ValueIsIncorrect => 5,
            Self::IncorrectDataFormat => 6,
            Self::CompIdProblem => 9,
            Self::SendingTimeAccuracy => 10,
            Self::InvalidMsgType => 11,
            Self::TagAppearsMoreThanOnce => 13,
            Self::IncorrectNumInGroupCount => 16,
            Self::Other(code) => code,
        }
    }

    /// Builds a reason from its FIX 4.4 numeric code, mapping any unnamed value
    /// to [`Other`](Self::Other) so no code is lost on decode.
    #[must_use]
    #[inline]
    pub const fn from_fix(code: u16) -> Self {
        match code {
            1 => Self::RequiredTagMissing,
            5 => Self::ValueIsIncorrect,
            6 => Self::IncorrectDataFormat,
            9 => Self::CompIdProblem,
            10 => Self::SendingTimeAccuracy,
            11 => Self::InvalidMsgType,
            13 => Self::TagAppearsMoreThanOnce,
            16 => Self::IncorrectNumInGroupCount,
            other => Self::Other(other),
        }
    }
}

/// How a [`FixDecodeError`] is turned into a reject — the typed classification
/// the session/order layers route on ([fix-dialect §5](../../../docs/specs/fix-dialect.md#5-validation-and-reject-behaviour),
/// [03 §8](../../../docs/03-protocol-surfaces.md#8-error-mapping-across-surfaces)).
///
/// Building the wire frame is out of scope for the vocabulary layer (#037/#039);
/// this only says *which* reject and *why*.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FixRejectRoute {
    /// A structural/format failure → session `Reject (3)` with this
    /// `SessionRejectReason (373)` and, where known, this `RefTagID (371)`.
    SessionReject {
        /// The `SessionRejectReason (373)` category.
        reason: SessionRejectReason,
        /// The offending tag for `RefTagID (371)`, when a single tag is at fault.
        ref_tag: Option<u16>,
    },
    /// An unsupported application `MsgType` → `BusinessMessageReject (j)`. The
    /// venue understood the frame but has no handler for that message type.
    BusinessMessageReject,
}

/// A typed FIX decode or validation failure over inbound bytes.
///
/// Constructed only by the FIX vocabulary's hand-written decoders; a caller
/// never sees a panic on malformed input. Use [`Self::reject_route`] to classify
/// it into the reject the session/order layer emits.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FixDecodeError {
    /// The underlying `ironfix-tagvalue` codec rejected the frame — checksum
    /// mismatch, missing/invalid `BodyLength (9)`, missing `MsgType (35)`,
    /// truncation, or invalid UTF-8. These are session-level framing failures.
    ///
    /// `Display` is deliberately a **static** string and does NOT interpolate the
    /// wrapped [`DecodeError`] (#179): as of `ironfix-tagvalue` 0.4 the codec's
    /// `DecodeError::InvalidTag` carries a bounded (16-byte) snippet of the raw,
    /// attacker-controlled inbound bytes, so rendering the inner error via `%`/
    /// `{:?}` would echo hostile input into a log line. The detail is retained on
    /// [`std::error::Error::source`] for programmatic inspection only, never in a
    /// user- or peer-visible string — mirroring the redaction on `FixEncodeError`
    /// and the `CompId` reason. The reject path already emits `text: None`.
    #[error("fix framing error")]
    Framing(#[from] DecodeError),
    /// `BeginString (8)` was not the pinned `FIX.4.4`.
    #[error("begin string mismatch: expected '{expected}', got '{actual}'")]
    BeginStringMismatch {
        /// The dialect-pinned begin string (`FIX.4.4`).
        expected: &'static str,
        /// The begin string the frame carried.
        actual: String,
    },
    /// A required (`R`) tag was absent.
    #[error("missing required tag {tag}")]
    MissingRequiredField {
        /// The absent tag number.
        tag: u16,
    },
    /// A conditionally-required (`C`) tag was absent while its condition held
    /// (e.g. `Price (44)` absent for `OrdType=2`, `ExpireTime (126)` absent for
    /// `TimeInForce=GTD`).
    #[error("missing conditionally-required tag {tag}: {condition}")]
    MissingConditionalField {
        /// The absent tag number.
        tag: u16,
        /// The condition that made the tag required.
        condition: &'static str,
    },
    /// Exactly one of a required either/or pair was expected but neither was
    /// present (e.g. `OrderStatusRequest` needs `OrderID (37)` or `ClOrdID (11)`).
    #[error("exactly one of tags {first} / {second} is required")]
    MissingRequiredChoice {
        /// The first tag of the either/or pair.
        first: u16,
        /// The second tag of the either/or pair.
        second: u16,
    },
    /// A field value was not in the required data format (not an integer, not a
    /// single character, an out-of-format timestamp, …).
    #[error("incorrect data format for tag {tag}: {reason}")]
    IncorrectDataFormat {
        /// The offending tag number.
        tag: u16,
        /// Why the value was rejected.
        reason: String,
    },
    /// A closed-set enum field carried a value outside its allowed set — a typed
    /// reject, never a silent default.
    #[error("value '{value}' is not valid for tag {tag}")]
    ValueIsIncorrect {
        /// The offending tag number.
        tag: u16,
        /// The unrecognised value.
        value: String,
    },
    /// A `Symbol (55)` did not parse as a canonical
    /// `UNDERLYING-YYYYMMDD-STRIKE-STYLE` symbol.
    #[error("malformed symbol in tag 55: {reason}")]
    MalformedSymbol {
        /// The upstream `SymbolParser` rejection reason.
        reason: String,
    },
    /// The checked decimal-`Price` ↔ cents seam rejected a price-typed field.
    #[error("price seam error on tag {tag}: {source}")]
    Price {
        /// The price-typed tag (`44`, `31`, `270`, …).
        tag: u16,
        /// The underlying seam failure.
        source: PriceSeamError,
    },
    /// A required repeating group declared zero entries where at least one is
    /// required.
    #[error("repeating group {count_tag} must have at least one entry")]
    EmptyGroup {
        /// The `NoXXX` count tag.
        count_tag: u16,
    },
    /// A repeating group's declared `NoXXX` count did not match the number of
    /// decoded entries (a delimiter/order violation).
    #[error("repeating group {count_tag} declared {declared} entries but decoded {decoded}")]
    GroupCountMismatch {
        /// The `NoXXX` count tag.
        count_tag: u16,
        /// The count the frame declared.
        declared: u32,
        /// The number of entries actually decoded.
        decoded: u32,
    },
    /// The `MsgType (35)` is an application message the venue understands as a
    /// type but has no typed struct / handler for — routes to
    /// `BusinessMessageReject (j)`.
    #[error("unsupported application message type '{msg_type}'")]
    UnsupportedApplicationMsgType {
        /// The unsupported `MsgType (35)` value.
        msg_type: String,
    },
    /// The `MsgType (35)` was structurally unknown at the session level (neither
    /// a supported admin nor a recognised application type).
    #[error("unsupported message type '{msg_type}'")]
    UnsupportedMsgType {
        /// The unsupported `MsgType (35)` value.
        msg_type: String,
    },
    // NOTE (#140): the `InvalidBodyLength` and `MalformedChecksum` variants were
    // removed here — they were only ever produced by the venue's own pre-decode
    // guards (`validate_body_length` / `reject_malformed_checksum`), retired in
    // #140. As of `ironfix-tagvalue` 0.4 the decoder folds `BodyLength (9)` and
    // `CheckSum (10)` with checked, non-wrapping arithmetic itself, so a malformed
    // body length or out-of-range checksum now surfaces as a `DecodeError`
    // (`InvalidBodyLength` / `InvalidFieldValue`) through [`Self::Framing`], which
    // routes to the SAME session `Reject (3)` / `IncorrectDataFormat` class.
    /// A tag appeared more than once outside a repeating group — a session
    /// violation (`SessionRejectReason=13`), rejected rather than silently
    /// first-wins.
    #[error("tag {tag} appears more than once outside a repeating group")]
    DuplicateTag {
        /// The duplicated tag.
        tag: u16,
    },
    /// The message carried more fields than the decode-layer ceiling
    /// ([`MAX_FIELDS_PER_MESSAGE`](super::limits::MAX_FIELDS_PER_MESSAGE)) — a
    /// pathological-frame DoS guard.
    #[error("message has {count} fields, exceeding the {max} ceiling")]
    TooManyFields {
        /// The number of fields decoded.
        count: usize,
        /// The ceiling.
        max: usize,
    },
    /// A repeating group declared more entries than the decode-layer ceiling
    /// ([`MAX_GROUP_ENTRIES`](super::limits::MAX_GROUP_ENTRIES)) — a
    /// huge-group DoS guard.
    #[error("repeating group {count_tag} declared {declared} entries, exceeding the {max} ceiling")]
    TooManyGroupEntries {
        /// The `NoXXX` count tag.
        count_tag: u16,
        /// The declared entry count.
        declared: u32,
        /// The ceiling.
        max: usize,
    },
}

impl FixDecodeError {
    /// Builds a [`Self::MalformedSymbol`] from an upstream [`SymbolError`],
    /// truncating the (untrusted-value-bearing) reason to a bounded snippet.
    #[must_use]
    #[cold]
    pub(crate) fn from_symbol_error(err: &SymbolError) -> Self {
        Self::MalformedSymbol {
            reason: truncate_untrusted(&err.to_string()),
        }
    }

    /// Builds a [`Self::Price`] from a price-typed tag and a seam failure.
    #[must_use]
    #[cold]
    pub(crate) fn price(tag: u16, source: PriceSeamError) -> Self {
        Self::Price { tag, source }
    }

    /// The typed reject classification — which reject the session/order layer
    /// emits and why ([fix-dialect §5](../../../docs/specs/fix-dialect.md#5-validation-and-reject-behaviour)).
    ///
    /// Structural/format failures route to a session `Reject (3)` carrying a
    /// [`SessionRejectReason`] and, where a single tag is at fault, a
    /// `RefTagID (371)`; an unsupported application `MsgType` routes to
    /// `BusinessMessageReject (j)`.
    #[must_use]
    pub fn reject_route(&self) -> FixRejectRoute {
        use SessionRejectReason as R;
        let session =
            |reason: R, ref_tag: Option<u16>| FixRejectRoute::SessionReject { reason, ref_tag };
        match self {
            Self::Framing(_) => session(R::IncorrectDataFormat, None),
            Self::BeginStringMismatch { .. } => session(R::IncorrectDataFormat, Some(8)),
            Self::MissingRequiredField { tag } => session(R::RequiredTagMissing, Some(*tag)),
            Self::MissingConditionalField { tag, .. } => session(R::RequiredTagMissing, Some(*tag)),
            Self::MissingRequiredChoice { first, .. } => {
                session(R::RequiredTagMissing, Some(*first))
            }
            Self::IncorrectDataFormat { tag, .. } => session(R::IncorrectDataFormat, Some(*tag)),
            Self::ValueIsIncorrect { tag, .. } => session(R::ValueIsIncorrect, Some(*tag)),
            Self::MalformedSymbol { .. } => session(R::ValueIsIncorrect, Some(55)),
            Self::Price { tag, .. } => session(R::IncorrectDataFormat, Some(*tag)),
            Self::EmptyGroup { count_tag } => {
                session(R::IncorrectNumInGroupCount, Some(*count_tag))
            }
            Self::GroupCountMismatch { count_tag, .. } => {
                session(R::IncorrectNumInGroupCount, Some(*count_tag))
            }
            Self::UnsupportedMsgType { .. } => session(R::InvalidMsgType, Some(35)),
            Self::UnsupportedApplicationMsgType { .. } => FixRejectRoute::BusinessMessageReject,
            Self::DuplicateTag { tag } => session(R::TagAppearsMoreThanOnce, Some(*tag)),
            Self::TooManyFields { .. } => session(R::Other(99), None),
            Self::TooManyGroupEntries { count_tag, .. } => {
                session(R::IncorrectNumInGroupCount, Some(*count_tag))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_reject_reason_to_fix_round_trips() {
        for reason in [
            SessionRejectReason::RequiredTagMissing,
            SessionRejectReason::ValueIsIncorrect,
            SessionRejectReason::IncorrectDataFormat,
            SessionRejectReason::CompIdProblem,
            SessionRejectReason::SendingTimeAccuracy,
            SessionRejectReason::InvalidMsgType,
            SessionRejectReason::IncorrectNumInGroupCount,
        ] {
            assert_eq!(SessionRejectReason::from_fix(reason.to_fix()), reason);
        }
    }

    #[test]
    fn test_session_reject_reason_unknown_code_preserved_as_other() {
        assert_eq!(
            SessionRejectReason::from_fix(99),
            SessionRejectReason::Other(99)
        );
        assert_eq!(SessionRejectReason::Other(99).to_fix(), 99);
    }

    #[test]
    fn test_missing_required_field_routes_to_session_reject_with_ref_tag() {
        let err = FixDecodeError::MissingRequiredField { tag: 44 };
        match err.reject_route() {
            FixRejectRoute::SessionReject { reason, ref_tag } => {
                assert_eq!(reason, SessionRejectReason::RequiredTagMissing);
                assert_eq!(ref_tag, Some(44));
            }
            other => panic!("expected SessionReject, got {other:?}"),
        }
    }

    #[test]
    fn test_unsupported_application_msg_type_routes_to_business_reject() {
        let err = FixDecodeError::UnsupportedApplicationMsgType {
            msg_type: "AE".to_string(),
        };
        assert_eq!(err.reject_route(), FixRejectRoute::BusinessMessageReject);
    }

    #[test]
    fn test_unknown_enum_value_routes_to_value_is_incorrect() {
        let err = FixDecodeError::ValueIsIncorrect {
            tag: 54,
            value: "9".to_string(),
        };
        match err.reject_route() {
            FixRejectRoute::SessionReject { reason, ref_tag } => {
                assert_eq!(reason, SessionRejectReason::ValueIsIncorrect);
                assert_eq!(ref_tag, Some(54));
            }
            other => panic!("expected SessionReject, got {other:?}"),
        }
    }

    #[test]
    fn test_framing_error_routes_to_session_reject_incorrect_data_format() {
        // #140: a malformed BodyLength / out-of-range CheckSum now surfaces from the
        // checked `ironfix-tagvalue` decoder as a `DecodeError` wrapped in
        // `Framing`, and MUST route to the same session `Reject (3)` class the
        // retired `InvalidBodyLength` / `MalformedChecksum` guards produced —
        // `IncorrectDataFormat` (without the optional `RefTagID`).
        let err = FixDecodeError::Framing(DecodeError::InvalidBodyLength);
        match err.reject_route() {
            FixRejectRoute::SessionReject { reason, .. } => {
                assert_eq!(reason, SessionRejectReason::IncorrectDataFormat);
            }
            other => panic!("expected SessionReject, got {other:?}"),
        }
    }

    #[test]
    fn test_framing_display_never_echoes_hostile_inbound_bytes() {
        // #179: ironfix-tagvalue 0.4's `DecodeError::InvalidTag` carries a bounded
        // snippet of the raw, attacker-controlled inbound bytes. `Framing`'s Display
        // MUST NOT surface it — a static string only — so no future log line that
        // renders the error via `%`/`{:?}` can leak hostile input. The detail stays
        // reachable on `source()` for programmatic inspection.
        let hostile = "8=HOSTILE\x01\x02\x03NEVER_LOG_ME";
        let err = FixDecodeError::Framing(DecodeError::InvalidTag(hostile.to_string()));
        let rendered = err.to_string();
        assert_eq!(rendered, "fix framing error");
        assert!(
            !rendered.contains("HOSTILE") && !rendered.contains("NEVER_LOG_ME"),
            "Framing Display leaked inbound bytes: {rendered:?}"
        );
        // The detail is retained on the error source for programmatic inspection.
        let source = std::error::Error::source(&err).expect("Framing wraps a source");
        assert!(source.to_string().contains("HOSTILE"));
    }

    #[test]
    fn test_malformed_symbol_routes_to_session_reject_on_tag_55() {
        let err = FixDecodeError::MalformedSymbol {
            reason: "bad".to_string(),
        };
        match err.reject_route() {
            FixRejectRoute::SessionReject { ref_tag, .. } => assert_eq!(ref_tag, Some(55)),
            other => panic!("expected SessionReject, got {other:?}"),
        }
    }
}
