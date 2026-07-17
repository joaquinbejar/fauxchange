//! The FIX standard header and the validated UTC-timestamp field type.
//!
//! Every supported message carries the standard header tags `49`/`56`/`34`/`52`
//! (with `8`/`9`/`35`/`10` framed by the codec) ([fix-dialect §2](../../../docs/specs/fix-dialect.md#2-supported-messages-and-requiredness)).
//! [`StandardHeader`] decodes and encodes them; [`UtcTimestamp`] is the
//! structurally-validated `SendingTime`/`TransactTime`/`ExpireTime` type that
//! round-trips its exact wire form.

use ironfix_core::types::{CompId, SeqNum};

use super::codec::{FieldBag, FieldWriter, tags};
use super::error::FixDecodeError;
use super::limits::truncate_untrusted;

/// A structurally-validated FIX UTC timestamp (`YYYYMMDD-HH:MM:SS` with an
/// optional `.sss` / `.ssssss` / `.sssssssss` fraction).
///
/// The exact wire string is stored, so decode→encode is byte-identical. The
/// validation is structural (field positions and digit runs); the venue
/// interprets the instant semantically in the session/order layer, not here.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UtcTimestamp(String);

impl UtcTimestamp {
    /// Validates `value` as a FIX UTC timestamp for `tag` and stores it verbatim.
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::IncorrectDataFormat`] if the value is not a
    /// well-formed FIX UTC timestamp.
    pub fn parse(tag: u16, value: &str) -> Result<Self, FixDecodeError> {
        if is_valid_utc_timestamp(value) {
            Ok(Self(value.to_string()))
        } else {
            // `SendingTime (52)` is a standard-header field decoded on EVERY
            // message, so this is the most-reached untrusted-value error site —
            // bound the echoed value so a hostile timestamp cannot inflate a
            // future `Text (58)` reject render.
            Err(FixDecodeError::IncorrectDataFormat {
                tag,
                reason: format!(
                    "'{}' is not a FIX UTC timestamp (YYYYMMDD-HH:MM:SS[.sss])",
                    truncate_untrusted(value)
                ),
            })
        }
    }

    /// Formats a Unix-epoch **milliseconds** instant as a FIX UTC timestamp
    /// `YYYYMMDD-HH:MM:SS.sss` (millisecond precision) — the form
    /// `SendingTime (52)` carries on venue-originated frames.
    ///
    /// Infallible: the output is always a well-formed FIX UTC timestamp (it
    /// round-trips through [`Self::parse`]). Hand-rolled via Howard Hinnant's
    /// `civil_from_days` (the same algorithm the REST layer's RFC3339 formatter
    /// uses) so the venue needs no date-library dependency. The `ms` is a read of
    /// the **injected venue clock**, so a fixed-seed run stamps identically.
    #[must_use]
    pub fn from_epoch_ms(ms: u64) -> Self {
        let secs = ms / 1_000;
        let millis = ms % 1_000;
        let days = (secs / 86_400) as i64;
        let rem = secs % 86_400;
        let (hour, minute, second) = (rem / 3_600, (rem % 3_600) / 60, rem % 60);

        // civil_from_days: days since 1970-01-01 -> (year, month, day).
        let z = days + 719_468;
        let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
        let doe = z - era * 146_097; // [0, 146096]
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
        let year = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
        let mp = (5 * doy + 2) / 153; // [0, 11]
        let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
        let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
        let year = if month <= 2 { year + 1 } else { year };

        Self(format!(
            "{year:04}{month:02}{day:02}-{hour:02}:{minute:02}:{second:02}.{millis:03}"
        ))
    }

    /// Returns the timestamp's exact wire string.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Returns `true` iff `value` is a well-formed FIX UTC timestamp:
/// `YYYYMMDD-HH:MM:SS` optionally followed by `.` and 3, 6, or 9 fractional
/// digits.
#[must_use]
fn is_valid_utc_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    // Base form `YYYYMMDD-HH:MM:SS` is exactly 17 bytes.
    if bytes.len() < 17 {
        return false;
    }
    let digits = |range: std::ops::Range<usize>| bytes[range].iter().all(u8::is_ascii_digit);
    if !(digits(0..8)
        && bytes[8] == b'-'
        && digits(9..11)
        && bytes[11] == b':'
        && digits(12..14)
        && bytes[14] == b':'
        && digits(15..17))
    {
        return false;
    }
    // Optional fractional seconds: `.` then 3 / 6 / 9 digits, nothing else.
    match bytes.len() {
        17 => true,
        len if len == 21 || len == 24 || len == 27 => {
            bytes[17] == b'.' && bytes[18..].iter().all(u8::is_ascii_digit)
        }
        _ => false,
    }
}

/// The FIX standard header carried on every supported message
/// (`SenderCompID (49)`, `TargetCompID (56)`, `MsgSeqNum (34)`,
/// `SendingTime (52)`; the codec frames `8`/`9`/`35`/`10`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandardHeader {
    /// `SenderCompID (49)`.
    pub sender_comp_id: CompId,
    /// `TargetCompID (56)`.
    pub target_comp_id: CompId,
    /// `MsgSeqNum (34)`.
    pub msg_seq_num: SeqNum,
    /// `SendingTime (52)`.
    pub sending_time: UtcTimestamp,
}

impl StandardHeader {
    /// Builds a header from its parts (used by outbound message constructors and
    /// tests).
    #[must_use]
    #[inline]
    pub fn new(
        sender_comp_id: CompId,
        target_comp_id: CompId,
        msg_seq_num: SeqNum,
        sending_time: UtcTimestamp,
    ) -> Self {
        Self {
            sender_comp_id,
            target_comp_id,
            msg_seq_num,
            sending_time,
        }
    }

    /// Decodes the standard header tags from a message's fields.
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::MissingRequiredField`] for an absent header tag, or
    /// [`FixDecodeError::IncorrectDataFormat`] for an over-long `CompID`, a
    /// non-integer `MsgSeqNum`, or a malformed `SendingTime`.
    pub fn decode(fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let sender_comp_id =
            decode_comp_id(tags::SENDER_COMP_ID, fields.req_str(tags::SENDER_COMP_ID)?)?;
        let target_comp_id =
            decode_comp_id(tags::TARGET_COMP_ID, fields.req_str(tags::TARGET_COMP_ID)?)?;
        // `MsgSeqNum` is parsed as a well-formed integer here; the sequence-number
        // semantics (the `>= 1` rule, gap detection, expected-next matching against
        // the durable per-session counter) are the session layer's (#038), not the
        // vocabulary's — this layer only validates the wire shape.
        let msg_seq_num = SeqNum::new(fields.req_u64(tags::MSG_SEQ_NUM)?);
        let sending_time =
            UtcTimestamp::parse(tags::SENDING_TIME, fields.req_str(tags::SENDING_TIME)?)?;
        Ok(Self {
            sender_comp_id,
            target_comp_id,
            msg_seq_num,
            sending_time,
        })
    }

    /// Writes the standard header tags in canonical order (after `MsgType (35)`,
    /// before the message body).
    pub fn encode(&self, writer: &mut FieldWriter) {
        writer.str(tags::SENDER_COMP_ID, self.sender_comp_id.as_str());
        writer.str(tags::TARGET_COMP_ID, self.target_comp_id.as_str());
        writer.u64(tags::MSG_SEQ_NUM, self.msg_seq_num.value());
        writer.str(tags::SENDING_TIME, self.sending_time.as_str());
    }
}

/// Decodes a `CompID` value, rejecting one that exceeds the 32-byte FIX limit.
fn decode_comp_id(tag: u16, value: &str) -> Result<CompId, FixDecodeError> {
    CompId::new(value).ok_or(FixDecodeError::IncorrectDataFormat {
        tag,
        reason: "comp id exceeds the 32-byte limit".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utc_timestamp_accepts_second_precision() {
        assert!(UtcTimestamp::parse(52, "20240329-12:00:00").is_ok());
    }

    #[test]
    fn test_utc_timestamp_accepts_millis_and_micros() {
        assert!(UtcTimestamp::parse(52, "20240329-12:00:00.000").is_ok());
        assert!(UtcTimestamp::parse(52, "20240329-12:00:00.123456").is_ok());
    }

    #[test]
    fn test_utc_timestamp_preserves_exact_wire_form() {
        let raw = "20240329-12:00:00.500";
        let ts = match UtcTimestamp::parse(52, raw) {
            Ok(ts) => ts,
            Err(e) => panic!("parse failed: {e:?}"),
        };
        assert_eq!(ts.as_str(), raw);
    }

    #[test]
    fn test_utc_timestamp_rejects_malformed() {
        for bad in [
            "",
            "2024-03-29",
            "20240329T12:00:00",
            "20240329-12:00",
            "20240329-12:00:00.",
            "20240329-12:00:00.12",
            "abcdefgh-12:00:00",
        ] {
            match UtcTimestamp::parse(52, bad) {
                Err(FixDecodeError::IncorrectDataFormat { tag, .. }) => assert_eq!(tag, 52),
                other => panic!("expected reject for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_utc_timestamp_error_truncates_hostile_value() {
        // SendingTime(52) is decoded on every message; a hostile 100 KB value
        // must not inflate the error (which #038/#039 render into a Text(58)).
        let hostile = "X".repeat(100_000);
        let msg = UtcTimestamp::parse(52, &hostile).unwrap_err().to_string();
        assert!(msg.len() < 256, "error not bounded: {} bytes", msg.len());
        assert!(msg.contains("..."));
    }
}
