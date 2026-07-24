//! Session-admin messages: `Logon (A)`, `Logout (5)`, `Heartbeat (0)`,
//! `TestRequest (1)`, `ResendRequest (2)`, `SequenceReset (4)`, `Reject (3)`
//! ([fix-dialect §2.1](../../../docs/specs/fix-dialect.md#21-session-admin-no-permission-authenticated-by-the-logon)).
//!
//! The `Logon` plaintext `Password (554)` is held in a [`SecretField`] whose
//! `Debug` is redacted, so it is never logged; the acceptor's logon path (#038)
//! verifies it against the account registry's Argon2id hash and drops it
//! ([fix-dialect §3](../../../docs/specs/fix-dialect.md#3-logon-credentials--the-venue-verifies-plaintext-553554)).

use std::fmt;

use super::FixBody;
use super::codec::{FieldBag, FieldWriter, tags};
use super::error::{FixDecodeError, FixEncodeError, SessionRejectReason};
use super::header::StandardHeader;
use ironfix_core::types::SeqNum;

/// A plaintext secret whose `Debug` is redacted so it never reaches a log.
///
/// Equality and clone are kept for round-trip testing; only `Debug` is
/// overridden. `PartialEq` here is ordinary (not constant-time) — the
/// constant-time credential check is the registry's Argon2id verify (#038), not
/// this wire type.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretField(String);

impl SecretField {
    /// Wraps a plaintext secret.
    #[must_use]
    #[inline]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Exposes the plaintext — call only at the credential-verification seam,
    /// never on a logging path.
    #[must_use]
    #[inline]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretField(<redacted>)")
    }
}

/// `Logon (A)` — `EncryptMethod (98)=0`, `HeartBtInt (108)`, plaintext
/// `Username (553)` / `Password (554)`, optional `ResetSeqNumFlag (141)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Logon {
    /// Standard header.
    pub header: StandardHeader,
    /// `HeartBtInt (108)` — heartbeat interval in seconds.
    pub heart_bt_int: u32,
    /// `Username (553)` — the account credential username.
    pub username: String,
    /// `Password (554)` — the plaintext password (redacted in `Debug`).
    pub password: SecretField,
    /// `ResetSeqNumFlag (141)` — reset sequence numbers on this logon.
    pub reset_seq_num_flag: Option<bool>,
}

impl FixBody for Logon {
    const MSG_TYPE: &'static str = "A";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        // EncryptMethod (98) is required and must be 0 (no venue-side encryption).
        let encrypt_method = fields.req_u64(tags::ENCRYPT_METHOD)?;
        if encrypt_method != 0 {
            return Err(FixDecodeError::ValueIsIncorrect {
                tag: tags::ENCRYPT_METHOD,
                value: encrypt_method.to_string(),
            });
        }
        Ok(Self {
            header,
            heart_bt_int: fields.req_u32(tags::HEART_BT_INT)?,
            username: fields.req_str(tags::USERNAME)?.to_string(),
            password: SecretField::new(fields.req_str(tags::PASSWORD)?),
            reset_seq_num_flag: fields.opt_bool(tags::RESET_SEQ_NUM_FLAG)?,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.u64(tags::ENCRYPT_METHOD, 0);
        writer.u64(tags::HEART_BT_INT, u64::from(self.heart_bt_int));
        writer.str(tags::USERNAME, &self.username);
        writer.str(tags::PASSWORD, self.password.expose());
        writer.opt_bool(tags::RESET_SEQ_NUM_FLAG, self.reset_seq_num_flag);
        writer.finish()
    }
}

/// `Logout (5)` — optional redacted `Text (58)` reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Logout {
    /// Standard header.
    pub header: StandardHeader,
    /// `Text (58)` — the (redacted) logout reason.
    pub text: Option<String>,
}

impl FixBody for Logout {
    const MSG_TYPE: &'static str = "5";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        Ok(Self {
            header,
            text: fields.opt_str(tags::TEXT)?.map(str::to_string),
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.opt_str(tags::TEXT, self.text.as_deref());
        writer.finish()
    }
}

/// `Heartbeat (0)` — `TestReqID (112)` present iff replying to a `TestRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    /// Standard header.
    pub header: StandardHeader,
    /// `TestReqID (112)` — echoed only when replying to a `TestRequest (1)`.
    pub test_req_id: Option<String>,
}

impl FixBody for Heartbeat {
    const MSG_TYPE: &'static str = "0";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        Ok(Self {
            header,
            test_req_id: fields.opt_str(tags::TEST_REQ_ID)?.map(str::to_string),
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.opt_str(tags::TEST_REQ_ID, self.test_req_id.as_deref());
        writer.finish()
    }
}

/// `TestRequest (1)` — required `TestReqID (112)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestRequest {
    /// Standard header.
    pub header: StandardHeader,
    /// `TestReqID (112)`.
    pub test_req_id: String,
}

impl FixBody for TestRequest {
    const MSG_TYPE: &'static str = "1";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        Ok(Self {
            header,
            test_req_id: fields.req_str(tags::TEST_REQ_ID)?.to_string(),
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.str(tags::TEST_REQ_ID, &self.test_req_id);
        writer.finish()
    }
}

/// `ResendRequest (2)` — `BeginSeqNo (7)` / `EndSeqNo (16)` (session `MsgSeqNum`
/// gaps only; `EndSeqNo=0` means "to infinity").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResendRequest {
    /// Standard header.
    pub header: StandardHeader,
    /// `BeginSeqNo (7)`.
    pub begin_seq_no: SeqNum,
    /// `EndSeqNo (16)`.
    pub end_seq_no: SeqNum,
}

impl FixBody for ResendRequest {
    const MSG_TYPE: &'static str = "2";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        Ok(Self {
            header,
            begin_seq_no: SeqNum::new(fields.req_u64(tags::BEGIN_SEQ_NO)?),
            end_seq_no: SeqNum::new(fields.req_u64(tags::END_SEQ_NO)?),
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.u64(tags::BEGIN_SEQ_NO, self.begin_seq_no.value());
        writer.u64(tags::END_SEQ_NO, self.end_seq_no.value());
        writer.finish()
    }
}

/// `SequenceReset (4)` — `NewSeqNo (36)` and optional `GapFillFlag (123)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceReset {
    /// Standard header.
    pub header: StandardHeader,
    /// `NewSeqNo (36)`.
    pub new_seq_no: SeqNum,
    /// `GapFillFlag (123)`.
    pub gap_fill_flag: Option<bool>,
}

impl FixBody for SequenceReset {
    const MSG_TYPE: &'static str = "4";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        Ok(Self {
            header,
            new_seq_no: SeqNum::new(fields.req_u64(tags::NEW_SEQ_NO)?),
            gap_fill_flag: fields.opt_bool(tags::GAP_FILL_FLAG)?,
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.u64(tags::NEW_SEQ_NO, self.new_seq_no.value());
        writer.opt_bool(tags::GAP_FILL_FLAG, self.gap_fill_flag);
        writer.finish()
    }
}

/// `Reject (3)` — session-level reject: `RefSeqNum (45)`, conditional
/// `SessionRejectReason (373)` / `RefTagID (371)`, optional redacted `Text (58)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reject {
    /// Standard header.
    pub header: StandardHeader,
    /// `RefSeqNum (45)` — the sequence number of the rejected message.
    pub ref_seq_num: SeqNum,
    /// `SessionRejectReason (373)`.
    pub session_reject_reason: Option<SessionRejectReason>,
    /// `RefTagID (371)` — the offending tag.
    pub ref_tag_id: Option<u16>,
    /// `Text (58)` — the (redacted) reason text.
    pub text: Option<String>,
}

impl FixBody for Reject {
    const MSG_TYPE: &'static str = "3";

    fn header(&self) -> &StandardHeader {
        &self.header
    }

    fn decode_body(header: StandardHeader, fields: &FieldBag<'_>) -> Result<Self, FixDecodeError> {
        let session_reject_reason = fields
            .opt_u16(tags::SESSION_REJECT_REASON)?
            .map(SessionRejectReason::from_fix);
        Ok(Self {
            header,
            ref_seq_num: SeqNum::new(fields.req_u64(tags::REF_SEQ_NUM)?),
            session_reject_reason,
            ref_tag_id: fields.opt_u16(tags::REF_TAG_ID)?,
            text: fields.opt_str(tags::TEXT)?.map(str::to_string),
        })
    }

    fn encode(&self) -> Result<Vec<u8>, FixEncodeError> {
        let mut writer = FieldWriter::new(Self::MSG_TYPE);
        self.header.encode(&mut writer);
        writer.u64(tags::REF_SEQ_NUM, self.ref_seq_num.value());
        writer.opt_u16(tags::REF_TAG_ID, self.ref_tag_id);
        writer.opt_u16(
            tags::SESSION_REJECT_REASON,
            self.session_reject_reason.map(SessionRejectReason::to_fix),
        );
        writer.opt_str(tags::TEXT, self.text.as_deref());
        writer.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::decode;
    use super::*;
    use ironfix_core::types::CompId;

    fn header() -> StandardHeader {
        StandardHeader::new(
            CompId::new("CLIENT").expect("comp id"),
            CompId::new("FAUXCHANGE").expect("comp id"),
            SeqNum::new(1),
            super::super::header::UtcTimestamp::parse(52, "20240329-12:00:00.000").expect("ts"),
        )
    }

    #[test]
    fn test_secret_field_debug_is_redacted() {
        let secret = SecretField::new("hunter2");
        assert_eq!(format!("{secret:?}"), "SecretField(<redacted>)");
        assert!(!format!("{secret:?}").contains("hunter2"));
        assert_eq!(secret.expose(), "hunter2");
    }

    #[test]
    fn test_logon_round_trips_and_hides_password() {
        let logon = Logon {
            header: header(),
            heart_bt_int: 30,
            username: "acct-1".to_string(),
            password: SecretField::new("s3cr3t"),
            reset_seq_num_flag: Some(true),
        };
        let bytes = logon.encode().expect("test encode");
        // The plaintext is on the wire (it is verified server-side) but a Debug
        // of the struct never shows it.
        assert!(!format!("{logon:?}").contains("s3cr3t"));
        match decode(&bytes) {
            Ok(super::super::DecodedMessage::Logon(back)) => assert_eq!(back, logon),
            other => panic!("expected Logon, got {other:?}"),
        }
    }

    #[test]
    fn test_logon_rejects_non_zero_encrypt_method() {
        // Build a logon with EncryptMethod=1 by hand-encoding.
        let mut writer = FieldWriter::new(Logon::MSG_TYPE);
        header().encode(&mut writer);
        writer.u64(tags::ENCRYPT_METHOD, 1);
        writer.u64(tags::HEART_BT_INT, 30);
        writer.str(tags::USERNAME, "acct-1");
        writer.str(tags::PASSWORD, "s3cr3t");
        let bytes = writer.finish().expect("test finish");
        match decode(&bytes) {
            Err(FixDecodeError::ValueIsIncorrect { tag, value }) => {
                assert_eq!(tag, tags::ENCRYPT_METHOD);
                assert_eq!(value, "1");
            }
            other => panic!("expected ValueIsIncorrect, got {other:?}"),
        }
    }

    #[test]
    fn test_logon_missing_password_is_typed_error() {
        let mut writer = FieldWriter::new(Logon::MSG_TYPE);
        header().encode(&mut writer);
        writer.u64(tags::ENCRYPT_METHOD, 0);
        writer.u64(tags::HEART_BT_INT, 30);
        writer.str(tags::USERNAME, "acct-1");
        let bytes = writer.finish().expect("test finish");
        match decode(&bytes) {
            Err(FixDecodeError::MissingRequiredField { tag }) => assert_eq!(tag, tags::PASSWORD),
            other => panic!("expected MissingRequiredField(554), got {other:?}"),
        }
    }

    #[test]
    fn test_all_session_admin_messages_round_trip() {
        let msgs = [
            super::super::DecodedMessage::Logout(Logout {
                header: header(),
                text: Some("bye".to_string()),
            }),
            super::super::DecodedMessage::Heartbeat(Heartbeat {
                header: header(),
                test_req_id: Some("TR-1".to_string()),
            }),
            super::super::DecodedMessage::TestRequest(TestRequest {
                header: header(),
                test_req_id: "TR-1".to_string(),
            }),
            super::super::DecodedMessage::ResendRequest(ResendRequest {
                header: header(),
                begin_seq_no: SeqNum::new(5),
                end_seq_no: SeqNum::new(0),
            }),
            super::super::DecodedMessage::SequenceReset(SequenceReset {
                header: header(),
                new_seq_no: SeqNum::new(42),
                gap_fill_flag: Some(true),
            }),
            super::super::DecodedMessage::Reject(Reject {
                header: header(),
                ref_seq_num: SeqNum::new(7),
                session_reject_reason: Some(SessionRejectReason::RequiredTagMissing),
                ref_tag_id: Some(44),
                text: Some("missing price".to_string()),
            }),
        ];
        for msg in msgs {
            let bytes = msg.encode().expect("test encode");
            match decode(&bytes) {
                Ok(back) => assert_eq!(back, msg),
                Err(e) => panic!("round trip failed for {msg:?}: {e:?}"),
            }
        }
    }

    #[test]
    fn test_test_request_missing_id_is_typed_error() {
        let mut writer = FieldWriter::new(TestRequest::MSG_TYPE);
        header().encode(&mut writer);
        let bytes = writer.finish().expect("test finish");
        match decode(&bytes) {
            Err(FixDecodeError::MissingRequiredField { tag }) => assert_eq!(tag, tags::TEST_REQ_ID),
            other => panic!("expected MissingRequiredField(112), got {other:?}"),
        }
    }
}
