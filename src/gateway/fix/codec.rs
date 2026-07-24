//! Low-level tag-value plumbing over `ironfix-tagvalue`: the FIX tag constants,
//! the typed field reader ([`FieldBag`]) with a repeating-group parser, and the
//! field writer ([`FieldWriter`]) that frames the standard header and trailer.
//!
//! The reader turns a decoded [`RawMessage`] into typed field access that
//! returns a [`FixDecodeError`] on every failure — a missing required tag, a bad
//! data format, or a repeating-group delimiter/order violation — so **no
//! `.unwrap()` ever runs on caller bytes**. The writer wraps the `ironfix`
//! [`Encoder`], which frames `BeginString (8)` / `BodyLength (9)` / `CheckSum
//! (10)` automatically; the typed structs write the standard header and their
//! body fields in canonical order between those.

use ironfix_core::message::RawMessage;
use ironfix_tagvalue::Encoder;

use super::error::{FixDecodeError, FixEncodeError};
use super::limits::{MAX_GROUP_ENTRIES, SYMBOL_TAG, is_pure_group_member, truncate_untrusted};

/// The FIX tag numbers the dialect uses ([fix-dialect §2](../../../docs/specs/fix-dialect.md#2-supported-messages-and-requiredness)).
///
/// Named so the message code never carries a bare magic number.
pub mod tags {
    // Standard header / trailer.
    /// `BeginString`.
    pub const BEGIN_STRING: u16 = 8;
    /// `BodyLength`.
    pub const BODY_LENGTH: u16 = 9;
    /// `MsgType`.
    pub const MSG_TYPE: u16 = 35;
    /// `MsgSeqNum`.
    pub const MSG_SEQ_NUM: u16 = 34;
    /// `SenderCompID`.
    pub const SENDER_COMP_ID: u16 = 49;
    /// `TargetCompID`.
    pub const TARGET_COMP_ID: u16 = 56;
    /// `SendingTime`.
    pub const SENDING_TIME: u16 = 52;
    /// `CheckSum`.
    pub const CHECK_SUM: u16 = 10;

    // Session admin.
    /// `EncryptMethod`.
    pub const ENCRYPT_METHOD: u16 = 98;
    /// `HeartBtInt`.
    pub const HEART_BT_INT: u16 = 108;
    /// `Username`.
    pub const USERNAME: u16 = 553;
    /// `Password`.
    pub const PASSWORD: u16 = 554;
    /// `ResetSeqNumFlag`.
    pub const RESET_SEQ_NUM_FLAG: u16 = 141;
    /// `Text`.
    pub const TEXT: u16 = 58;
    /// `TestReqID`.
    pub const TEST_REQ_ID: u16 = 112;
    /// `BeginSeqNo`.
    pub const BEGIN_SEQ_NO: u16 = 7;
    /// `EndSeqNo`.
    pub const END_SEQ_NO: u16 = 16;
    /// `NewSeqNo`.
    pub const NEW_SEQ_NO: u16 = 36;
    /// `GapFillFlag`.
    pub const GAP_FILL_FLAG: u16 = 123;
    /// `RefSeqNum`.
    pub const REF_SEQ_NUM: u16 = 45;
    /// `SessionRejectReason`.
    pub const SESSION_REJECT_REASON: u16 = 373;
    /// `RefTagID`.
    pub const REF_TAG_ID: u16 = 371;
    /// `RefMsgType`.
    pub const REF_MSG_TYPE: u16 = 372;

    // Order entry / execution.
    /// `Account`.
    pub const ACCOUNT: u16 = 1;
    /// `ClOrdID`.
    pub const CL_ORD_ID: u16 = 11;
    /// `OrigClOrdID`.
    pub const ORIG_CL_ORD_ID: u16 = 41;
    /// `Symbol`.
    pub const SYMBOL: u16 = 55;
    /// `Side`.
    pub const SIDE: u16 = 54;
    /// `TransactTime`.
    pub const TRANSACT_TIME: u16 = 60;
    /// `OrdType`.
    pub const ORD_TYPE: u16 = 40;
    /// `Price`.
    pub const PRICE: u16 = 44;
    /// `OrderQty`.
    pub const ORDER_QTY: u16 = 38;
    /// `TimeInForce`.
    pub const TIME_IN_FORCE: u16 = 59;
    /// `ExpireTime`.
    pub const EXPIRE_TIME: u16 = 126;
    /// `OrderID`.
    pub const ORDER_ID: u16 = 37;
    /// `ExecID`.
    pub const EXEC_ID: u16 = 17;
    /// `ExecType`.
    pub const EXEC_TYPE: u16 = 150;
    /// `OrdStatus`.
    pub const ORD_STATUS: u16 = 39;
    /// `LeavesQty`.
    pub const LEAVES_QTY: u16 = 151;
    /// `CumQty`.
    pub const CUM_QTY: u16 = 14;
    /// `LastQty`.
    pub const LAST_QTY: u16 = 32;
    /// `LastPx`.
    pub const LAST_PX: u16 = 31;
    /// `SecondaryExecID`.
    pub const SECONDARY_EXEC_ID: u16 = 527;
    /// `Commission`.
    pub const COMMISSION: u16 = 12;
    /// `CommType`.
    pub const COMM_TYPE: u16 = 13;
    /// `LastLiquidityInd`.
    pub const LAST_LIQUIDITY_IND: u16 = 851;
    /// `OrdRejReason`.
    pub const ORD_REJ_REASON: u16 = 103;
    /// `CxlRejResponseTo`.
    pub const CXL_REJ_RESPONSE_TO: u16 = 434;
    /// `CxlRejReason`.
    pub const CXL_REJ_REASON: u16 = 102;
    /// `MassCancelRequestType`.
    pub const MASS_CANCEL_REQUEST_TYPE: u16 = 530;
    /// `MassCancelResponse`.
    pub const MASS_CANCEL_RESPONSE: u16 = 531;
    /// `TotalAffectedOrders`.
    pub const TOTAL_AFFECTED_ORDERS: u16 = 533;
    /// `NoAffectedOrders`.
    pub const NO_AFFECTED_ORDERS: u16 = 534;
    /// `AffectedOrderID`.
    pub const AFFECTED_ORDER_ID: u16 = 535;

    // Market data.
    /// `MDReqID`.
    pub const MD_REQ_ID: u16 = 262;
    /// `SubscriptionRequestType`.
    pub const SUBSCRIPTION_REQUEST_TYPE: u16 = 263;
    /// `MarketDepth`.
    pub const MARKET_DEPTH: u16 = 264;
    /// `NoMDEntryTypes`.
    pub const NO_MD_ENTRY_TYPES: u16 = 267;
    /// `MDEntryType`.
    pub const MD_ENTRY_TYPE: u16 = 269;
    /// `NoRelatedSym`.
    pub const NO_RELATED_SYM: u16 = 146;
    /// `RptSeq`.
    pub const RPT_SEQ: u16 = 83;
    /// `NoMDEntries`.
    pub const NO_MD_ENTRIES: u16 = 268;
    /// `MDEntryPx`.
    pub const MD_ENTRY_PX: u16 = 270;
    /// `MDEntrySize`.
    pub const MD_ENTRY_SIZE: u16 = 271;
    /// `MDUpdateAction`.
    pub const MD_UPDATE_ACTION: u16 = 279;
    /// `MDReqRejReason`.
    pub const MD_REQ_REJ_REASON: u16 = 281;
    /// `BusinessRejectReason`.
    pub const BUSINESS_REJECT_REASON: u16 = 380;
}

/// An ordered, typed view over the fields of a decoded FIX message (or one
/// repeating-group entry).
///
/// Field order is preserved from the wire so the repeating-group parser
/// ([`Self::group`]) can honour delimiter/order rules.
#[derive(Debug, Clone)]
pub struct FieldBag<'a> {
    entries: Vec<(u32, &'a [u8])>,
}

impl<'a> FieldBag<'a> {
    /// Collects the fields of a decoded message in wire order.
    #[must_use]
    pub fn collect(raw: &RawMessage<'a>) -> Self {
        let entries = raw
            .fields()
            .map(|field| (field.tag, field.as_bytes()))
            .collect();
        Self { entries }
    }

    /// The first raw value for `tag`, if present.
    #[inline]
    fn find(&self, tag: u16) -> Option<&'a [u8]> {
        self.entries
            .iter()
            .find(|(t, _)| *t == u32::from(tag))
            .map(|(_, value)| *value)
    }

    /// Returns `true` if `tag` is present at all.
    #[must_use]
    #[inline]
    pub fn has(&self, tag: u16) -> bool {
        self.find(tag).is_some()
    }

    /// The optional string value for `tag`.
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::IncorrectDataFormat`] if the value is not valid UTF-8.
    pub fn opt_str(&self, tag: u16) -> Result<Option<&'a str>, FixDecodeError> {
        match self.find(tag) {
            Some(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => Ok(Some(text)),
                Err(_) => Err(FixDecodeError::IncorrectDataFormat {
                    tag,
                    reason: "value is not valid utf-8".to_string(),
                }),
            },
            None => Ok(None),
        }
    }

    /// The required string value for `tag`.
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::MissingRequiredField`] if absent, or
    /// [`FixDecodeError::IncorrectDataFormat`] if not valid UTF-8.
    pub fn req_str(&self, tag: u16) -> Result<&'a str, FixDecodeError> {
        self.opt_str(tag)?
            .ok_or(FixDecodeError::MissingRequiredField { tag })
    }

    /// The optional unsigned-integer value for `tag`.
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::IncorrectDataFormat`] if present but not a valid
    /// non-negative integer (including overflow of `u64`).
    pub fn opt_u64(&self, tag: u16) -> Result<Option<u64>, FixDecodeError> {
        match self.opt_str(tag)? {
            Some(text) => {
                text.parse::<u64>()
                    .map(Some)
                    .map_err(|_| FixDecodeError::IncorrectDataFormat {
                        tag,
                        reason: format!(
                            "'{}' is not a valid unsigned integer",
                            truncate_untrusted(text)
                        ),
                    })
            }
            None => Ok(None),
        }
    }

    /// The required unsigned-integer value for `tag`.
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::MissingRequiredField`] if absent, or
    /// [`FixDecodeError::IncorrectDataFormat`] if not a valid integer.
    pub fn req_u64(&self, tag: u16) -> Result<u64, FixDecodeError> {
        self.opt_u64(tag)?
            .ok_or(FixDecodeError::MissingRequiredField { tag })
    }

    /// The required `u32` value for `tag` (a repeating-group count / small
    /// counter).
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::MissingRequiredField`] / [`FixDecodeError::IncorrectDataFormat`].
    pub fn req_u32(&self, tag: u16) -> Result<u32, FixDecodeError> {
        let value = self.req_u64(tag)?;
        u32::try_from(value).map_err(|_| FixDecodeError::IncorrectDataFormat {
            tag,
            reason: format!("'{value}' exceeds the 32-bit range"),
        })
    }

    /// The optional `u16` value for `tag` (a reason code).
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::IncorrectDataFormat`] if present but not a valid `u16`.
    pub fn opt_u16(&self, tag: u16) -> Result<Option<u16>, FixDecodeError> {
        match self.opt_str(tag)? {
            Some(text) => {
                text.parse::<u16>()
                    .map(Some)
                    .map_err(|_| FixDecodeError::IncorrectDataFormat {
                        tag,
                        reason: format!(
                            "'{}' is not a valid 16-bit reason code",
                            truncate_untrusted(text)
                        ),
                    })
            }
            None => Ok(None),
        }
    }

    /// The required `u16` value for `tag`.
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::MissingRequiredField`] / [`FixDecodeError::IncorrectDataFormat`].
    pub fn req_u16(&self, tag: u16) -> Result<u16, FixDecodeError> {
        self.opt_u16(tag)?
            .ok_or(FixDecodeError::MissingRequiredField { tag })
    }

    /// The optional FIX boolean (`Y`/`N`) value for `tag`.
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::IncorrectDataFormat`] if present but not `Y`/`N`.
    pub fn opt_bool(&self, tag: u16) -> Result<Option<bool>, FixDecodeError> {
        match self.opt_str(tag)? {
            Some("Y") => Ok(Some(true)),
            Some("N") => Ok(Some(false)),
            Some(other) => Err(FixDecodeError::IncorrectDataFormat {
                tag,
                reason: format!("'{}' is not a FIX boolean (Y/N)", truncate_untrusted(other)),
            }),
            None => Ok(None),
        }
    }

    /// Parses a required repeating group, enforcing the declared `NoXXX` count,
    /// the delimiter as each entry's first field, and that only `member_tags`
    /// appear inside the group before it ends.
    ///
    /// Returns one [`FieldBag`] per group entry, in wire order.
    ///
    /// # Errors
    ///
    /// - [`FixDecodeError::MissingRequiredField`] if the count tag is absent.
    /// - [`FixDecodeError::GroupCountMismatch`] if the number of decoded entries
    ///   does not match the declared count, or a member field appears before the
    ///   first delimiter (an order violation).
    pub fn group(
        &self,
        count_tag: u16,
        delimiter_tag: u16,
        member_tags: &[u16],
    ) -> Result<Vec<FieldBag<'a>>, FixDecodeError> {
        let declared = self.req_u32(count_tag)?;
        // Reject a huge declared count cheaply, before any per-entry work.
        if declared as usize > MAX_GROUP_ENTRIES {
            return Err(FixDecodeError::TooManyGroupEntries {
                count_tag,
                declared,
                max: MAX_GROUP_ENTRIES,
            });
        }
        let count_pos = self
            .entries
            .iter()
            .position(|(t, _)| *t == u32::from(count_tag))
            .ok_or(FixDecodeError::MissingRequiredField { tag: count_tag })?;

        let is_member = |tag: u32| member_tags.iter().any(|&m| u32::from(m) == tag);

        let mut groups: Vec<FieldBag<'a>> = Vec::new();
        let mut current: Option<Vec<(u32, &'a [u8])>> = None;
        for &(tag, value) in &self.entries[count_pos + 1..] {
            if tag == u32::from(delimiter_tag) {
                if let Some(entry) = current.take() {
                    groups.push(FieldBag { entries: entry });
                }
                current = Some(vec![(tag, value)]);
            } else if is_member(tag) {
                match current.as_mut() {
                    Some(entry) => entry.push((tag, value)),
                    None => {
                        // A member field before any delimiter is an order violation.
                        return Err(FixDecodeError::GroupCountMismatch {
                            count_tag,
                            declared,
                            decoded: 0,
                        });
                    }
                }
            } else {
                // A non-member tag ends the group region.
                break;
            }
        }
        if let Some(entry) = current.take() {
            groups.push(FieldBag { entries: entry });
        }

        let decoded = u32::try_from(groups.len()).unwrap_or(u32::MAX);
        if decoded != declared {
            return Err(FixDecodeError::GroupCountMismatch {
                count_tag,
                declared,
                decoded,
            });
        }
        Ok(groups)
    }

    /// Like [`Self::group`], but additionally rejects a declared-empty group
    /// where the dialect requires at least one entry.
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::EmptyGroup`] if the group has zero entries, plus every
    /// [`Self::group`] error.
    pub fn required_group(
        &self,
        count_tag: u16,
        delimiter_tag: u16,
        member_tags: &[u16],
    ) -> Result<Vec<FieldBag<'a>>, FixDecodeError> {
        let groups = self.group(count_tag, delimiter_tag, member_tags)?;
        if groups.is_empty() {
            return Err(FixDecodeError::EmptyGroup { count_tag });
        }
        Ok(groups)
    }

    /// Rejects a tag that appears more than once when it is not a legitimate
    /// repeating-group member.
    ///
    /// A pure group-member tag ([`is_pure_group_member`] — `MDEntryType`,
    /// `MDEntryPx`, `AffectedOrderID`, …) may always repeat. `Symbol (55)` may
    /// repeat only when `symbol_repeatable` is set — i.e. in `MarketDataRequest
    /// (V)` / `MarketDataIncrementalRefresh (X)`, its two group-member messages
    /// (see [`symbol_repeats_in_msg_type`]). Any other tag, including `Symbol`
    /// in an order message, must be unique; a duplicate is a session violation
    /// (`SessionRejectReason=13`), rejected here rather than silently dropped by
    /// [`Self::find`]'s first-wins lookup.
    ///
    /// Keying `Symbol`'s repeatability on the **message type** — not on
    /// in-stream group-span position — is deliberate: a position/span heuristic
    /// could be defeated by injecting a bogus `NoRelatedSym` count tag into a
    /// `NewOrderSingle` to re-open a fake span and smuggle a duplicate `Symbol`.
    /// The message type cannot be spoofed that way (it is `MsgType (35)`, already
    /// dispatched on).
    ///
    /// [`is_pure_group_member`]: super::limits::is_pure_group_member
    /// [`symbol_repeats_in_msg_type`]: super::limits::symbol_repeats_in_msg_type
    ///
    /// # Errors
    ///
    /// [`FixDecodeError::DuplicateTag`] on the first illegitimately-duplicated
    /// tag.
    pub fn reject_duplicate_scalar_tags(
        &self,
        symbol_repeatable: bool,
    ) -> Result<(), FixDecodeError> {
        let mut seen: Vec<u32> = Vec::with_capacity(self.entries.len());
        for &(tag, _) in &self.entries {
            // Pure group members always repeat; Symbol repeats only in its two
            // group-member message types.
            if is_pure_group_member(tag) || (tag == SYMBOL_TAG && symbol_repeatable) {
                continue;
            }
            if seen.contains(&tag) {
                return Err(FixDecodeError::DuplicateTag {
                    tag: u16::try_from(tag).unwrap_or(u16::MAX),
                });
            }
            seen.push(tag);
        }
        Ok(())
    }
}

/// A field writer that frames the standard header/trailer via the `ironfix`
/// [`Encoder`] and appends body fields in the order they are written.
///
/// `BeginString (8)` / `BodyLength (9)` are prepended and `CheckSum (10)` is
/// appended by [`Encoder::finish`]; the caller writes `MsgType (35)` first (via
/// [`Self::new`]), then the standard header, then the message body.
#[derive(Debug)]
pub struct FieldWriter {
    encoder: Encoder,
}

impl FieldWriter {
    /// Starts a message of `msg_type` on the pinned begin string, writing
    /// `MsgType (35)` as the first body field.
    #[must_use]
    pub fn new(msg_type: &str) -> Self {
        let mut encoder = Encoder::new(super::BEGIN_STRING);
        encoder.put_str(u32::from(tags::MSG_TYPE), msg_type);
        Self { encoder }
    }

    /// Appends a string field.
    #[inline]
    pub fn str(&mut self, tag: u16, value: &str) {
        self.encoder.put_str(u32::from(tag), value);
    }

    /// Appends a string field only when present.
    #[inline]
    pub fn opt_str(&mut self, tag: u16, value: Option<&str>) {
        if let Some(value) = value {
            self.str(tag, value);
        }
    }

    /// Appends an unsigned-integer field.
    #[inline]
    pub fn u64(&mut self, tag: u16, value: u64) {
        self.encoder.put_uint(u32::from(tag), value);
    }

    /// Appends an unsigned-integer field only when present.
    #[inline]
    pub fn opt_u64(&mut self, tag: u16, value: Option<u64>) {
        if let Some(value) = value {
            self.u64(tag, value);
        }
    }

    /// Appends a `u16` field (a reason code).
    #[inline]
    pub fn u16(&mut self, tag: u16, value: u16) {
        self.encoder.put_uint(u32::from(tag), u64::from(value));
    }

    /// Appends a `u16` field only when present.
    #[inline]
    pub fn opt_u16(&mut self, tag: u16, value: Option<u16>) {
        if let Some(value) = value {
            self.u16(tag, value);
        }
    }

    /// Appends a FIX boolean (`Y`/`N`) field only when present.
    #[inline]
    pub fn opt_bool(&mut self, tag: u16, value: Option<bool>) {
        if let Some(value) = value {
            self.encoder.put_bool(u32::from(tag), value);
        }
    }

    /// Finalises the message, computing `BodyLength (9)` and `CheckSum (10)`, and
    /// returns the complete wire bytes.
    ///
    /// # Errors
    ///
    /// [`FixEncodeError`] if the `ironfix-tagvalue` encoder rejects the frame at
    /// finish — a **deferred** field-write error surfaced here (an over-long/invalid
    /// field, or a missing `MsgType (35)`). This is a venue-side invariant violation
    /// on the outbound path (ironfix 0.4 made `Encoder::finish` fallible), surfaced
    /// typed rather than panicked.
    pub fn finish(mut self) -> Result<Vec<u8>, FixEncodeError> {
        Ok(self.encoder.finish()?.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY: &[u8] = b"";

    /// Builds a `FieldBag` from a bare tag sequence (the duplicate check ignores
    /// values, so they are irrelevant here).
    fn bag(tags: &[u32]) -> FieldBag<'static> {
        FieldBag {
            entries: tags.iter().map(|&t| (t, EMPTY)).collect(),
        }
    }

    // `false` = Symbol is scalar (order/session messages); `true` = Symbol is a
    // group member (MarketDataRequest V / IncrementalRefresh X).
    const SYMBOL_SCALAR: bool = false;
    const SYMBOL_GROUP: bool = true;

    #[test]
    fn test_reject_duplicate_scalar_tag() {
        // Two ClOrdID(11) — a scalar duplicate, rejected regardless of msg type.
        match bag(&[35, 11, 44, 11]).reject_duplicate_scalar_tags(SYMBOL_SCALAR) {
            Err(FixDecodeError::DuplicateTag { tag }) => assert_eq!(tag, 11),
            other => panic!("expected DuplicateTag(11), got {other:?}"),
        }
    }

    #[test]
    fn test_reject_duplicate_scalar_symbol_in_order_message() {
        // Symbol(55) is SCALAR in NewOrderSingle — two different 55s must reject,
        // not silently first-wins (the P2.3 dual-role gap).
        match bag(&[35, 55, 54, 40, 38, 55]).reject_duplicate_scalar_tags(SYMBOL_SCALAR) {
            Err(FixDecodeError::DuplicateTag { tag }) => assert_eq!(tag, 55),
            other => panic!("expected DuplicateTag(55), got {other:?}"),
        }
    }

    #[test]
    fn test_fake_group_count_cannot_re_exempt_a_scalar_symbol() {
        // The message-type key closes the fake-span bypass: injecting a bogus
        // NoRelatedSym(146) count into a NewOrderSingle must NOT re-exempt the
        // duplicate Symbol — with Symbol scalar, the second 55 is still rejected.
        match bag(&[35, 146, 55, 40, 55]).reject_duplicate_scalar_tags(SYMBOL_SCALAR) {
            Err(FixDecodeError::DuplicateTag { tag }) => assert_eq!(tag, 55),
            other => panic!("expected DuplicateTag(55) despite the fake 146, got {other:?}"),
        }
    }

    #[test]
    fn test_repeated_symbol_allowed_in_market_data_messages() {
        // Symbol(55) repeats legitimately in V/X (its group-member messages).
        assert!(
            bag(&[263, 146, 55, 55])
                .reject_duplicate_scalar_tags(SYMBOL_GROUP)
                .is_ok()
        );
    }

    #[test]
    fn test_repeated_pure_group_members_always_allowed() {
        // Pure group members (MDEntryType 269) repeat regardless of msg type.
        assert!(
            bag(&[263, 267, 269, 269])
                .reject_duplicate_scalar_tags(SYMBOL_SCALAR)
                .is_ok()
        );
    }

    #[test]
    fn test_duplicate_group_count_tag_is_rejected() {
        // A group count tag itself is scalar-unique — two NoRelatedSym(146) is a
        // malformed frame.
        match bag(&[263, 146, 55, 146]).reject_duplicate_scalar_tags(SYMBOL_GROUP) {
            Err(FixDecodeError::DuplicateTag { tag }) => assert_eq!(tag, 146),
            other => panic!("expected DuplicateTag(146), got {other:?}"),
        }
    }
}
