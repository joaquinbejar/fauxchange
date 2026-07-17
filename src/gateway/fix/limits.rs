//! Decode-layer resource ceilings and untrusted-value truncation — the venue's
//! own bounds on a hostile FIX frame, independent of the (future) socket byte
//! cap (#037, [08 threat model](../../../docs/08-threat-model.md)).
//!
//! Every one of these is a **defensive DoS ceiling**, not a business limit: a
//! conformant `fauxchange.fix44.v1` message is far below them. They bound work
//! and memory *at the parser* so a hostile frame is rejected with a typed error
//! here, before #037's byte cap and before any renderer can echo an unbounded
//! payload.

use ironfix_core::message::MsgType;

/// The maximum number of bytes of an **untrusted** field value stored inside an
/// error (and therefore potentially rendered into a `Text (58)` reject by
/// #038/#039). A longer value is truncated to a bounded snippet at construction
/// ([`truncate_untrusted`]) so no future renderer can echo an unbounded hostile
/// payload onto the wire.
pub const MAX_UNTRUSTED_SNIPPET_BYTES: usize = 64;

/// The maximum number of fields (tag=value pairs) a single decoded message may
/// carry. A conformant FIX 4.4 message — even a market-data snapshot or a
/// mass-cancel report — is bounded by the venue's book depth / order-admission
/// caps and sits far below this; the ceiling bounds a pathological
/// thousands-of-fields frame at the parser. The socket byte cap (#037) is the
/// coarser outer bound.
pub const MAX_FIELDS_PER_MESSAGE: usize = 4096;

/// The maximum number of entries a single repeating group (`NoMDEntries`,
/// `NoRelatedSym`, `NoAffectedOrders`, `NoMDEntryTypes`) may declare/decode. A
/// legitimate group is bounded by the option-chain size and the per-instrument
/// book depth; the ceiling rejects a huge-declared-count frame cheaply before
/// any per-entry work.
pub const MAX_GROUP_ENTRIES: usize = 1024;

/// Tags that are **only ever** a repeating-group member and never a scalar field
/// in any supported message, so they may always legitimately repeat. Every other
/// tag must be unique; a duplicate of a scalar tag is a session violation
/// (`SessionRejectReason=13`, [codec][super::codec] duplicate check).
///
/// `Symbol (55)` is deliberately **not** here: it is a `NoRelatedSym` /
/// `NoMDEntries` group member in `MarketDataRequest (V)` and
/// `MarketDataIncrementalRefresh (X)`, but a **scalar** in `NewOrderSingle (D)`,
/// `OrderCancelRequest (F)`, `OrderCancelReplaceRequest (G)`,
/// `OrderMassCancelRequest (q)`, `OrderStatusRequest (H)`, and
/// `MarketDataSnapshotFullRefresh (W)`. Its repeatability is therefore keyed on
/// the **message type** ([`symbol_repeats_in_msg_type`]), not on in-stream field
/// position — an in-stream group-span heuristic could be defeated by injecting a
/// bogus `NoRelatedSym` count tag into a `NewOrderSingle` to re-open a fake span,
/// which the message-type key cannot be.
pub const PURE_GROUP_MEMBER_TAGS: &[u16] = &[
    269, // MDEntryType (member/delimiter)
    270, // MDEntryPx (member)
    271, // MDEntrySize (member)
    279, // MDUpdateAction (member/delimiter)
    535, // AffectedOrderID (member)
];

/// `Symbol (55)`'s FIX tag number.
pub const SYMBOL_TAG: u32 = 55;

/// Returns `true` if `tag` is only ever a repeating-group member (see
/// [`PURE_GROUP_MEMBER_TAGS`]).
#[must_use]
#[inline]
pub fn is_pure_group_member(tag: u32) -> bool {
    PURE_GROUP_MEMBER_TAGS.iter().any(|&t| u32::from(t) == tag)
}

/// Returns `true` if `Symbol (55)` is a repeating-group member — and so may
/// legitimately appear more than once — for the given `MsgType (35)`. True only
/// for `MarketDataRequest (V)` and `MarketDataIncrementalRefresh (X)`; in every
/// other message `Symbol` is a scalar and a duplicate is rejected. This is the
/// single source of the V/X rule — [`super::decode`] calls it directly so the
/// duplicate check and the dispatch cannot drift.
#[must_use]
#[inline]
pub fn symbol_repeats_in_msg_type(msg_type: &MsgType) -> bool {
    matches!(
        msg_type,
        MsgType::MarketDataRequest | MsgType::MarketDataIncrementalRefresh
    )
}

/// Truncates an untrusted string to at most [`MAX_UNTRUSTED_SNIPPET_BYTES`] on a
/// UTF-8 char boundary, appending an ASCII `...` marker when it was shortened.
///
/// Applied at every point an untrusted field value is stored in a typed error so
/// the stored snippet is bounded regardless of the inbound size.
#[must_use]
pub fn truncate_untrusted(value: &str) -> String {
    if value.len() <= MAX_UNTRUSTED_SNIPPET_BYTES {
        return value.to_string();
    }
    // Back off to the nearest char boundary at or below the cap.
    let mut end = MAX_UNTRUSTED_SNIPPET_BYTES;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = String::with_capacity(end + 3);
    out.push_str(&value[..end]);
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_untrusted_passes_through_short_values() {
        assert_eq!(truncate_untrusted("short"), "short");
        let exact = "a".repeat(MAX_UNTRUSTED_SNIPPET_BYTES);
        assert_eq!(truncate_untrusted(&exact), exact);
    }

    #[test]
    fn test_truncate_untrusted_caps_long_values_with_marker() {
        let hostile = "x".repeat(10_000);
        let snippet = truncate_untrusted(&hostile);
        assert!(snippet.len() <= MAX_UNTRUSTED_SNIPPET_BYTES + 3);
        assert!(snippet.ends_with("..."));
        assert!(snippet.starts_with("xxxx"));
    }

    #[test]
    fn test_truncate_untrusted_respects_char_boundaries() {
        // A multi-byte char straddling the cap must not split mid-codepoint.
        let hostile = "é".repeat(1_000); // 2 bytes each
        let snippet = truncate_untrusted(&hostile);
        // Valid UTF-8 (String guarantees it) and bounded.
        assert!(snippet.len() <= MAX_UNTRUSTED_SNIPPET_BYTES + 3);
        assert!(snippet.ends_with("..."));
    }

    #[test]
    fn test_symbol_repeats_only_in_market_data_request_and_incremental() {
        // Symbol(55) is a group member (repeatable) only in V and X.
        assert!(symbol_repeats_in_msg_type(&MsgType::MarketDataRequest));
        assert!(symbol_repeats_in_msg_type(
            &MsgType::MarketDataIncrementalRefresh
        ));
        // Scalar in the order messages and the MD snapshot.
        for scalar in [
            MsgType::NewOrderSingle,
            MsgType::OrderCancelRequest,
            MsgType::OrderCancelReplaceRequest,
            MsgType::OrderStatusRequest,
            MsgType::MarketDataSnapshotFullRefresh,
            MsgType::Logon,
            MsgType::Heartbeat,
        ] {
            assert!(
                !symbol_repeats_in_msg_type(&scalar),
                "Symbol must be scalar-unique in {scalar:?}"
            );
        }
        // Symbol is never a pure group member (its repeatability is msg-keyed).
        assert!(!is_pure_group_member(SYMBOL_TAG));
    }

    #[test]
    fn test_pure_group_members_cover_md_and_masscancel_members_not_scalars() {
        assert!(is_pure_group_member(269)); // MDEntryType
        assert!(is_pure_group_member(535)); // AffectedOrderID
        // Scalar order fields are not pure group members.
        assert!(!is_pure_group_member(54)); // Side
        assert!(!is_pure_group_member(44)); // Price
        assert!(!is_pure_group_member(11)); // ClOrdID
        assert!(!is_pure_group_member(34)); // MsgSeqNum
    }
}
