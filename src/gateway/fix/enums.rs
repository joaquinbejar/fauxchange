//! The closed-set FIX enums the dialect admits, each matched **exhaustively**.
//!
//! Every enum here has a fixed, closed value domain ([fix-dialect §2](../../../docs/specs/fix-dialect.md#2-supported-messages-and-requiredness)):
//! `from_fix` maps each admitted wire value to a variant and rejects anything
//! else with a typed [`FixDecodeError::ValueIsIncorrect`] — there is **no `_`
//! arm that silently accepts** an unknown value, and no silent default. The one
//! defaulted field, `TimeInForce (59)`, defaults to `GTC` only when the tag is
//! *absent* ([`TimeInForce::from_fix_or_default`]), never when it is present
//! with an unknown value.

use super::error::FixDecodeError;
use super::limits::truncate_untrusted;

/// Generates a closed-set FIX enum over single-token wire values, with a `TAG`
/// constant, a `to_fix` renderer, and a `from_fix` decoder whose final arm
/// **rejects** unknown values (never silently accepts).
macro_rules! fix_closed_enum {
    (
        $(#[$emeta:meta])*
        pub enum $name:ident = tag $tag:literal {
            $( $(#[$vmeta:meta])* $variant:ident => $wire:literal ),+ $(,)?
        }
    ) => {
        $(#[$emeta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(u8)]
        pub enum $name {
            $( $(#[$vmeta])* $variant ),+
        }

        impl $name {
            /// The FIX tag number this enum is the value domain of.
            pub const TAG: u16 = $tag;

            /// The FIX wire value for this variant.
            #[must_use]
            #[inline]
            pub const fn to_fix(self) -> &'static str {
                match self {
                    $( Self::$variant => $wire ),+
                }
            }

            /// Decodes a wire value into the closed set.
            ///
            /// # Errors
            ///
            /// Returns [`FixDecodeError::ValueIsIncorrect`] if `value` is not one
            /// of the admitted values — a typed reject, never a silent default.
            #[inline]
            pub fn from_fix(value: &str) -> Result<Self, FixDecodeError> {
                match value {
                    $( $wire => Ok(Self::$variant), )+
                    other => Err(FixDecodeError::ValueIsIncorrect {
                        tag: $tag,
                        value: truncate_untrusted(other),
                    }),
                }
            }
        }
    };
}

fix_closed_enum! {
    /// `Side (54)` — the dialect admits only Buy/Sell (no short/cross variants).
    pub enum OrderSide = tag 54 {
        /// `1` — Buy.
        Buy => "1",
        /// `2` — Sell.
        Sell => "2",
    }
}

fix_closed_enum! {
    /// `OrdType (40)` — Market or Limit only.
    pub enum OrdType = tag 40 {
        /// `1` — Market order (the true non-resting primitive, [ADR-0009 §3](../../../docs/adr/0009-lossless-venue-envelope-outcomes.md)).
        Market => "1",
        /// `2` — Limit order (requires `Price (44)`).
        Limit => "2",
    }
}

fix_closed_enum! {
    /// `TimeInForce (59)` — Day/GTC/IOC/FOK/GTD. Defaults to GTC when absent
    /// (see [`TimeInForce::from_fix_or_default`]).
    pub enum TimeInForce = tag 59 {
        /// `0` — Day.
        Day => "0",
        /// `1` — Good 'Til Cancel (the default when the tag is absent).
        Gtc => "1",
        /// `3` — Immediate Or Cancel.
        Ioc => "3",
        /// `4` — Fill Or Kill.
        Fok => "4",
        /// `6` — Good 'Til Date (requires `ExpireTime (126)`).
        Gtd => "6",
    }
}

impl Default for TimeInForce {
    /// The dialect default when `TimeInForce (59)` is absent is `GTC`.
    #[inline]
    fn default() -> Self {
        Self::Gtc
    }
}

impl TimeInForce {
    /// Decodes `TimeInForce (59)`, applying the `GTC` default only when the tag
    /// is **absent** — a present-but-unknown value is still a typed reject.
    ///
    /// # Errors
    ///
    /// Returns [`FixDecodeError::ValueIsIncorrect`] if the tag is present with a
    /// value outside the admitted set.
    #[inline]
    pub fn from_fix_or_default(value: Option<&str>) -> Result<Self, FixDecodeError> {
        match value {
            Some(raw) => Self::from_fix(raw),
            None => Ok(Self::default()),
        }
    }
}

fix_closed_enum! {
    /// `ExecType (150)` — the execution-report transition on the venue's report
    /// stream ([fix-dialect §2.2](../../../docs/specs/fix-dialect.md#22-order-entry-and-execution)).
    pub enum ExecType = tag 150 {
        /// `0` — New (order accepted).
        New => "0",
        /// `F` — Trade (a fill).
        Trade => "F",
        /// `4` — Canceled.
        Canceled => "4",
        /// `5` — Replaced.
        Replaced => "5",
        /// `8` — Rejected.
        Rejected => "8",
        /// `C` — Expired (TIF/contract expiry).
        Expired => "C",
    }
}

fix_closed_enum! {
    /// `OrdStatus (39)` — the order lifecycle status.
    pub enum OrdStatus = tag 39 {
        /// `0` — New.
        New => "0",
        /// `1` — Partially filled.
        PartiallyFilled => "1",
        /// `2` — Filled.
        Filled => "2",
        /// `4` — Canceled.
        Canceled => "4",
        /// `5` — Replaced.
        Replaced => "5",
        /// `8` — Rejected.
        Rejected => "8",
        /// `C` — Expired.
        Expired => "C",
    }
}

fix_closed_enum! {
    /// `MassCancelRequestType (530)` — the scope of a mass cancel.
    pub enum MassCancelRequestType = tag 530 {
        /// `1` — Cancel orders for a security (`Symbol (55)` required).
        Security => "1",
        /// `7` — Cancel all orders (venue-wide for the account).
        All => "7",
    }
}

fix_closed_enum! {
    /// `MassCancelResponse (531)` — the outcome on the mass-cancel report.
    pub enum MassCancelResponse = tag 531 {
        /// `0` — Cancel request rejected.
        Rejected => "0",
        /// `1` — Cancel orders for a security.
        Security => "1",
        /// `7` — Cancel all orders.
        All => "7",
    }
}

fix_closed_enum! {
    /// `MDEntryType (269)` — market-data entry type. The dialect admits only
    /// Bid/Offer/Trade.
    pub enum MdEntryType = tag 269 {
        /// `0` — Bid.
        Bid => "0",
        /// `1` — Offer.
        Offer => "1",
        /// `2` — Trade.
        Trade => "2",
    }
}

fix_closed_enum! {
    /// `SubscriptionRequestType (263)` — snapshot+updates or unsubscribe. The
    /// dialect does not admit a bare snapshot (`0`).
    pub enum SubscriptionRequestType = tag 263 {
        /// `1` — Snapshot plus updates.
        SnapshotPlusUpdates => "1",
        /// `2` — Disable previous snapshot plus updates (unsubscribe).
        Unsubscribe => "2",
    }
}

fix_closed_enum! {
    /// `MDUpdateAction (279)` — the incremental-refresh action on a market-data
    /// entry (resulting-quantity semantics, [fix-dialect §2.3](../../../docs/specs/fix-dialect.md#23-market-data-subscription-surfaces-03-54)).
    pub enum MdUpdateAction = tag 279 {
        /// `0` — New level.
        New => "0",
        /// `1` — Change (resulting quantity).
        Change => "1",
        /// `2` — Delete (level removed).
        Delete => "2",
    }
}

fix_closed_enum! {
    /// `CxlRejResponseTo (434)` — which request an `OrderCancelReject (9)` answers.
    pub enum CxlRejResponseTo = tag 434 {
        /// `1` — Order Cancel Request.
        OrderCancelRequest => "1",
        /// `2` — Order Cancel/Replace Request.
        OrderCancelReplaceRequest => "2",
    }
}

fix_closed_enum! {
    /// `LastLiquidityInd (851)` — whether the fill leg made or took liquidity.
    pub enum LastLiquidityInd = tag 851 {
        /// `1` — Added liquidity (maker).
        Maker => "1",
        /// `2` — Removed liquidity (taker).
        Taker => "2",
    }
}

fix_closed_enum! {
    /// `CommType (13)` — the venue reports the per-leg fee as an absolute amount.
    pub enum CommType = tag 13 {
        /// `3` — Absolute (a per-leg fee in currency units, [fix-dialect §2.2](../../../docs/specs/fix-dialect.md#22-order-entry-and-execution)).
        Absolute => "3",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_side_round_trips() {
        assert_eq!(OrderSide::from_fix("1"), Ok(OrderSide::Buy));
        assert_eq!(OrderSide::from_fix("2"), Ok(OrderSide::Sell));
        assert_eq!(OrderSide::Buy.to_fix(), "1");
        assert_eq!(OrderSide::Sell.to_fix(), "2");
    }

    #[test]
    fn test_order_side_rejects_short_and_cross() {
        // FIX Side has 3..=G values; the dialect admits only 1/2.
        for rejected in ["3", "5", "8", "G", "0", ""] {
            match OrderSide::from_fix(rejected) {
                Err(FixDecodeError::ValueIsIncorrect { tag, value }) => {
                    assert_eq!(tag, 54);
                    assert_eq!(value, rejected);
                }
                other => panic!("expected reject for {rejected}, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_ord_type_closed_set() {
        assert_eq!(OrdType::from_fix("1"), Ok(OrdType::Market));
        assert_eq!(OrdType::from_fix("2"), Ok(OrdType::Limit));
        assert!(OrdType::from_fix("3").is_err());
    }

    #[test]
    fn test_time_in_force_defaults_to_gtc_only_when_absent() {
        assert_eq!(TimeInForce::from_fix_or_default(None), Ok(TimeInForce::Gtc));
        assert_eq!(
            TimeInForce::from_fix_or_default(Some("3")),
            Ok(TimeInForce::Ioc)
        );
        assert_eq!(
            TimeInForce::from_fix_or_default(Some("6")),
            Ok(TimeInForce::Gtd)
        );
        // A present-but-unknown value is a reject, never the default.
        match TimeInForce::from_fix_or_default(Some("2")) {
            Err(FixDecodeError::ValueIsIncorrect { tag, .. }) => assert_eq!(tag, 59),
            other => panic!("expected reject, got {other:?}"),
        }
    }

    #[test]
    fn test_all_admitted_time_in_force_values() {
        assert_eq!(TimeInForce::from_fix("0"), Ok(TimeInForce::Day));
        assert_eq!(TimeInForce::from_fix("1"), Ok(TimeInForce::Gtc));
        assert_eq!(TimeInForce::from_fix("3"), Ok(TimeInForce::Ioc));
        assert_eq!(TimeInForce::from_fix("4"), Ok(TimeInForce::Fok));
        assert_eq!(TimeInForce::from_fix("6"), Ok(TimeInForce::Gtd));
        // 2 and 5 are not admitted by the dialect.
        assert!(TimeInForce::from_fix("2").is_err());
        assert!(TimeInForce::from_fix("5").is_err());
    }

    #[test]
    fn test_exec_type_and_ord_status_transitions() {
        assert_eq!(ExecType::from_fix("F"), Ok(ExecType::Trade));
        assert_eq!(ExecType::from_fix("C"), Ok(ExecType::Expired));
        assert_eq!(OrdStatus::from_fix("1"), Ok(OrdStatus::PartiallyFilled));
        assert_eq!(OrdStatus::from_fix("2"), Ok(OrdStatus::Filled));
        assert!(ExecType::from_fix("Z").is_err());
        assert!(OrdStatus::from_fix("9").is_err());
    }

    #[test]
    fn test_md_entry_type_closed_set() {
        assert_eq!(MdEntryType::from_fix("0"), Ok(MdEntryType::Bid));
        assert_eq!(MdEntryType::from_fix("1"), Ok(MdEntryType::Offer));
        assert_eq!(MdEntryType::from_fix("2"), Ok(MdEntryType::Trade));
        // 3 (index value) etc. are not admitted.
        assert!(MdEntryType::from_fix("3").is_err());
    }

    #[test]
    fn test_subscription_request_type_rejects_bare_snapshot() {
        assert_eq!(
            SubscriptionRequestType::from_fix("1"),
            Ok(SubscriptionRequestType::SnapshotPlusUpdates)
        );
        assert_eq!(
            SubscriptionRequestType::from_fix("2"),
            Ok(SubscriptionRequestType::Unsubscribe)
        );
        // `0` (snapshot only) is not admitted by the dialect.
        assert!(SubscriptionRequestType::from_fix("0").is_err());
    }

    #[test]
    fn test_mass_cancel_request_type_scope() {
        assert_eq!(
            MassCancelRequestType::from_fix("1"),
            Ok(MassCancelRequestType::Security)
        );
        assert_eq!(
            MassCancelRequestType::from_fix("7"),
            Ok(MassCancelRequestType::All)
        );
        assert!(MassCancelRequestType::from_fix("2").is_err());
    }

    #[test]
    fn test_unknown_enum_value_is_truncated_in_the_error() {
        // A hostile oversized value is bounded at construction, so no renderer
        // can echo an unbounded payload onto the wire.
        let hostile = "9".repeat(10_000);
        match OrderSide::from_fix(&hostile) {
            Err(FixDecodeError::ValueIsIncorrect { value, .. }) => {
                assert!(value.len() <= super::super::limits::MAX_UNTRUSTED_SNIPPET_BYTES + 3);
                assert!(value.ends_with("..."));
            }
            other => panic!("expected ValueIsIncorrect, got {other:?}"),
        }
    }

    #[test]
    fn test_md_update_action_and_liquidity() {
        assert_eq!(MdUpdateAction::from_fix("2"), Ok(MdUpdateAction::Delete));
        assert_eq!(LastLiquidityInd::from_fix("1"), Ok(LastLiquidityInd::Maker));
        assert_eq!(LastLiquidityInd::from_fix("2"), Ok(LastLiquidityInd::Taker));
        assert!(LastLiquidityInd::from_fix("3").is_err());
    }
}
