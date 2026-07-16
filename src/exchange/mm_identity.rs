//! The venue-reserved **market-maker identity marker** — a venue-wide contract
//! that lives beside the [`VenueCommand`] it tags
//! ([015](../../../milestones/v0.1-backend-core/015-market-maker-on-sequenced-path.md),
//! [02 §4, §6](../../../docs/02-matching-architecture.md)).
//!
//! The market maker attributes every requote order to this reserved
//! [`AccountId`](market_maker_account) and STP [`MARKET_MAKER_OWNER`] so (a) fills
//! attribute to the maker and (b) the WS subscription manager can suppress the
//! `orderbook_delta` for a requote (MM requotes land in the next periodic
//! snapshot, not an incremental delta).
//!
//! It lives in [`crate::exchange`] — the domain **core**, next to the envelope —
//! rather than in [`crate::market_maker`], because it is consumed by **two**
//! peers: the market-maker domain (which *tags* commands with it) and the
//! [`crate::subscription`] WS service (whose requote-no-delta rule *keys on* it).
//! A shared contract used by both belongs in the lower layer they both depend on,
//! so neither reaches sideways into the other ([01 §6.1](../../../docs/01-domain-model.md)).

use crate::exchange::boundary::Hash32;
use crate::exchange::envelope::VenueCommand;
use crate::models::AccountId;

/// The venue-reserved market-maker account id — the attribution marker on every
/// requote order. A sentinel unlikely to collide with a provisioned account.
pub const MARKET_MAKER_ACCOUNT: &str = "@market-maker";

/// The venue-reserved market-maker STP owner hash — the by-user grouping key for
/// the maker's own resting quotes.
pub const MARKET_MAKER_OWNER: Hash32 = Hash32([0xEE; 32]);

/// The venue-reserved market-maker [`AccountId`].
#[must_use]
#[inline]
pub fn market_maker_account() -> AccountId {
    AccountId::new(MARKET_MAKER_ACCOUNT)
}

/// Whether `account` is the venue-reserved market-maker account.
#[must_use]
#[inline]
pub fn is_market_maker_account(account: &AccountId) -> bool {
    account.as_str() == MARKET_MAKER_ACCOUNT
}

/// Whether `command` is a market-maker requote order (an `AddOrder` /
/// `CancelOrder` / `Replace` attributed to the venue-reserved market-maker
/// account) — the predicate the WS subscription manager's requote-no-delta rule
/// keys on ([02 §6](../../../docs/02-matching-architecture.md)).
#[must_use]
pub fn is_market_maker_command(command: &VenueCommand) -> bool {
    match command {
        VenueCommand::AddOrder { account, .. }
        | VenueCommand::CancelOrder { account, .. }
        | VenueCommand::Replace { account, .. } => is_market_maker_account(account),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::envelope::VenueCommand;
    use crate::exchange::event::EventTimestamp;
    use crate::exchange::money::Cents;
    use crate::exchange::symbol::Symbol;
    use crate::exchange::{STPMode, Side, TimeInForce};
    use crate::models::{OrderType, VenueOrderId};

    fn sym(raw: &str) -> Symbol {
        Symbol::parse(raw).expect("valid fixture symbol")
    }

    fn add(account: AccountId, owner: Hash32) -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: sym("BTC-20240329-50000-C"),
            order_id: VenueOrderId::new("id-1"),
            account,
            owner,
            client_order_id: None,
            side: Side::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(100)),
            quantity: 1,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        }
    }

    #[test]
    fn test_marker_identifies_requotes() {
        assert!(is_market_maker_account(&market_maker_account()));
        assert!(!is_market_maker_account(&AccountId::new("alice")));
        assert!(is_market_maker_command(&add(
            market_maker_account(),
            MARKET_MAKER_OWNER
        )));
        assert!(!is_market_maker_command(&add(
            AccountId::new("alice"),
            Hash32([0x11; 32])
        )));
    }

    #[test]
    fn test_market_maker_owner_is_the_reserved_sentinel() {
        assert_eq!(MARKET_MAKER_OWNER, Hash32([0xEE; 32]));
    }

    #[test]
    fn test_control_command_is_not_a_market_maker_order() {
        assert!(!is_market_maker_command(&VenueCommand::Clock {
            now_ms: EventTimestamp::new(1),
        }));
    }
}
