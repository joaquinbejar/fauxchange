//! Boundary newtypes re-exported from the upstream matching stack.
//!
//! These types are **not defined here** — redefining any of them (a
//! `fauxchange`-local `OrderId` or `Price`) would be a review blocker
//! ([01 §4](../../../docs/01-domain-model.md)). They are re-exported so the
//! venue can name them at the matching seam.
//!
//! | Newtype          | Reached through                                | Origin           |
//! |------------------|------------------------------------------------|------------------|
//! | [`OrderId`]      | `option_chain_orderbook::OrderId`              | `orderbook-rs`   |
//! | [`Side`]         | `option_chain_orderbook::Side`                 | `orderbook-rs`   |
//! | [`Price`]        | `option_chain_orderbook::Price`                | `pricelevel`     |
//! | [`Quantity`]     | `option_chain_orderbook::Quantity`             | `pricelevel`     |
//! | [`TimeInForce`]  | `option_chain_orderbook::TimeInForce`          | `pricelevel`     |
//! | [`TimestampMs`]  | `option_chain_orderbook::TimestampMs`          | `pricelevel`     |
//! | [`Hash32`]       | `option_chain_orderbook::Hash32`               | `pricelevel`     |
//! | [`InstrumentStatus`] | `option_chain_orderbook::InstrumentStatus` | `option-chain-orderbook` |
//! | [`OptionStyle`]  | `optionstratlib::OptionStyle`                  | `optionstratlib` |
//! | [`ExpirationDate`] | `optionstratlib::ExpirationDate`             | `optionstratlib` |
//!
//! `OptionStyle` and `ExpirationDate` are re-exported from `optionstratlib`
//! directly because `option-chain-orderbook` v0.7.0 does **not** re-export them
//! at its crate root (verified against the crate source). The
//! [`SymbolParser`] / [`ParsedSymbol`] grammar service is also re-exported here
//! so the venue routes every symbol parse through the single upstream source of
//! truth ([`crate::exchange::symbol`]).

// Boundary newtypes from `orderbook-rs` / `pricelevel`, surfaced through
// `option-chain-orderbook`.
pub use option_chain_orderbook::ParsedSymbol;
pub use option_chain_orderbook::{
    Hash32, InstrumentStatus, OrderId, Price, Quantity, Side, SymbolParser, TimeInForce,
    TimestampMs,
};

// Option-style and absolute-expiry newtypes live in `optionstratlib`.
pub use optionstratlib::{ExpirationDate, OptionStyle};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_side_reexport_is_upstream_type() {
        // Naming both variants proves the re-export resolves to the upstream
        // enum without a local redefinition.
        let buy = Side::Buy;
        let sell = Side::Sell;
        assert_ne!(buy, sell);
    }

    #[test]
    fn test_option_style_reexport_is_upstream_type() {
        assert_ne!(OptionStyle::Call, OptionStyle::Put);
    }

    #[test]
    fn test_instrument_status_reexport_has_lifecycle_variants() {
        // The venue lifecycle sequence is Active -> Settling -> Expired.
        assert!(InstrumentStatus::Active < InstrumentStatus::Settling);
        assert!(InstrumentStatus::Settling < InstrumentStatus::Expired);
    }
}
