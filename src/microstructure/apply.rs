//! Applying the resolved [`MicrostructureConfig`] to an upstream
//! `UnderlyingOrderBook` at book creation — the single seam the venue calls so
//! every leaf vivified under the book inherits the identical fee schedule, STP
//! mode, and contract specs.
//!
//! This is the **application** half of the surface-upstream-types principle: the
//! resolved config carries the upstream `FeeSchedule` / `STPMode` / `ContractSpecs`
//! ([`MicrostructureConfig::fee_schedule`] / [`stp_mode`](MicrostructureConfig::stp_mode)
//! / [`specs_for`](MicrostructureConfig::specs_for)), and this seam hands them to
//! the upstream `UnderlyingOrderBook` setters, which propagate them to every leaf
//! created afterwards. The venue calls it once per underlying at construction —
//! **the same call on the live path and on the replay/recovery path** — so a book
//! vivified during replay inherits the identical schedule and specs, and a
//! fee/STP-sensitive scenario replays exactly
//! ([02 §5](../../../docs/02-matching-architecture.md#5-determinism),
//! [05 §4](../../../docs/05-microstructure-config.md#4-fee-schedules)).
//!
//! The **venue-owned price band** (`min_price_cents` / `max_price_cents`) is *not*
//! applied here: the upstream `ContractSpecs` / `ValidationConfig` carries no price
//! bound, so the band is enforced separately via
//! [`MicrostructureConfig::admit_price`], **before matching**, at **every**
//! order-producer seam using that **one** shared check — the **gateway
//! order-admission seam** (`AppState::submit`), the **replay re-execution seam**
//! (the recovery reducer that reconstructs a book from a journal / bundle), and,
//! since **#109**, the venue's two internal producers: the **market-maker requote
//! sink** ([`ActorCommandSink`](crate::market_maker::ActorCommandSink)) and the
//! **price-simulator step sink** ([`VenueStepSink`](crate::simulation::VenueStepSink)).
//! A band-violating market-maker quote is **dropped** before it is submitted (never
//! posted, never journaled; the in-band side of the requote still quotes), and a
//! simulation step whose reference price falls outside the band is **rejected**
//! (never sequenced, drives no requote) — each consistent with `admit_price`'s reject
//! semantics and each a pure function of config + price, so the decision is identical
//! on a live run and on replay (the journal simply never contains the dropped item).
//! The band is therefore a true venue-wide admission invariant, which makes the
//! checked-fee proof (`FeeSchedule::try_calculate_fee`) unconditional rather than
//! conditional on the gateway/replay seams alone.
//! ([05 §4.1](../../../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable)).

use option_chain_orderbook::UnderlyingOrderBook;

use crate::microstructure::config::MicrostructureConfig;
use crate::microstructure::error::MicrostructureConfigError;

/// Applies the venue fee schedule, STP mode, and per-underlying contract specs to
/// a freshly-created upstream `UnderlyingOrderBook`, so every leaf vivified under
/// it inherits them.
///
/// Call this **once per underlying, at book creation, before any order** — on both
/// the live path and the replay/recovery path — so vivification is a pure function
/// of config and replay is exact. The venue-owned price band is enforced
/// separately at admission ([`MicrostructureConfig::admit_price`]), not here.
///
/// # Errors
///
/// [`MicrostructureConfigError::ContractSpecsRejected`] if the resolved specs are
/// rejected by the upstream `ContractSpecsBuilder` — unreachable for the
/// range-validated resolved specs (the resolver already proved them buildable),
/// surfaced rather than unwrapped.
pub fn apply_to_underlying(
    book: &UnderlyingOrderBook,
    config: &MicrostructureConfig,
    underlying: &str,
) -> Result<(), MicrostructureConfigError> {
    // Fee schedule and STP mode are venue-wide; contract specs are per-underlying.
    // `set_specs` also derives and applies the upstream `ValidationConfig`
    // (tick / lot / order-size limits) from the specs, so it must precede any leaf
    // vivification to propagate.
    book.set_fee_schedule(config.fee_schedule());
    book.set_stp_mode(config.stp_mode());
    book.set_specs(config.specs_for(underlying).to_contract_specs()?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use option_chain_orderbook::{
        OptionOrderBook, OrderId, STPMode, Side, SymbolParser, TimeInForce, UnderlyingOrderBook,
    };
    use optionstratlib::OptionStyle;

    use super::apply_to_underlying;
    use crate::microstructure::config::{FileMicrostructure, MicrostructureConfig};
    use crate::microstructure::fees::FeeConfig;
    use crate::microstructure::specs::ContractSpecsConfig;
    use crate::microstructure::stp::{StpConfig, StpMode};

    const CONTRACT: &str = "BTC-20260327-50000-C";
    const CROSS_PRICE: u128 = 50_000;

    fn owner(byte: u8) -> option_chain_orderbook::Hash32 {
        option_chain_orderbook::Hash32([byte; 32])
    }

    /// Vivifies the leaf for `CONTRACT` under `book` (inheriting whatever config
    /// was applied first).
    fn leaf(book: &UnderlyingOrderBook) -> Arc<OptionOrderBook> {
        let parsed = SymbolParser::parse(CONTRACT).expect("contract parses");
        let expiration = book.get_or_create_expiration(*parsed.expiration());
        let strike = expiration.get_or_create_strike(parsed.strike());
        match parsed.option_style() {
            OptionStyle::Call => strike.call_arc(),
            OptionStyle::Put => strike.put_arc(),
        }
    }

    /// Rests a sell then submits a crossing buy of equal size; returns the number
    /// of trades the crossing produced. A self-trade-prevented aggressor surfaces
    /// as an upstream `Err` (the taker was cancelled before matching), counted here
    /// as zero trades.
    fn cross(leaf: &OptionOrderBook, resting_owner: u8, taker_owner: u8) -> usize {
        let sell = leaf.add_limit_order_with_tif_and_user_full(
            OrderId::sequential(1),
            Side::Sell,
            CROSS_PRICE,
            5,
            TimeInForce::Gtc,
            owner(resting_owner),
        );
        assert!(sell.is_ok(), "resting sell should be accepted");
        match leaf.add_limit_order_with_tif_and_user_full(
            OrderId::sequential(2),
            Side::Buy,
            CROSS_PRICE,
            5,
            TimeInForce::Gtc,
            owner(taker_owner),
        ) {
            Ok(result) => result.match_result.trades().len(),
            // STP cancelled the aggressor before it could match: no trade printed.
            Err(_) => 0,
        }
    }

    fn config_with(
        fees: FeeConfig,
        stp: StpMode,
        specs: ContractSpecsConfig,
    ) -> MicrostructureConfig {
        let file = FileMicrostructure {
            fees: Some(fees),
            stp: Some(StpConfig { mode: stp }),
            specs: Some(specs),
            ..FileMicrostructure::default()
        };
        MicrostructureConfig::resolve(&file, &std::collections::BTreeMap::new()).expect("resolves")
    }

    #[test]
    fn test_apply_sets_fee_and_stp_on_a_vivified_leaf() {
        let book = UnderlyingOrderBook::new("BTC");
        let config = config_with(
            FeeConfig {
                maker_bps: -10,
                taker_bps: 35,
            },
            StpMode::CancelTaker,
            ContractSpecsConfig::default(),
        );
        apply_to_underlying(&book, &config, "BTC").expect("applies");

        // A leaf vivified AFTER the apply inherits the venue schedule and mode.
        let leaf = leaf(&book);
        let schedule = leaf.fee_schedule().expect("leaf inherits a fee schedule");
        assert_eq!(schedule.maker_fee_bps, -10);
        assert_eq!(schedule.taker_fee_bps, 35);
        assert_eq!(leaf.stp_mode(), STPMode::CancelTaker);
    }

    #[test]
    fn test_stp_off_allows_self_trade_cancel_taker_prevents_it() {
        // `off`: an account's two crossing orders self-trade (one trade prints).
        let off_book = UnderlyingOrderBook::new("BTC");
        let off = config_with(
            FeeConfig::default(),
            StpMode::Off,
            ContractSpecsConfig::default(),
        );
        apply_to_underlying(&off_book, &off, "BTC").expect("applies");
        assert_eq!(
            cross(&leaf(&off_book), 0xAA, 0xAA),
            1,
            "off allows the self-trade"
        );

        // `cancel_taker`: the same account's aggressor is cancelled — no trade.
        let stp_book = UnderlyingOrderBook::new("BTC");
        let stp = config_with(
            FeeConfig::default(),
            StpMode::CancelTaker,
            ContractSpecsConfig::default(),
        );
        apply_to_underlying(&stp_book, &stp, "BTC").expect("applies");
        assert_eq!(
            cross(&leaf(&stp_book), 0xAA, 0xAA),
            0,
            "cancel_taker prevents the self-trade"
        );

        // A DIFFERENT account still crosses under cancel_taker (keyed on owner).
        let other_book = UnderlyingOrderBook::new("BTC");
        apply_to_underlying(&other_book, &stp, "BTC").expect("applies");
        assert_eq!(
            cross(&leaf(&other_book), 0xAA, 0xBB),
            1,
            "distinct owners cross normally"
        );
    }

    #[test]
    fn test_same_config_two_fresh_books_replay_identical_fills() {
        // Two fresh books with the IDENTICAL resolved config and the IDENTICAL
        // command stream produce identical fills — the "a book vivified during
        // replay inherits the identical schedule and specs" determinism property.
        let config = config_with(
            FeeConfig {
                maker_bps: -10,
                taker_bps: 35,
            },
            StpMode::Off,
            ContractSpecsConfig {
                tick_size_cents: Some(5),
                ..ContractSpecsConfig::default()
            },
        );

        let build_and_cross = || {
            let book = UnderlyingOrderBook::new("BTC");
            apply_to_underlying(&book, &config, "BTC").expect("applies");
            let leaf = leaf(&book);
            let _rest = leaf.add_limit_order_with_tif_and_user_full(
                OrderId::sequential(1),
                Side::Sell,
                CROSS_PRICE,
                5,
                TimeInForce::Gtc,
                owner(0xAA),
            );
            let taker = leaf
                .add_limit_order_with_tif_and_user_full(
                    OrderId::sequential(2),
                    Side::Buy,
                    CROSS_PRICE,
                    5,
                    TimeInForce::Gtc,
                    owner(0xBB),
                )
                .expect("crossing buy");
            let trades = taker.match_result.trades();
            let vec = trades.as_vec();
            // The fee the venue would record is a deterministic function of the
            // inherited schedule and the fill notional.
            let schedule = leaf.fee_schedule().expect("schedule");
            let notional = u128::from(5u64) * CROSS_PRICE;
            (
                vec.len(),
                vec.first().map(|trade| trade.price().as_u128()),
                schedule.calculate_fee(notional, true),
                schedule.calculate_fee(notional, false),
            )
        };

        let live = build_and_cross();
        let replay = build_and_cross();
        assert_eq!(
            live, replay,
            "a fresh book replays the fee-sensitive fill exactly"
        );
        // And the fill really occurred with the configured maker rebate / taker fee.
        assert_eq!(live.0, 1);
        assert!(live.2 < 0, "maker leg is a rebate (negative)");
        assert!(live.3 > 0, "taker leg is a fee (positive)");
    }

    #[test]
    fn test_specs_reject_off_tick_price_at_the_leaf() {
        // A 5-cent tick makes an off-tick price rejected by the upstream
        // ValidationConfig the specs derive.
        let book = UnderlyingOrderBook::new("BTC");
        let config = config_with(
            FeeConfig::default(),
            StpMode::Off,
            ContractSpecsConfig {
                tick_size_cents: Some(5),
                ..ContractSpecsConfig::default()
            },
        );
        apply_to_underlying(&book, &config, "BTC").expect("applies");
        let leaf = leaf(&book);
        let off_tick = leaf.add_limit_order_with_tif_and_user_full(
            OrderId::sequential(1),
            Side::Buy,
            50_003, // not a multiple of the 5-cent tick
            5,
            TimeInForce::Gtc,
            owner(0xAA),
        );
        assert!(
            off_tick.is_err(),
            "an off-tick price must be rejected at the leaf"
        );
    }
}
