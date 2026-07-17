//! Typed microstructure errors — the startup [`MicrostructureConfigError`] and the
//! runtime admission [`PriceBoundError`].
//!
//! [`MicrostructureConfigError`] is a **startup** failure: it is raised while the
//! venue config is validated (before it serves a request) and is folded into the
//! crate-wide `ConfigError` at the config seam
//! ([`crate::config::ConfigError::Microstructure`]). [`PriceBoundError`] is a
//! **request-boundary** failure: the venue-owned `max_price_cents` / `min_price_cents`
//! admission cap raised per order, mapped onto `VenueError::InvalidOrder` at the
//! order-admission seam so an over-cap price never reaches the leaf
//! ([05 §4.1](../../../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable)).

/// A failure validating the `[microstructure.*]` / `[instruments."<SYM>".specs]`
/// config at boot.
///
/// Every variant fails the process fast before it serves a request
/// (`rules/global_rules.md` *Configuration*), and every message is lowercase and
/// names the offending value. The load-bearing variants are
/// [`FeeBoundUnprovable`](Self::FeeBoundUnprovable) and
/// [`FeePersistOverflow`](Self::FeePersistOverflow): together they are the
/// **checked-fee startup proof** that makes the upstream
/// `FeeSchedule::calculate_fee` saturating branch provably unreachable by
/// bounding config, rather than the venue inventing private fee math
/// ([05 §4.1](../../../docs/05-microstructure-config.md#41-the-checked-fee-contract-saturation-made-unreachable)).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MicrostructureConfigError {
    /// The taker fee was negative. The upstream `FeeSchedule` contract requires a
    /// non-negative taker rate (only the maker rate may be a rebate).
    #[error("taker_bps must be non-negative (got {taker_bps}); only maker_bps may be a rebate")]
    TakerFeeNegative {
        /// The offending taker basis-point rate.
        taker_bps: i32,
    },
    /// A contract-spec knob that must be at least one was zero
    /// (`tick_size_cents` / `lot_size` / `min_price_cents` / `max_order_qty`).
    #[error("{field} must be at least 1 (got 0)")]
    SpecKnobZero {
        /// The offending field name.
        field: &'static str,
    },
    /// The venue-owned `max_price_cents` cap was below the `min_price_cents`
    /// floor — an empty admissible price band.
    #[error("max_price_cents ({max}) must be at or above min_price_cents ({min})")]
    MaxPriceBelowMin {
        /// The configured minimum price (cents).
        min: u64,
        /// The configured maximum price (cents).
        max: u64,
    },
    /// **Checked-fee proof, part A.** The widest admissible notional
    /// (`max_price_cents × max_order_qty`) exceeds the upstream
    /// multiplication-safety bound `FeeSchedule::max_guaranteed_exact_notional()`
    /// for the configured maker/taker rates, so `FeeSchedule::calculate_fee` could
    /// reach its `saturating_mul` / `i128::MAX` branch and journal a clamped,
    /// unverifiable fee. Rejected at startup so the saturating branch is provably
    /// unreachable at runtime.
    #[error(
        "fee bound unprovable: widest notional {max_notional} (max_price_cents × max_order_qty) \
         exceeds the guaranteed-exact bound {guaranteed_bound} for maker_bps={maker_bps} \
         taker_bps={taker_bps}; lower max_price_cents, max_order_qty, or the fee rate"
    )]
    FeeBoundUnprovable {
        /// The widest admissible notional in cents (`max_price_cents × max_order_qty`).
        max_notional: u128,
        /// The upstream guaranteed-exact notional bound for this schedule.
        guaranteed_bound: u128,
        /// The configured maker basis-point rate.
        maker_bps: i32,
        /// The configured taker basis-point rate.
        taker_bps: i32,
    },
    /// **Checked-fee proof, part B.** The worst-case fee magnitude on the widest
    /// admissible notional would not fit the persisted `i64` cents column
    /// (a durable `BIGINT`), so a fill's fee could not be recorded losslessly.
    #[error(
        "fee would not fit persisted i64 cents: worst-case fee magnitude {fee_magnitude} \
         on notional {max_notional} at {max_abs_bps} bps exceeds i64::MAX; lower max_price_cents, \
         max_order_qty, or the fee rate"
    )]
    FeePersistOverflow {
        /// The worst-case fee magnitude in cents.
        fee_magnitude: u128,
        /// The widest admissible notional in cents.
        max_notional: u128,
        /// The larger of `|maker_bps|` and `|taker_bps|`.
        max_abs_bps: u32,
    },
    /// A checked multiplication in the fee-bound proof overflowed its integer
    /// width. Unreachable for the venue's bounded knobs (`max_price_cents` and
    /// `max_order_qty` are each `u64`, so their product fits `u128`); the proof
    /// fails loud rather than wrap.
    #[error("fee-bound proof arithmetic overflow")]
    ProofArithmeticOverflow,
    /// The upstream `ContractSpecsBuilder::build` rejected the resolved knobs.
    /// Carries the upstream reason (safe to echo — no secret).
    #[error("contract specs rejected by the matching engine: {reason}")]
    ContractSpecsRejected {
        /// The upstream rejection reason.
        reason: String,
    },
}

/// A venue-owned price-band admission failure raised per order at the
/// order-admission and replay seams.
///
/// The venue defines its own `min_price_cents` / `max_price_cents` band because
/// the upstream `ValidationConfig` carries no price bound (verified against the
/// pinned `option-chain-orderbook` 0.7.0 / `orderbook-rs` 0.10.5); an order whose
/// price falls outside the band is rejected **before matching**, so it never
/// reaches the leaf, and the cap also keeps the persisted `BIGINT` cents columns
/// lossless
/// ([governance-precedence §2.1](../../../docs/governance-precedence.md#21-cents-at-the-database-boundary-lossless-encoding)).
/// Mapped onto `VenueError::InvalidOrder` at the admission seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PriceBoundError {
    /// The order price exceeded the venue-owned `max_price_cents` cap.
    #[error("price {price} cents exceeds the venue max_price_cents cap of {max}")]
    AboveMax {
        /// The offending order price (cents).
        price: u64,
        /// The configured maximum price (cents).
        max: u64,
    },
    /// The order price fell below the venue-owned `min_price_cents` floor.
    #[error("price {price} cents is below the venue min_price_cents floor of {min}")]
    BelowMin {
        /// The offending order price (cents).
        price: u64,
        /// The configured minimum price (cents).
        min: u64,
    },
}
