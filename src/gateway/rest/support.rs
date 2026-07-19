//! REST gateway support: the small, pure translations between the #004 wire
//! DTOs and the domain seam — DTO enum → upstream newtype, path segments →
//! canonical [`Symbol`], provisional venue-order-id minting, and the
//! post-submit fill read the order-entry handlers render their responses from.
//!
//! Every function here is a **translation**, never a decision: matching,
//! pricing, and sequencing stay in [`crate::exchange`]. Money crosses as integer
//! [`Cents`]; the upstream newtypes are reached only through the
//! [`crate::exchange`] boundary re-exports (never redefined).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::VenueError;
use crate::exchange::{
    Cents, EventTimestamp, ExecutionFilter, ExecutionsStore, LineageId, SequenceNumber,
    Side as SeamSide, Symbol, TimeInForce as SeamTif,
};
use crate::models::{
    AccountId, LiquidityFlag, OptionStyle, Side as DtoSide, TimeInForce as DtoTif, VenueOrderId,
};
use crate::state::AppState;

/// A process-wide monotonic counter that disambiguates the **provisional**
/// venue-order-ids the gateway mints (see [`mint_order_id`]).
static ORDER_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Maps the wire [`DtoSide`] onto the upstream matching-seam [`SeamSide`] the
/// `VenueCommand` carries. Total, allocation-free.
#[must_use]
#[inline]
pub(crate) fn seam_side(side: DtoSide) -> SeamSide {
    match side {
        DtoSide::Buy => SeamSide::Buy,
        DtoSide::Sell => SeamSide::Sell,
    }
}

/// The path segment for an option style in the canonical symbol grammar
/// (`call` → `C`, `put` → `P`).
#[must_use]
#[inline]
pub(crate) fn style_char(style: OptionStyle) -> char {
    match style {
        OptionStyle::Call => 'C',
        OptionStyle::Put => 'P',
    }
}

/// Parses the `{style}` REST path segment (`call` / `put`) into the wire
/// [`OptionStyle`], rejecting anything else as a `400`.
///
/// # Errors
///
/// [`VenueError::InvalidOrder`] when the segment is not `call` or `put`.
pub(crate) fn parse_style(segment: &str) -> Result<OptionStyle, VenueError> {
    match segment {
        "call" => Ok(OptionStyle::Call),
        "put" => Ok(OptionStyle::Put),
        other => Err(VenueError::InvalidOrder(format!(
            "option style must be 'call' or 'put', got '{other}'"
        ))),
    }
}

/// Maps the wire [`DtoTif`] onto the upstream [`SeamTif`], folding the separate
/// `gtd_expires_at` field into the `Gtd(ms)` payload the upstream carries.
///
/// # Errors
///
/// [`VenueError::InvalidOrder`] when the time-in-force is `GTD` but no
/// `gtd_expires_at` instant was supplied — a `GTD` order with no expiry is
/// unrepresentable and is refused at the boundary.
pub(crate) fn seam_tif(
    tif: DtoTif,
    gtd_expires_at: Option<EventTimestamp>,
) -> Result<SeamTif, VenueError> {
    match tif {
        DtoTif::Gtc => Ok(SeamTif::Gtc),
        DtoTif::Ioc => Ok(SeamTif::Ioc),
        DtoTif::Fok => Ok(SeamTif::Fok),
        DtoTif::Gtd => match gtd_expires_at {
            Some(ts) => Ok(SeamTif::Gtd(ts.get())),
            None => Err(VenueError::InvalidOrder(
                "GTD order requires a gtd_expires_at instant".to_string(),
            )),
        },
    }
}

/// Builds the canonical [`Symbol`] `UNDERLYING-YYYYMMDD-STRIKE-STYLE` from the
/// per-contract REST path segments, routing the parse through the single
/// upstream grammar so a malformed segment is a typed `400`.
///
/// # Errors
///
/// [`VenueError::InvalidOrder`] when the assembled symbol does not parse (bad
/// underlying, non-`YYYYMMDD` expiration, zero strike, …).
pub(crate) fn build_symbol(
    underlying: &str,
    expiration: &str,
    strike: u64,
    style: OptionStyle,
) -> Result<Symbol, VenueError> {
    let raw = format!("{underlying}-{expiration}-{strike}-{}", style_char(style));
    Symbol::parse(&raw).map_err(VenueError::from)
}

/// Mints a **provisional** venue order id for a gateway-submitted order.
///
/// The canonical composite id is `{lineage}:{underlying}:{underlying_sequence}:{index}`
/// ([01 §6.1](../../../docs/01-domain-model.md)), but the assigning sequence is
/// not known until the actor commits the `AddOrder` — and the current
/// [`crate::exchange::Receipt`] does not return a re-minted id. So the gateway
/// mints an id in a **grammar-compatible, colon-delimited** shape using the run
/// lineage, the underlying, and a monotonic gateway counter (the `g`-prefixed
/// slot keeps it disjoint from the sequence-derived `execution_id` namespace).
/// This id is what is journaled in the command (so replay reproduces it) and
/// what the client uses to cancel/replace; aligning it with the assigned
/// sequence requires the order path to surface the minted id in the receipt
/// (a `matching-expert` seam extension).
#[must_use]
pub(crate) fn mint_order_id(lineage: &LineageId, underlying: &str) -> VenueOrderId {
    let n = ORDER_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    VenueOrderId::new(format!("{}:{}:g{n}:0", lineage.as_str(), underlying))
}

/// Resolves the STP / mass-cancel **owner** [`Hash32`](crate::exchange::Hash32)
/// the matching engine keys on, from the authenticated account.
///
/// # Errors
///
/// [`VenueError::Unauthorized`] if the admitted account cannot be resolved in
/// the registry (unreachable in practice — admission already required the
/// account to be known — but never fabricates an owner).
pub(crate) fn owner_for(
    state: &Arc<AppState>,
    account: &AccountId,
) -> Result<crate::exchange::Hash32, VenueError> {
    use crate::auth::AccountStore as _;
    state
        .accounts()
        .account(account)
        .map(|a| a.owner)
        .ok_or(VenueError::Unauthorized)
}

/// The immediate fills of a just-submitted aggressing order: the **taker** legs
/// recorded on the shared executions store at this command's sequence, keyed by
/// the order's venue id.
///
/// The actor fans committed fills into the shared store **synchronously** inside
/// its turn, before the receipt is returned ([02 §6](../../../docs/02-matching-architecture.md)),
/// so a read here after `submit().await` observes exactly this order's fills.
/// Returns each leg's `(price, quantity)` in journal order.
#[must_use]
pub(crate) fn immediate_fills(
    state: &Arc<AppState>,
    account: &AccountId,
    order_id: &VenueOrderId,
    sequence: SequenceNumber,
) -> Vec<(Cents, u64)> {
    let records = match state
        .executions()
        .list(account, &ExecutionFilter::default())
    {
        Ok(records) => records,
        // The in-memory store never errors; a defensive empty keeps the handler
        // total without an `unwrap`.
        Err(_) => return Vec::new(),
    };
    records
        .into_iter()
        .filter(|record| {
            record.order_id == *order_id
                && record.underlying_sequence == sequence
                && record.liquidity == LiquidityFlag::Taker
        })
        .map(|record| (record.price_cents, record.quantity))
        .collect()
}

/// The volume-weighted average price over a set of `(price, quantity)` fill
/// legs, in **integer cents**, or `None` when there were no fills. Realized
/// money — kept as `Cents` on the wire, never a float (the review-fixed contract).
///
/// The volume-weighted average `Σ(pᵢ·qᵢ) / Σqᵢ` is computed in `u128`
/// (`Notional`) space and **truncated toward zero** to whole cents — the
/// documented wire rounding rule, consistent with `Position.avg_price`. The
/// quotient is bounded by the largest leg price (a `u64` cents value), so the
/// `try_from` never actually rejects for real fills.
#[must_use]
pub(crate) fn vwap_cents(fills: &[(Cents, u64)]) -> Option<Cents> {
    let total_qty: u128 = fills.iter().map(|(_, q)| u128::from(*q)).sum();
    if total_qty == 0 {
        return None;
    }
    let notional: u128 = fills
        .iter()
        .map(|(p, q)| u128::from(p.get()) * u128::from(*q))
        .sum();
    u64::try_from(notional / total_qty).ok().map(Cents::new)
}

/// Formats a Unix-epoch **seconds** instant as an RFC3339 / ISO-8601 UTC
/// timestamp (`YYYY-MM-DDTHH:MM:SSZ`) — used for the token `expires_at`, a
/// credential-plane (wall-clock) value.
///
/// Hand-rolled (via Howard Hinnant's `civil_from_days`) to avoid a date-library
/// dependency for a single format; correct for all UTC instants.
#[must_use]
pub(crate) fn format_rfc3339_utc(secs: u64) -> String {
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

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_rfc3339_utc_epoch_and_known_instant() {
        assert_eq!(format_rfc3339_utc(0), "1970-01-01T00:00:00Z");
        // 2021-01-01T00:00:00Z is 1_609_459_200 seconds since the epoch.
        assert_eq!(format_rfc3339_utc(1_609_459_200), "2021-01-01T00:00:00Z");
    }

    #[test]
    fn test_seam_side_maps_both_variants() {
        assert_eq!(seam_side(DtoSide::Buy), SeamSide::Buy);
        assert_eq!(seam_side(DtoSide::Sell), SeamSide::Sell);
    }

    #[test]
    fn test_parse_style_accepts_call_and_put_rejects_other() {
        assert_eq!(parse_style("call").ok(), Some(OptionStyle::Call));
        assert_eq!(parse_style("put").ok(), Some(OptionStyle::Put));
        assert!(parse_style("straddle").is_err());
    }

    #[test]
    fn test_seam_tif_gtd_requires_expiry() {
        assert!(matches!(seam_tif(DtoTif::Gtc, None), Ok(SeamTif::Gtc)));
        assert!(seam_tif(DtoTif::Gtd, None).is_err());
        assert!(matches!(
            seam_tif(DtoTif::Gtd, Some(EventTimestamp::new(42))),
            Ok(SeamTif::Gtd(42))
        ));
    }

    #[test]
    fn test_build_symbol_round_trips_canonical_grammar() {
        let symbol = build_symbol("BTC", "20240329", 50_000, OptionStyle::Call)
            .expect("canonical symbol must parse");
        assert_eq!(symbol.as_str(), "BTC-20240329-50000-C");
    }

    #[test]
    fn test_build_symbol_rejects_bad_expiration() {
        assert!(build_symbol("BTC", "not-a-date", 50_000, OptionStyle::Put).is_err());
    }

    #[test]
    fn test_mint_order_id_is_grammar_compatible_and_unique() {
        let lineage = LineageId::new("run-1");
        let a = mint_order_id(&lineage, "BTC");
        let b = mint_order_id(&lineage, "BTC");
        assert_ne!(a, b, "the gateway counter disambiguates minted ids");
        assert!(a.as_str().starts_with("run-1:BTC:g"));
        assert!(a.as_str().ends_with(":0"));
    }

    #[test]
    fn test_vwap_cents_is_none_without_fills() {
        assert_eq!(vwap_cents(&[]), None);
    }

    #[test]
    fn test_vwap_cents_weights_by_quantity() {
        // (100c × 1) + (200c × 3) = 700 over 4 = 175, exact — integer cents.
        let vwap = vwap_cents(&[(Cents::new(100), 1), (Cents::new(200), 3)]);
        assert_eq!(vwap, Some(Cents::new(175)));
    }

    #[test]
    fn test_vwap_cents_truncates_toward_zero() {
        // (100c × 1) + (101c × 1) = 201 over 2 = 100.5 → truncated to 100 cents
        // (the documented wire rounding rule; never a fractional-cent float).
        let vwap = vwap_cents(&[(Cents::new(100), 1), (Cents::new(101), 1)]);
        assert_eq!(vwap, Some(Cents::new(100)));
    }
}
