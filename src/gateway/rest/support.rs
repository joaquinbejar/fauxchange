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
    Cents, EventTimestamp, ExecutionFilter, ExecutionsStore, Hash32, LineageId, STPMode,
    SequenceNumber, Side as SeamSide, Symbol, TimeInForce as SeamTif, VenueCommand,
};
use crate::models::{
    AccountId, ClientOrderId, ExecutionRecord, LiquidityFlag, OptionStyle, OrderType,
    Side as DtoSide, TimeInForce as DtoTif, VenueOrderId,
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

/// Builds the `VenueCommand::AddOrder` a placement submits — the **one shared
/// order-command construction** both the REST order-entry handlers and the FIX
/// `NewOrderSingle (D)` translation call, so an order arriving on either surface
/// derives the byte-identical command (parity by construction,
/// [03 §7](../../../docs/03-protocol-surfaces.md#7-protocol-parity-guarantees)).
///
/// The caller resolves the surface-specific inputs to the matching-seam newtypes
/// first — [`seam_side`] / [`seam_tif`] for the wire enums, [`owner_for`] for the
/// STP owner, [`mint_order_id`] for the provisional venue order id — and this
/// stamps the fixed `stp_mode: None` (per-account STP is venue config, not carried
/// on the order in either dialect).
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn add_order_command(
    symbol: Symbol,
    order_id: VenueOrderId,
    account: AccountId,
    owner: Hash32,
    client_order_id: Option<ClientOrderId>,
    side: SeamSide,
    order_type: OrderType,
    limit_price: Option<Cents>,
    quantity: u64,
    time_in_force: SeamTif,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol,
        order_id,
        account,
        owner,
        client_order_id,
        side,
        order_type,
        limit_price,
        quantity,
        time_in_force,
        stp_mode: STPMode::None,
    }
}

/// The **taker** [`ExecutionRecord`] legs of a just-submitted aggressing order:
/// the fills recorded on the shared executions store at this command's sequence,
/// keyed by the order's venue id.
///
/// The actor fans committed fills into the shared store **synchronously** inside
/// its turn, before the receipt is returned ([02 §6](../../../docs/02-matching-architecture.md)),
/// so a read here after `submit().await` observes exactly this order's fills. The
/// full record (execution id, price, quantity, and signed per-leg fee) is what
/// the FIX `ExecutionReport (8)` rendering needs; the REST handler projects only
/// `(price, quantity)` via [`immediate_fills`]. Returned in journal order.
#[must_use]
pub(crate) fn immediate_execution_records(
    state: &Arc<AppState>,
    account: &AccountId,
    order_id: &VenueOrderId,
    sequence: SequenceNumber,
) -> Vec<ExecutionRecord> {
    let records = match state
        .executions()
        .list(account, &ExecutionFilter::default())
    {
        Ok(records) => records,
        // The in-memory store never errors; a defensive empty keeps the caller
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
        .collect()
}

/// Every **taker** fill leg the account has on `order_id`, across all sequences —
/// the `OrderStatusRequest (H)` read (a status folds an order's whole fill
/// history, not one command's sequence). Returned in journal order.
#[must_use]
pub(crate) fn taker_legs_for_order(
    state: &Arc<AppState>,
    account: &AccountId,
    order_id: &VenueOrderId,
) -> Vec<ExecutionRecord> {
    let records = match state
        .executions()
        .list(account, &ExecutionFilter::default())
    {
        Ok(records) => records,
        Err(_) => return Vec::new(),
    };
    records
        .into_iter()
        .filter(|record| record.order_id == *order_id && record.liquidity == LiquidityFlag::Taker)
        .collect()
}

/// The volume-weighted average price over a set of `(price, quantity)` fill
/// legs, in **integer cents** — `Ok(None)` when there were no fills. Realized
/// money — kept as `Cents` on the wire, never a float (the review-fixed contract).
///
/// The volume-weighted average `Σ(pᵢ·qᵢ) / Σqᵢ` is computed in `u128`
/// (`Notional`) space and **truncated toward zero** to whole cents — the
/// documented wire rounding rule, consistent with `Position.avg_price`.
///
/// # Errors
///
/// Every step is **checked** (`rules/global_rules.md` — no `saturating_*`/
/// `wrapping_*`, and `Iterator::sum` panics-in-debug / wraps-in-release on
/// overflow). Each `pᵢ·qᵢ` is a `u64 × u64` product that always fits `u128`; the
/// running sums could only overflow past ~2^64 legs (unreachable for a real
/// order). Any such overflow returns [`VenueError::Overflow`] (a redacted `500`)
/// rather than panicking or wrapping to a wrong wire price.
pub(crate) fn vwap_cents(fills: &[(Cents, u64)]) -> Result<Option<Cents>, VenueError> {
    let mut total_qty: u128 = 0;
    let mut notional: u128 = 0;
    for (price, qty) in fills {
        let q = u128::from(*qty);
        let leg = u128::from(price.get())
            .checked_mul(q)
            .ok_or(VenueError::Overflow)?;
        total_qty = total_qty.checked_add(q).ok_or(VenueError::Overflow)?;
        notional = notional.checked_add(leg).ok_or(VenueError::Overflow)?;
    }
    if total_qty == 0 {
        return Ok(None);
    }
    let vwap = u64::try_from(notional / total_qty).map_err(|_| VenueError::Overflow)?;
    Ok(Some(Cents::new(vwap)))
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
    fn test_build_symbol_rejects_relative_day_count_expiration() {
        // #032 DTO boundary: the expiration path segment is an absolute `YYYYMMDD`
        // date, never a wall-clock-relative day count. A bare `30` cannot become an
        // `ExpirationDate::Days` here — the symbol grammar refuses it as a typed
        // `VenueError::InvalidOrder` (HTTP 400), so a relative expiry never reaches
        // the sequenced path over REST/WS.
        match build_symbol("BTC", "30", 50_000, OptionStyle::Call) {
            Err(VenueError::InvalidOrder(_)) => {}
            other => panic!("expected InvalidOrder for a relative day-count expiry, got {other:?}"),
        }
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
        assert_eq!(vwap_cents(&[]).expect("no overflow"), None);
    }

    #[test]
    fn test_vwap_cents_weights_by_quantity() {
        // (100c × 1) + (200c × 3) = 700 over 4 = 175, exact — integer cents.
        let vwap = vwap_cents(&[(Cents::new(100), 1), (Cents::new(200), 3)]).expect("no overflow");
        assert_eq!(vwap, Some(Cents::new(175)));
    }

    #[test]
    fn test_vwap_cents_truncates_toward_zero() {
        // (100c × 1) + (101c × 1) = 201 over 2 = 100.5 → truncated to 100 cents
        // (the documented wire rounding rule; never a fractional-cent float).
        let vwap = vwap_cents(&[(Cents::new(100), 1), (Cents::new(101), 1)]).expect("no overflow");
        assert_eq!(vwap, Some(Cents::new(100)));
    }
}
