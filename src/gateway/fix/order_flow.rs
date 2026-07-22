//! FIX order-flow translation and `ExecutionReport (8)` rendering — the pure seam
//! between the typed order-entry messages (#036) and the sequenced order path
//! (#007), keyed to the context-sensitive reject matrix (#003).
//!
//! ## Parity by construction (REST ≡ FIX order entry)
//!
//! [`to_add_command`] turns a `NewOrderSingle (D)` into the **identical**
//! [`VenueCommand::AddOrder`] a REST `POST .../orders` produces: it resolves the
//! wire enums to the matching-seam newtypes and calls the **one shared**
//! [`add_order_command`](crate::gateway::rest::support::add_order_command) builder,
//! passing the same `client_order_id` (the account-scoped idempotency key). So an
//! order over FIX and the same order over REST submit the same command, run the
//! same single-writer actor, and produce identical fills / resting state /
//! `underlying_sequence` ([03 §7](../../../docs/03-protocol-surfaces.md#7-protocol-parity-guarantees)).
//! The gateway **translates**; the exchange **decides** — nothing here matches,
//! prices, or sequences.
//!
//! ## Reports render the observed outcome, never invented state
//!
//! The accept/fill [`ExecReportSpec`] stream is emitted **only** for a committed,
//! *accepted* placement: the session first inspects the sequenced
//! [`Receipt`](crate::exchange::Receipt)'s observed
//! [`VenueOutcome`](crate::exchange::VenueOutcome), and a journaled
//! [`VenueOutcome::Rejected`](crate::exchange::VenueOutcome::Rejected) — a place into
//! a halted / `Settling` / `Expired` instrument, or a cancel/replace the order path
//! refused — renders `ExecutionReport (8) Rejected` (`D`) or `OrderCancelReject (9)`
//! (`F`/`G`) instead of a false accept, exactly as the REST handler renders the same
//! command's observed reject (REST ≡ FIX order entry, #118). When the outcome *is* an
//! accept, the stream is derived from the **committed** taker fill legs read back from
//! the shared executions store (the same read REST renders from), plus the resolved
//! time-in-force: `New` on accept, a `Trade` per fill leg with the running
//! `CumQty`/`LeavesQty` and the per-leg `Commission`, and a terminal `Canceled` for a
//! killed `IOC`/`FOK`/market remainder ([03 §5.3](../../../docs/03-protocol-surfaces.md#53-order-entry-and-execution-reports),
//! [fix-dialect §2.2](../../../docs/specs/fix-dialect.md#22-order-entry-and-execution)).
//! Money is integer [`Cents`] internally; the decimal `Price` seam lives only at
//! the wire edge (#036).
//!
//! ## Reason codes live in one place
//!
//! The reject **message** is chosen by the inbound message context in
//! [`src/error.rs`](crate::error) ([`VenueError::fix_reject`](crate::error::VenueError::fix_reject));
//! the **numeric** reason code that message carries
//! ([`OrdRejReason (103)`](ord_rej_reason) / [`CxlRejReason (102)`](cxl_rej_reason))
//! is rendered here against the pinned dialect — the single exhaustive boundary,
//! never ad-hoc in the session handler.

use crate::error::FixRejectReason;
use crate::error::VenueError;
use crate::exchange::{
    Cents, Hash32, LineageId, SequenceNumber, Side as SeamSide, Symbol, TimeInForce as SeamTif,
    VenueCommand,
};
use crate::gateway::rest::support::add_order_command;
use crate::models::{
    AccountId, ExecutionId, ExecutionRecord, LiquidityFlag, OrderType, VenueOrderId,
};

use super::enums::{
    CommType, ExecType, LastLiquidityInd, OrdStatus, OrdType, OrderSide, TimeInForce as FixTif,
};
use super::execution::ExecutionReport;
use super::header::{StandardHeader, UtcTimestamp};
use super::order::{NewOrderSingle, OrderCancelReplaceRequest, OrderCancelRequest};

/// The `OrdRejReason (103)` the venue emits when a `NewOrderSingle (D)` reuses a
/// `ClOrdID` the session already placed with **different** economics — `6`
/// (Duplicate Order), the standard code for a conflicting idempotency-key reuse
/// ([fix-dialect §4](../../../docs/specs/fix-dialect.md#4-identifiers-correlation-and-idempotency)).
pub const ORD_REJ_REASON_DUPLICATE: u16 = 6;

/// A short, non-secret `Text (58)` for a conflicting-`ClOrdID` reject — names the
/// policy (a reused key), never a credential or internal state.
pub const DUPLICATE_CLORDID_TEXT: &str = "client_order_id reused with a different order";

/// The **uniform**, client-safe `Text (58)` an `OrderCancelReject (9)` carries when the
/// referenced order cannot be cancelled/replaced — whether it was never placed this
/// session, is owned by another account, or is already gone. It deliberately collapses
/// all three so the reject is never a cross-account existence/ownership enumeration
/// oracle (BOLA/IDOR); the specific journaled reason (`order not found`, the
/// not-owner reason, or `order is not resting`) stays **internal** (journal + tracing),
/// never on the wire (#118). Both the never-placed reject and the observed
/// [`VenueOutcome::Rejected`](crate::exchange::VenueOutcome::Rejected) reject render
/// with this one text + `CxlRejReason (102) = 1` (Unknown order), so they are
/// indistinguishable to a client.
pub const CANCEL_REJECT_MASKED_REASON: &str = "unknown order";

/// The `BusinessRejectReason (380)` the venue emits for an unsupported application
/// `MsgType` — `3` (Unsupported Message Type), the only `BusinessMessageReject (j)`
/// path in v0.4 (an application type the venue has no handler for). The order
/// messages route to their own order-level rejects (`8`/`9`), never to `j`.
pub const BUSINESS_REJECT_UNSUPPORTED_MSG_TYPE: u16 = 3;

// ============================================================================
// Wire enum → matching-seam newtype (the same seam values REST resolves to)
// ============================================================================

/// Maps the FIX `Side (54)` onto the matching-seam [`SeamSide`] — the same value
/// the REST `seam_side` produces, so a `Buy`/`Sell` order is byte-identical across
/// surfaces.
#[must_use]
#[inline]
pub(crate) fn seam_side(side: OrderSide) -> SeamSide {
    match side {
        OrderSide::Buy => SeamSide::Buy,
        OrderSide::Sell => SeamSide::Sell,
    }
}

/// Maps a matching-seam [`SeamSide`] back onto the FIX `Side (54)` wire enum — the
/// inverse of [`seam_side`], for rendering a resting order's own side (carried on a
/// [`CancelledLeg`](crate::exchange::CancelledLeg)) into an `ExecutionReport (8)`.
#[must_use]
#[inline]
pub(crate) fn fix_side(side: SeamSide) -> OrderSide {
    match side {
        SeamSide::Buy => OrderSide::Buy,
        SeamSide::Sell => OrderSide::Sell,
    }
}

/// Maps the FIX `LastLiquidityInd` source (a fill leg's [`LiquidityFlag`]) onto the
/// wire enum.
#[must_use]
#[inline]
fn liquidity_ind(flag: LiquidityFlag) -> LastLiquidityInd {
    match flag {
        LiquidityFlag::Maker => LastLiquidityInd::Maker,
        LiquidityFlag::Taker => LastLiquidityInd::Taker,
    }
}

/// Maps the FIX `TimeInForce (59)` onto the matching-seam [`SeamTif`], folding a
/// `GTD` order's `ExpireTime (126)` into the `Gtd(ms)` payload.
///
/// For the four time-in-forces REST can also express (`GTC`/`IOC`/`FOK`/`GTD`)
/// this produces the **identical** seam value REST's `seam_tif` produces, so an
/// order over either surface derives the same command. `Day` is FIX-only (REST has
/// no `Day` DTO), mapped straight to [`SeamTif::Day`].
///
/// # Errors
///
/// [`VenueError::InvalidOrder`] when a `GTD` order carries no `ExpireTime` (the
/// decode layer already enforces its presence, so this is defensive) or when the
/// `ExpireTime` is before the Unix epoch (unrepresentable as a `u64` ms instant).
pub(crate) fn seam_time_in_force(
    tif: FixTif,
    expire_time: Option<&UtcTimestamp>,
) -> Result<SeamTif, VenueError> {
    match tif {
        FixTif::Day => Ok(SeamTif::Day),
        FixTif::Gtc => Ok(SeamTif::Gtc),
        FixTif::Ioc => Ok(SeamTif::Ioc),
        FixTif::Fok => Ok(SeamTif::Fok),
        FixTif::Gtd => {
            let expire = expire_time.ok_or_else(|| {
                VenueError::InvalidOrder("GTD order requires ExpireTime (126)".to_string())
            })?;
            let ms = expire.to_epoch_ms().ok_or_else(|| {
                VenueError::InvalidOrder(
                    "GTD ExpireTime (126) is before the Unix epoch".to_string(),
                )
            })?;
            Ok(SeamTif::Gtd(ms))
        }
    }
}

/// Whether a resolved time-in-force **rests** its unfilled remainder (`GTC` /
/// `GTD` / `Day`) rather than cancelling it (`IOC` / `FOK`) — the discriminator
/// between a `New` (resting) terminal report and a `Canceled` (killed) one.
#[must_use]
#[inline]
pub(crate) fn tif_rests(tif: SeamTif) -> bool {
    matches!(tif, SeamTif::Gtc | SeamTif::Gtd(_) | SeamTif::Day)
}

// ============================================================================
// D / F / G → VenueCommand
// ============================================================================

/// Translates a `NewOrderSingle (D)` into the [`VenueCommand::AddOrder`] a REST
/// `POST .../orders` produces (parity by construction).
///
/// A market order (`OrdType=1`) resolves to the true non-resting primitive
/// (`limit_price: None`, `time_in_force: Ioc`) exactly as REST's market handler
/// does; a limit order (`OrdType=2`) carries its `Price (44)` cents and its
/// `TimeInForce (59)`. `client_order_id` is the `ClOrdID (11)` — the one
/// account-scoped idempotency key shared with REST.
///
/// # Errors
///
/// [`VenueError::InvalidOrder`] from [`seam_time_in_force`] (a `GTD` order whose
/// `ExpireTime` cannot be resolved).
///
/// Public so the REST≡FIX order-entry parity test can assert, for the same
/// logical order, that this and the REST
/// [`add_order_command`](crate::gateway::rest::support::add_order_command) derive
/// the byte-identical [`VenueCommand`].
pub fn to_add_command(
    order: &NewOrderSingle,
    order_id: VenueOrderId,
    account: AccountId,
    owner: Hash32,
) -> Result<VenueCommand, VenueError> {
    let (order_type, limit_price, time_in_force) = match order.ord_type {
        // Market is the non-resting primitive: no price, IOC — identical to REST's
        // `place_market_order`, so the journaled command matches across surfaces.
        OrdType::Market => (OrderType::Market, None, SeamTif::Ioc),
        OrdType::Limit => (
            OrderType::Limit,
            order.price,
            seam_time_in_force(order.time_in_force, order.expire_time.as_ref())?,
        ),
    };
    Ok(add_order_command(
        order.symbol.clone(),
        order_id,
        account,
        owner,
        Some(order.cl_ord_id.clone()),
        seam_side(order.side),
        order_type,
        limit_price,
        order.order_qty,
        time_in_force,
    ))
}

/// Translates an `OrderCancelRequest (F)` into a [`VenueCommand::CancelOrder`],
/// given the resolved venue order id of the resting order (from the session's
/// `(ClOrdID → order_id)` correlation of the order it placed).
#[must_use]
pub(crate) fn to_cancel_command(
    cancel: &OrderCancelRequest,
    account: AccountId,
    target: VenueOrderId,
) -> VenueCommand {
    VenueCommand::CancelOrder {
        symbol: cancel.symbol.clone(),
        order_id: target,
        account,
    }
}

/// Translates an `OrderCancelReplaceRequest (G)` into a [`VenueCommand::Replace`]
/// — non-atomic cancel-then-add ([ADR-0006](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
///
/// `G` carries no `TimeInForce`, so the replacement rests as `GTC` (the dialect
/// default). A market replacement (`OrdType=1`, no price) yields `limit_price:
/// None`, which the executor's add leg rejects — a market order does not rest.
#[must_use]
pub(crate) fn to_replace_command(
    replace: &OrderCancelReplaceRequest,
    account: AccountId,
    target: VenueOrderId,
    new_order_id: VenueOrderId,
) -> VenueCommand {
    let limit_price = match replace.ord_type {
        OrdType::Limit => replace.price,
        OrdType::Market => None,
    };
    VenueCommand::Replace {
        symbol: replace.symbol.clone(),
        order_id: target,
        new_order_id,
        account,
        side: seam_side(replace.side),
        limit_price,
        quantity: replace.order_qty,
        time_in_force: SeamTif::Gtc,
        stp_mode: crate::exchange::STPMode::None,
    }
}

// ============================================================================
// Context-sensitive reason codes (the single exhaustive boundary)
// ============================================================================

/// The `OrdRejReason (103)` a `NewOrderSingle (D)` reject carries, from the
/// context-free [`FixRejectReason`] the error seam produced.
///
/// `Authorization` reuses the session permission gate's code (`6`) for a coherent
/// venue vocabulary; `Throttle`/`Internal` collapse to `99` (Other) with the
/// (redacted) detail in `Text (58)`, since FIX 4.4 has no dedicated throttle
/// `OrdRejReason`.
#[must_use]
pub(crate) fn ord_rej_reason(reason: FixRejectReason) -> u16 {
    match reason {
        FixRejectReason::Invalid => 11, // Unsupported order characteristic
        FixRejectReason::NotFound => 5, // Unknown order
        FixRejectReason::Authorization => 6, // matches the session permission gate
        FixRejectReason::Throttle => 99, // Other (no standard throttle code)
        FixRejectReason::Internal => 99, // Other (cause redacted)
    }
}

/// The `CxlRejReason (102)` an `OrderCancelReject (9)` carries, from the
/// context-free [`FixRejectReason`].
///
/// A referenced order that does not exist is `1` (Unknown order); every other
/// failure is `2` (Broker/Exchange Option), the standard catch-all, with the
/// detail in the redacted `Text (58)`.
#[must_use]
pub(crate) fn cxl_rej_reason(reason: FixRejectReason) -> u16 {
    match reason {
        FixRejectReason::NotFound => 1, // Unknown order
        FixRejectReason::Invalid
        | FixRejectReason::Authorization
        | FixRejectReason::Throttle
        | FixRejectReason::Internal => 2, // Broker/Exchange Option
    }
}

// ============================================================================
// ExecutionReport (8) rendering
// ============================================================================

/// The header-free fields of an outbound `ExecutionReport (8)` — the session layer
/// supplies the standard header (with the next checked sender `MsgSeqNum`) at emit
/// time via [`Self::into_report`], so a report is sequenced and resend-persisted
/// exactly like every other venue-originated frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecReportSpec {
    /// `OrderID (37)` — the venue order id.
    pub order_id: VenueOrderId,
    /// `ExecID (17)` — the composite execution id (a fill's, or a synthesized one
    /// for a `New`/terminal report).
    pub exec_id: ExecutionId,
    /// `ExecType (150)`.
    pub exec_type: ExecType,
    /// `OrdStatus (39)`.
    pub ord_status: OrdStatus,
    /// `Symbol (55)`.
    pub symbol: Symbol,
    /// `Side (54)`.
    pub side: OrderSide,
    /// `LeavesQty (151)`.
    pub leaves_qty: u64,
    /// `CumQty (14)`.
    pub cum_qty: u64,
    /// `LastQty (32)`.
    pub last_qty: Option<u64>,
    /// `LastPx (31)` — cents.
    pub last_px: Option<Cents>,
    /// `Price (44)` — the order's limit price, cents.
    pub price: Option<Cents>,
    /// `SecondaryExecID (527)` — the `underlying_sequence` join key.
    pub secondary_exec_id: SequenceNumber,
    /// `Commission (12)` — the signed per-leg fee.
    pub commission: Option<crate::exchange::SignedCents>,
    /// `CommType (13)`.
    pub comm_type: Option<CommType>,
    /// `LastLiquidityInd (851)`.
    pub last_liquidity_ind: Option<LastLiquidityInd>,
    /// `OrdRejReason (103)`.
    pub ord_rej_reason: Option<u16>,
    /// `Text (58)` — redacted.
    pub text: Option<String>,
}

impl ExecReportSpec {
    /// Assembles the full [`ExecutionReport`] by stamping the venue-supplied
    /// standard header onto the spec.
    #[must_use]
    pub(crate) fn into_report(self, header: StandardHeader) -> ExecutionReport {
        ExecutionReport {
            header,
            order_id: self.order_id,
            exec_id: self.exec_id,
            exec_type: self.exec_type,
            ord_status: self.ord_status,
            symbol: self.symbol,
            side: self.side,
            leaves_qty: self.leaves_qty,
            cum_qty: self.cum_qty,
            last_qty: self.last_qty,
            last_px: self.last_px,
            price: self.price,
            secondary_exec_id: self.secondary_exec_id,
            commission: self.commission,
            comm_type: self.comm_type,
            last_liquidity_ind: self.last_liquidity_ind,
            ord_rej_reason: self.ord_rej_reason,
            text: self.text,
        }
    }
}

/// The committed report stream for an accepted placement (`D`) or replacement
/// (`G`): the accept report, a `Trade` per committed fill leg, and a terminal
/// `Canceled` for a killed remainder.
///
/// `accept_exec_type` / `accept_ord_status` are `New`/`New` for a `D` and
/// `Replaced`/`Replaced` for a `G`. `taker_legs` are the committed taker fills
/// read back from the shared executions store at this command's sequence; the
/// per-leg `Commission (12)` is `taker_leg.fee_cents`, `LastLiquidityInd (851)` is
/// its liquidity flag. A `New`/terminal report's `ExecID (17)` is synthesized from
/// the composite id grammar at an index above every fill index, so no report id
/// collides with a fill's `execution_id`.
// governance O-4 forbids `saturating_sub`; the `checked_sub(..).unwrap_or(0)`
// below is the same value under the `cum <= quantity` fill invariant, so the
// clippy manual-saturating suggestion is intentionally overridden.
#[allow(clippy::too_many_arguments, clippy::manual_saturating_arithmetic)]
fn render_placement_reports(
    accept_exec_type: ExecType,
    accept_ord_status: OrdStatus,
    symbol: &Symbol,
    side: OrderSide,
    quantity: u64,
    price: Option<Cents>,
    order_id: &VenueOrderId,
    sequence: SequenceNumber,
    lineage: &LineageId,
    underlying: &str,
    tif: SeamTif,
    taker_legs: &[ExecutionRecord],
) -> Vec<ExecReportSpec> {
    // Synthetic `ExecID` indices for the non-fill reports, above every fill index
    // (fills use indices `0..leg_count`), so no id collides. These are
    // id-disambiguation indices bounded by the (tiny) fill count, not a sequence
    // / money value; `checked_add` keeps the crate free of `saturating_*` (O-4).
    let leg_count = taker_legs.len();
    let accept_index = u32::try_from(leg_count).unwrap_or(u32::MAX);
    let terminal_index =
        u32::try_from(leg_count.checked_add(1).unwrap_or(leg_count)).unwrap_or(u32::MAX);

    let mut specs: Vec<ExecReportSpec> =
        Vec::with_capacity(leg_count.checked_add(2).unwrap_or(leg_count));

    // 1) Accept on acknowledgement (New for D, Replaced for G).
    specs.push(ExecReportSpec {
        order_id: order_id.clone(),
        exec_id: lineage.execution_id(underlying, sequence, accept_index),
        exec_type: accept_exec_type,
        ord_status: accept_ord_status,
        symbol: symbol.clone(),
        side,
        leaves_qty: quantity,
        cum_qty: 0,
        last_qty: None,
        last_px: None,
        price,
        secondary_exec_id: sequence,
        commission: None,
        comm_type: None,
        last_liquidity_ind: None,
        ord_rej_reason: None,
        text: None,
    });

    // 2) One Trade report per committed fill leg, with the running CumQty/LeavesQty.
    let mut cum: u64 = 0;
    for leg in taker_legs {
        // A taker can never fill more than it submitted, so the checked add never
        // overflows; the `unwrap_or(cum)` floor keeps the crate free of `saturating_*`.
        cum = cum.checked_add(leg.quantity).unwrap_or(cum);
        let ord_status = if cum >= quantity {
            OrdStatus::Filled
        } else {
            OrdStatus::PartiallyFilled
        };
        let leaves = quantity.checked_sub(cum).unwrap_or(0);
        specs.push(ExecReportSpec {
            order_id: order_id.clone(),
            exec_id: leg.execution_id.clone(),
            exec_type: ExecType::Trade,
            ord_status,
            symbol: symbol.clone(),
            side,
            leaves_qty: leaves,
            cum_qty: cum,
            last_qty: Some(leg.quantity),
            last_px: Some(leg.price_cents),
            price,
            secondary_exec_id: sequence,
            commission: Some(leg.fee_cents),
            comm_type: Some(CommType::Absolute),
            last_liquidity_ind: Some(liquidity_ind(leg.liquidity)),
            ord_rej_reason: None,
            text: None,
        });
    }

    // 3) Terminal Canceled for a killed remainder (IOC/FOK/market did not rest).
    let filled = cum;
    if filled < quantity && !tif_rests(tif) {
        specs.push(ExecReportSpec {
            order_id: order_id.clone(),
            exec_id: lineage.execution_id(underlying, sequence, terminal_index),
            exec_type: ExecType::Canceled,
            ord_status: OrdStatus::Canceled,
            symbol: symbol.clone(),
            side,
            leaves_qty: 0,
            cum_qty: filled,
            last_qty: None,
            last_px: None,
            price,
            secondary_exec_id: sequence,
            commission: None,
            comm_type: None,
            last_liquidity_ind: None,
            ord_rej_reason: None,
            text: None,
        });
    }

    specs
}

/// The committed `ExecutionReport (8)` stream for an accepted `NewOrderSingle (D)`.
#[must_use]
pub(crate) fn render_new_order_reports(
    order: &NewOrderSingle,
    order_id: &VenueOrderId,
    sequence: SequenceNumber,
    lineage: &LineageId,
    underlying: &str,
    tif: SeamTif,
    taker_legs: &[ExecutionRecord],
) -> Vec<ExecReportSpec> {
    render_placement_reports(
        ExecType::New,
        OrdStatus::New,
        &order.symbol,
        order.side,
        order.order_qty,
        order.price,
        order_id,
        sequence,
        lineage,
        underlying,
        tif,
        taker_legs,
    )
}

/// The committed `ExecutionReport (8)` stream for an accepted
/// `OrderCancelReplaceRequest (G)` — a `Replaced` accept then the add leg's fills.
#[must_use]
pub(crate) fn render_replace_reports(
    replace: &OrderCancelReplaceRequest,
    new_order_id: &VenueOrderId,
    sequence: SequenceNumber,
    lineage: &LineageId,
    underlying: &str,
    taker_legs: &[ExecutionRecord],
) -> Vec<ExecReportSpec> {
    // A `G` carries no TIF; the replacement rests as GTC (see `to_replace_command`).
    render_placement_reports(
        ExecType::Replaced,
        OrdStatus::Replaced,
        &replace.symbol,
        replace.side,
        replace.order_qty,
        match replace.ord_type {
            OrdType::Limit => replace.price,
            OrdType::Market => None,
        },
        new_order_id,
        sequence,
        lineage,
        underlying,
        SeamTif::Gtc,
        taker_legs,
    )
}

/// The `ExecutionReport (8)` `Canceled` for an accepted `OrderCancelRequest (F)`.
///
/// Rendered **only** when the sequenced [`Receipt`](crate::exchange::Receipt)'s observed
/// outcome is [`VenueOutcome::Cancelled`](crate::exchange::VenueOutcome::Cancelled); a
/// cancel the order path refused (unknown / unowned / already-gone) is a journaled
/// [`VenueOutcome::Rejected`](crate::exchange::VenueOutcome::Rejected) rendered as a
/// masked `OrderCancelReject (9)` instead (#118), never this `Canceled`. The pre-cancel
/// fill count is not surfaced by the receipt, so `CumQty` is `0` and `LeavesQty` is `0`
/// (the order is gone) — the same honest limitation the REST cancel handler documents.
#[must_use]
pub(crate) fn render_cancel_report(
    symbol: Symbol,
    side: OrderSide,
    order_id: VenueOrderId,
    sequence: SequenceNumber,
    lineage: &LineageId,
    underlying: &str,
) -> ExecReportSpec {
    ExecReportSpec {
        order_id,
        exec_id: lineage.execution_id(underlying, sequence, 0),
        exec_type: ExecType::Canceled,
        ord_status: OrdStatus::Canceled,
        symbol,
        side,
        leaves_qty: 0,
        cum_qty: 0,
        last_qty: None,
        last_px: None,
        price: None,
        secondary_exec_id: sequence,
        commission: None,
        comm_type: None,
        last_liquidity_ind: None,
        ord_rej_reason: None,
        text: None,
    }
}

/// One `ExecutionReport (8) Canceled` for a resting order swept by an accepted
/// `OrderMassCancelRequest (q)` ([03 §5.3](../../../docs/03-protocol-surfaces.md#53-order-entry-and-execution-reports)).
///
/// The same shape as [`render_cancel_report`] — `CumQty`/`LeavesQty` are `0` (the
/// swept order is gone and the receipt does not surface the pre-cancel fill count,
/// the same honest limitation the single-cancel path documents) — but it takes the
/// leg's `index` so each order in one sweep gets a **collision-free** composite
/// `ExecID` under the shared mass-cancel `underlying_sequence`
/// (`"{lineage}:{underlying}:{sequence}:{index}"`). `SecondaryExecID (527)` is that
/// same `underlying_sequence`, so a client can join every per-order report to the
/// one sequenced sweep turn.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub(crate) fn render_mass_cancel_leg_report(
    symbol: Symbol,
    side: OrderSide,
    order_id: VenueOrderId,
    sequence: SequenceNumber,
    lineage: &LineageId,
    underlying: &str,
    index: u32,
) -> ExecReportSpec {
    ExecReportSpec {
        order_id,
        exec_id: lineage.execution_id(underlying, sequence, index),
        exec_type: ExecType::Canceled,
        ord_status: OrdStatus::Canceled,
        symbol,
        side,
        leaves_qty: 0,
        cum_qty: 0,
        last_qty: None,
        last_px: None,
        price: None,
        secondary_exec_id: sequence,
        commission: None,
        comm_type: None,
        last_liquidity_ind: None,
        ord_rej_reason: None,
        text: None,
    }
}

/// The `ExecutionReport (8)` current-status report for an `OrderStatusRequest (H)`
/// over the orders the session observed placed.
///
/// The gateway cannot read the resting book, so status is derived from the
/// committed fills alone: `Filled` when fully filled, `PartiallyFilled` when
/// partially, `New` (resting) otherwise — with `ExecType=Trade` when any fill was
/// observed, else `ExecType=New`.
// governance O-4 forbids `saturating_sub`; `checked_sub(..).unwrap_or(0)` is the
// same value under the `cum <= quantity` invariant.
#[must_use]
#[allow(clippy::too_many_arguments, clippy::manual_saturating_arithmetic)]
pub(crate) fn render_status_report(
    symbol: Symbol,
    side: OrderSide,
    order_id: VenueOrderId,
    quantity: u64,
    cum: u64,
    last_leg: Option<&ExecutionRecord>,
    lineage: &LineageId,
    underlying: &str,
) -> ExecReportSpec {
    let ord_status = if cum >= quantity && quantity > 0 {
        OrdStatus::Filled
    } else if cum > 0 {
        OrdStatus::PartiallyFilled
    } else {
        OrdStatus::New
    };
    let exec_type = if cum > 0 {
        ExecType::Trade
    } else {
        ExecType::New
    };
    let leaves = quantity.checked_sub(cum).unwrap_or(0);
    // A status report re-references the order's own composite id; there is no new
    // execution, so the ExecID is synthesized (index 0) on that command's namespace.
    let sequence = last_leg
        .map(|leg| leg.underlying_sequence)
        .unwrap_or(SequenceNumber::new(0));
    ExecReportSpec {
        order_id,
        exec_id: lineage.execution_id(underlying, sequence, 0),
        exec_type,
        ord_status,
        symbol,
        side,
        leaves_qty: leaves,
        cum_qty: cum,
        last_qty: last_leg.map(|leg| leg.quantity),
        last_px: last_leg.map(|leg| leg.price_cents),
        price: None,
        secondary_exec_id: sequence,
        commission: None,
        comm_type: None,
        last_liquidity_ind: None,
        ord_rej_reason: None,
        text: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ClientOrderId, Side as DtoSide};

    fn sym() -> Symbol {
        Symbol::parse("BTC-20240329-50000-C").expect("symbol")
    }

    fn header() -> StandardHeader {
        use ironfix_core::types::{CompId, SeqNum};
        StandardHeader::new(
            CompId::new("CLIENT").expect("comp"),
            CompId::new("FAUXCHANGE").expect("comp"),
            SeqNum::new(7),
            UtcTimestamp::from_epoch_ms(0),
        )
    }

    fn limit_d(tif: FixTif) -> NewOrderSingle {
        NewOrderSingle {
            header: header(),
            cl_ord_id: ClientOrderId::new("CLIENT-1"),
            account: None,
            symbol: sym(),
            side: OrderSide::Buy,
            transact_time: UtcTimestamp::from_epoch_ms(0),
            ord_type: OrdType::Limit,
            price: Some(Cents::new(50_005)),
            order_qty: 3,
            time_in_force: tif,
            expire_time: None,
        }
    }

    fn taker_leg(exec_index: u32, price: u64, qty: u64, fee: i64) -> ExecutionRecord {
        let lineage = LineageId::new("run-1");
        ExecutionRecord {
            execution_id: lineage.execution_id("BTC", SequenceNumber::new(7), exec_index),
            order_id: VenueOrderId::new("run-1:BTC:g0:0"),
            account: AccountId::new("acct-1"),
            symbol: "BTC".to_string(),
            instrument: sym(),
            side: DtoSide::Buy,
            liquidity: LiquidityFlag::Taker,
            quantity: qty,
            price_cents: Cents::new(price),
            fee_cents: crate::exchange::SignedCents::new(fee),
            theo_value_cents: Cents::new(price),
            edge_cents: crate::exchange::SignedCents::new(0),
            underlying_sequence: SequenceNumber::new(7),
            latency_us: 0,
            executed_at: crate::exchange::EventTimestamp::new(0),
        }
    }

    #[test]
    fn test_to_add_command_limit_matches_rest_shape() {
        let owner = Hash32([0x11; 32]);
        let order_id = VenueOrderId::new("run-1:BTC:g0:0");
        let command = to_add_command(
            &limit_d(FixTif::Gtc),
            order_id.clone(),
            AccountId::new("acct-1"),
            owner,
        )
        .expect("add command");
        let rest = add_order_command(
            sym(),
            order_id,
            AccountId::new("acct-1"),
            owner,
            Some(ClientOrderId::new("CLIENT-1")),
            SeamSide::Buy,
            OrderType::Limit,
            Some(Cents::new(50_005)),
            3,
            SeamTif::Gtc,
        );
        assert_eq!(command, rest, "FIX D and REST derive the same AddOrder");
    }

    #[test]
    fn test_to_add_command_market_is_ioc_no_price() {
        let order = NewOrderSingle {
            ord_type: OrdType::Market,
            price: None,
            time_in_force: FixTif::Gtc, // ignored for a market order
            ..limit_d(FixTif::Gtc)
        };
        let command = to_add_command(
            &order,
            VenueOrderId::new("run-1:BTC:g0:0"),
            AccountId::new("acct-1"),
            Hash32([0; 32]),
        )
        .expect("add command");
        match command {
            VenueCommand::AddOrder {
                order_type,
                limit_price,
                time_in_force,
                ..
            } => {
                assert_eq!(order_type, OrderType::Market);
                assert_eq!(limit_price, None);
                assert_eq!(time_in_force, SeamTif::Ioc);
            }
            other => panic!("expected AddOrder, got {other:?}"),
        }
    }

    #[test]
    fn test_seam_time_in_force_gtd_requires_resolvable_expire_time() {
        assert!(seam_time_in_force(FixTif::Gtd, None).is_err());
        let expire = UtcTimestamp::from_epoch_ms(1_711_713_600_000);
        // `VenueError` is not `PartialEq`, so match rather than assert_eq.
        match seam_time_in_force(FixTif::Gtd, Some(&expire)) {
            Ok(SeamTif::Gtd(ms)) => assert_eq!(ms, 1_711_713_600_000),
            other => panic!("expected Gtd(ms), got {other:?}"),
        }
    }

    #[test]
    fn test_ord_rej_reason_table() {
        assert_eq!(ord_rej_reason(FixRejectReason::Invalid), 11);
        assert_eq!(ord_rej_reason(FixRejectReason::NotFound), 5);
        assert_eq!(ord_rej_reason(FixRejectReason::Authorization), 6);
        assert_eq!(ord_rej_reason(FixRejectReason::Throttle), 99);
        assert_eq!(ord_rej_reason(FixRejectReason::Internal), 99);
    }

    #[test]
    fn test_cxl_rej_reason_table() {
        assert_eq!(cxl_rej_reason(FixRejectReason::NotFound), 1);
        assert_eq!(cxl_rej_reason(FixRejectReason::Invalid), 2);
        assert_eq!(cxl_rej_reason(FixRejectReason::Authorization), 2);
    }

    #[test]
    fn test_render_resting_limit_is_a_single_new_report() {
        let lineage = LineageId::new("run-1");
        let order_id = VenueOrderId::new("run-1:BTC:g0:0");
        let specs = render_new_order_reports(
            &limit_d(FixTif::Gtc),
            &order_id,
            SequenceNumber::new(7),
            &lineage,
            "BTC",
            SeamTif::Gtc,
            &[],
        );
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].exec_type, ExecType::New);
        assert_eq!(specs[0].ord_status, OrdStatus::New);
        assert_eq!(specs[0].leaves_qty, 3);
        assert_eq!(specs[0].cum_qty, 0);
        assert_eq!(specs[0].secondary_exec_id, SequenceNumber::new(7));
    }

    #[test]
    fn test_render_full_cross_is_new_then_trade_filled_with_fee() {
        let lineage = LineageId::new("run-1");
        let order_id = VenueOrderId::new("run-1:BTC:g0:0");
        let legs = [taker_leg(0, 50_005, 3, 15)];
        let specs = render_new_order_reports(
            &limit_d(FixTif::Gtc),
            &order_id,
            SequenceNumber::new(7),
            &lineage,
            "BTC",
            SeamTif::Gtc,
            &legs,
        );
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].exec_type, ExecType::New);
        let trade = &specs[1];
        assert_eq!(trade.exec_type, ExecType::Trade);
        assert_eq!(trade.ord_status, OrdStatus::Filled);
        assert_eq!(trade.cum_qty, 3);
        assert_eq!(trade.leaves_qty, 0);
        assert_eq!(trade.last_qty, Some(3));
        assert_eq!(trade.last_px, Some(Cents::new(50_005)));
        assert_eq!(
            trade.commission,
            Some(crate::exchange::SignedCents::new(15))
        );
        assert_eq!(trade.comm_type, Some(CommType::Absolute));
        assert_eq!(trade.last_liquidity_ind, Some(LastLiquidityInd::Taker));
        // The Trade report's ExecID is the fill's execution_id; the New report's is
        // synthesized above the fill index, so they never collide.
        assert_ne!(specs[0].exec_id, trade.exec_id);
        assert_eq!(
            trade.exec_id,
            lineage.execution_id("BTC", SequenceNumber::new(7), 0)
        );
    }

    #[test]
    fn test_render_killed_ioc_is_new_then_canceled() {
        let lineage = LineageId::new("run-1");
        let order_id = VenueOrderId::new("run-1:BTC:g0:0");
        let specs = render_new_order_reports(
            &limit_d(FixTif::Ioc),
            &order_id,
            SequenceNumber::new(7),
            &lineage,
            "BTC",
            SeamTif::Ioc,
            &[],
        );
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].exec_type, ExecType::New);
        assert_eq!(specs[1].exec_type, ExecType::Canceled);
        assert_eq!(specs[1].ord_status, OrdStatus::Canceled);
        assert_eq!(specs[1].cum_qty, 0);
        assert_eq!(specs[1].leaves_qty, 0);
    }

    #[test]
    fn test_render_partial_ioc_is_new_trade_then_canceled() {
        let lineage = LineageId::new("run-1");
        let order_id = VenueOrderId::new("run-1:BTC:g0:0");
        let legs = [taker_leg(0, 50_005, 2, 10)];
        let specs = render_new_order_reports(
            &limit_d(FixTif::Ioc),
            &order_id,
            SequenceNumber::new(7),
            &lineage,
            "BTC",
            SeamTif::Ioc,
            &legs,
        );
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].exec_type, ExecType::New);
        assert_eq!(specs[1].exec_type, ExecType::Trade);
        assert_eq!(specs[1].ord_status, OrdStatus::PartiallyFilled);
        assert_eq!(specs[1].cum_qty, 2);
        assert_eq!(specs[2].exec_type, ExecType::Canceled);
        assert_eq!(specs[2].cum_qty, 2);
        assert_eq!(specs[2].leaves_qty, 0);
    }

    #[test]
    fn test_into_report_stamps_header_and_encodes() {
        let lineage = LineageId::new("run-1");
        let order_id = VenueOrderId::new("run-1:BTC:g0:0");
        let specs = render_new_order_reports(
            &limit_d(FixTif::Gtc),
            &order_id,
            SequenceNumber::new(7),
            &lineage,
            "BTC",
            SeamTif::Gtc,
            &[],
        );
        let report = specs
            .into_iter()
            .next()
            .expect("spec")
            .into_report(header());
        assert_eq!(report.order_id, order_id);
        // Round-trips through the wire encoder.
        let bytes = super::super::FixBody::encode(&report);
        assert!(!bytes.is_empty());
    }
}
