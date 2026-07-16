//! Shared boundary: the REST/WS DTO layer — the canonical value objects and
//! their `serde` + `utoipa::ToSchema` projection onto the wire.
//!
//! This module is the serde projection of the venue value objects and events
//! ([01 §1–§10](../docs/01-domain-model.md)); there is no separate `domain`
//! module. It carries **no business logic** beyond the order-shape validation
//! helpers ([`validate_order_shape`]) — the order path, stores, and handlers
//! live elsewhere ([`crate::exchange`], #007/#008/#013).
//!
//! Wire contract, pinned and golden-tested ([01 §10](../docs/01-domain-model.md),
//! [SEMVER.md](../docs/SEMVER.md)):
//!
//! - **Money is integer cents** — every monetary field is a [`Cents`] /
//!   [`SignedCents`] newtype from #002 (`#[serde(transparent)]`, bare integer on
//!   the wire), never `f64`. The only floats are genuinely derived analytics —
//!   Greeks, IV, mid/micro-price estimators, basis points (spread / slippage),
//!   imbalance, and market-maker scalars — each documented at its field. A
//!   realized or volume-weighted **average price** (market-order VWAP,
//!   book-side VWAP, market-impact average price) is money and stays integer
//!   cents, truncated toward zero to the nearest cent.
//! - **Timestamps** are venue-clock milliseconds ([`EventTimestamp`]) or
//!   ISO-8601 strings.
//! - **Casing is pinned per enum family** and preserves the inherited Backend
//!   wire: [`Permission`] / [`Side`] / [`OptionStyle`] / [`OrderStatus`]
//!   lowercase, [`TimeInForce`] `UPPERCASE`, [`OrderType`] / [`LiquidityFlag`]
//!   `snake_case`.
//! - **Request DTOs reject unknown fields** (`#[serde(deny_unknown_fields)]`) so
//!   a typo is a `400`, not a silent accept.
//! - **`WsMessage`** is internally-adjacently tagged
//!   (`#[serde(tag = "type", content = "data")]`); the `type` discriminant is
//!   fixed per variant.
//!
//! Governed by `docs/01-domain-model.md` and `docs/03-protocol-surfaces.md`.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::{VenueError, WsError};
use crate::exchange::{Cents, EventTimestamp, SequenceNumber, SignedCents, Symbol};

// ============================================================================
// Identity newtypes (venue-owned, opaque wire strings)
// ============================================================================
//
// The composite-id GRAMMAR and minting (lineage, per-underlying sequence) land
// with the venue envelope / order path (#005/#006, [01 §6.1](../docs/01-domain-model.md));
// here these are the wire types only — opaque, `#[serde(transparent)]` strings.

/// A venue account identity — the JWT `sub` for REST/WS and the resolved
/// account for a FIX logon, under **one** account registry
/// ([01 §8](../docs/01-domain-model.md)). Opaque on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct AccountId(String);

impl AccountId {
    /// Wraps a raw account identity string.
    #[must_use]
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the account identity as a string slice.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The client-supplied order identifier — the account-scoped idempotency key
/// (`ClOrdID (11)` on FIX, `client_order_id` on REST,
/// [01 §6.1](../docs/01-domain-model.md)). Opaque on the wire; the venue owns
/// its idempotency semantics (#006), not this DTO layer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct ClientOrderId(String);

impl ClientOrderId {
    /// Wraps a raw client-order-id string.
    #[must_use]
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the client order id as a string slice.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The venue-assigned order identity carried on the wire — the §6.1 composite
/// id `"{lineage_id}:{underlying}:{underlying_sequence}:{index}"`
/// ([01 §6.1](../docs/01-domain-model.md)); `OrderID (37)` on FIX. Opaque here;
/// minting is #006's.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct VenueOrderId(String);

impl VenueOrderId {
    /// Wraps a raw venue-order-id string.
    #[must_use]
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the venue order id as a string slice.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The venue-assigned execution identity — the §6.1 composite id shared by the
/// two legs of one match and the cross-surface join key
/// ([01 §6.1, §7](../docs/01-domain-model.md)); `ExecID (17)` on FIX. Opaque
/// here; minting is #006's.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct ExecutionId(String);

impl ExecutionId {
    /// Wraps a raw execution-id string.
    #[must_use]
    #[inline]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the execution id as a string slice.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// ============================================================================
// Wire enums (casing pinned per family — a wire contract)
// ============================================================================

/// A venue permission carried by the authenticated session across every
/// protocol surface — REST/WS via the JWT `Claims`, FIX via the logon
/// credentials — under **one** permission model
/// ([01 §8](../docs/01-domain-model.md), [03 §6](../docs/03-protocol-surfaces.md)).
///
/// `Admin` **implies** `Read` and `Trade`; that implication is enforced by the
/// auth layer (#011), not encoded structurally here. The wire casing is
/// **lowercase** (`"read"` / `"trade"` / `"admin"`), inherited verbatim from
/// `option-chain-orderbook-backend` v0.4.0 and pinned as a wire contract
/// ([01 §10](../docs/01-domain-model.md)); changing it is a breaking wire
/// change that must move the DTO examples, OpenAPI, and golden tests together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum Permission {
    /// Read-only access: query the hierarchy, prices, market data, and
    /// public prints. The minimum any authenticated session holds.
    Read,
    /// Order entry: place, cancel, and replace orders (REST + FIX). Implies the
    /// ability to observe the resulting fills.
    Trade,
    /// Administrative access: venue controls, snapshots, and every lower
    /// permission. `Admin` implies `Read` and `Trade`.
    Admin,
}

/// The side of an order or fill. Wire casing is **lowercase** (`"buy"` /
/// `"sell"`), the inherited Backend wire; the gateway converts to the upstream
/// [`Side`](crate::exchange::Side) at the orderbook seam (#013/#036).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    /// Buy side (bid).
    Buy,
    /// Sell side (ask).
    Sell,
}

/// The side of a book price level in an orderbook delta. Wire casing is
/// **lowercase** (`"bid"` / `"ask"`) — distinct from an order [`Side`].
///
/// `Ord` (declaration order, `Bid` < `Ask`) is derived so the WS subscription
/// manager can key its touched-level set deterministically (#014); it does not
/// affect the wire form.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum BookSide {
    /// Bid side of the book.
    Bid,
    /// Ask side of the book.
    Ask,
}

/// Option style (call or put). Wire casing is **lowercase** (`"call"` /
/// `"put"`), matching the option-style path segment; the gateway converts to
/// the upstream [`OptionStyle`](crate::exchange::OptionStyle) at the seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum OptionStyle {
    /// Call option.
    Call,
    /// Put option.
    Put,
}

/// The kind of an order. Wire casing is **`snake_case`** (`"limit"` /
/// `"market"`) ([01 §6, §10](../docs/01-domain-model.md)).
///
/// `Limit` requires a limit price; `Market` must carry none — enforced by
/// [`validate_order_shape`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    /// A limit order priced at [`Order::limit_price`].
    Limit,
    /// A market order that crosses the book at the best available prices.
    Market,
}

/// Time in force for an order. Wire casing is **`UPPERCASE`** (`"GTC"` /
/// `"IOC"` / `"FOK"` / `"GTD"`), the inherited Backend wire; the gateway
/// converts to the upstream [`TimeInForce`](crate::exchange::TimeInForce) at the
/// seam. A `Gtd` order carries its expiry in a separate `gtd_expires_at` field
/// (venue-clock ms), never in this discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "UPPERCASE")]
pub enum TimeInForce {
    /// Good 'til canceled — the default.
    #[default]
    Gtc,
    /// Immediate or cancel — fill what is marketable now, cancel the rest.
    Ioc,
    /// Fill or kill — fill in full immediately, or cancel entirely.
    Fok,
    /// Good 'til date — rests until the configured expiry instant.
    Gtd,
}

/// The lifecycle status of an order. Wire casing is **lowercase**.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum OrderStatus {
    /// Accepted but not yet resting in the book.
    Pending,
    /// Resting in the book.
    Active,
    /// Partially filled; the remainder rests or was canceled.
    Partial,
    /// Completely filled.
    Filled,
    /// Canceled before completion.
    Canceled,
}

/// Whether a fill leg provided liquidity (`Maker`, resting) or removed it
/// (`Taker`, aggressing). Wire casing is **`snake_case`**
/// ([01 §7, §10](../docs/01-domain-model.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum LiquidityFlag {
    /// The resting (liquidity-providing) leg; its fee may be a rebate (negative).
    Maker,
    /// The aggressing (liquidity-removing) leg.
    Taker,
}

/// The immediate outcome of a limit-order placement. Wire casing is
/// **lowercase**.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum LimitOrderStatus {
    /// Accepted and resting in the book.
    Accepted,
    /// Filled in full on submit.
    Filled,
    /// Partially filled on submit (IOC).
    Partial,
    /// Rejected (FOK not fillable, or a validation failure).
    Rejected,
}

/// The immediate outcome of a market-order submission. Wire casing is
/// **lowercase**.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum MarketOrderStatus {
    /// Filled in full.
    Filled,
    /// Partially filled (insufficient liquidity for the full quantity).
    Partial,
    /// Rejected (no liquidity).
    Rejected,
}

/// The outcome of an order modification. Wire casing is **lowercase**.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum ModifyOrderStatus {
    /// The order was modified.
    Modified,
    /// The modification was rejected.
    Rejected,
}

/// The outcome of a single order within a bulk operation. Wire casing is
/// **lowercase**.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum BulkOrderStatus {
    /// Accepted by the matching engine (placed or filled).
    Accepted,
    /// Rejected.
    Rejected,
}

/// The projected lifecycle status of an instrument — the wire form of the
/// upstream `InstrumentStatus` ([01 §5](../docs/01-domain-model.md)). Wire
/// casing is **lowercase**.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum InstrumentLifecycle {
    /// Pending activation (not yet trading).
    Pending,
    /// Active and accepting orders.
    Active,
    /// Temporarily halted (no new orders).
    Halted,
    /// In settlement (no new orders).
    Settling,
    /// Expired (no new orders; resting orders canceled).
    Expired,
}

/// A market-data subscription channel ([03 §4.1](../docs/03-protocol-surfaces.md)).
/// Wire casing is **lowercase**.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionChannel {
    /// Order-book snapshots and deltas.
    Orderbook,
    /// Public trade prints.
    Trades,
    /// Quote updates.
    Quotes,
    /// Underlying price updates.
    Prices,
    /// Public, anonymised fill prints.
    Fills,
}

/// An OHLC bar interval. Wire form is the abbreviated label (`"1m"` … `"1d"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[repr(u8)]
pub enum OhlcInterval {
    /// 1-minute bars.
    #[serde(rename = "1m")]
    OneMinute,
    /// 5-minute bars.
    #[serde(rename = "5m")]
    FiveMinutes,
    /// 15-minute bars.
    #[serde(rename = "15m")]
    FifteenMinutes,
    /// 1-hour bars.
    #[serde(rename = "1h")]
    OneHour,
    /// 4-hour bars.
    #[serde(rename = "4h")]
    FourHours,
    /// 1-day bars.
    #[serde(rename = "1d")]
    OneDay,
}

impl OhlcInterval {
    /// Returns the interval duration in **seconds**.
    #[must_use]
    #[inline]
    pub const fn seconds(self) -> u64 {
        match self {
            Self::OneMinute => 60,
            Self::FiveMinutes => 300,
            Self::FifteenMinutes => 900,
            Self::OneHour => 3_600,
            Self::FourHours => 14_400,
            Self::OneDay => 86_400,
        }
    }
}

// ============================================================================
// Order-shape validation (the only business logic this DTO layer carries)
// ============================================================================

/// The venue-owned **maximum accepted / resting order price**, in **cents** — the
/// order economic-field ceiling the threat model names as the *required* bound
/// ([08 §4](../docs/08-threat-model.md#4-untrusted-input-hardening)). A limit
/// order whose price exceeds it is a typed `400` (`InvalidOrder`) reject **before**
/// the sequenced path, never accepted.
///
/// The value (`10^12` cents = `$10` billion per contract) is generous for any
/// realistic option premium yet bounds the economic fields so the downstream fee
/// arithmetic stays **off both saturation branches**. The widest accepted notional
/// is `MAX_PRICE_CENTS × MAX_ORDER_QUANTITY = 10^12 × 10^6 = 10^18` cents, which
/// sits ~9.2× below `i64::MAX` (`≈ 9.22 × 10^18`); so the per-leg fee — at most the
/// full notional for any fee rate ≤ 100%, and vastly less for realistic bps — always
/// fits the narrowing to `SignedCents` (`i64`) in `crate::exchange`'s `per_leg_fee`
/// and never trips its `MoneyError::Overflow` arm, while the upstream `notional × bps`
/// product is computed in `u128` with vast headroom. The compile-time assertion below
/// pins the `MAX_PRICE_CENTS × MAX_ORDER_QUANTITY ≤ i64::MAX` invariant that guarantees
/// it. The live per-instrument value becomes venue config (#046); this is the
/// bounded default until then.
pub const MAX_PRICE_CENTS: u64 = 1_000_000_000_000;

/// The venue-owned **maximum accepted order quantity**, in **contracts** — the lot
/// ceiling paired with [`MAX_PRICE_CENTS`]
/// ([08 §4](../docs/08-threat-model.md#4-untrusted-input-hardening)). An order (limit
/// or market) whose quantity exceeds it is a typed `400` (`InvalidOrder`) reject
/// before the sequenced path. The live per-instrument value becomes venue config
/// (#046); this is the bounded default until then.
pub const MAX_ORDER_QUANTITY: u64 = 1_000_000;

// The fee-bound proof, pinned at compile time: the widest accepted notional
// (`MAX_PRICE_CENTS × MAX_ORDER_QUANTITY`) must fit `i64`, so the per-leg fee —
// at most the full notional for any fee rate ≤ 100%, and vastly less for realistic
// bps — always fits the `SignedCents` narrowing in `per_leg_fee` and never reaches
// its checked-overflow arm. This is what keeps the economic math off both
// saturation branches ([08 §4](../docs/08-threat-model.md#4-untrusted-input-hardening)).
const _: () = assert!(
    (MAX_PRICE_CENTS as u128) * (MAX_ORDER_QUANTITY as u128) <= i64::MAX as u128,
    "max accepted notional must fit i64 so the per-leg fee narrowing never overflows",
);

/// Validates the boundary order-shape invariants before an order reaches the
/// sequencer ([01 §6](../docs/01-domain-model.md)), returning a typed
/// [`VenueError`] the gateway maps to a `400` / FIX reject:
///
/// - `Limit` ⇒ `limit_price.is_some()`;
/// - `Market` ⇒ `limit_price.is_none()`;
/// - `0 < quantity ≤` [`MAX_ORDER_QUANTITY`] (contracts);
/// - `0 < limit_price ≤` [`MAX_PRICE_CENTS`] cents when present — the venue-owned
///   max accepted/resting price ceiling, enforced **here, before the sequenced
///   path** ([08 §4](../docs/08-threat-model.md#4-untrusted-input-hardening)).
///
/// # Errors
///
/// Returns [`VenueError::InvalidOrder`] describing the first violated rule.
///
/// # Examples
///
/// ```
/// use fauxchange::{MAX_PRICE_CENTS, OrderType, validate_order_shape};
/// use fauxchange::exchange::Cents;
/// // A limit order needs a positive, in-range price and quantity.
/// assert!(validate_order_shape(OrderType::Limit, Some(Cents::new(500)), 10).is_ok());
/// // A market order must not carry a price.
/// assert!(validate_order_shape(OrderType::Market, Some(Cents::new(500)), 10).is_err());
/// // A price above the venue ceiling is a typed reject before the sequenced path.
/// assert!(
///     validate_order_shape(OrderType::Limit, Some(Cents::new(MAX_PRICE_CENTS + 1)), 10).is_err()
/// );
/// ```
pub fn validate_order_shape(
    order_type: OrderType,
    limit_price: Option<Cents>,
    quantity: u64,
) -> Result<(), VenueError> {
    if quantity == 0 {
        return Err(VenueError::InvalidOrder(
            "order quantity must be positive".to_string(),
        ));
    }
    if quantity > MAX_ORDER_QUANTITY {
        return Err(VenueError::InvalidOrder(format!(
            "order quantity {quantity} exceeds MAX_ORDER_QUANTITY ({MAX_ORDER_QUANTITY})"
        )));
    }
    match (order_type, limit_price) {
        (OrderType::Limit, None) => Err(VenueError::InvalidOrder(
            "limit order requires a limit price".to_string(),
        )),
        (OrderType::Market, Some(_)) => Err(VenueError::InvalidOrder(
            "market order must not carry a limit price".to_string(),
        )),
        (OrderType::Limit, Some(price)) if price.get() == 0 => Err(VenueError::InvalidOrder(
            "limit price must be positive".to_string(),
        )),
        (OrderType::Limit, Some(price)) if price.get() > MAX_PRICE_CENTS => {
            Err(VenueError::InvalidOrder(format!(
                "limit price {} cents exceeds MAX_PRICE_CENTS ({MAX_PRICE_CENTS})",
                price.get()
            )))
        }
        (OrderType::Limit, Some(_)) | (OrderType::Market, None) => Ok(()),
    }
}

// ============================================================================
// Order value objects and projections
// ============================================================================

/// A venue order record — the canonical order value object and its wire
/// projection (`GET /api/v1/orders/{id}`, [01 §6](../docs/01-domain-model.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Order {
    /// The venue-assigned order id ([01 §6.1](../docs/01-domain-model.md)).
    pub id: VenueOrderId,
    /// The client-supplied idempotency key, when one was provided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<ClientOrderId>,
    /// The owning account.
    pub account: AccountId,
    /// The canonical contract symbol.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// Order side.
    pub side: Side,
    /// Order kind.
    pub order_type: OrderType,
    /// Limit price in **cents** — present for `Limit`, absent for `Market`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub limit_price: Option<Cents>,
    /// Original order quantity in **contracts**.
    pub quantity: u64,
    /// Quantity filled so far, in **contracts**.
    pub filled_quantity: u64,
    /// Quantity remaining in the book or unfilled, in **contracts**.
    pub remaining_quantity: u64,
    /// Time in force.
    pub time_in_force: TimeInForce,
    /// Lifecycle status.
    pub status: OrderStatus,
    /// Submission time on the venue clock, in **milliseconds**.
    #[schema(value_type = u64)]
    pub submitted_at: EventTimestamp,
    /// The `underlying_sequence` correlating this order to the WS/FIX fan-out
    /// ([01 §9.1](../docs/01-domain-model.md)).
    #[schema(value_type = u64)]
    pub sequence: SequenceNumber,
}

/// Request body for a limit-order placement
/// (`POST .../options/{style}/orders`). The contract is taken from the path;
/// the money field is **cents**.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PlaceLimitOrderRequest {
    /// Order side.
    pub side: Side,
    /// Limit price in **cents** (must be positive).
    #[schema(value_type = u64)]
    pub price: Cents,
    /// Order quantity in **contracts** (must be positive).
    pub quantity: u64,
    /// Time in force (defaults to `GTC` when omitted).
    #[serde(default)]
    pub time_in_force: Option<TimeInForce>,
    /// For a `GTD` order, the expiry instant on the venue clock, in
    /// **milliseconds**.
    #[serde(default)]
    #[schema(value_type = Option<u64>)]
    pub gtd_expires_at: Option<EventTimestamp>,
    /// Optional account-scoped idempotency key.
    #[serde(default)]
    pub client_order_id: Option<ClientOrderId>,
}

impl PlaceLimitOrderRequest {
    /// Validates this request's order shape ([`validate_order_shape`]).
    ///
    /// # Errors
    ///
    /// Returns [`VenueError::InvalidOrder`] if the quantity or price is
    /// non-positive.
    pub fn validate(&self) -> Result<(), VenueError> {
        validate_order_shape(OrderType::Limit, Some(self.price), self.quantity)
    }
}

/// Request body for a market-order submission
/// (`POST .../options/{style}/orders/market`). The contract is taken from the
/// path; a market order carries no price.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PlaceMarketOrderRequest {
    /// Order side.
    pub side: Side,
    /// Order quantity in **contracts** (must be positive).
    pub quantity: u64,
    /// Optional account-scoped idempotency key.
    #[serde(default)]
    pub client_order_id: Option<ClientOrderId>,
}

impl PlaceMarketOrderRequest {
    /// Validates this request's order shape ([`validate_order_shape`]).
    ///
    /// # Errors
    ///
    /// Returns [`VenueError::InvalidOrder`] if the quantity is zero.
    pub fn validate(&self) -> Result<(), VenueError> {
        validate_order_shape(OrderType::Market, None, self.quantity)
    }
}

/// Acknowledgement of a limit-order placement, carrying the resulting
/// `underlying_sequence` for cross-surface correlation
/// ([03 §3](../docs/03-protocol-surfaces.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PlaceLimitOrderResponse {
    /// The assigned venue order id.
    pub order_id: VenueOrderId,
    /// Immediate placement outcome.
    pub status: LimitOrderStatus,
    /// Quantity filled immediately, in **contracts**.
    pub filled_quantity: u64,
    /// Quantity resting or unfilled, in **contracts**.
    pub remaining_quantity: u64,
    /// The `underlying_sequence` of the resulting event.
    #[schema(value_type = u64)]
    pub sequence: SequenceNumber,
    /// Human-readable, client-safe message.
    pub message: String,
}

/// A single fill leg of a market-order execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FillPrint {
    /// Execution price in **cents**.
    #[schema(value_type = u64)]
    pub price: Cents,
    /// Executed quantity in **contracts**.
    pub quantity: u64,
}

/// Response after submitting a market order ([03 §3](../docs/03-protocol-surfaces.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PlaceMarketOrderResponse {
    /// The assigned venue order id.
    pub order_id: VenueOrderId,
    /// Execution outcome.
    pub status: MarketOrderStatus,
    /// Total quantity filled, in **contracts**.
    pub filled_quantity: u64,
    /// Quantity that could not be filled, in **contracts**.
    pub remaining_quantity: u64,
    /// Volume-weighted average execution price, in **integer cents** (`None`
    /// when there are no fills).
    ///
    /// A realized average of actual fill prices is a monetary value crossing
    /// the wire, so it obeys the integer-cents contract like every other
    /// price — it is **not** a float-exempt analytic. It is computed as
    /// `Σ(priceᵢ × qtyᵢ) / Σ(qtyᵢ)` in `u128` notional space with checked
    /// arithmetic and **truncated toward zero** to the nearest whole cent — the
    /// venue's single rounding rule for every volume-weighted average price.
    /// The exact per-fill prices in [`FillPrint`] are the settled amounts.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub average_price: Option<Cents>,
    /// The `underlying_sequence` of the resulting event.
    #[schema(value_type = u64)]
    pub sequence: SequenceNumber,
    /// The individual fill legs.
    pub fills: Vec<FillPrint>,
}

/// Response for canceling an order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CancelOrderResponse {
    /// Whether the cancel command was **accepted and sequenced** (not a
    /// confirmation the resting order existed — the found/not-found outcome is
    /// not carried on the receipt until the `Receipt`→`VenueOutcome` seam lands).
    pub success: bool,
    /// The `underlying_sequence` of the resulting event, for cross-surface
    /// correlation with the WS/FIX fan-out ([03 §3](../docs/03-protocol-surfaces.md)).
    #[schema(value_type = u64)]
    pub sequence: SequenceNumber,
    /// Human-readable, client-safe message.
    pub message: String,
}

/// Request body to modify a resting order (price and/or quantity).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ModifyOrderRequest {
    /// New limit price in **cents**, when changing it.
    #[serde(default)]
    #[schema(value_type = Option<u64>)]
    pub price: Option<Cents>,
    /// New quantity in **contracts**, when changing it.
    #[serde(default)]
    pub quantity: Option<u64>,
}

/// Response after modifying an order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ModifyOrderResponse {
    /// The order id that was modified.
    pub order_id: VenueOrderId,
    /// Modification outcome.
    pub status: ModifyOrderStatus,
    /// New price in **cents**, if changed.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub new_price: Option<Cents>,
    /// New quantity in **contracts**, if changed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_quantity: Option<u64>,
    /// Whether the order lost time priority due to the modification.
    pub priority_changed: bool,
    /// Human-readable, client-safe message.
    pub message: String,
}

/// Query parameters for listing orders.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct OrderListQuery {
    /// Filter by underlying ticker.
    #[serde(default)]
    pub underlying: Option<String>,
    /// Filter by order status.
    #[serde(default)]
    pub status: Option<OrderStatus>,
    /// Filter by order side.
    #[serde(default)]
    pub side: Option<Side>,
    /// Pagination limit.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Pagination offset.
    #[serde(default)]
    pub offset: Option<usize>,
}

/// Response for listing orders.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct OrderListResponse {
    /// The matching orders.
    pub orders: Vec<Order>,
    /// Total number of matching orders (before pagination).
    pub total: usize,
    /// The limit applied.
    pub limit: usize,
    /// The offset applied.
    pub offset: usize,
}

// ============================================================================
// Bulk and cancel-all
// ============================================================================

/// The maximum number of orders one `POST /api/v1/orders/bulk` request may
/// carry — a **DoS bound** ([08 §5](../docs/08-threat-model.md)). Axum's 2 MB
/// body cap alone still admits thousands of items, and each accepted item is
/// submitted **sequentially** onto one underlying's single-writer actor mailbox
/// (bounded, [`DEFAULT_MAILBOX_CAPACITY`](crate::state::DEFAULT_MAILBOX_CAPACITY)
/// = 1024), so an unbounded batch lets one `Trade` account monopolize an
/// underlying. `500` is a generous batch that stays well under the mailbox
/// capacity even if every item routes to the same underlying; the gateway
/// rejects an over-limit request with a `400` before the loop begins.
pub const MAX_BULK_ORDER_ITEMS: usize = 500;

/// The maximum number of order ids one `DELETE /api/v1/orders/bulk` request may
/// carry — the same DoS bound as [`MAX_BULK_ORDER_ITEMS`], for the cancel path.
pub const MAX_BULK_CANCEL_ITEMS: usize = 500;

/// A single item in a bulk limit-order submission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BulkOrderItem {
    /// The canonical contract symbol.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// Order side.
    pub side: Side,
    /// Limit price in **cents** (must be positive).
    #[schema(value_type = u64)]
    pub price: Cents,
    /// Order quantity in **contracts** (must be positive).
    pub quantity: u64,
    /// Time in force (defaults to `GTC` when omitted).
    #[serde(default)]
    pub time_in_force: Option<TimeInForce>,
    /// Optional account-scoped idempotency key.
    #[serde(default)]
    pub client_order_id: Option<ClientOrderId>,
}

impl BulkOrderItem {
    /// Validates this item's order shape ([`validate_order_shape`]).
    ///
    /// # Errors
    ///
    /// Returns [`VenueError::InvalidOrder`] if the quantity or price is
    /// non-positive.
    pub fn validate(&self) -> Result<(), VenueError> {
        validate_order_shape(OrderType::Limit, Some(self.price), self.quantity)
    }
}

/// Request for a bulk limit-order submission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BulkOrderRequest {
    /// The orders to submit.
    pub orders: Vec<BulkOrderItem>,
    /// If `true`, all orders must succeed or none are left resting (atomic).
    #[serde(default)]
    pub atomic: bool,
}

/// Result for one order in a bulk submission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BulkOrderResultItem {
    /// Index of the order in the request array.
    pub index: usize,
    /// The assigned venue order id, if accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<VenueOrderId>,
    /// The `underlying_sequence` of the resulting event, for cross-surface
    /// correlation — present only for an accepted item (a rejected item never
    /// reached the sequencer).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub sequence: Option<SequenceNumber>,
    /// Outcome for this order.
    pub status: BulkOrderStatus,
    /// Error message, if rejected (client-safe).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response for a bulk limit-order submission.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BulkOrderResponse {
    /// Number of orders accepted by the matching engine.
    pub success_count: usize,
    /// Number of orders not left resting (rejected, not attempted, or rolled
    /// back).
    pub failure_count: usize,
    /// Per-order results.
    pub results: Vec<BulkOrderResultItem>,
    /// Whether an atomic rollback was performed.
    pub rolled_back: bool,
    /// Best-effort rollback warnings (client-safe); omitted when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rollback_warnings: Vec<String>,
}

/// Request for a bulk cancellation by order id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BulkCancelRequest {
    /// The order ids to cancel.
    pub order_ids: Vec<VenueOrderId>,
}

/// Result for one cancellation in a bulk cancel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BulkCancelResultItem {
    /// The order id attempted.
    pub order_id: VenueOrderId,
    /// Whether the cancellation succeeded.
    pub canceled: bool,
    /// Error message, if it failed (client-safe).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response for a bulk cancellation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BulkCancelResponse {
    /// Number of orders canceled.
    pub success_count: usize,
    /// Number of cancellations that failed.
    pub failure_count: usize,
    /// Per-order results.
    pub results: Vec<BulkCancelResultItem>,
}

/// Query parameters for the cancel-all endpoint (all filters optional).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CancelAllQuery {
    /// Filter by underlying ticker.
    #[serde(default)]
    pub underlying: Option<String>,
    /// Filter by expiration date string (`YYYYMMDD`).
    #[serde(default)]
    pub expiration: Option<String>,
    /// Filter by order side.
    #[serde(default)]
    pub side: Option<Side>,
    /// Filter by option style.
    #[serde(default)]
    pub style: Option<OptionStyle>,
}

/// Response for the cancel-all endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CancelAllResponse {
    /// Number of orders canceled.
    pub canceled_count: usize,
    /// Number of orders that failed to cancel.
    pub failed_count: usize,
}

// ============================================================================
// Fills, executions, and positions
// ============================================================================

/// One **account-attributed leg** of a match — the venue's internal fill value
/// object ([01 §7](../docs/01-domain-model.md)). Its wire projections are the
/// account-scoped [`ExecutionRecord`] (REST/FIX) and the public anonymised
/// `WsMessage::Fill`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Fill {
    /// The composite execution id, shared by the two legs of one match.
    pub execution_id: ExecutionId,
    /// The venue order id of **this** leg.
    pub order_id: VenueOrderId,
    /// The owner of **this** leg.
    pub account: AccountId,
    /// The canonical contract symbol.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// This leg's side.
    pub side: Side,
    /// Execution price in **cents**.
    #[schema(value_type = u64)]
    pub price: Cents,
    /// Executed quantity in **contracts**.
    pub quantity: u64,
    /// This leg's role (maker or taker).
    pub liquidity: LiquidityFlag,
    /// This leg's fee in **cents** — a maker rebate is negative.
    #[schema(value_type = i64)]
    pub fee: SignedCents,
    /// The `underlying_sequence` (per underlying, journaled).
    #[schema(value_type = u64)]
    pub sequence: SequenceNumber,
    /// Capture versus the quote-time theoretical value, in **cents per
    /// contract** (signed).
    #[schema(value_type = i64)]
    pub edge: SignedCents,
    /// Venue-clock execution time, in **milliseconds**.
    #[schema(value_type = u64)]
    pub ts: EventTimestamp,
}

/// The authoritative, **account-scoped** execution record — the REST
/// `/api/v1/executions` resource and the substrate of a FIX
/// `ExecutionReport (8)` ([01 §7](../docs/01-domain-model.md)). Carries account
/// and fee; the WS `fill` print does not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionRecord {
    /// The composite execution id (cross-surface join key).
    pub execution_id: ExecutionId,
    /// The venue order id of this leg.
    pub order_id: VenueOrderId,
    /// The owning account.
    pub account: AccountId,
    /// The underlying ticker (e.g. `BTC`).
    pub symbol: String,
    /// The canonical contract identifier `UNDERLYING-YYYYMMDD-STRIKE-STYLE`.
    #[schema(value_type = String)]
    pub instrument: Symbol,
    /// This leg's side.
    pub side: Side,
    /// This leg's role (maker or taker).
    pub liquidity: LiquidityFlag,
    /// Executed quantity in **contracts**.
    pub quantity: u64,
    /// Execution price in **cents**.
    #[schema(value_type = u64)]
    pub price_cents: Cents,
    /// Fee in **cents** — a maker rebate is negative.
    #[schema(value_type = i64)]
    pub fee_cents: SignedCents,
    /// Quote-time theoretical value in **cents**.
    #[schema(value_type = u64)]
    pub theo_value_cents: Cents,
    /// Capture versus theoretical, in **cents per contract** (signed).
    #[schema(value_type = i64)]
    pub edge_cents: SignedCents,
    /// The `underlying_sequence` (order key across surfaces).
    #[schema(value_type = u64)]
    pub underlying_sequence: SequenceNumber,
    /// Injected admission-to-execution latency, in **microseconds**.
    pub latency_us: u64,
    /// Venue-clock execution time, in **milliseconds**.
    #[schema(value_type = u64)]
    pub executed_at: EventTimestamp,
}

/// Aggregate statistics over an execution set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionSummary {
    /// Total number of executions.
    pub total_executions: u64,
    /// Total volume executed, in **contracts**.
    pub total_volume: u64,
    /// Total edge captured, in **cents** (signed).
    #[schema(value_type = i64)]
    pub total_edge: SignedCents,
    /// Fraction of maker executions (`0.0`–`1.0`) — a derived analytic float.
    pub maker_ratio: f64,
}

/// Response for listing executions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ExecutionsListResponse {
    /// The matching execution records.
    pub executions: Vec<ExecutionRecord>,
    /// Aggregate statistics.
    pub summary: ExecutionSummary,
}

/// Query parameters for listing executions.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecutionsQuery {
    /// Filter by start date (ISO-8601).
    #[serde(default)]
    pub from: Option<String>,
    /// Filter by end date (ISO-8601).
    #[serde(default)]
    pub to: Option<String>,
    /// Filter by underlying ticker.
    #[serde(default)]
    pub underlying: Option<String>,
    /// Pagination limit.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// A net position, folded over the execution log per `(account, symbol)`
/// ([01 §7](../docs/01-domain-model.md)). Money fields are **cents**;
/// `unrealized_pnl` / `current_price` are omitted when the symbol is unpriced
/// (no mark), never reported as zero.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Position {
    /// The owning account.
    pub account: AccountId,
    /// The canonical contract symbol.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// The underlying ticker.
    pub underlying: String,
    /// Net position in **signed contracts** (positive = long).
    pub net_quantity: i64,
    /// Volume-weighted entry price in **cents**.
    #[schema(value_type = u64)]
    pub avg_price: Cents,
    /// Current mark price in **cents**; omitted when unpriced.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub current_price: Option<Cents>,
    /// Realized P&L in **cents** (signed).
    #[schema(value_type = i64)]
    pub realized_pnl: SignedCents,
    /// Unrealized P&L in **cents** (signed); omitted when unpriced.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<i64>)]
    pub unrealized_pnl: Option<SignedCents>,
    /// Delta exposure (`net_quantity × delta`) — a derived analytic float.
    pub delta_exposure: f64,
}

/// Aggregate statistics over a position set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PositionSummary {
    /// Total unrealized P&L across **priced** positions, in **cents** (signed).
    #[schema(value_type = i64)]
    pub total_unrealized_pnl: SignedCents,
    /// Total realized P&L across all positions, in **cents** (signed).
    #[schema(value_type = i64)]
    pub total_realized_pnl: SignedCents,
    /// Net delta across **priced** positions — a derived analytic float.
    pub net_delta: f64,
    /// Number of open positions.
    pub position_count: usize,
    /// Number of open positions excluded from the priced aggregates because
    /// they have no mark.
    pub unpriced_count: usize,
}

/// Response for listing positions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PositionsListResponse {
    /// The positions.
    pub positions: Vec<Position>,
    /// Aggregate statistics.
    pub summary: PositionSummary,
}

/// Query parameters for listing positions.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PositionQuery {
    /// Filter by underlying ticker.
    #[serde(default)]
    pub underlying: Option<String>,
}

// ============================================================================
// Accounts and instruments
// ============================================================================

/// The public projection of an account — its identity and permission set
/// ([01 §8](../docs/01-domain-model.md)). Credentials, the STP owner hash, and
/// the revocation epoch are auth-registry internals (#011) and never appear on
/// this DTO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Account {
    /// The account identity (the JWT `sub`).
    pub id: AccountId,
    /// The permission set (`Admin` implies `Read` + `Trade`).
    pub permissions: Vec<Permission>,
}

/// The wire projection of a venue instrument — a flat view over the domain
/// [`Instrument`](crate::exchange::Instrument) ([01 §5](../docs/01-domain-model.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InstrumentView {
    /// The canonical contract symbol.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// The underlying ticker (e.g. `BTC`).
    pub underlying: String,
    /// The expiry date string (`YYYYMMDD`).
    pub expiration: String,
    /// The strike in **whole units**.
    pub strike: u64,
    /// Call or put.
    pub style: OptionStyle,
    /// The lifecycle status.
    pub status: InstrumentLifecycle,
}

// ============================================================================
// Prices
// ============================================================================

/// Request body to insert / override an underlying price
/// (`POST /api/v1/prices`). Money fields are **cents**.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct InsertPriceRequest {
    /// The underlying ticker.
    pub symbol: String,
    /// The price in **cents**.
    #[schema(value_type = u64)]
    pub price: Cents,
    /// Optional bid in **cents**.
    #[serde(default)]
    #[schema(value_type = Option<u64>)]
    pub bid: Option<Cents>,
    /// Optional ask in **cents**.
    #[serde(default)]
    #[schema(value_type = Option<u64>)]
    pub ask: Option<Cents>,
    /// Optional traded volume, in **contracts**.
    #[serde(default)]
    pub volume: Option<u64>,
}

/// Response after inserting a price.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InsertPriceResponse {
    /// Whether the insert succeeded.
    pub success: bool,
    /// The updated underlying ticker.
    pub symbol: String,
    /// The stored price in **cents**.
    #[schema(value_type = u64)]
    pub price_cents: Cents,
    /// The price time on the venue clock, in **milliseconds**.
    #[schema(value_type = u64)]
    pub timestamp: EventTimestamp,
}

/// Response for a latest-price query. Money fields are **cents** (the inherited
/// Backend surface reported dollars as `f64`; `fauxchange` keeps cents).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LatestPriceResponse {
    /// The underlying ticker.
    pub symbol: String,
    /// The price in **cents**.
    #[schema(value_type = u64)]
    pub price_cents: Cents,
    /// Bid in **cents**, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub bid_cents: Option<Cents>,
    /// Ask in **cents**, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub ask_cents: Option<Cents>,
    /// Traded volume in **contracts**, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<u64>,
    /// The price time on the venue clock, in **milliseconds**.
    #[schema(value_type = u64)]
    pub timestamp: EventTimestamp,
}

// ============================================================================
// Hierarchy CRUD views
// ============================================================================

/// A best quote for one book. Prices are **cents**; sizes are **contracts**.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct QuoteResponse {
    /// Best bid price in **cents**, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub bid_price: Option<Cents>,
    /// Total size at the best bid, in **contracts**.
    pub bid_size: u64,
    /// Best ask price in **cents**, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub ask_price: Option<Cents>,
    /// Total size at the best ask, in **contracts**.
    pub ask_size: u64,
    /// Quote time on the venue clock, in **milliseconds**.
    #[schema(value_type = u64)]
    pub timestamp: EventTimestamp,
}

/// A per-book depth snapshot summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct OrderBookSnapshotResponse {
    /// The canonical contract symbol.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// Total bid depth, in **contracts**.
    pub total_bid_depth: u64,
    /// Total ask depth, in **contracts**.
    pub total_ask_depth: u64,
    /// Number of distinct bid price levels.
    pub bid_level_count: usize,
    /// Number of distinct ask price levels.
    pub ask_level_count: usize,
    /// Total resting order count.
    pub order_count: usize,
    /// The best quote.
    pub quote: QuoteResponse,
}

/// A summary of one underlying.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UnderlyingSummary {
    /// The underlying ticker.
    pub symbol: String,
    /// Number of expirations.
    pub expiration_count: usize,
    /// Total strike count across expirations.
    pub total_strike_count: usize,
    /// Total resting order count.
    pub total_order_count: usize,
}

/// Response for listing underlyings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UnderlyingsListResponse {
    /// The underlying tickers.
    pub underlyings: Vec<String>,
}

/// Response for deleting an underlying.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DeleteUnderlyingResponse {
    /// Whether the underlying was deleted.
    pub success: bool,
    /// Human-readable, client-safe message.
    pub message: String,
}

/// A summary of one expiration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExpirationSummary {
    /// The expiration date string (`YYYYMMDD`).
    pub expiration: String,
    /// Number of strikes.
    pub strike_count: usize,
    /// Total resting order count.
    pub total_order_count: usize,
}

/// Response for listing expirations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ExpirationsListResponse {
    /// The expiration date strings (`YYYYMMDD`).
    pub expirations: Vec<String>,
}

/// A summary of one strike (call and put books).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StrikeSummary {
    /// The strike in **whole units**.
    pub strike: u64,
    /// Resting call order count.
    pub call_order_count: usize,
    /// Resting put order count.
    pub put_order_count: usize,
    /// The call best quote.
    pub call_quote: QuoteResponse,
    /// The put best quote.
    pub put_quote: QuoteResponse,
}

/// Response for listing strikes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StrikesListResponse {
    /// The strikes in **whole units**.
    pub strikes: Vec<u64>,
}

// ============================================================================
// Chain matrix and volatility surface
// ============================================================================

/// Quote data for one option (call or put) in a chain row. Prices are
/// **cents**; the Greeks/IV fields are derived analytic floats.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct OptionQuoteData {
    /// Best bid in **cents**, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub bid: Option<Cents>,
    /// Best ask in **cents**, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub ask: Option<Cents>,
    /// Size at the best bid, in **contracts**.
    pub bid_size: u64,
    /// Size at the best ask, in **contracts**.
    pub ask_size: u64,
    /// Last trade price in **cents**, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub last_trade: Option<Cents>,
    /// Traded volume, in **contracts**.
    pub volume: u64,
    /// Delta — a derived analytic float, if computed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta: Option<f64>,
    /// Implied volatility — a derived analytic float, if computed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iv: Option<f64>,
}

/// One strike row of an option-chain matrix (call and put).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ChainStrikeRow {
    /// The strike in **whole units**.
    pub strike: u64,
    /// Call quote data.
    pub call: OptionQuoteData,
    /// Put quote data.
    pub put: OptionQuoteData,
}

/// Response for the option-chain matrix endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct OptionChainResponse {
    /// The underlying ticker.
    pub underlying: String,
    /// The expiration date string (`YYYYMMDD`).
    pub expiration: String,
    /// The current spot price in **cents**, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub spot_price: Option<Cents>,
    /// The at-the-money strike (closest to spot), if determinable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub atm_strike: Option<u64>,
    /// The chain rows (one per strike).
    pub chain: Vec<ChainStrikeRow>,
}

/// Implied volatility for one strike (call and put) — derived analytic floats.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StrikeIV {
    /// Call implied volatility, if computed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_iv: Option<f64>,
    /// Put implied volatility, if computed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put_iv: Option<f64>,
}

/// A point in the ATM term structure — derived analytic floats.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct AtmTermStructurePoint {
    /// The expiration date string (`YYYYMMDD`).
    pub expiration: String,
    /// Days to expiration.
    pub days: u64,
    /// ATM implied volatility.
    pub iv: f64,
}

/// Response for the volatility-surface endpoint. IV values are derived analytic
/// floats; the spot price is **cents**.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct VolatilitySurfaceResponse {
    /// The underlying ticker.
    pub underlying: String,
    /// The current spot price in **cents**, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub spot_price: Option<Cents>,
    /// Calculation time on the venue clock, in **milliseconds**.
    #[schema(value_type = u64)]
    pub timestamp: EventTimestamp,
    /// The expiration date strings (`YYYYMMDD`).
    pub expirations: Vec<String>,
    /// The strikes in **whole units**.
    pub strikes: Vec<u64>,
    /// The ATM term structure.
    pub atm_term_structure: Vec<AtmTermStructurePoint>,
}

// ============================================================================
// Greeks
// ============================================================================

/// Greeks for one option — **all derived analytic floats**, the documented
/// exception to the integer-cents rule ([01 §3](../docs/01-domain-model.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct GreeksData {
    /// Delta.
    pub delta: f64,
    /// Gamma.
    pub gamma: f64,
    /// Theta (daily).
    pub theta: f64,
    /// Vega.
    pub vega: f64,
    /// Rho.
    pub rho: f64,
}

/// Response for the Greeks endpoint — derived analytic floats plus a venue-clock
/// timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct GreeksResponse {
    /// The canonical contract symbol.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// The Greeks values.
    pub greeks: GreeksData,
    /// Implied volatility used in the calculation.
    pub iv: f64,
    /// Theoretical (Black-Scholes) option value — a pricing-model output, the
    /// documented Greeks/IV-family **derived analytic float** exception to the
    /// integer-cents rule, not a settled or executable price.
    pub theoretical_value: f64,
    /// Calculation time on the venue clock, in **milliseconds**.
    #[schema(value_type = u64)]
    pub timestamp: EventTimestamp,
}

// ============================================================================
// Orderbook metrics — mostly derived analytic floats, but any monetary average
// (book-side VWAP, market-impact average price) stays integer cents
// ============================================================================

/// Spread metrics — derived analytics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SpreadMetrics {
    /// Current spread in **cents**, if two-sided.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub current: Option<Cents>,
    /// Spread in basis points (analytic float).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spread_bps: Option<f64>,
}

/// Depth metrics — sizes in contracts, imbalance an analytic float.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DepthMetrics {
    /// Total bid depth, in **contracts**.
    pub bid_depth_total: u64,
    /// Total ask depth, in **contracts**.
    pub ask_depth_total: u64,
    /// Book imbalance in `[-1, 1]` (positive = more bids) — analytic float.
    pub imbalance: f64,
}

/// Price metrics for one book. `mid_price` / `micro_price` are **derived
/// analytic floats** (synthetic estimators, sub-cent by nature — the documented
/// Greeks/IV-family exception); `vwap_bid` / `vwap_ask` are volume-weighted
/// averages of the real, integer-cent book prices, so they are money and stay
/// **integer cents**.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PriceMetrics {
    /// Mid price `(bid + ask) / 2` — a synthetic midpoint, **derived analytic
    /// float** (not a settled amount; genuinely sub-cent on a one-tick market).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mid_price: Option<f64>,
    /// Micro price (size-weighted mid) — a synthetic estimator, **derived
    /// analytic float**.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub micro_price: Option<f64>,
    /// Bid-side VWAP in **integer cents** — the volume-weighted average of the
    /// real bid prices, truncated toward zero to the nearest cent (checked
    /// `u128` arithmetic). A monetary average, not a float-exempt analytic.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub vwap_bid: Option<Cents>,
    /// Ask-side VWAP in **integer cents** — the volume-weighted average of the
    /// real ask prices, truncated toward zero to the nearest cent (checked
    /// `u128` arithmetic).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub vwap_ask: Option<Cents>,
}

/// Market-impact metrics for one side. `avg_price` is the average execution
/// price of sweeping a fixed clip — a monetary value, so **integer cents**;
/// `slippage_bps` is a dimensionless basis-point ratio, a **derived analytic
/// float**.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ImpactMetrics {
    /// Average execution price of the swept clip, in **integer cents** — the
    /// volume-weighted average of the real levels consumed, truncated toward
    /// zero to the nearest cent (checked `u128` arithmetic).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub avg_price: Option<Cents>,
    /// Slippage from mid, in basis points — a **derived analytic float**.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slippage_bps: Option<f64>,
}

/// Market-impact metrics for both sides.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct MarketImpactMetrics {
    /// Impact of buying a fixed clip.
    pub buy: ImpactMetrics,
    /// Impact of selling a fixed clip.
    pub sell: ImpactMetrics,
}

/// Response for the orderbook-metrics endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct OrderbookMetricsResponse {
    /// The canonical contract symbol.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// Calculation time on the venue clock, in **milliseconds**.
    #[schema(value_type = u64)]
    pub timestamp: EventTimestamp,
    /// Spread metrics.
    pub spread: SpreadMetrics,
    /// Depth metrics.
    pub depth: DepthMetrics,
    /// Price metrics.
    pub prices: PriceMetrics,
    /// Market-impact metrics.
    pub market_impact: MarketImpactMetrics,
}

// ============================================================================
// OHLC
// ============================================================================

/// A single OHLC bar (candlestick). Prices are **cents**; volume is
/// **contracts**.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct OhlcBar {
    /// Bar start time, in **seconds** since the Unix epoch.
    pub timestamp: u64,
    /// Opening price in **cents**.
    #[schema(value_type = u64)]
    pub open: Cents,
    /// Highest price in **cents**.
    #[schema(value_type = u64)]
    pub high: Cents,
    /// Lowest price in **cents**.
    #[schema(value_type = u64)]
    pub low: Cents,
    /// Closing price in **cents**.
    #[schema(value_type = u64)]
    pub close: Cents,
    /// Volume traded in the bar, in **contracts**.
    pub volume: u64,
    /// Number of trades in the bar.
    pub trade_count: u64,
}

/// Response for the OHLC endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct OhlcResponse {
    /// The canonical contract symbol.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// The bar interval.
    pub interval: OhlcInterval,
    /// The bars.
    pub bars: Vec<OhlcBar>,
}

/// Query parameters for the OHLC endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct OhlcQuery {
    /// The bar interval.
    pub interval: OhlcInterval,
    /// Start time in **seconds**, optional.
    #[serde(default)]
    pub from: Option<u64>,
    /// End time in **seconds**, optional.
    #[serde(default)]
    pub to: Option<u64>,
    /// Maximum number of bars to return.
    #[serde(default)]
    pub limit: Option<usize>,
}

// ============================================================================
// Controls
// ============================================================================

/// Response for the system-control status endpoint. The market-maker knobs are
/// documented analytic floats.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SystemControlResponse {
    /// Whether the master (kill) switch is enabled.
    pub master_enabled: bool,
    /// Global spread multiplier.
    pub spread_multiplier: f64,
    /// Global size scalar (`0.0`–`1.0`).
    pub size_scalar: f64,
    /// Global directional skew (`-1.0`–`1.0`).
    pub directional_skew: f64,
}

/// Request body to set the kill switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct KillSwitchRequest {
    /// The desired master-enabled state (`false` disables all quoting).
    pub enabled: bool,
}

/// Response for a kill-switch or enable action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct KillSwitchResponse {
    /// Whether the action succeeded.
    pub success: bool,
    /// Human-readable, client-safe message.
    pub message: String,
    /// The current master-enabled state.
    pub master_enabled: bool,
}

/// Request body to update the market-maker parameters. All fields optional;
/// omitted fields are unchanged. Values are documented analytic floats.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateParametersRequest {
    /// New spread multiplier.
    #[serde(default)]
    pub spread_multiplier: Option<f64>,
    /// New size scalar (`0.0`–`1.0`).
    #[serde(default)]
    pub size_scalar: Option<f64>,
    /// New directional skew (`-1.0`–`1.0`).
    #[serde(default)]
    pub directional_skew: Option<f64>,
}

/// Response after updating the market-maker parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct UpdateParametersResponse {
    /// Whether the update succeeded.
    pub success: bool,
    /// The resulting spread multiplier.
    pub spread_multiplier: f64,
    /// The resulting size scalar.
    pub size_scalar: f64,
    /// The resulting directional skew.
    pub directional_skew: f64,
}

/// Response for an instrument status toggle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InstrumentToggleResponse {
    /// Whether the toggle command was **accepted and sequenced** — not a
    /// confirmation the halt/resume took effect (the applied/rejected outcome is
    /// not carried on the receipt until the `Receipt`→`VenueOutcome` seam lands).
    pub success: bool,
    /// The canonical contract symbol addressed.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// The **requested** enabled state (`true` = resume, `false` = halt) — the
    /// state the caller asked for, not an observed effect.
    pub enabled: bool,
    /// The `underlying_sequence` of the resulting event, for cross-surface
    /// correlation.
    #[schema(value_type = u64)]
    pub sequence: SequenceNumber,
}

/// A per-instrument control status entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InstrumentControlStatus {
    /// The canonical contract symbol.
    #[schema(value_type = String)]
    pub symbol: Symbol,
    /// Whether market-maker quoting is enabled for it.
    pub quoting_enabled: bool,
    /// The current mark price in **cents**, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<u64>)]
    pub current_price: Option<Cents>,
}

/// Response for listing instrument control statuses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct InstrumentsListResponse {
    /// The instrument control statuses.
    pub instruments: Vec<InstrumentControlStatus>,
}

// ============================================================================
// Admin snapshots
// ============================================================================

/// Response for creating an admin snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateSnapshotResponse {
    /// `true` when every book was captured; `false` for a partial snapshot.
    pub success: bool,
    /// The snapshot identifier.
    pub snapshot_id: String,
    /// Number of books captured.
    pub orderbooks_saved: u64,
    /// Total orders captured.
    pub orders_saved: u64,
    /// Number of books skipped.
    pub orderbooks_failed: u64,
    /// Capture time on the venue clock, in **milliseconds**.
    #[schema(value_type = u64)]
    pub timestamp: EventTimestamp,
}

/// A snapshot summary for listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SnapshotSummary {
    /// The snapshot identifier.
    pub snapshot_id: String,
    /// Number of books in the snapshot.
    pub orderbook_count: u64,
    /// Total orders in the snapshot.
    pub total_orders: u64,
    /// Creation time on the venue clock, in **milliseconds**.
    #[schema(value_type = u64)]
    pub created_at: EventTimestamp,
}

/// Response for listing snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SnapshotsListResponse {
    /// The snapshot summaries.
    pub snapshots: Vec<SnapshotSummary>,
    /// Total number of snapshots.
    pub total: u64,
}

/// Response for restoring an admin snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RestoreSnapshotResponse {
    /// `true` when every book was restored; `false` for a partial restore.
    pub success: bool,
    /// The snapshot identifier restored.
    pub snapshot_id: String,
    /// Number of books restored.
    pub orderbooks_restored: u64,
    /// Total orders restored (as counted at capture time).
    pub orders_restored: u64,
    /// Number of books that could not be restored.
    pub orderbooks_failed: u64,
    /// Restore time on the venue clock, in **milliseconds**.
    #[schema(value_type = u64)]
    pub timestamp: EventTimestamp,
}

// ============================================================================
// Auth and meta
// ============================================================================

/// Request body for `POST /api/v1/auth/token`, gated by the operator bootstrap
/// secret ([03 §6](../docs/03-protocol-surfaces.md)).
///
/// [`std::fmt::Debug`] is **hand-rolled to redact** the bootstrap secret — a
/// derived `Debug` would print it in a log/error line
/// ([08 §7](../docs/08-threat-model.md#7-secrets-handling)), matching the
/// redacting pattern on `BootstrapGate` / `AccountProvision` in
/// [`crate::auth`].
#[derive(Clone, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TokenRequest {
    /// The operator bootstrap secret (`AUTH_BOOTSTRAP_SECRET`). A secret —
    /// never logged or echoed (redacted in `Debug`).
    pub secret: String,
    /// The account to mint a token for (its `sub`).
    pub account: AccountId,
    /// Permissions to embed (`Admin` implies all).
    pub permissions: Vec<Permission>,
    /// Optional token lifetime, in **seconds** (defaults to the server TTL).
    #[serde(default)]
    pub ttl_secs: Option<u64>,
}

impl std::fmt::Debug for TokenRequest {
    /// Redacts the bootstrap `secret` so it never reaches a log or error line.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenRequest")
            .field("secret", &"<redacted>")
            .field("account", &self.account)
            .field("permissions", &self.permissions)
            .field("ttl_secs", &self.ttl_secs)
            .finish()
    }
}

/// Response for `POST /api/v1/auth/token`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenResponse {
    /// The signed JWT — use as `Authorization: Bearer <token>`. A secret.
    pub token: String,
    /// Expiry as an ISO-8601 / RFC3339 UTC timestamp.
    pub expires_at: String,
}

/// Health-check response (the auth-exempt `GET /health`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct HealthResponse {
    /// Service status (`"ok"`).
    pub status: String,
    /// Service version.
    pub version: String,
}

/// Global venue statistics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GlobalStatsResponse {
    /// Number of underlyings.
    pub underlying_count: usize,
    /// Total expirations.
    pub total_expirations: usize,
    /// Total strikes.
    pub total_strikes: usize,
    /// Total resting orders.
    pub total_orders: usize,
}

// ============================================================================
// WebSocket protocol
// ============================================================================

/// A price level in an orderbook snapshot. Price is **cents**; quantity is
/// **contracts**.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PriceLevelData {
    /// Price in **cents**.
    #[schema(value_type = u64)]
    pub price: Cents,
    /// Total resting quantity at this level, in **contracts**.
    pub quantity: u64,
}

/// A price-level change in an orderbook delta — **resulting-quantity**
/// semantics (`quantity == 0` means the level was removed,
/// [03 §4](../docs/03-protocol-surfaces.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PriceLevelChange {
    /// The book side of the level.
    pub side: BookSide,
    /// Price in **cents**.
    #[schema(value_type = u64)]
    pub price: Cents,
    /// The resulting total quantity at this level, in **contracts** (`0` =
    /// removed).
    pub quantity: u64,
}

/// The result of one subscription action in a batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SubscriptionResult {
    /// The channel acted on.
    pub channel: SubscriptionChannel,
    /// The symbol or filter, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// The underlying filter, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub underlying: Option<String>,
    /// Status (`"ok"` or an error message).
    pub status: String,
}

/// An active subscription entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ActiveSubscription {
    /// The channel.
    pub channel: SubscriptionChannel,
    /// The symbol or filter, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// The underlying filter, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub underlying: Option<String>,
    /// The orderbook depth requested, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub depth: Option<usize>,
}

/// A server → client WebSocket message ([03 §4](../docs/03-protocol-surfaces.md)).
///
/// Internally-adjacently tagged (`#[serde(tag = "type", content = "data")]`):
/// each variant serialises as `{ "type": "<discriminant>", "data": { … } }`
/// with the `type` discriminant fixed per variant (golden-tested). Money is
/// **cents**; timestamps are venue-clock **milliseconds**.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", content = "data")]
pub enum WsMessage {
    /// Connection established (welcome).
    #[serde(rename = "connected")]
    Connected {
        /// Welcome message.
        message: String,
    },
    /// Liveness heartbeat.
    #[serde(rename = "heartbeat")]
    Heartbeat {
        /// Timestamp on the venue clock, in **milliseconds**.
        #[schema(value_type = u64)]
        timestamp: EventTimestamp,
    },
    /// A quote update for one contract.
    #[serde(rename = "quote")]
    Quote {
        /// The canonical contract symbol.
        #[schema(value_type = String)]
        symbol: Symbol,
        /// The expiration date string (`YYYYMMDD`).
        expiration: String,
        /// The strike in **whole units**.
        strike: u64,
        /// Call or put.
        style: OptionStyle,
        /// Best bid in **cents**, if any.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(value_type = Option<u64>)]
        bid_price: Option<Cents>,
        /// Best ask in **cents**, if any.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(value_type = Option<u64>)]
        ask_price: Option<Cents>,
        /// Size at the best bid, in **contracts**.
        bid_size: u64,
        /// Size at the best ask, in **contracts**.
        ask_size: u64,
    },
    /// An underlying price update.
    #[serde(rename = "price")]
    Price {
        /// The underlying ticker.
        symbol: String,
        /// Price in **cents**.
        #[schema(value_type = u64)]
        price_cents: Cents,
    },
    /// A market-maker configuration change. The knobs are documented analytic
    /// floats.
    #[serde(rename = "config")]
    Config {
        /// Whether quoting is enabled.
        enabled: bool,
        /// Spread multiplier.
        spread_multiplier: f64,
        /// Size scalar (`0.0`–`1.0`).
        size_scalar: f64,
        /// Directional skew (`-1.0`–`1.0`).
        directional_skew: f64,
    },
    /// A **public, anonymised** fill print ([03 §4](../docs/03-protocol-surfaces.md)).
    ///
    /// Carries the four cross-surface join keys (`execution_id`,
    /// `underlying_sequence`, `venue_ts`, `liquidity`) but **omits** `account`
    /// and `fee` — account-scoped detail is authenticated REST/FIX only
    /// ([`ExecutionRecord`]).
    #[serde(rename = "fill")]
    Fill {
        /// The composite execution id (join key).
        execution_id: ExecutionId,
        /// The `underlying_sequence` (order key).
        #[schema(value_type = u64)]
        underlying_sequence: SequenceNumber,
        /// The venue-clock print time, in **milliseconds** (join key).
        #[schema(value_type = u64)]
        venue_ts: EventTimestamp,
        /// The leg's role (join key).
        liquidity: LiquidityFlag,
        /// The underlying ticker.
        symbol: String,
        /// The canonical contract identifier.
        #[schema(value_type = String)]
        instrument: Symbol,
        /// The leg's side.
        side: Side,
        /// Filled quantity, in **contracts**.
        quantity: u64,
        /// Fill price in **cents**.
        #[schema(value_type = u64)]
        price: Cents,
        /// Capture versus quote-time theoretical, in **cents per contract**
        /// (signed).
        #[schema(value_type = i64)]
        edge: SignedCents,
    },
    /// A full orderbook snapshot.
    #[serde(rename = "orderbook_snapshot")]
    OrderbookSnapshot {
        /// The channel (`orderbook`).
        channel: SubscriptionChannel,
        /// The canonical contract symbol.
        #[schema(value_type = String)]
        symbol: Symbol,
        /// The per-instrument `instrument_sequence` baseline
        /// ([01 §9.1](../docs/01-domain-model.md)).
        sequence: u64,
        /// Bid levels.
        bids: Vec<PriceLevelData>,
        /// Ask levels.
        asks: Vec<PriceLevelData>,
    },
    /// An incremental orderbook delta (resulting-quantity semantics).
    #[serde(rename = "orderbook_delta")]
    OrderbookDelta {
        /// The canonical contract symbol.
        #[schema(value_type = String)]
        symbol: Symbol,
        /// The per-instrument `instrument_sequence`, strictly increasing from
        /// the snapshot baseline.
        sequence: u64,
        /// The price-level changes.
        changes: Vec<PriceLevelChange>,
    },
    /// A public trade print.
    #[serde(rename = "trade")]
    Trade {
        /// The trade identifier.
        trade_id: String,
        /// The canonical contract symbol.
        #[schema(value_type = String)]
        symbol: Symbol,
        /// Execution price in **cents**.
        #[schema(value_type = u64)]
        price: Cents,
        /// Executed quantity, in **contracts**.
        quantity: u64,
        /// The venue-clock print time, in **milliseconds**.
        #[schema(value_type = u64)]
        timestamp: EventTimestamp,
        /// The maker order id.
        maker_order_id: VenueOrderId,
        /// The taker order id.
        taker_order_id: VenueOrderId,
    },
    /// A single-subscription confirmation.
    #[serde(rename = "subscribed")]
    Subscribed {
        /// The channel.
        channel: SubscriptionChannel,
        /// The symbol subscribed to.
        symbol: String,
    },
    /// A single-unsubscription confirmation.
    #[serde(rename = "unsubscribed")]
    Unsubscribed {
        /// The channel.
        channel: SubscriptionChannel,
        /// The symbol unsubscribed from.
        symbol: String,
    },
    /// A batch-subscription result.
    #[serde(rename = "batch_subscribed")]
    BatchSubscribed {
        /// Correlation id echoed from the request, when present.
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        /// The per-action results.
        subscriptions: Vec<SubscriptionResult>,
    },
    /// A batch-unsubscription result.
    #[serde(rename = "batch_unsubscribed")]
    BatchUnsubscribed {
        /// Correlation id echoed from the request, when present.
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        /// The per-action results.
        subscriptions: Vec<SubscriptionResult>,
    },
    /// The active-subscription list.
    #[serde(rename = "subscriptions")]
    SubscriptionList {
        /// The active subscriptions.
        active: Vec<ActiveSubscription>,
    },
    /// A typed error envelope ([03 §4.2](../docs/03-protocol-surfaces.md)) — the
    /// versioned [`WsError`] from the shared error boundary (#003), reused
    /// verbatim as the payload.
    #[serde(rename = "error")]
    Error(WsError),
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Permission casing (inherited Backend wire) -----------------------

    #[test]
    fn test_permission_serializes_lowercase() {
        for (permission, expected) in [
            (Permission::Read, "\"read\""),
            (Permission::Trade, "\"trade\""),
            (Permission::Admin, "\"admin\""),
        ] {
            match serde_json::to_string(&permission) {
                Ok(json) => assert_eq!(json, expected),
                Err(e) => panic!("serialize failed for {permission:?}: {e}"),
            }
        }
    }

    #[test]
    fn test_permission_deserializes_from_lowercase() {
        match serde_json::from_str::<Permission>("\"trade\"") {
            Ok(permission) => assert_eq!(permission, Permission::Trade),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }

    // ---- Enum casing contracts --------------------------------------------

    #[test]
    fn test_side_serializes_lowercase() {
        match serde_json::to_string(&Side::Buy) {
            Ok(json) => assert_eq!(json, "\"buy\""),
            Err(e) => panic!("serialize failed: {e}"),
        }
    }

    #[test]
    fn test_option_style_serializes_lowercase() {
        match serde_json::to_string(&OptionStyle::Put) {
            Ok(json) => assert_eq!(json, "\"put\""),
            Err(e) => panic!("serialize failed: {e}"),
        }
    }

    #[test]
    fn test_order_type_serializes_snake_case() {
        match serde_json::to_string(&OrderType::Market) {
            Ok(json) => assert_eq!(json, "\"market\""),
            Err(e) => panic!("serialize failed: {e}"),
        }
    }

    #[test]
    fn test_time_in_force_serializes_uppercase() {
        match serde_json::to_string(&TimeInForce::Gtc) {
            Ok(json) => assert_eq!(json, "\"GTC\""),
            Err(e) => panic!("serialize failed: {e}"),
        }
    }

    #[test]
    fn test_liquidity_flag_serializes_snake_case() {
        for (flag, expected) in [
            (LiquidityFlag::Maker, "\"maker\""),
            (LiquidityFlag::Taker, "\"taker\""),
        ] {
            match serde_json::to_string(&flag) {
                Ok(json) => assert_eq!(json, expected),
                Err(e) => panic!("serialize failed for {flag:?}: {e}"),
            }
        }
    }

    #[test]
    fn test_order_status_serializes_lowercase() {
        match serde_json::to_string(&OrderStatus::Canceled) {
            Ok(json) => assert_eq!(json, "\"canceled\""),
            Err(e) => panic!("serialize failed: {e}"),
        }
    }

    #[test]
    fn test_book_side_serializes_lowercase() {
        match serde_json::to_string(&BookSide::Bid) {
            Ok(json) => assert_eq!(json, "\"bid\""),
            Err(e) => panic!("serialize failed: {e}"),
        }
    }

    #[test]
    fn test_time_in_force_default_is_gtc() {
        assert_eq!(TimeInForce::default(), TimeInForce::Gtc);
    }

    #[test]
    fn test_ohlc_interval_serializes_abbreviated_label() {
        match serde_json::to_string(&OhlcInterval::FifteenMinutes) {
            Ok(json) => assert_eq!(json, "\"15m\""),
            Err(e) => panic!("serialize failed: {e}"),
        }
    }

    #[test]
    fn test_ohlc_interval_seconds_are_exact() {
        assert_eq!(OhlcInterval::OneMinute.seconds(), 60);
        assert_eq!(OhlcInterval::OneDay.seconds(), 86_400);
    }

    // ---- Identity newtypes ------------------------------------------------

    #[test]
    fn test_account_id_serializes_as_bare_string() {
        match serde_json::to_string(&AccountId::new("acct-1")) {
            Ok(json) => assert_eq!(json, "\"acct-1\""),
            Err(e) => panic!("serialize failed: {e}"),
        }
    }

    #[test]
    fn test_execution_id_roundtrips_as_bare_string() {
        let id = ExecutionId::new("lin:BTC:7:0");
        let json = match serde_json::to_string(&id) {
            Ok(j) => j,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(json, "\"lin:BTC:7:0\"");
        match serde_json::from_str::<ExecutionId>(&json) {
            Ok(back) => assert_eq!(back.as_str(), "lin:BTC:7:0"),
            Err(e) => panic!("deserialize failed: {e}"),
        }
    }

    // ---- Order-shape validation (happy + every documented rejection) ------

    #[test]
    fn test_validate_order_shape_accepts_valid_limit() {
        assert!(validate_order_shape(OrderType::Limit, Some(Cents::new(500)), 10).is_ok());
    }

    #[test]
    fn test_validate_order_shape_accepts_valid_market() {
        assert!(validate_order_shape(OrderType::Market, None, 10).is_ok());
    }

    #[test]
    fn test_validate_order_shape_rejects_limit_without_price() {
        match validate_order_shape(OrderType::Limit, None, 10) {
            Err(VenueError::InvalidOrder(msg)) => assert!(msg.contains("limit price")),
            other => panic!("expected InvalidOrder, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_order_shape_rejects_market_with_price() {
        match validate_order_shape(OrderType::Market, Some(Cents::new(500)), 10) {
            Err(VenueError::InvalidOrder(msg)) => assert!(msg.contains("market order")),
            other => panic!("expected InvalidOrder, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_order_shape_rejects_zero_quantity() {
        match validate_order_shape(OrderType::Limit, Some(Cents::new(500)), 0) {
            Err(VenueError::InvalidOrder(msg)) => assert!(msg.contains("quantity")),
            other => panic!("expected InvalidOrder, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_order_shape_rejects_zero_limit_price() {
        match validate_order_shape(OrderType::Limit, Some(Cents::new(0)), 10) {
            Err(VenueError::InvalidOrder(msg)) => assert!(msg.contains("limit price must be")),
            other => panic!("expected InvalidOrder, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_order_shape_rejects_price_above_max_price_cents() {
        // The venue-owned max accepted/resting price ceiling: a price one cent over
        // is a typed 400, not accepted (caught before the sequenced path).
        match validate_order_shape(OrderType::Limit, Some(Cents::new(MAX_PRICE_CENTS + 1)), 10) {
            Err(VenueError::InvalidOrder(msg)) => assert!(msg.contains("MAX_PRICE_CENTS")),
            other => panic!("expected InvalidOrder, got {other:?}"),
        }
        // Exactly at the ceiling is accepted.
        assert!(
            validate_order_shape(OrderType::Limit, Some(Cents::new(MAX_PRICE_CENTS)), 10).is_ok()
        );
    }

    #[test]
    fn test_validate_order_shape_rejects_quantity_above_max_order_quantity() {
        // The lot ceiling: over-limit quantity is a typed 400 on both order kinds.
        match validate_order_shape(
            OrderType::Limit,
            Some(Cents::new(500)),
            MAX_ORDER_QUANTITY + 1,
        ) {
            Err(VenueError::InvalidOrder(msg)) => assert!(msg.contains("MAX_ORDER_QUANTITY")),
            other => panic!("expected InvalidOrder, got {other:?}"),
        }
        match validate_order_shape(OrderType::Market, None, MAX_ORDER_QUANTITY + 1) {
            Err(VenueError::InvalidOrder(msg)) => assert!(msg.contains("MAX_ORDER_QUANTITY")),
            other => panic!("expected InvalidOrder, got {other:?}"),
        }
        // Exactly at the ceiling is accepted.
        assert!(validate_order_shape(OrderType::Market, None, MAX_ORDER_QUANTITY).is_ok());
    }

    #[test]
    fn test_max_accepted_notional_fits_i64_keeping_fee_math_off_both_branches() {
        // The fee-bound invariant the compile-time assertion pins: the widest
        // accepted notional fits i64, so the per-leg fee narrowing never overflows.
        let max_notional = u128::from(MAX_PRICE_CENTS) * u128::from(MAX_ORDER_QUANTITY);
        assert!(max_notional <= i64::MAX as u128);
    }

    #[test]
    fn test_place_limit_order_request_validate_rejects_price_over_ceiling() {
        let req = PlaceLimitOrderRequest {
            side: Side::Buy,
            price: Cents::new(MAX_PRICE_CENTS + 1),
            quantity: 5,
            time_in_force: None,
            gtd_expires_at: None,
            client_order_id: None,
        };
        assert!(req.validate().is_err());
    }

    #[test]
    fn test_place_limit_order_request_validate_delegates() {
        let ok = PlaceLimitOrderRequest {
            side: Side::Buy,
            price: Cents::new(500),
            quantity: 5,
            time_in_force: None,
            gtd_expires_at: None,
            client_order_id: None,
        };
        assert!(ok.validate().is_ok());
        let bad = PlaceLimitOrderRequest {
            price: Cents::new(0),
            ..ok
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn test_place_market_order_request_validate_rejects_zero_quantity() {
        let req = PlaceMarketOrderRequest {
            side: Side::Sell,
            quantity: 0,
            client_order_id: None,
        };
        assert!(req.validate().is_err());
    }

    // ---- deny_unknown_fields on request DTOs ------------------------------

    #[test]
    fn test_place_limit_order_request_rejects_unknown_field() {
        let json = r#"{"side":"buy","price":500,"quantity":10,"typo":true}"#;
        match serde_json::from_str::<PlaceLimitOrderRequest>(json) {
            Err(_) => {}
            Ok(parsed) => panic!("expected an unknown-field rejection, parsed {parsed:?}"),
        }
    }

    #[test]
    fn test_token_request_rejects_unknown_field() {
        let json = r#"{"secret":"s","account":"a","permissions":["trade"],"extra":1}"#;
        match serde_json::from_str::<TokenRequest>(json) {
            Err(_) => {}
            Ok(parsed) => panic!("expected an unknown-field rejection, parsed {parsed:?}"),
        }
    }

    #[test]
    fn test_token_request_debug_redacts_the_bootstrap_secret() {
        let request = TokenRequest {
            secret: "SUPER-SECRET-BOOTSTRAP-VALUE".to_string(),
            account: AccountId::new("acct-1"),
            permissions: vec![Permission::Trade],
            ttl_secs: Some(3_600),
        };
        let debug = format!("{request:?}");
        assert!(
            !debug.contains("SUPER-SECRET-BOOTSTRAP-VALUE"),
            "Debug must not leak the bootstrap secret, got: {debug}"
        );
        assert!(debug.contains("<redacted>"));
        // The non-secret fields still render, so Debug stays useful.
        assert!(debug.contains("acct-1"));
    }

    // ---- Money serialises as bare integer cents ---------------------------

    #[test]
    fn test_place_limit_order_request_price_is_bare_integer_cents() {
        let req = PlaceLimitOrderRequest {
            side: Side::Buy,
            price: Cents::new(50_000),
            quantity: 3,
            time_in_force: Some(TimeInForce::Gtc),
            gtd_expires_at: None,
            client_order_id: None,
        };
        let value = match serde_json::to_value(&req) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(value["price"], serde_json::json!(50_000));
        assert!(
            value["price"].is_u64(),
            "price must be an integer, not a float"
        );
    }

    // ---- WsMessage tag/content discriminants ------------------------------

    #[test]
    fn test_ws_message_price_has_type_discriminant_and_integer_cents() {
        let msg = WsMessage::Price {
            symbol: "BTC".to_string(),
            price_cents: Cents::new(4_200_000),
        };
        let value = match serde_json::to_value(&msg) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(value["type"], serde_json::json!("price"));
        assert_eq!(value["data"]["price_cents"], serde_json::json!(4_200_000));
        assert!(value["data"]["price_cents"].is_u64());
    }

    #[test]
    fn test_ws_message_fill_is_anonymised() {
        let symbol = match Symbol::parse("BTC-20240329-50000-C") {
            Ok(s) => s,
            Err(e) => panic!("symbol parse failed: {e:?}"),
        };
        let msg = WsMessage::Fill {
            execution_id: ExecutionId::new("lin:BTC:7:0"),
            underlying_sequence: SequenceNumber::new(7),
            venue_ts: EventTimestamp::new(1_700_000_000_000),
            liquidity: LiquidityFlag::Taker,
            symbol: "BTC".to_string(),
            instrument: symbol,
            side: Side::Buy,
            quantity: 2,
            price: Cents::new(50_000),
            edge: SignedCents::new(-125),
        };
        let value = match serde_json::to_value(&msg) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(value["type"], serde_json::json!("fill"));
        // The public print omits account-scoped detail.
        assert!(value["data"].get("account").is_none());
        assert!(value["data"].get("fee").is_none());
        // But carries the four join keys.
        assert_eq!(
            value["data"]["execution_id"],
            serde_json::json!("lin:BTC:7:0")
        );
        assert_eq!(value["data"]["underlying_sequence"], serde_json::json!(7));
        assert_eq!(value["data"]["liquidity"], serde_json::json!("taker"));
    }

    #[test]
    fn test_ws_message_error_reuses_ws_error_envelope() {
        let env = VenueError::Forbidden(Permission::Trade).ws_error(None);
        let msg = WsMessage::Error(env);
        let value = match serde_json::to_value(&msg) {
            Ok(v) => v,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(value["type"], serde_json::json!("error"));
        assert_eq!(value["data"]["schema"], serde_json::json!("ws-error.v1"));
        assert_eq!(value["data"]["code"], serde_json::json!("forbidden"));
    }
}
