//! Order-entry handlers — the `fauxchange` seam.
//!
//! Every order mutation is **re-pointed onto the sequenced order path**: the
//! handler translates the request into a [`VenueCommand`] and submits it through
//! [`AppState::submit`] (the sole entry to the single-writer actor), then renders
//! the result back — it NEVER calls the upstream books directly
//! ([03 §2](../../../docs/03-protocol-surfaces.md#2-the-shared-order-path)). Each
//! order-mutating response carries the resulting event's `underlying_sequence`
//! so a REST client can correlate with the WS/FIX fan-out.
//!
//! Operation class: place / cancel / replace / bulk are **Sequenced venue
//! commands** requiring [`Permission::Trade`]
//! ([03 §10](../../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
//!
//! **Receipt limitation (a `matching-expert` seam dependency).** `AppState::submit`
//! returns a [`Receipt`](crate::exchange::Receipt) carrying only the assigned
//! `underlying_sequence` + `venue_ts`, NOT the lossless `VenueOutcome`. Fill
//! counts are read back from the shared executions store (accurate for crossing
//! orders), and the limit-order status is derived from those fills **plus the
//! time-in-force** (`limit_status`) so a killed `IOC`/`FOK` is reported
//! `Rejected`, never a false `Accepted`. The disambiguations that still need the
//! outcome — a cancel `found` vs `not-found`, a `SetInstrumentStatus`
//! applied vs rejected — are reported as **accepted and sequenced** (with the
//! `underlying_sequence`), never as a confirmed effect, until the order path
//! surfaces the outcome in the receipt.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, Query, State};

use crate::auth::Authorized;
use crate::error::VenueError;
use crate::exchange::{STPMode, SymbolParser, VenueCommand};
use crate::gateway::rest::middleware::require;
use crate::gateway::rest::support::{
    add_order_command, build_symbol, immediate_fills, mint_order_id, owner_for, parse_style,
    seam_side, seam_tif, vwap_cents,
};
use crate::models::{
    BulkCancelRequest, BulkCancelResponse, BulkCancelResultItem, BulkOrderRequest,
    BulkOrderResponse, BulkOrderResultItem, BulkOrderStatus, CancelAllQuery, CancelAllResponse,
    CancelOrderResponse, FillPrint, LimitOrderStatus, MAX_BULK_CANCEL_ITEMS, MAX_BULK_ORDER_ITEMS,
    MarketOrderStatus, ModifyOrderRequest, ModifyOrderResponse, OrderListQuery, OrderListResponse,
    OrderType, Permission, PlaceLimitOrderRequest, PlaceLimitOrderResponse,
    PlaceMarketOrderRequest, PlaceMarketOrderResponse, TimeInForce,
};
use crate::state::AppState;

/// The per-contract path segments `(underlying, expiration, strike, style)`.
type ContractPath = (String, String, u64, String);
/// The per-contract path plus a trailing `{order_id}`.
type OrderPath = (String, String, u64, String, String);

/// The remainder of `total` after `taken`. A fill can never exceed the order
/// quantity, so the defaulted-to-`0` arm is unreachable; this is explicit
/// checked handling. The repo rules forbid `saturating_*` (it silently hides
/// overflow), so clippy's `manual_saturating_arithmetic` suggestion — which
/// would reintroduce `saturating_sub` — is allowed here (matching
/// `RateLimiter::decide`).
#[allow(clippy::manual_saturating_arithmetic)]
#[inline]
fn remaining(total: u64, taken: u64) -> u64 {
    total.checked_sub(taken).unwrap_or_default()
}

/// The honest limit-order status derivable from the **observed fills** and the
/// **time-in-force alone** — the only signals the gateway has (the
/// `Receipt` carries no `VenueOutcome`, so resting vs killed cannot be observed
/// directly). `GTC`/`GTD` rest their unfilled remainder, so a zero-fill result
/// is `Accepted`; `IOC`/`FOK` never rest, so a zero-fill result is `Rejected`
/// (killed) — this is what fixes the "FOK-killed reported as Accepted" hazard.
fn limit_status(tif: TimeInForce, filled: u64, quantity: u64) -> (LimitOrderStatus, &'static str) {
    if filled >= quantity {
        return (LimitOrderStatus::Filled, "order filled");
    }
    match tif {
        // Fill-or-kill is all-or-nothing: an unfilled result was killed.
        TimeInForce::Fok if filled == 0 => (
            LimitOrderStatus::Rejected,
            "fill-or-kill not fillable; killed",
        ),
        TimeInForce::Fok => (
            LimitOrderStatus::Partial,
            "fill-or-kill partial; remainder killed",
        ),
        // Immediate-or-cancel never rests: an unfilled result was killed, a
        // partial fill had its remainder canceled.
        TimeInForce::Ioc if filled == 0 => (
            LimitOrderStatus::Rejected,
            "immediate-or-cancel not marketable; killed",
        ),
        TimeInForce::Ioc => (
            LimitOrderStatus::Partial,
            "immediate-or-cancel partial; remainder canceled",
        ),
        // Good-'til-canceled / -date rest the unfilled remainder.
        TimeInForce::Gtc | TimeInForce::Gtd if filled == 0 => {
            (LimitOrderStatus::Accepted, "order accepted; resting")
        }
        TimeInForce::Gtc | TimeInForce::Gtd => (
            LimitOrderStatus::Partial,
            "order partially filled; remainder resting",
        ),
    }
}

/// Place a resting limit order on one contract — a **Sequenced** `AddOrder`
/// requiring `Trade`.
#[utoipa::path(
    post,
    path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/orders",
    tag = "orders",
    params(
        ("underlying" = String, Path, description = "Underlying ticker (e.g. BTC)"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
    ),
    request_body = PlaceLimitOrderRequest,
    responses(
        (status = 200, description = "Order accepted (carries underlying_sequence)", body = PlaceLimitOrderResponse),
        (status = 400, description = "Invalid order shape or symbol"),
        (status = 401, description = "Missing or invalid token"),
        (status = 403, description = "Missing Trade permission"),
        (status = 429, description = "Rate limited"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn place_limit_order(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path((underlying, expiration, strike, style)): Path<ContractPath>,
    Json(request): Json<PlaceLimitOrderRequest>,
) -> Result<Json<PlaceLimitOrderResponse>, VenueError> {
    require(&auth, Permission::Trade)?;
    request.validate()?;

    let style = parse_style(&style)?;
    let symbol = build_symbol(&underlying, &expiration, strike, style)?;
    let account = auth.claims.account().clone();
    let owner = owner_for(&state, &account)?;
    let tif = seam_tif(
        request.time_in_force.unwrap_or_default(),
        request.gtd_expires_at,
    )?;
    let order_id = mint_order_id(state.lineage_id(), &underlying);

    let receipt = state
        .submit(add_order_command(
            symbol,
            order_id.clone(),
            account.clone(),
            owner,
            request.client_order_id.clone(),
            seam_side(request.side),
            OrderType::Limit,
            Some(request.price),
            request.quantity,
            tif,
        ))
        .await?;

    let fills = immediate_fills(&state, &account, &order_id, receipt.underlying_sequence);
    let filled: u64 = fills.iter().map(|(_, q)| q).sum();
    let (status, message) = limit_status(
        request.time_in_force.unwrap_or_default(),
        filled,
        request.quantity,
    );

    Ok(Json(PlaceLimitOrderResponse {
        order_id,
        status,
        filled_quantity: filled,
        remaining_quantity: remaining(request.quantity, filled),
        sequence: receipt.underlying_sequence,
        message: message.to_string(),
    }))
}

/// Submit a market order on one contract — a **Sequenced** market `AddOrder`
/// requiring `Trade`.
#[utoipa::path(
    post,
    path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/orders/market",
    tag = "orders",
    params(
        ("underlying" = String, Path, description = "Underlying ticker (e.g. BTC)"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
    ),
    request_body = PlaceMarketOrderRequest,
    responses(
        (status = 200, description = "Market order outcome (carries underlying_sequence)", body = PlaceMarketOrderResponse),
        (status = 400, description = "Invalid order shape or symbol"),
        (status = 401, description = "Missing or invalid token"),
        (status = 403, description = "Missing Trade permission"),
        (status = 429, description = "Rate limited"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn place_market_order(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path((underlying, expiration, strike, style)): Path<ContractPath>,
    Json(request): Json<PlaceMarketOrderRequest>,
) -> Result<Json<PlaceMarketOrderResponse>, VenueError> {
    require(&auth, Permission::Trade)?;
    request.validate()?;

    let style = parse_style(&style)?;
    let symbol = build_symbol(&underlying, &expiration, strike, style)?;
    let account = auth.claims.account().clone();
    let owner = owner_for(&state, &account)?;
    let order_id = mint_order_id(state.lineage_id(), &underlying);

    let receipt = state
        .submit(add_order_command(
            symbol,
            order_id.clone(),
            account.clone(),
            owner,
            request.client_order_id.clone(),
            seam_side(request.side),
            OrderType::Market,
            None,
            request.quantity,
            crate::exchange::TimeInForce::Ioc,
        ))
        .await?;

    let fills = immediate_fills(&state, &account, &order_id, receipt.underlying_sequence);
    // Checked fold (never `Iterator::sum`, which panics-in-debug / wraps-in-release):
    // the filled quantity is bounded by the order quantity, so overflow is
    // unreachable, but the arithmetic stays checked per rules/global_rules.md.
    let filled: u64 = fills
        .iter()
        .try_fold(0u64, |acc, (_, q)| acc.checked_add(*q))
        .ok_or(VenueError::Overflow)?;
    let status = if filled == 0 {
        MarketOrderStatus::Rejected
    } else if filled >= request.quantity {
        MarketOrderStatus::Filled
    } else {
        MarketOrderStatus::Partial
    };
    let average_price = vwap_cents(&fills)?;
    let fill_prints = fills
        .into_iter()
        .map(|(price, quantity)| FillPrint { price, quantity })
        .collect();

    Ok(Json(PlaceMarketOrderResponse {
        order_id,
        status,
        filled_quantity: filled,
        remaining_quantity: remaining(request.quantity, filled),
        average_price,
        sequence: receipt.underlying_sequence,
        fills: fill_prints,
    }))
}

/// Cancel a resting order on one contract — a **Sequenced** `CancelOrder`
/// requiring `Trade`. The full contract symbol comes from the path.
#[utoipa::path(
    delete,
    path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/orders/{order_id}",
    tag = "orders",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
        ("order_id" = String, Path, description = "The venue order id to cancel"),
    ),
    responses(
        (status = 200, description = "Cancel accepted and sequenced", body = CancelOrderResponse),
        (status = 400, description = "Invalid symbol"),
        (status = 401, description = "Missing or invalid token"),
        (status = 403, description = "Missing Trade permission"),
        (status = 429, description = "Rate limited"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn cancel_order(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path((underlying, expiration, strike, style, order_id)): Path<OrderPath>,
) -> Result<Json<CancelOrderResponse>, VenueError> {
    require(&auth, Permission::Trade)?;
    let style = parse_style(&style)?;
    let symbol = build_symbol(&underlying, &expiration, strike, style)?;
    let account = auth.claims.account().clone();

    let receipt = state
        .submit(VenueCommand::CancelOrder {
            symbol,
            order_id: crate::models::VenueOrderId::new(order_id),
            account,
        })
        .await?;

    // The receipt confirms the cancel was accepted and sequenced; the
    // found/not-found outcome is not carried on the receipt (see module docs).
    // `sequence` is the typed cross-surface correlation key (#018 cannot parse
    // prose); `success` means accepted-and-sequenced, not confirmed-removed.
    Ok(Json(CancelOrderResponse {
        success: true,
        sequence: receipt.underlying_sequence,
        message: "cancel accepted and sequenced".to_string(),
    }))
}

/// Modify a resting order (price/quantity). **Not yet servable**: the sequenced
/// path has no atomic modify — an in-place change is a non-atomic `Replace`,
/// which needs the resting order's side and time-in-force, and those are not in
/// the request nor readable from the single-writer book yet. Returns a typed
/// `400` directing the client to cancel and re-place; wiring this needs the
/// actor's book-read path (a `matching-expert` seam extension).
#[utoipa::path(
    patch,
    path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/orders/{order_id}",
    tag = "orders",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
        ("order_id" = String, Path, description = "The venue order id to modify"),
    ),
    request_body = ModifyOrderRequest,
    responses(
        (status = 200, description = "Order modified", body = ModifyOrderResponse),
        (status = 400, description = "Modification not servable without the book-read path"),
        (status = 401, description = "Missing or invalid token"),
        (status = 403, description = "Missing Trade permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn modify_order(
    State(_state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path((_underlying, _expiration, _strike, _style, _order_id)): Path<OrderPath>,
    Json(_request): Json<ModifyOrderRequest>,
) -> Result<Json<ModifyOrderResponse>, VenueError> {
    require(&auth, Permission::Trade)?;
    Err(VenueError::InvalidOrder(
        "order modification requires the resting order's side and time-in-force, which the \
         book-read path does not yet expose; cancel and re-place instead"
            .to_string(),
    ))
}

/// List orders. **Not yet servable with real data**: resting orders live in the
/// single-writer books, which the gateway cannot read yet, so this returns an
/// empty page rather than fabricate a list. Requires `Read` (the auth baseline).
#[utoipa::path(
    get,
    path = "/api/v1/orders",
    tag = "orders",
    params(
        ("underlying" = Option<String>, Query, description = "Filter by underlying"),
        ("status" = Option<crate::models::OrderStatus>, Query, description = "Filter by status"),
        ("side" = Option<crate::models::Side>, Query, description = "Filter by side"),
        ("limit" = Option<usize>, Query, description = "Pagination limit"),
        ("offset" = Option<usize>, Query, description = "Pagination offset"),
    ),
    responses(
        (status = 200, description = "Matching orders (empty until the book-read path lands)", body = OrderListResponse),
        (status = 401, description = "Missing or invalid token"),
        (status = 429, description = "Rate limited"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn list_orders(
    State(_state): State<Arc<AppState>>,
    Query(query): Query<OrderListQuery>,
) -> Json<OrderListResponse> {
    Json(OrderListResponse {
        orders: Vec::new(),
        total: 0,
        limit: query.limit.unwrap_or(0),
        offset: query.offset.unwrap_or(0),
    })
}

/// Fetch one order by id. **Not yet servable**: the resting-order index is not
/// readable from the gateway, so an id lookup is a `404` rather than fabricated
/// state.
#[utoipa::path(
    get,
    path = "/api/v1/orders/{order_id}",
    tag = "orders",
    params(("order_id" = String, Path, description = "The venue order id")),
    responses(
        (status = 200, description = "The order", body = crate::models::Order),
        (status = 404, description = "Order not found / not readable"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn get_order(
    State(_state): State<Arc<AppState>>,
    Path(order_id): Path<String>,
) -> Result<Json<crate::models::Order>, VenueError> {
    Err(VenueError::NotFound(order_id))
}

/// Bulk place limit orders — each item is a **Sequenced** `AddOrder` requiring
/// `Trade`. Each accepted item's result carries its `underlying_sequence`. When
/// `atomic` is set and any item fails, the successfully-placed items are
/// best-effort canceled (each carries its own contract symbol), and
/// `rolled_back` is set with any cancel warnings.
///
/// The batch is capped at [`MAX_BULK_ORDER_ITEMS`] — an over-limit request is a
/// `400` **before** the loop starts, so one account cannot monopolize an
/// underlying's single-writer mailbox ([08 §5](../../../docs/08-threat-model.md)).
#[utoipa::path(
    post,
    path = "/api/v1/orders/bulk",
    tag = "orders",
    request_body = BulkOrderRequest,
    responses(
        (status = 200, description = "Per-order results", body = BulkOrderResponse),
        (status = 400, description = "Batch exceeds MAX_BULK_ORDER_ITEMS"),
        (status = 401, description = "Missing or invalid token"),
        (status = 403, description = "Missing Trade permission"),
        (status = 429, description = "Rate limited"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn bulk_place_orders(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Json(request): Json<BulkOrderRequest>,
) -> Result<Json<BulkOrderResponse>, VenueError> {
    require(&auth, Permission::Trade)?;
    if request.orders.len() > MAX_BULK_ORDER_ITEMS {
        return Err(VenueError::InvalidOrder(format!(
            "bulk order exceeds MAX_BULK_ORDER_ITEMS ({MAX_BULK_ORDER_ITEMS}); got {}",
            request.orders.len()
        )));
    }
    let account = auth.claims.account().clone();
    let owner = owner_for(&state, &account)?;

    let mut results: Vec<BulkOrderResultItem> = Vec::with_capacity(request.orders.len());
    let mut placed: Vec<(crate::exchange::Symbol, crate::models::VenueOrderId)> = Vec::new();
    let mut success_count = 0usize;
    let mut aborted = false;

    for (index, item) in request.orders.iter().enumerate() {
        if let Err(error) = item.validate() {
            results.push(BulkOrderResultItem {
                index,
                order_id: None,
                sequence: None,
                status: BulkOrderStatus::Rejected,
                error: Some(error.redacted_message()),
            });
            if request.atomic {
                aborted = true;
                break;
            }
            continue;
        }

        // The symbol validated on deserialize (`Symbol` parses via serde), so
        // this parse succeeds; derive the underlying for id minting + routing.
        let underlying = match SymbolParser::parse(item.symbol.as_str()) {
            Ok(parsed) => parsed.underlying().to_string(),
            Err(error) => {
                results.push(BulkOrderResultItem {
                    index,
                    order_id: None,
                    sequence: None,
                    status: BulkOrderStatus::Rejected,
                    error: Some(VenueError::from(error).redacted_message()),
                });
                if request.atomic {
                    aborted = true;
                    break;
                }
                continue;
            }
        };
        let tif = match seam_tif(item.time_in_force.unwrap_or_default(), None) {
            Ok(tif) => tif,
            Err(error) => {
                results.push(BulkOrderResultItem {
                    index,
                    order_id: None,
                    sequence: None,
                    status: BulkOrderStatus::Rejected,
                    error: Some(error.redacted_message()),
                });
                if request.atomic {
                    aborted = true;
                    break;
                }
                continue;
            }
        };
        let order_id = mint_order_id(state.lineage_id(), &underlying);

        let command = VenueCommand::AddOrder {
            symbol: item.symbol.clone(),
            order_id: order_id.clone(),
            account: account.clone(),
            owner,
            client_order_id: item.client_order_id.clone(),
            side: seam_side(item.side),
            order_type: OrderType::Limit,
            limit_price: Some(item.price),
            quantity: item.quantity,
            time_in_force: tif,
            stp_mode: STPMode::None,
        };
        match state.submit(command).await {
            Ok(receipt) => {
                success_count += 1;
                placed.push((item.symbol.clone(), order_id.clone()));
                results.push(BulkOrderResultItem {
                    index,
                    order_id: Some(order_id),
                    sequence: Some(receipt.underlying_sequence),
                    status: BulkOrderStatus::Accepted,
                    error: None,
                });
            }
            Err(error) => {
                results.push(BulkOrderResultItem {
                    index,
                    order_id: None,
                    sequence: None,
                    status: BulkOrderStatus::Rejected,
                    error: Some(error.redacted_message()),
                });
                if request.atomic {
                    aborted = true;
                    break;
                }
            }
        }
    }

    let mut rollback_warnings = Vec::new();
    let rolled_back = if request.atomic && aborted {
        // Best-effort rollback: cancel every order placed so far.
        for (symbol, order_id) in &placed {
            let cancel = VenueCommand::CancelOrder {
                symbol: symbol.clone(),
                order_id: order_id.clone(),
                account: account.clone(),
            };
            if let Err(error) = state.submit(cancel).await {
                rollback_warnings.push(format!(
                    "rollback cancel of {} failed: {}",
                    order_id.as_str(),
                    error.redacted_message()
                ));
            }
        }
        success_count = 0;
        true
    } else {
        false
    };

    let failure_count = results.len() - success_count;
    Ok(Json(BulkOrderResponse {
        success_count,
        failure_count,
        results,
        rolled_back,
        rollback_warnings,
    }))
}

/// Bulk cancel by order id. **Not yet servable**: a `CancelOrder` needs the
/// order's full contract symbol, and a bare venue order id does not carry the
/// strike/style/expiration (only the underlying), with no gateway-readable
/// order→symbol index yet. Every id is reported as a failure with that reason;
/// wiring this needs the by-id order index (a `matching-expert` seam extension).
#[utoipa::path(
    delete,
    path = "/api/v1/orders/bulk",
    tag = "orders",
    request_body = BulkCancelRequest,
    responses(
        (status = 200, description = "Per-order results", body = BulkCancelResponse),
        (status = 400, description = "Batch exceeds MAX_BULK_CANCEL_ITEMS"),
        (status = 401, description = "Missing or invalid token"),
        (status = 403, description = "Missing Trade permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn bulk_cancel_orders(
    State(_state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Json(request): Json<BulkCancelRequest>,
) -> Result<Json<BulkCancelResponse>, VenueError> {
    require(&auth, Permission::Trade)?;
    if request.order_ids.len() > MAX_BULK_CANCEL_ITEMS {
        return Err(VenueError::InvalidOrder(format!(
            "bulk cancel exceeds MAX_BULK_CANCEL_ITEMS ({MAX_BULK_CANCEL_ITEMS}); got {}",
            request.order_ids.len()
        )));
    }
    let results: Vec<BulkCancelResultItem> = request
        .order_ids
        .into_iter()
        .map(|order_id| BulkCancelResultItem {
            order_id,
            canceled: false,
            error: Some(
                "cancel-by-id needs the order's contract symbol; the by-id order index is not \
                 yet wired"
                    .to_string(),
            ),
        })
        .collect();
    let failure_count = results.len();
    Ok(Json(BulkCancelResponse {
        success_count: 0,
        failure_count,
        results,
    }))
}

/// Cancel all matching orders. **Not yet servable**: a filtered cancel-all is a
/// venue-wide / underlying-scoped `MassCancel`, which the per-underlying submit
/// path does not route (that broadcast routing is a control-plane concern). A
/// per-book `MassCancel` (fully specified contract) is the routable form the
/// control-plane issue wires; until then this returns a typed `400`.
#[utoipa::path(
    delete,
    path = "/api/v1/orders/cancel-all",
    tag = "orders",
    params(
        ("underlying" = Option<String>, Query, description = "Filter by underlying"),
        ("expiration" = Option<String>, Query, description = "Filter by expiration YYYYMMDD"),
        ("side" = Option<crate::models::Side>, Query, description = "Filter by side"),
        ("style" = Option<crate::models::OptionStyle>, Query, description = "Filter by style"),
    ),
    responses(
        (status = 200, description = "Cancel-all outcome", body = CancelAllResponse),
        (status = 400, description = "Venue-wide mass-cancel routing not yet wired"),
        (status = 401, description = "Missing or invalid token"),
        (status = 403, description = "Missing Trade permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn cancel_all_orders(
    State(_state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Query(_query): Query<CancelAllQuery>,
) -> Result<Json<CancelAllResponse>, VenueError> {
    require(&auth, Permission::Trade)?;
    Err(VenueError::InvalidOrder(
        "venue-wide / underlying-scoped cancel-all is a MassCancel the per-underlying submit \
         path does not route; the control-plane mass-cancel routing is not yet wired"
            .to_string(),
    ))
}
