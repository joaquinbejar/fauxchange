//! Hierarchy and per-contract market-data handlers.
//!
//! Two operation classes live here
//! ([03 §10](../../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)):
//!
//! - **Manifest input** — runtime hierarchy create/delete (`POST`/`DELETE` on
//!   underlyings / expirations / strikes) is **refused once the venue is
//!   serving**: the instrument set is a seed-time manifest input (there is no
//!   sequenced hierarchy-CRUD command, and a mid-run create/delete would make the
//!   registry unreproducible). These return a typed `400` naming the refusal;
//!   they require `Admin`.
//! - **Read-only** query routes require `Read` (the auth baseline).
//!
//! **Read limitation.** The hierarchy *list* reads project the shared symbol
//! index (instruments become visible as they trade); the *live-book* reads
//! (quote / depth / chain / metrics) return an empty projection, and the
//! pricing-derived reads (greeks / last-trade) return `404`, until the actor
//! exposes a book-read path and the option pricer (#015) is wired — a
//! `matching-expert` / `simulation-expert` seam dependency. No fabricated depth
//! or greeks are ever returned.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, Query, State};

use crate::auth::Authorized;
use crate::error::VenueError;
use crate::exchange::{EventTimestamp, SymbolParser};
use crate::gateway::rest::middleware::require;
use crate::gateway::rest::support::{build_symbol, parse_style};
use crate::models::{
    ChainStrikeRow, DeleteUnderlyingResponse, ExpirationSummary, ExpirationsListResponse,
    FillPrint, GreeksResponse, OhlcQuery, OhlcResponse, OptionChainResponse, OptionQuoteData,
    OrderBookSnapshotResponse, OrderbookMetricsResponse, Permission, QuoteResponse, StrikeSummary,
    StrikesListResponse, UnderlyingSummary, UnderlyingsListResponse, VolatilitySurfaceResponse,
};
use crate::state::AppState;

/// The typed, **phase-aware** refusal for a runtime hierarchy mutation (#024).
///
/// The instrument set is a seed-time manifest input: it is populated from the seed
/// manifest during the bounded **seeding** phase (not by a runtime REST create —
/// there is no sequenced hierarchy-CRUD command), and once the venue flips to
/// **serving** it is immutable. Either way this is a typed `400` naming the
/// manifest-input reason ([06 §7](../../../docs/06-deployment.md#7-seed-data-and-scenarios),
/// [03 §10](../../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
fn manifest_refused(kind: &str, serving: bool) -> VenueError {
    if serving {
        VenueError::InvalidOrder(format!(
            "runtime {kind} create/delete is refused: the instrument set is a seed-time manifest \
             input, immutable once the venue is serving (a new run is required to change it)"
        ))
    } else {
        VenueError::InvalidOrder(format!(
            "{kind} create/delete over REST is refused: the instrument set is populated from the \
             seed manifest during the bounded seeding phase, not by a runtime hierarchy create"
        ))
    }
}

/// An empty two-sided quote — no observable resting quote yet.
fn empty_quote() -> QuoteResponse {
    QuoteResponse {
        bid_price: None,
        bid_size: 0,
        ask_price: None,
        ask_size: 0,
        timestamp: EventTimestamp::new(0),
    }
}

/// Collects the distinct expirations registered for `underlying` from the shared
/// symbol index (instruments appear as they trade).
fn expirations_for(state: &Arc<AppState>, underlying: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for symbol in state.symbol_index().symbols() {
        if let Ok(parsed) = SymbolParser::parse(&symbol)
            && parsed.underlying() == underlying
        {
            out.insert(parsed.expiration_str().to_string());
        }
    }
    out
}

/// Collects the distinct strikes registered for `(underlying, expiration)`.
fn strikes_for(state: &Arc<AppState>, underlying: &str, expiration: &str) -> BTreeSet<u64> {
    let mut out = BTreeSet::new();
    for symbol in state.symbol_index().symbols() {
        if let Ok(parsed) = SymbolParser::parse(&symbol)
            && parsed.underlying() == underlying
            && parsed.expiration_str() == expiration
        {
            out.insert(parsed.strike());
        }
    }
    out
}

// ============================================================================
// Underlyings
// ============================================================================

/// List hosted underlyings (one single-writer actor each) — authoritative.
#[utoipa::path(
    get, path = "/api/v1/underlyings", tag = "hierarchy",
    responses(
        (status = 200, description = "Hosted underlyings", body = UnderlyingsListResponse),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn list_underlyings(State(state): State<Arc<AppState>>) -> Json<UnderlyingsListResponse> {
    Json(UnderlyingsListResponse {
        underlyings: state
            .underlyings()
            .into_iter()
            .map(str::to_string)
            .collect(),
    })
}

/// Create an underlying — **manifest input**, refused at runtime. Requires `Admin`.
#[utoipa::path(
    post, path = "/api/v1/underlyings/{underlying}", tag = "hierarchy",
    params(("underlying" = String, Path, description = "Underlying ticker")),
    responses(
        (status = 200, description = "Created", body = UnderlyingSummary),
        (status = 400, description = "Runtime hierarchy mutation refused (manifest input)"),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn create_underlying(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path(_underlying): Path<String>,
) -> Result<Json<UnderlyingSummary>, VenueError> {
    require(&auth, Permission::Admin)?;
    Err(manifest_refused("underlying", state.is_serving()))
}

/// Summary of one underlying — projected from the symbol index.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}", tag = "hierarchy",
    params(("underlying" = String, Path, description = "Underlying ticker")),
    responses(
        (status = 200, description = "Underlying summary", body = UnderlyingSummary),
        (status = 404, description = "Underlying not hosted"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn get_underlying(
    State(state): State<Arc<AppState>>,
    Path(underlying): Path<String>,
) -> Result<Json<UnderlyingSummary>, VenueError> {
    if !state.hosts_underlying(&underlying) {
        return Err(VenueError::NotFound(underlying));
    }
    let expirations = expirations_for(&state, &underlying);
    let total_strikes: usize = expirations
        .iter()
        .map(|exp| strikes_for(&state, &underlying, exp).len())
        .sum();
    Ok(Json(UnderlyingSummary {
        symbol: underlying,
        expiration_count: expirations.len(),
        total_strike_count: total_strikes,
        total_order_count: 0,
    }))
}

/// Delete an underlying — **manifest input**, refused at runtime. Requires `Admin`.
#[utoipa::path(
    delete, path = "/api/v1/underlyings/{underlying}", tag = "hierarchy",
    params(("underlying" = String, Path, description = "Underlying ticker")),
    responses(
        (status = 200, description = "Deleted", body = DeleteUnderlyingResponse),
        (status = 400, description = "Runtime hierarchy mutation refused (manifest input)"),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn delete_underlying(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path(_underlying): Path<String>,
) -> Result<Json<DeleteUnderlyingResponse>, VenueError> {
    require(&auth, Permission::Admin)?;
    Err(manifest_refused("underlying", state.is_serving()))
}

// ============================================================================
// Expirations
// ============================================================================

/// List expirations for an underlying — projected from the symbol index.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations", tag = "hierarchy",
    params(("underlying" = String, Path, description = "Underlying ticker")),
    responses(
        (status = 200, description = "Expirations", body = ExpirationsListResponse),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn list_expirations(
    State(state): State<Arc<AppState>>,
    Path(underlying): Path<String>,
) -> Json<ExpirationsListResponse> {
    Json(ExpirationsListResponse {
        expirations: expirations_for(&state, &underlying).into_iter().collect(),
    })
}

/// Create an expiration — **manifest input**, refused at runtime. Requires `Admin`.
#[utoipa::path(
    post, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}", tag = "hierarchy",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
    ),
    responses(
        (status = 200, description = "Created", body = ExpirationSummary),
        (status = 400, description = "Runtime hierarchy mutation refused (manifest input)"),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn create_expiration(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path((_underlying, _expiration)): Path<(String, String)>,
) -> Result<Json<ExpirationSummary>, VenueError> {
    require(&auth, Permission::Admin)?;
    Err(manifest_refused("expiration", state.is_serving()))
}

/// Summary of one expiration — projected from the symbol index.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}", tag = "hierarchy",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
    ),
    responses(
        (status = 200, description = "Expiration summary", body = ExpirationSummary),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn get_expiration(
    State(state): State<Arc<AppState>>,
    Path((underlying, expiration)): Path<(String, String)>,
) -> Json<ExpirationSummary> {
    let strikes = strikes_for(&state, &underlying, &expiration);
    Json(ExpirationSummary {
        expiration,
        strike_count: strikes.len(),
        total_order_count: 0,
    })
}

/// The ATM volatility surface — an empty projection until the option pricer is
/// wired (#015). Requires `Read`.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/volatility-surface", tag = "hierarchy",
    params(("underlying" = String, Path, description = "Underlying ticker")),
    responses(
        (status = 200, description = "Volatility surface", body = VolatilitySurfaceResponse),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn volatility_surface(
    State(state): State<Arc<AppState>>,
    Path(underlying): Path<String>,
) -> Json<VolatilitySurfaceResponse> {
    let expirations: Vec<String> = expirations_for(&state, &underlying).into_iter().collect();
    Json(VolatilitySurfaceResponse {
        underlying,
        spot_price: None,
        timestamp: EventTimestamp::new(0),
        expirations,
        strikes: Vec::new(),
        atm_term_structure: Vec::new(),
    })
}

/// The option-chain matrix — an empty projection until the live book-read and
/// pricer paths land. Requires `Read`.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/chain", tag = "hierarchy",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
    ),
    responses(
        (status = 200, description = "Option chain", body = OptionChainResponse),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn option_chain(
    State(state): State<Arc<AppState>>,
    Path((underlying, expiration)): Path<(String, String)>,
) -> Json<OptionChainResponse> {
    let empty_side = || OptionQuoteData {
        bid: None,
        ask: None,
        bid_size: 0,
        ask_size: 0,
        last_trade: None,
        volume: 0,
        delta: None,
        iv: None,
    };
    let chain: Vec<ChainStrikeRow> = strikes_for(&state, &underlying, &expiration)
        .into_iter()
        .map(|strike| ChainStrikeRow {
            strike,
            call: empty_side(),
            put: empty_side(),
        })
        .collect();
    Json(OptionChainResponse {
        underlying,
        expiration,
        spot_price: None,
        atm_strike: None,
        chain,
    })
}

// ============================================================================
// Strikes
// ============================================================================

/// List strikes for an expiration — projected from the symbol index.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes", tag = "hierarchy",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
    ),
    responses(
        (status = 200, description = "Strikes", body = StrikesListResponse),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn list_strikes(
    State(state): State<Arc<AppState>>,
    Path((underlying, expiration)): Path<(String, String)>,
) -> Json<StrikesListResponse> {
    Json(StrikesListResponse {
        strikes: strikes_for(&state, &underlying, &expiration)
            .into_iter()
            .collect(),
    })
}

/// Create a strike — **manifest input**, refused at runtime. Requires `Admin`.
#[utoipa::path(
    post, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}", tag = "hierarchy",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
    ),
    responses(
        (status = 200, description = "Created", body = StrikeSummary),
        (status = 400, description = "Runtime hierarchy mutation refused (manifest input)"),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn create_strike(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path((_underlying, _expiration, _strike)): Path<(String, String, u64)>,
) -> Result<Json<StrikeSummary>, VenueError> {
    require(&auth, Permission::Admin)?;
    Err(manifest_refused("strike", state.is_serving()))
}

/// Summary of one strike (call and put books) — empty quotes until the book-read
/// path lands. Requires `Read`.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}", tag = "hierarchy",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
    ),
    responses(
        (status = 200, description = "Strike summary", body = StrikeSummary),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn get_strike(
    Path((_underlying, _expiration, strike)): Path<(String, String, u64)>,
) -> Json<StrikeSummary> {
    Json(StrikeSummary {
        strike,
        call_order_count: 0,
        put_order_count: 0,
        call_quote: empty_quote(),
        put_quote: empty_quote(),
    })
}

// ============================================================================
// Per-contract reads
// ============================================================================

/// Contract path segments `(underlying, expiration, strike, style)`.
type ContractPath = (String, String, u64, String);

/// The order-book depth summary for one contract — an empty book projection
/// until the actor exposes a book-read path.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}", tag = "market-data",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
    ),
    responses(
        (status = 200, description = "Order-book snapshot summary", body = OrderBookSnapshotResponse),
        (status = 400, description = "Invalid symbol"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn contract_book(
    Path((underlying, expiration, strike, style)): Path<ContractPath>,
) -> Result<Json<OrderBookSnapshotResponse>, VenueError> {
    let style = parse_style(&style)?;
    let symbol = build_symbol(&underlying, &expiration, strike, style)?;
    Ok(Json(OrderBookSnapshotResponse {
        symbol,
        total_bid_depth: 0,
        total_ask_depth: 0,
        bid_level_count: 0,
        ask_level_count: 0,
        order_count: 0,
        quote: empty_quote(),
    }))
}

/// The best quote for one contract — an empty two-sided quote until the
/// book-read path lands.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/quote", tag = "market-data",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
    ),
    responses(
        (status = 200, description = "Best quote", body = QuoteResponse),
        (status = 400, description = "Invalid symbol"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn contract_quote(
    Path((underlying, expiration, strike, style)): Path<ContractPath>,
) -> Result<Json<QuoteResponse>, VenueError> {
    let style = parse_style(&style)?;
    let _symbol = build_symbol(&underlying, &expiration, strike, style)?;
    Ok(Json(empty_quote()))
}

/// The order-book depth snapshot for one contract (alias of the book summary).
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/snapshot", tag = "market-data",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
    ),
    responses(
        (status = 200, description = "Order-book snapshot summary", body = OrderBookSnapshotResponse),
        (status = 400, description = "Invalid symbol"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn contract_snapshot(
    path: Path<ContractPath>,
) -> Result<Json<OrderBookSnapshotResponse>, VenueError> {
    contract_book(path).await
}

/// The Greeks for one contract. **`404` until the option pricer is wired**
/// (#015): no computed Greeks exist, and a fabricated zero delta would be a
/// false claim.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/greeks", tag = "market-data",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
    ),
    responses(
        (status = 200, description = "The Greeks", body = GreeksResponse),
        (status = 404, description = "No computed Greeks (pricer not wired)"),
        (status = 400, description = "Invalid symbol"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn contract_greeks(
    Path((underlying, expiration, strike, style)): Path<ContractPath>,
) -> Result<Json<GreeksResponse>, VenueError> {
    let style = parse_style(&style)?;
    let symbol = build_symbol(&underlying, &expiration, strike, style)?;
    Err(VenueError::NotFound(symbol.as_str().to_string()))
}

/// The last trade print for one contract. **`404` when none observed** — the
/// public last-trade store keyed by contract is not yet wired.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/last-trade", tag = "market-data",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
    ),
    responses(
        (status = 200, description = "The last trade print", body = FillPrint),
        (status = 404, description = "No last trade observed"),
        (status = 400, description = "Invalid symbol"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn contract_last_trade(
    Path((underlying, expiration, strike, style)): Path<ContractPath>,
) -> Result<Json<FillPrint>, VenueError> {
    let style = parse_style(&style)?;
    let symbol = build_symbol(&underlying, &expiration, strike, style)?;
    Err(VenueError::NotFound(symbol.as_str().to_string()))
}

/// OHLC bars for one contract — empty until the OHLC aggregator lands.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/ohlc", tag = "market-data",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
        ("interval" = crate::models::OhlcInterval, Query, description = "Bar interval"),
        ("from" = Option<u64>, Query, description = "Start seconds"),
        ("to" = Option<u64>, Query, description = "End seconds"),
        ("limit" = Option<usize>, Query, description = "Max bars"),
    ),
    responses(
        (status = 200, description = "OHLC bars", body = OhlcResponse),
        (status = 400, description = "Invalid symbol"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn contract_ohlc(
    Path((underlying, expiration, strike, style)): Path<ContractPath>,
    Query(query): Query<OhlcQuery>,
) -> Result<Json<OhlcResponse>, VenueError> {
    let style = parse_style(&style)?;
    let symbol = build_symbol(&underlying, &expiration, strike, style)?;
    Ok(Json(OhlcResponse {
        symbol,
        interval: query.interval,
        bars: Vec::new(),
    }))
}

/// Order-book microstructure metrics for one contract — an empty-book projection
/// (zero depth, no spreads) until the book-read path lands.
#[utoipa::path(
    get, path = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}/metrics", tag = "market-data",
    params(
        ("underlying" = String, Path, description = "Underlying ticker"),
        ("expiration" = String, Path, description = "Expiration date YYYYMMDD"),
        ("strike" = u64, Path, description = "Strike in whole units"),
        ("style" = String, Path, description = "Option style: call or put"),
    ),
    responses(
        (status = 200, description = "Order-book metrics", body = OrderbookMetricsResponse),
        (status = 400, description = "Invalid symbol"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn contract_metrics(
    Path((underlying, expiration, strike, style)): Path<ContractPath>,
) -> Result<Json<OrderbookMetricsResponse>, VenueError> {
    use crate::models::{
        DepthMetrics, ImpactMetrics, MarketImpactMetrics, PriceMetrics, SpreadMetrics,
    };
    let style = parse_style(&style)?;
    let symbol = build_symbol(&underlying, &expiration, strike, style)?;
    Ok(Json(OrderbookMetricsResponse {
        symbol,
        timestamp: EventTimestamp::new(0),
        spread: SpreadMetrics {
            current: None,
            spread_bps: None,
        },
        depth: DepthMetrics {
            bid_depth_total: 0,
            ask_depth_total: 0,
            imbalance: 0.0,
        },
        prices: PriceMetrics {
            mid_price: None,
            micro_price: None,
            vwap_bid: None,
            vwap_ask: None,
        },
        market_impact: MarketImpactMetrics {
            buy: ImpactMetrics {
                avg_price: None,
                slippage_bps: None,
            },
            sell: ImpactMetrics {
                avg_price: None,
                slippage_bps: None,
            },
        },
    }))
}
