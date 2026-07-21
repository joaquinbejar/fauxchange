//! Underlying-price handlers.
//!
//! `POST /api/v1/prices` is the **SimStep-class** price command: a manual
//! underlying-price override is wrapped as a [`VenueCommand::SimStep`] and
//! submitted through the actor so it is journaled and replays — a bare price
//! write is **never** allowed to bypass the sequencer
//! ([03 §10](../../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
//! It requires [`Permission::Admin`] (an operator/simulation control). The `GET`
//! reads require `Read`.
//!
//! **Read limitation.** The price a `SimStep` sets is not yet projected into a
//! gateway-readable store (the price-feed → mark → chain wiring is
//! simulation-owned, #016), so the `GET` reads return no data until then rather
//! than fabricate a last price.

use std::sync::Arc;

use axum::extract::{Extension, Path, State};

use crate::auth::Authorized;
use crate::error::VenueError;
use crate::exchange::{EventTimestamp, VenueCommand};
use crate::gateway::rest::extract::Json;
use crate::gateway::rest::middleware::require;
use crate::models::{InsertPriceRequest, InsertPriceResponse, LatestPriceResponse, Permission};
use crate::state::AppState;

/// Insert / override an underlying price — a **Sequenced** `SimStep` requiring
/// `Admin`. Journaled through the actor so the override replays.
#[utoipa::path(
    post,
    path = "/api/v1/prices",
    tag = "prices",
    request_body = InsertPriceRequest,
    responses(
        (status = 200, description = "Price override accepted and sequenced", body = InsertPriceResponse),
        (status = 400, description = "Invalid price request or unhosted underlying"),
        (status = 401, description = "Missing or invalid token"),
        (status = 403, description = "Missing Admin permission"),
        (status = 429, description = "Rate limited"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn insert_price(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Json(request): Json<InsertPriceRequest>,
) -> Result<Json<InsertPriceResponse>, VenueError> {
    require(&auth, Permission::Admin)?;

    // The venue clock is the deterministic instant source; the gateway does not
    // yet read it (fixed venue clock until the seeded/stepped clock, #016), so
    // the step carries the venue-clock origin. The actor stamps `venue_ts`.
    let receipt = state
        .submit(VenueCommand::SimStep {
            now_ms: EventTimestamp::new(0),
            underlying: request.symbol.clone(),
            price: request.price,
            bid: request.bid,
            ask: request.ask,
        })
        .await?;

    Ok(Json(InsertPriceResponse {
        success: true,
        symbol: request.symbol,
        price_cents: request.price,
        timestamp: receipt.venue_ts,
    }))
}

/// List latest underlying prices. **Not yet readable**: a `SimStep` price is not
/// projected into a gateway-readable store yet (simulation-owned, #016), so this
/// returns an empty list rather than fabricate prices.
#[utoipa::path(
    get,
    path = "/api/v1/prices",
    tag = "prices",
    responses(
        (status = 200, description = "Latest prices (empty until the price feed lands)", body = [LatestPriceResponse]),
        (status = 401, description = "Missing or invalid token"),
        (status = 429, description = "Rate limited"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn list_prices(State(_state): State<Arc<AppState>>) -> Json<Vec<LatestPriceResponse>> {
    Json(Vec::new())
}

/// Latest price for one underlying. **Not yet readable** (see [`list_prices`]);
/// returns `404` rather than fabricate a price.
#[utoipa::path(
    get,
    path = "/api/v1/prices/{symbol}",
    tag = "prices",
    params(("symbol" = String, Path, description = "Underlying ticker")),
    responses(
        (status = 200, description = "The latest price", body = LatestPriceResponse),
        (status = 404, description = "No price observed for this underlying"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn get_price(
    State(_state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<Json<LatestPriceResponse>, VenueError> {
    Err(VenueError::NotFound(symbol))
}
