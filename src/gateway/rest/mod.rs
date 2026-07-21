//! Transport layer: REST gateway — the Axum 0.8 router, the `#[utoipa::path]`
//! handlers, and the served OpenAPI document + Swagger UI. Order-entry and
//! observation surface, tier T1 (v0.1).
//!
//! [`create_router`] assembles the ~50-route Backend surface behind one
//! admission model ([`middleware`]): `GET /health` is auth-exempt,
//! `POST /api/v1/auth/token` is JWT-exempt but peer-rate-limited, and every
//! other route runs behind the shared JWT + rate-limit layer with a baseline
//! [`Permission::Read`] — each mutating handler then gates its own `Trade`/`Admin`
//! requirement. Order mutations enter **only** through the sequenced order path
//! ([`AppState::submit`]); a handler never calls the upstream books directly.
//! The Swagger UI is merged at `/swagger-ui` over `/api-docs/openapi.json`.
//!
//! Governed by `docs/03-protocol-surfaces.md`.

pub mod account;
pub mod admin;
pub mod controls;
pub mod market;
pub mod meta;
pub mod middleware;
pub mod openapi;
pub mod orders;
pub mod prices;
pub mod support;

use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use axum::Router;
use axum::middleware::{from_fn, from_fn_with_state};
use axum::routing::{delete, get, post};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::auth::RATE_LIMIT_WINDOW_MS;
use crate::gateway::rest::middleware::{
    AppStateAuthGuard, app_state_auth_middleware, peer_addr_middleware, peer_rate_limit_middleware,
};
use crate::gateway::rest::openapi::ApiDoc;
use crate::models::Permission;
use crate::state::AppState;

/// The path prefix under which per-contract routes live.
const CONTRACT: &str =
    "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}";

/// Builds the complete REST [`Router`] over the shared [`AppState`].
///
/// Route groups (full inventory in `docs/specs/option-chain-orderbook-backend.md`
/// §1): health/meta, auth, controls, prices, hierarchy CRUD, per-contract market
/// data + order entry, orders, positions, executions, admin snapshots. Every
/// handler carries `#[utoipa::path]` and its schemas are registered in
/// [`ApiDoc`]; the Swagger UI is merged at boot.
///
/// The returned router is stateless (`Router<()>`) — the shared state is baked
/// in via `with_state`, and the peer-injection layer wraps everything so the
/// real socket peer reaches the rate-limit key when served with
/// `into_make_service_with_connect_info::<SocketAddr>()`.
pub fn create_router(state: Arc<AppState>) -> Router {
    let protected = protected_routes().route_layer(from_fn_with_state(
        AppStateAuthGuard::new(Arc::clone(&state), Permission::Read),
        app_state_auth_middleware,
    ));

    // The token route is JWT-exempt (a caller without a token requests one) but
    // peer-rate-limited so the bootstrap secret cannot be brute-forced.
    let token = Router::new()
        .route("/api/v1/auth/token", post(meta::issue_token))
        .route_layer(from_fn_with_state(
            Arc::clone(&state),
            peer_rate_limit_middleware,
        ));

    // `/health` is exempt from both auth and rate limiting.
    let public = Router::new().route("/health", get(meta::health));

    let api = protected
        .merge(token)
        .merge(public)
        .with_state(state)
        // The peer-injection layer is OUTERMOST so `PeerAddr` is set before the
        // auth layer reads it (per-route `route_layer`s run inside this).
        .layer(from_fn(peer_addr_middleware));

    api.merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
}

/// The routes behind the shared JWT + rate-limit auth layer (baseline `Read`).
fn protected_routes() -> Router<Arc<AppState>> {
    Router::new()
        // ---- meta ----------------------------------------------------------
        .route("/api/v1/stats", get(meta::stats))
        // ---- controls ------------------------------------------------------
        .route("/api/v1/controls", get(controls::get_controls))
        .route("/api/v1/controls/kill-switch", post(controls::kill_switch))
        .route("/api/v1/controls/enable", post(controls::set_enabled))
        .route(
            "/api/v1/controls/parameters",
            post(controls::update_parameters),
        )
        .route(
            "/api/v1/controls/instruments",
            get(controls::list_instrument_controls),
        )
        .route(
            "/api/v1/controls/instrument/{symbol}/toggle",
            post(controls::toggle_instrument),
        )
        // ---- prices --------------------------------------------------------
        .route(
            "/api/v1/prices",
            get(prices::list_prices).post(prices::insert_price),
        )
        .route("/api/v1/prices/{symbol}", get(prices::get_price))
        // ---- hierarchy: underlyings ---------------------------------------
        .route("/api/v1/underlyings", get(market::list_underlyings))
        .route(
            "/api/v1/underlyings/{underlying}",
            post(market::create_underlying)
                .get(market::get_underlying)
                .delete(market::delete_underlying),
        )
        // ---- hierarchy: expirations ---------------------------------------
        .route(
            "/api/v1/underlyings/{underlying}/expirations",
            get(market::list_expirations),
        )
        .route(
            "/api/v1/underlyings/{underlying}/expirations/{expiration}",
            post(market::create_expiration).get(market::get_expiration),
        )
        .route(
            "/api/v1/underlyings/{underlying}/volatility-surface",
            get(market::volatility_surface),
        )
        .route(
            "/api/v1/underlyings/{underlying}/expirations/{expiration}/chain",
            get(market::option_chain),
        )
        // ---- hierarchy: strikes -------------------------------------------
        .route(
            "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes",
            get(market::list_strikes),
        )
        .route(
            "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}",
            post(market::create_strike).get(market::get_strike),
        )
        // ---- per-contract: reads + order entry ----------------------------
        .route(CONTRACT, get(market::contract_book))
        .route(
            &format!("{CONTRACT}/orders"),
            post(orders::place_limit_order),
        )
        .route(
            &format!("{CONTRACT}/orders/market"),
            post(orders::place_market_order),
        )
        .route(
            &format!("{CONTRACT}/orders/{{order_id}}"),
            delete(orders::cancel_order).patch(orders::modify_order),
        )
        .route(&format!("{CONTRACT}/quote"), get(market::contract_quote))
        .route(&format!("{CONTRACT}/greeks"), get(market::contract_greeks))
        .route(
            &format!("{CONTRACT}/snapshot"),
            get(market::contract_snapshot),
        )
        .route(
            &format!("{CONTRACT}/last-trade"),
            get(market::contract_last_trade),
        )
        .route(&format!("{CONTRACT}/ohlc"), get(market::contract_ohlc))
        .route(
            &format!("{CONTRACT}/metrics"),
            get(market::contract_metrics),
        )
        // ---- orders (global) ----------------------------------------------
        .route("/api/v1/orders", get(orders::list_orders))
        .route(
            "/api/v1/orders/bulk",
            post(orders::bulk_place_orders).delete(orders::bulk_cancel_orders),
        )
        .route(
            "/api/v1/orders/cancel-all",
            delete(orders::cancel_all_orders),
        )
        .route("/api/v1/orders/{order_id}", get(orders::get_order))
        // ---- positions -----------------------------------------------------
        .route("/api/v1/positions", get(account::list_positions))
        .route("/api/v1/positions/{symbol}", get(account::get_position))
        // ---- executions ----------------------------------------------------
        .route("/api/v1/executions", get(account::list_executions))
        .route(
            "/api/v1/executions/{execution_id}",
            get(account::get_execution),
        )
        // ---- admin snapshots ----------------------------------------------
        .route("/api/v1/admin/snapshot", post(admin::create_snapshot))
        .route("/api/v1/admin/snapshots", get(admin::list_snapshots))
        .route(
            "/api/v1/admin/snapshots/{snapshot_id}",
            get(admin::get_snapshot),
        )
        .route(
            "/api/v1/admin/snapshots/{snapshot_id}/restore",
            post(admin::restore_snapshot),
        )
}

/// Spawns the periodic [`RateLimiter::sweep_expired`](crate::auth::RateLimiter::sweep_expired)
/// reclaim task on a bounded interval (the venue rate-limit window,
/// [`RATE_LIMIT_WINDOW_MS`]), so idle rate-limit buckets are reclaimed off the
/// request path ([08 §5](../../../docs/08-threat-model.md#5-resource-exhaustion)).
///
/// **Shutdown path.** The task holds only a [`Weak`] handle to [`AppState`], so
/// when the last strong `Arc<AppState>` drops (server shutdown) the next tick
/// fails to upgrade and the task exits cleanly — it never keeps the venue alive.
#[must_use]
pub fn spawn_rate_limit_sweeper(state: &Arc<AppState>) -> JoinHandle<()> {
    let weak: Weak<AppState> = Arc::downgrade(state);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(RATE_LIMIT_WINDOW_MS));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match weak.upgrade() {
                Some(state) => state.auth().rate_limiter().sweep_expired(),
                None => break,
            }
        }
    })
}

/// Binds `addr` and serves the REST router, spawning the rate-limit sweeper and
/// wiring `ConnectInfo<SocketAddr>` so the real socket peer reaches the
/// rate-limit key. Runs until the listener errors or the process exits.
///
/// # Errors
///
/// Propagates a bind / accept [`std::io::Error`].
pub async fn serve(state: Arc<AppState>, addr: SocketAddr) -> std::io::Result<()> {
    let sweeper = spawn_rate_limit_sweeper(&state);
    let router = create_router(Arc::clone(&state));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "REST gateway listening");
    let result = axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await;
    sweeper.abort();
    result
}
