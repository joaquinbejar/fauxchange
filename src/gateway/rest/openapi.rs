//! The OpenAPI document for the REST surface.
//!
//! Every `#[utoipa::path]` handler is registered in [`ApiDoc`] and every #004
//! DTO is registered as a component schema, so the served
//! `/api-docs/openapi.json` (and the Swagger UI over it) is the **public wire
//! contract** for the whole surface. A route or DTO missing here is a review
//! blocker.

use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};

/// Registers the venue's single bearer-JWT security scheme referenced by the
/// protected paths (`security(("bearer_jwt" = []))`).
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer_jwt",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            );
        }
    }
}

/// The OpenAPI document for the `fauxchange` REST surface.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "fauxchange REST API",
        description = "The REST surface of the fauxchange options-exchange simulator. Money is \
                       integer cents on every field; order mutations enter the sequenced order \
                       path and return the resulting event's underlying_sequence.",
        version = env!("CARGO_PKG_VERSION"),
    ),
    modifiers(&SecurityAddon),
    tags(
        (name = "meta", description = "Health and venue statistics"),
        (name = "auth", description = "JWT token issuance (bootstrap-gated)"),
        (name = "orders", description = "Order entry — sequenced venue commands"),
        (name = "prices", description = "Underlying prices — SimStep-class writes"),
        (name = "hierarchy", description = "Underlying / expiration / strike hierarchy"),
        (name = "market-data", description = "Per-contract book, quote, greeks, metrics"),
        (name = "positions", description = "Account-scoped positions"),
        (name = "executions", description = "Account-scoped execution records"),
        (name = "controls", description = "Venue control plane (admin)"),
        (name = "replay", description = "Record / replay control plane (admin)"),
        (name = "admin", description = "Admin snapshots"),
    ),
    paths(
        crate::gateway::rest::meta::health,
        crate::gateway::rest::meta::stats,
        crate::gateway::rest::meta::issue_token,
        crate::gateway::rest::orders::place_limit_order,
        crate::gateway::rest::orders::place_market_order,
        crate::gateway::rest::orders::cancel_order,
        crate::gateway::rest::orders::modify_order,
        crate::gateway::rest::orders::list_orders,
        crate::gateway::rest::orders::get_order,
        crate::gateway::rest::orders::bulk_place_orders,
        crate::gateway::rest::orders::bulk_cancel_orders,
        crate::gateway::rest::orders::cancel_all_orders,
        crate::gateway::rest::prices::insert_price,
        crate::gateway::rest::prices::list_prices,
        crate::gateway::rest::prices::get_price,
        crate::gateway::rest::market::list_underlyings,
        crate::gateway::rest::market::create_underlying,
        crate::gateway::rest::market::get_underlying,
        crate::gateway::rest::market::delete_underlying,
        crate::gateway::rest::market::list_expirations,
        crate::gateway::rest::market::create_expiration,
        crate::gateway::rest::market::get_expiration,
        crate::gateway::rest::market::volatility_surface,
        crate::gateway::rest::market::option_chain,
        crate::gateway::rest::market::list_strikes,
        crate::gateway::rest::market::create_strike,
        crate::gateway::rest::market::get_strike,
        crate::gateway::rest::market::contract_book,
        crate::gateway::rest::market::contract_quote,
        crate::gateway::rest::market::contract_snapshot,
        crate::gateway::rest::market::contract_greeks,
        crate::gateway::rest::market::contract_last_trade,
        crate::gateway::rest::market::contract_ohlc,
        crate::gateway::rest::market::contract_metrics,
        crate::gateway::rest::account::list_positions,
        crate::gateway::rest::account::get_position,
        crate::gateway::rest::account::list_executions,
        crate::gateway::rest::account::get_execution,
        crate::gateway::rest::controls::get_controls,
        crate::gateway::rest::controls::kill_switch,
        crate::gateway::rest::controls::set_enabled,
        crate::gateway::rest::controls::update_parameters,
        crate::gateway::rest::controls::list_instrument_controls,
        crate::gateway::rest::controls::toggle_instrument,
        crate::gateway::rest::replay::get_record_status,
        crate::gateway::rest::replay::set_record,
        crate::gateway::rest::replay::export_bundle,
        crate::gateway::rest::replay::replay_bundle,
        crate::gateway::rest::admin::create_snapshot,
        crate::gateway::rest::admin::list_snapshots,
        crate::gateway::rest::admin::get_snapshot,
        crate::gateway::rest::admin::restore_snapshot,
    ),
    components(schemas(
        // Error boundary
        crate::error::ErrorEnvelope,
        // Identity newtypes
        crate::models::AccountId,
        crate::models::ClientOrderId,
        crate::models::VenueOrderId,
        crate::models::ExecutionId,
        // Wire enums
        crate::models::Permission,
        crate::models::Side,
        crate::models::BookSide,
        crate::models::OptionStyle,
        crate::models::OrderType,
        crate::models::TimeInForce,
        crate::models::OrderStatus,
        crate::models::LiquidityFlag,
        crate::models::LimitOrderStatus,
        crate::models::MarketOrderStatus,
        crate::models::ModifyOrderStatus,
        crate::models::BulkOrderStatus,
        crate::models::InstrumentLifecycle,
        crate::models::SubscriptionChannel,
        crate::models::OhlcInterval,
        // Meta / auth
        crate::models::HealthResponse,
        crate::models::GlobalStatsResponse,
        crate::models::TokenRequest,
        crate::models::TokenResponse,
        crate::models::Account,
        crate::models::InstrumentView,
        // Orders
        crate::models::Order,
        crate::models::PlaceLimitOrderRequest,
        crate::models::PlaceLimitOrderResponse,
        crate::models::PlaceMarketOrderRequest,
        crate::models::PlaceMarketOrderResponse,
        crate::models::FillPrint,
        crate::models::CancelOrderResponse,
        crate::models::ModifyOrderRequest,
        crate::models::ModifyOrderResponse,
        crate::models::OrderListResponse,
        crate::models::BulkOrderItem,
        crate::models::BulkOrderRequest,
        crate::models::BulkOrderResultItem,
        crate::models::BulkOrderResponse,
        crate::models::BulkCancelRequest,
        crate::models::BulkCancelResultItem,
        crate::models::BulkCancelResponse,
        crate::models::CancelAllResponse,
        // Fills / executions / positions
        crate::models::Fill,
        crate::models::ExecutionRecord,
        crate::models::ExecutionSummary,
        crate::models::ExecutionsListResponse,
        crate::models::Position,
        crate::models::PositionSummary,
        crate::models::PositionsListResponse,
        // Prices
        crate::models::InsertPriceRequest,
        crate::models::InsertPriceResponse,
        crate::models::LatestPriceResponse,
        // Hierarchy
        crate::models::QuoteResponse,
        crate::models::OrderBookSnapshotResponse,
        crate::models::UnderlyingSummary,
        crate::models::UnderlyingsListResponse,
        crate::models::DeleteUnderlyingResponse,
        crate::models::ExpirationSummary,
        crate::models::ExpirationsListResponse,
        crate::models::StrikeSummary,
        crate::models::StrikesListResponse,
        // Chain / vol surface
        crate::models::OptionQuoteData,
        crate::models::ChainStrikeRow,
        crate::models::OptionChainResponse,
        crate::models::StrikeIV,
        crate::models::AtmTermStructurePoint,
        crate::models::VolatilitySurfaceResponse,
        // Greeks / metrics
        crate::models::GreeksData,
        crate::models::GreeksResponse,
        crate::models::SpreadMetrics,
        crate::models::DepthMetrics,
        crate::models::PriceMetrics,
        crate::models::ImpactMetrics,
        crate::models::MarketImpactMetrics,
        crate::models::OrderbookMetricsResponse,
        // OHLC
        crate::models::OhlcBar,
        crate::models::OhlcResponse,
        // Controls
        crate::models::SystemControlResponse,
        crate::models::KillSwitchRequest,
        crate::models::KillSwitchResponse,
        crate::models::UpdateParametersRequest,
        crate::models::UpdateParametersResponse,
        crate::models::InstrumentToggleResponse,
        crate::models::InstrumentControlStatus,
        crate::models::InstrumentsListResponse,
        // Record / replay (#030)
        crate::models::RecordControlRequest,
        crate::models::RecordingStateResponse,
        crate::models::UnderlyingReplaySummary,
        crate::models::ReplayReportResponse,
        crate::simulation::ScenarioBundle,
        crate::simulation::JournalStream,
        crate::simulation::RunManifest,
        crate::simulation::DependencyVersions,
        // Admin snapshots
        crate::models::CreateSnapshotResponse,
        crate::models::SnapshotSummary,
        crate::models::SnapshotsListResponse,
        crate::models::RestoreSnapshotResponse,
    )),
)]
pub struct ApiDoc;
