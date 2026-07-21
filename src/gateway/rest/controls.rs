//! Venue-control handlers — the control plane.
//!
//! Operation class: controls are **Sequenced venue commands** requiring
//! [`Permission::Admin`]
//! ([03 §10](../../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
//! The instrument toggle is a per-symbol [`VenueCommand::SetInstrumentStatus`],
//! which the per-underlying submit path **routes** — so the command reaches the
//! actor and is journaled; its response reports *accepted and sequenced*, not a
//! confirmed halt/resume (the applied outcome waits on the `Receipt`→`VenueOutcome`
//! seam, `matching-expert`).
//!
//! **Venue-global controls (kill-switch / enable / parameters)** translate to a
//! [`VenueCommand::MarketMakerControl`], which [`AppState::submit`] **fans out** to
//! every underlying's actor, each journaling it (#47) — so the control is sequenced
//! and replayable. The live persona apply (mapping the knobs onto the market-maker
//! engine on the sequenced path) is wired by the persona layer (#47 phase 2); until
//! then the command is journaled and dispatched to the apply seam with no live sink
//! installed. The `GET` status reads return the placeholder default until the
//! market-maker engine surfaces live state.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, State};

use crate::auth::Authorized;
use crate::error::VenueError;
use crate::exchange::{InstrumentStatus, VenueCommand};
use crate::gateway::rest::middleware::require;
use crate::gateway::rest::support::parse_style;
use crate::models::{
    InstrumentToggleResponse, InstrumentsListResponse, KillSwitchRequest, KillSwitchResponse,
    Permission, SystemControlResponse, UpdateParametersRequest, UpdateParametersResponse,
};
use crate::state::AppState;

/// Current control-plane status — a `Read` introspection read. Returns the
/// placeholder default until the market-maker engine (#015) is wired.
#[utoipa::path(
    get, path = "/api/v1/controls", tag = "controls",
    responses(
        (status = 200, description = "Control status", body = SystemControlResponse),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn get_controls() -> Json<SystemControlResponse> {
    Json(SystemControlResponse {
        master_enabled: true,
        spread_multiplier: 1.0,
        size_scalar: 1.0,
        directional_skew: 0.0,
    })
}

/// Set the master kill switch — a **Sequenced** `MarketMakerControl` requiring
/// `Admin`. The venue-global control fans out to every underlying's actor (#47).
#[utoipa::path(
    post, path = "/api/v1/controls/kill-switch", tag = "controls",
    request_body = KillSwitchRequest,
    responses(
        (status = 200, description = "Kill switch set", body = KillSwitchResponse),
        (status = 404, description = "No hosted underlyings for the venue-global control"),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn kill_switch(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Json(request): Json<KillSwitchRequest>,
) -> Result<Json<KillSwitchResponse>, VenueError> {
    require(&auth, Permission::Admin)?;
    state
        .submit(VenueCommand::MarketMakerControl {
            spread_multiplier: None,
            size_scalar: None,
            directional_skew: None,
            enabled: Some(request.enabled),
        })
        .await?;
    Ok(Json(KillSwitchResponse {
        success: true,
        message: "kill switch updated".to_string(),
        master_enabled: request.enabled,
    }))
}

/// Enable market-maker quoting — a **Sequenced** `MarketMakerControl` requiring
/// `Admin`. The venue-global control fans out to every underlying's actor (#47).
#[utoipa::path(
    post, path = "/api/v1/controls/enable", tag = "controls",
    request_body = KillSwitchRequest,
    responses(
        (status = 200, description = "Enable state set", body = KillSwitchResponse),
        (status = 404, description = "No hosted underlyings for the venue-global control"),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn set_enabled(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Json(request): Json<KillSwitchRequest>,
) -> Result<Json<KillSwitchResponse>, VenueError> {
    require(&auth, Permission::Admin)?;
    state
        .submit(VenueCommand::MarketMakerControl {
            spread_multiplier: None,
            size_scalar: None,
            directional_skew: None,
            enabled: Some(request.enabled),
        })
        .await?;
    Ok(Json(KillSwitchResponse {
        success: true,
        message: "enable state updated".to_string(),
        master_enabled: request.enabled,
    }))
}

/// Update market-maker parameters — a **Sequenced** `MarketMakerControl`
/// requiring `Admin`. The venue-global control fans out to every underlying (#47).
#[utoipa::path(
    post, path = "/api/v1/controls/parameters", tag = "controls",
    request_body = UpdateParametersRequest,
    responses(
        (status = 200, description = "Parameters updated", body = UpdateParametersResponse),
        (status = 404, description = "No hosted underlyings for the venue-global control"),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn update_parameters(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Json(request): Json<UpdateParametersRequest>,
) -> Result<Json<UpdateParametersResponse>, VenueError> {
    require(&auth, Permission::Admin)?;
    // Reject an out-of-range / NaN knob at the boundary (rule 4) so it never enters
    // the journal; only validated controls are sequenced.
    crate::market_maker::validate_control_knobs(
        request.spread_multiplier,
        request.size_scalar,
        request.directional_skew,
    )
    .map_err(VenueError::InvalidOrder)?;
    state
        .submit(VenueCommand::MarketMakerControl {
            spread_multiplier: request.spread_multiplier,
            size_scalar: request.size_scalar,
            directional_skew: request.directional_skew,
            enabled: None,
        })
        .await?;
    Ok(Json(UpdateParametersResponse {
        success: true,
        spread_multiplier: request.spread_multiplier.unwrap_or(1.0),
        size_scalar: request.size_scalar.unwrap_or(1.0),
        directional_skew: request.directional_skew.unwrap_or(0.0),
    }))
}

/// List per-instrument control status — a `Read` read. Empty until the
/// market-maker engine (#015) is wired.
#[utoipa::path(
    get, path = "/api/v1/controls/instruments", tag = "controls",
    responses(
        (status = 200, description = "Instrument control statuses", body = InstrumentsListResponse),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn list_instrument_controls() -> Json<InstrumentsListResponse> {
    Json(InstrumentsListResponse {
        instruments: Vec::new(),
    })
}

/// Toggle an instrument's trading status — a **Sequenced**
/// [`VenueCommand::SetInstrumentStatus`] requiring `Admin`. The per-underlying
/// submit path **routes** it by symbol, so the command reaches the actor and is
/// journaled; `enabled=true` requests resume (`Active`), `enabled=false` requests
/// halt (`Halted`), per the §10 mapping.
///
/// The response reports the command was **accepted and sequenced** (with its
/// `underlying_sequence`), **not** that the halt/resume took effect: the executor
/// applies the transition to the sequenced instrument-status registry (#47), but the
/// [`Receipt`](crate::exchange::Receipt) cannot see that outcome, so an applied
/// confirmation waits on the `Receipt`→`VenueOutcome` surfacing seam.
///
/// The `{symbol}` path segment is the canonical contract symbol
/// `UNDERLYING-YYYYMMDD-STRIKE-STYLE`; the trailing `-C`/`-P` is the style, so a
/// `call`/`put` word segment is also accepted for the last position.
#[utoipa::path(
    post, path = "/api/v1/controls/instrument/{symbol}/toggle", tag = "controls",
    params(("symbol" = String, Path, description = "Canonical contract symbol")),
    request_body = KillSwitchRequest,
    responses(
        (status = 200, description = "Toggle command accepted and sequenced", body = InstrumentToggleResponse),
        (status = 400, description = "Invalid symbol"),
        (status = 403, description = "Missing Admin permission"),
        (status = 404, description = "Underlying not hosted"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn toggle_instrument(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path(symbol): Path<String>,
    Json(request): Json<KillSwitchRequest>,
) -> Result<Json<InstrumentToggleResponse>, VenueError> {
    require(&auth, Permission::Admin)?;
    let symbol = parse_contract_symbol(&symbol)?;
    let status = if request.enabled {
        InstrumentStatus::Active
    } else {
        InstrumentStatus::Halted
    };
    let receipt = state
        .submit(VenueCommand::SetInstrumentStatus {
            symbol: symbol.clone(),
            status,
        })
        .await?;
    Ok(Json(InstrumentToggleResponse {
        // Accepted and sequenced — not a confirmation the status changed.
        success: true,
        symbol,
        enabled: request.enabled,
        sequence: receipt.underlying_sequence,
    }))
}

/// Parses the `{symbol}` toggle path segment into a canonical [`Symbol`], routing
/// through the single upstream grammar.
fn parse_contract_symbol(raw: &str) -> Result<crate::exchange::Symbol, VenueError> {
    // Accept both the canonical `...-C`/`...-P` form and a trailing `call`/`put`
    // word, normalising to the canonical style char.
    if let Ok(symbol) = crate::exchange::Symbol::parse(raw) {
        return Ok(symbol);
    }
    if let Some((prefix, style_word)) = raw.rsplit_once('-') {
        let style = parse_style(style_word)?;
        let char = match style {
            crate::models::OptionStyle::Call => 'C',
            crate::models::OptionStyle::Put => 'P',
        };
        return crate::exchange::Symbol::parse(&format!("{prefix}-{char}"))
            .map_err(VenueError::from);
    }
    crate::exchange::Symbol::parse(raw).map_err(VenueError::from)
}
