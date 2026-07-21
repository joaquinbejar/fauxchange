//! Venue-control handlers — the control plane.
//!
//! Operation class: controls are **Sequenced venue commands** requiring
//! [`Permission::Admin`]
//! ([03 §10](../../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
//! The instrument toggle is a per-symbol [`VenueCommand::SetInstrumentStatus`],
//! which the per-underlying submit path **routes** — so the command reaches the
//! actor and is journaled; its response renders the **observed** sequenced
//! [`VenueOutcome`] off the receipt (#118), so an applied transition is a success
//! and an illegal lifecycle transition surfaces as a typed `409` rather than a
//! false `success:true`.
//!
//! **Venue-global controls (kill-switch / enable / parameters)** translate to a
//! [`VenueCommand::MarketMakerControl`], which [`AppState::submit`] **fans out** to
//! every underlying's actor, each journaling it (#47) — so the control is sequenced
//! and replayable. Each response surfaces the fan-out delivery
//! ([`FanoutSummary`](crate::exchange::FanoutSummary): `ok_count` / `total` /
//! `fully_applied`) so a **partial** fan-out is reported rather than hidden, and an
//! emergency-stop control reports `success` only when it committed on every
//! underlying (#118). The live persona apply (mapping the knobs onto the
//! market-maker engine on the sequenced path) is wired by the persona layer (#47
//! phase 2). The `GET` status reads return the placeholder default until the
//! market-maker engine surfaces live state.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, State};

use crate::auth::Authorized;
use crate::error::VenueError;
use crate::exchange::{FanoutSummary, InstrumentStatus, Receipt, VenueCommand, VenueOutcome};
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
    let receipt = state
        .submit(VenueCommand::MarketMakerControl {
            spread_multiplier: None,
            size_scalar: None,
            directional_skew: None,
            enabled: Some(request.enabled),
        })
        .await?;
    Ok(Json(kill_switch_response(
        &receipt,
        request.enabled,
        "kill switch",
    )))
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
    let receipt = state
        .submit(VenueCommand::MarketMakerControl {
            spread_multiplier: None,
            size_scalar: None,
            directional_skew: None,
            enabled: Some(request.enabled),
        })
        .await?;
    Ok(Json(kill_switch_response(
        &receipt,
        request.enabled,
        "enable state",
    )))
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
    let receipt = state
        .submit(VenueCommand::MarketMakerControl {
            spread_multiplier: request.spread_multiplier,
            size_scalar: request.size_scalar,
            directional_skew: request.directional_skew,
            enabled: None,
        })
        .await?;
    let fanout = fanout_of(&receipt);
    Ok(Json(UpdateParametersResponse {
        // The observed fan-out delivery: `success` only when every underlying applied.
        success: fanout.fully_applied(),
        spread_multiplier: request.spread_multiplier.unwrap_or(1.0),
        size_scalar: request.size_scalar.unwrap_or(1.0),
        directional_skew: request.directional_skew.unwrap_or(0.0),
        ok_count: fanout.ok_count,
        total: fanout.total,
        fully_applied: fanout.fully_applied(),
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
/// The response renders the **observed** sequenced outcome (#118): the executor
/// applies the transition to the sequenced instrument-status registry (#47) and the
/// receipt now carries that [`VenueOutcome`](crate::exchange::VenueOutcome), so an
/// applied transition reports `success:true` while an **illegal** lifecycle
/// transition (e.g. resume-an-`Expired`, which the upstream state machine rejects)
/// surfaces as a typed `409` rather than a false `success:true`.
///
/// The `{symbol}` path segment is the canonical contract symbol
/// `UNDERLYING-YYYYMMDD-STRIKE-STYLE`; the trailing `-C`/`-P` is the style, so a
/// `call`/`put` word segment is also accepted for the last position.
#[utoipa::path(
    post, path = "/api/v1/controls/instrument/{symbol}/toggle", tag = "controls",
    params(("symbol" = String, Path, description = "Canonical contract symbol")),
    request_body = KillSwitchRequest,
    responses(
        (status = 200, description = "Toggle applied and sequenced", body = InstrumentToggleResponse),
        (status = 400, description = "Invalid symbol"),
        (status = 403, description = "Missing Admin permission"),
        (status = 404, description = "Underlying not hosted"),
        (status = 409, description = "Illegal lifecycle transition (rejected by the registry)"),
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
    // Render the OBSERVED sequenced outcome, not the requested state (#118): an
    // applied transition is a success; an illegal lifecycle transition (e.g.
    // resume-an-`Expired`) is a journaled `Rejected` the registry produced, surfaced
    // as a typed `409` (`InstrumentHalted`) rather than a false `success:true`.
    match &receipt.outcome {
        Some(VenueOutcome::Rejected { reason, .. }) => {
            Err(VenueError::InstrumentHalted(reason.clone()))
        }
        _ => Ok(Json(InstrumentToggleResponse {
            success: true,
            symbol,
            enabled: request.enabled,
            sequence: receipt.underlying_sequence,
        })),
    }
}

/// The venue-global fan-out delivery of a control receipt. A `MarketMakerControl`
/// is always fanned to every underlying, so [`Receipt::fanout`] is present; the
/// `None` arm is defensive (a single-underlying-only receipt is treated as fully
/// applied over one delivery) and never taken on this control path.
fn fanout_of(receipt: &Receipt) -> FanoutSummary {
    receipt.fanout.unwrap_or(FanoutSummary {
        ok_count: 1,
        total: 1,
    })
}

/// Builds a [`KillSwitchResponse`] from the observed venue-global fan-out: an
/// emergency-stop control reports `success` only when it committed on **every**
/// underlying, and always surfaces the `ok_count` / `total` / `fully_applied`
/// delivery so a partial fan-out is never hidden (#118).
fn kill_switch_response(receipt: &Receipt, master_enabled: bool, what: &str) -> KillSwitchResponse {
    let fanout = fanout_of(receipt);
    let fully_applied = fanout.fully_applied();
    let message = if fully_applied {
        format!("{what} updated")
    } else {
        format!(
            "{what} partially applied to {}/{} underlyings",
            fanout.ok_count, fanout.total
        )
    };
    KillSwitchResponse {
        success: fully_applied,
        message,
        master_enabled,
        ok_count: fanout.ok_count,
        total: fanout.total,
        fully_applied,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::{EventTimestamp, SequenceNumber};

    /// A representative venue-global control receipt carrying `fanout`.
    fn receipt_with(fanout: Option<FanoutSummary>) -> Receipt {
        Receipt {
            underlying_sequence: SequenceNumber::new(0),
            venue_ts: EventTimestamp::new(0),
            outcome: Some(VenueOutcome::ControlApplied { swept: vec![] }),
            fanout,
        }
    }

    #[test]
    fn test_partial_fanout_reports_not_successful_with_counts() {
        // Two of three underlyings committed: an emergency-stop control must NOT
        // report an unqualified success (#118 Gap 2).
        let receipt = receipt_with(Some(FanoutSummary {
            ok_count: 2,
            total: 3,
        }));
        let response = kill_switch_response(&receipt, false, "kill switch");
        assert!(
            !response.success,
            "a partial emergency-stop is not a success"
        );
        assert!(!response.fully_applied);
        assert_eq!(response.ok_count, 2);
        assert_eq!(response.total, 3);
        assert!(!response.master_enabled);
        assert!(
            response.message.contains("2/3"),
            "the message names the partial delivery: {}",
            response.message
        );
    }

    #[test]
    fn test_full_fanout_reports_successful() {
        let receipt = receipt_with(Some(FanoutSummary {
            ok_count: 2,
            total: 2,
        }));
        let response = kill_switch_response(&receipt, true, "enable state");
        assert!(response.success);
        assert!(response.fully_applied);
        assert_eq!(response.ok_count, 2);
        assert_eq!(response.total, 2);
        assert!(response.master_enabled);
    }

    #[test]
    fn test_absent_fanout_defaults_to_single_full_delivery() {
        // Defensive only: a control receipt is always fanned, but a `None` summary
        // is treated as one fully-applied delivery rather than fabricated failure.
        let summary = fanout_of(&receipt_with(None));
        assert_eq!(
            summary,
            FanoutSummary {
                ok_count: 1,
                total: 1
            }
        );
        assert!(summary.fully_applied());
    }
}
