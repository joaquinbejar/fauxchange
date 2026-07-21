//! Record / replay control handlers — the persistent-determinism control plane
//! the Backend never had (#030,
//! [04 §4](../../../docs/04-market-data-and-replay.md#4-historical-replay)).
//!
//! Operation class: these are **venue controls** requiring [`Permission::Admin`]
//! ([03 §10](../../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)),
//! except the recording-status read (`Read`). Each has a WS control-message
//! equivalent (`record` / `replay_bundle`) that flips the **same** venue flag or
//! runs the **same** replay, so control parity holds (REST ≡ WS); there is **no**
//! FIX control surface (control parity is REST/WS only).
//!
//! - `GET /api/v1/replay/record` — the recording status (`Read`).
//! - `POST /api/v1/replay/record` — flip the scenario-capture window (`Admin`).
//! - `GET /api/v1/replay/export` — export the current venue's journal as a portable
//!   [`ScenarioBundle`] (`Admin`).
//! - `POST /api/v1/replay/bundle` — replay a submitted [`ScenarioBundle`] **offline**
//!   into a fresh registry and return the reconstructed summary (`Admin`). A
//!   corrupt / version-mismatched / malformed bundle is a typed `400`, never a
//!   panic (the driver returns a typed [`ReplayError`](crate::simulation::ReplayError)).

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, State};

use crate::auth::Authorized;
use crate::error::VenueError;
use crate::gateway::rest::middleware::require;
use crate::models::{
    Permission, RecordControlRequest, RecordingStateResponse, ReplayReportResponse,
};
use crate::simulation::ScenarioBundle;
use crate::state::AppState;

/// Current recording status — a `Read` introspection read (#030).
#[utoipa::path(
    get, path = "/api/v1/replay/record", tag = "replay",
    responses(
        (status = 200, description = "Recording status", body = RecordingStateResponse),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn get_record_status(State(state): State<Arc<AppState>>) -> Json<RecordingStateResponse> {
    Json(RecordingStateResponse {
        recording: state.is_recording(),
    })
}

/// Flip the scenario-capture window on or off — a venue **control** requiring
/// `Admin` (#030). The durable write-ahead journal is unaffected; this marks
/// whether a capture window is active for bundle export. Mirrors the WS `record`
/// action (control parity).
#[utoipa::path(
    post, path = "/api/v1/replay/record", tag = "replay",
    request_body = RecordControlRequest,
    responses(
        (status = 200, description = "Recording state set", body = RecordingStateResponse),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn set_record(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Json(request): Json<RecordControlRequest>,
) -> Result<Json<RecordingStateResponse>, VenueError> {
    require(&auth, Permission::Admin)?;
    state.set_recording(request.enabled);
    Ok(Json(RecordingStateResponse {
        recording: state.is_recording(),
    }))
}

/// Export the current venue's journal as a portable [`ScenarioBundle`] — a venue
/// **control** requiring `Admin` (#030). The bundle is self-describing (journal
/// streams + run manifest with the pinned versions) and replayable on any machine.
#[utoipa::path(
    get, path = "/api/v1/replay/export", tag = "replay",
    responses(
        (status = 200, description = "The exported scenario bundle", body = ScenarioBundle),
        (status = 403, description = "Missing Admin permission"),
        (status = 500, description = "A journal snapshot could not be taken"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn export_bundle(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
) -> Result<Json<ScenarioBundle>, VenueError> {
    require(&auth, Permission::Admin)?;
    let bundle = state.export_bundle().await?;
    Ok(Json(bundle))
}

/// Replay a submitted [`ScenarioBundle`] **offline** into a fresh registry and
/// return the reconstructed summary — a venue **control** requiring `Admin` (#030).
///
/// The bundle's schema + pinned versions are verified against this binary first (a
/// mismatch is a typed `400`), then every underlying's journal is re-executed with
/// the stored event as the integrity oracle; a corrupted event halts as a typed
/// `400` naming the exact `(underlying, sequence)`. Replay does not mutate this
/// live venue. Mirrors the WS `replay_bundle` action (control parity).
#[utoipa::path(
    post, path = "/api/v1/replay/bundle", tag = "replay",
    request_body = ScenarioBundle,
    responses(
        (status = 200, description = "The reconstructed replay summary", body = ReplayReportResponse),
        (status = 400, description = "Version mismatch, journal corruption, schema refused, or a malformed bundle"),
        (status = 403, description = "Missing Admin permission"),
        (status = 500, description = "A durable-store read failed during replay"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn replay_bundle(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Json(bundle): Json<ScenarioBundle>,
) -> Result<Json<ReplayReportResponse>, VenueError> {
    require(&auth, Permission::Admin)?;
    let report = state.replay_bundle(&bundle).await?;
    Ok(Json(report.to_response()))
}
