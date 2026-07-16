//! Admin snapshot handlers — an operator escape hatch.
//!
//! Operation class ([03 §10](../../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)):
//! snapshot **capture** is a **replay exclusion** (a read-of-state, not
//! journaled) and snapshot **restore** starts a fresh journal epoch. Both
//! require [`Permission::Admin`].
//!
//! **Capture** is wired to the available read: it aggregates each underlying's
//! journal via [`AppState::journal_snapshot`] (the read-only, single-writer-safe
//! mailbox path) and reports how many books and sequenced commands were
//! captured.
//!
//! **Durable registry + live restore are a `matching-expert` seam dependency.**
//! The consistent-cut capture/restore of #009 (`UnderlyingActor::capture` /
//! `restore`) are synchronous-under-writer methods that are **not exposed over
//! the spawned actor's mailbox** (`ActorMessage` carries only `Command` and
//! `Snapshot`), and there is no durable snapshot store at the gateway. So the
//! snapshot *listing* returns empty and *restore* is a `404` until that mailbox
//! plumbing and the snapshot store land — the routes never fabricate a restore.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::Json;
use axum::extract::{Extension, Path, State};

use crate::auth::Authorized;
use crate::error::VenueError;
use crate::exchange::{EventTimestamp, RecordKind};
use crate::gateway::rest::middleware::require;
use crate::models::{
    CreateSnapshotResponse, Permission, RestoreSnapshotResponse, SnapshotSummary,
    SnapshotsListResponse,
};
use crate::state::AppState;

/// Disambiguates generated snapshot ids within a process.
static SNAPSHOT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Capture a consistent read of the venue's journals — a **replay exclusion**
/// requiring `Admin`. Aggregates each underlying's journal (a read-only mailbox
/// path) and reports the books and sequenced commands captured.
#[utoipa::path(
    post, path = "/api/v1/admin/snapshot", tag = "admin",
    responses(
        (status = 200, description = "Snapshot captured", body = CreateSnapshotResponse),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn create_snapshot(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
) -> Result<Json<CreateSnapshotResponse>, VenueError> {
    require(&auth, Permission::Admin)?;

    let mut orderbooks_saved: u64 = 0;
    let mut orders_saved: u64 = 0;
    let mut orderbooks_failed: u64 = 0;

    for underlying in state.underlyings() {
        match state.journal_snapshot(underlying).await {
            Ok(snapshot) => {
                orderbooks_saved = orderbooks_saved
                    .checked_add(1)
                    .ok_or(VenueError::Overflow)?;
                let commands = snapshot
                    .records
                    .iter()
                    .filter(|record| record.kind() == RecordKind::Command)
                    .count() as u64;
                orders_saved = orders_saved
                    .checked_add(commands)
                    .ok_or(VenueError::Overflow)?;
            }
            Err(_) => {
                orderbooks_failed = orderbooks_failed
                    .checked_add(1)
                    .ok_or(VenueError::Overflow)?;
            }
        }
    }

    let snapshot_id = format!(
        "{}-snap-{}",
        state.lineage_id().as_str(),
        SNAPSHOT_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    Ok(Json(CreateSnapshotResponse {
        success: orderbooks_failed == 0,
        snapshot_id,
        orderbooks_saved,
        orders_saved,
        orderbooks_failed,
        // The venue clock is not exposed to the gateway yet (fixed clock, #016).
        timestamp: EventTimestamp::new(0),
    }))
}

/// List captured snapshots. **Empty**: capture is not persisted in a durable
/// gateway registry yet (the snapshot store is a persistence dependency).
#[utoipa::path(
    get, path = "/api/v1/admin/snapshots", tag = "admin",
    responses(
        (status = 200, description = "Snapshots (empty until the snapshot store lands)", body = SnapshotsListResponse),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn list_snapshots(
    Extension(auth): Extension<Authorized>,
) -> Result<Json<SnapshotsListResponse>, VenueError> {
    require(&auth, Permission::Admin)?;
    Ok(Json(SnapshotsListResponse {
        snapshots: Vec::new(),
        total: 0,
    }))
}

/// Fetch one snapshot summary. **`404`**: no durable snapshot registry yet.
#[utoipa::path(
    get, path = "/api/v1/admin/snapshots/{snapshot_id}", tag = "admin",
    params(("snapshot_id" = String, Path, description = "The snapshot id")),
    responses(
        (status = 200, description = "Snapshot summary", body = SnapshotSummary),
        (status = 404, description = "No such snapshot"),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn get_snapshot(
    Extension(auth): Extension<Authorized>,
    Path(snapshot_id): Path<String>,
) -> Result<Json<SnapshotSummary>, VenueError> {
    require(&auth, Permission::Admin)?;
    Err(VenueError::NotFound(snapshot_id))
}

/// Restore a snapshot. **`404`**: live restore over the spawned actor is a
/// `matching-expert` seam dependency (no `Restore` mailbox message yet), so this
/// never fabricates a restore.
#[utoipa::path(
    post, path = "/api/v1/admin/snapshots/{snapshot_id}/restore", tag = "admin",
    params(("snapshot_id" = String, Path, description = "The snapshot id to restore")),
    responses(
        (status = 200, description = "Snapshot restored", body = RestoreSnapshotResponse),
        (status = 404, description = "No such snapshot / restore not yet wired"),
        (status = 403, description = "Missing Admin permission"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn restore_snapshot(
    Extension(auth): Extension<Authorized>,
    Path(snapshot_id): Path<String>,
) -> Result<Json<RestoreSnapshotResponse>, VenueError> {
    require(&auth, Permission::Admin)?;
    Err(VenueError::NotFound(snapshot_id))
}
