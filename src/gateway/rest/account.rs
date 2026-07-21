//! Account-scoped read handlers: positions and executions.
//!
//! These are **fully wired** query routes — they read the shared, post-journal
//! stores [`AppState::positions`] / [`AppState::executions`] the fan-out writes
//! into, scoped to the authenticated account, and require `Read` (the auth
//! baseline). Money is integer cents; the derived P&L / delta aggregates are the
//! documented analytic-float exception.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, Query, State};

use crate::auth::Authorized;
use crate::error::VenueError;
use crate::exchange::{
    ExecutionFilter, ExecutionsStore, MarkSource, PositionsStore, SignedCents, Symbol,
};
use crate::models::{
    ExecutionId, ExecutionRecord, ExecutionSummary, ExecutionsListResponse, ExecutionsQuery,
    LiquidityFlag, Position, PositionQuery, PositionSummary, PositionsListResponse,
};
use crate::state::AppState;

/// Sums an iterator of signed cents with checked `i128` accumulation, mapping an
/// overflow to a redacted internal error rather than wrapping.
fn sum_signed<I: Iterator<Item = i64>>(values: I) -> Result<SignedCents, VenueError> {
    let mut acc: i128 = 0;
    for value in values {
        acc = acc
            .checked_add(i128::from(value))
            .ok_or(VenueError::Overflow)?;
    }
    i64::try_from(acc)
        .map(SignedCents::new)
        .map_err(|_| VenueError::Overflow)
}

/// List the authenticated account's positions, marked live against the shared
/// mark book, with priced/unpriced aggregates.
#[utoipa::path(
    get,
    path = "/api/v1/positions",
    tag = "positions",
    params(("underlying" = Option<String>, Query, description = "Filter by underlying")),
    responses(
        (status = 200, description = "The account's positions", body = PositionsListResponse),
        (status = 401, description = "Missing or invalid token"),
        (status = 429, description = "Rate limited"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn list_positions(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Query(query): Query<PositionQuery>,
) -> Result<Json<PositionsListResponse>, VenueError> {
    let account = auth.claims.account();
    let marks = state.marks().as_ref();
    let mut positions = state
        .positions()
        .list(account, marks)
        .map_err(|_| VenueError::Overflow)?;

    if let Some(underlying) = &query.underlying {
        positions.retain(|p| &p.underlying == underlying);
    }

    let summary = summarize_positions(&positions)?;
    Ok(Json(PositionsListResponse { positions, summary }))
}

/// The net position for one contract of the authenticated account.
#[utoipa::path(
    get,
    path = "/api/v1/positions/{symbol}",
    tag = "positions",
    params(("symbol" = String, Path, description = "Canonical contract symbol UNDERLYING-YYYYMMDD-STRIKE-STYLE")),
    responses(
        (status = 200, description = "The position", body = Position),
        (status = 404, description = "No position held for this contract"),
        (status = 400, description = "Invalid symbol"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn get_position(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path(symbol): Path<String>,
) -> Result<Json<Position>, VenueError> {
    let symbol = Symbol::parse(&symbol).map_err(VenueError::from)?;
    let account = auth.claims.account();
    let mark = state.marks().mark(&symbol);
    match state
        .positions()
        .get(account, &symbol, mark)
        .map_err(|_| VenueError::Overflow)?
    {
        Some(position) => Ok(Json(position)),
        None => Err(VenueError::NotFound(symbol.as_str().to_string())),
    }
}

/// Folds the priced/unpriced position aggregates the summary reports.
fn summarize_positions(positions: &[Position]) -> Result<PositionSummary, VenueError> {
    let total_realized = sum_signed(positions.iter().map(|p| p.realized_pnl.get()))?;
    let total_unrealized = sum_signed(
        positions
            .iter()
            .filter_map(|p| p.unrealized_pnl.map(|c| c.get())),
    )?;
    let net_delta: f64 = positions
        .iter()
        .filter(|p| p.unrealized_pnl.is_some())
        .map(|p| p.delta_exposure)
        .sum();
    let unpriced_count = positions
        .iter()
        .filter(|p| p.current_price.is_none())
        .count();

    Ok(PositionSummary {
        total_unrealized_pnl: total_unrealized,
        total_realized_pnl: total_realized,
        net_delta,
        position_count: positions.len(),
        unpriced_count,
    })
}

/// List the authenticated account's execution records with aggregate summary.
///
/// The `underlying` and `limit` filters are applied by the store; the ISO-8601
/// `from`/`to` date filters are accepted but not yet applied (date parsing lands
/// with the durable executions store, #023).
#[utoipa::path(
    get,
    path = "/api/v1/executions",
    tag = "executions",
    params(
        ("from" = Option<String>, Query, description = "Start date ISO-8601 (not yet applied)"),
        ("to" = Option<String>, Query, description = "End date ISO-8601 (not yet applied)"),
        ("underlying" = Option<String>, Query, description = "Filter by underlying"),
        ("limit" = Option<usize>, Query, description = "Max records"),
    ),
    responses(
        (status = 200, description = "The account's executions", body = ExecutionsListResponse),
        (status = 401, description = "Missing or invalid token"),
        (status = 429, description = "Rate limited"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn list_executions(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Query(query): Query<ExecutionsQuery>,
) -> Result<Json<ExecutionsListResponse>, VenueError> {
    let account = auth.claims.account();
    let filter = ExecutionFilter {
        underlying: query.underlying,
        limit: query.limit,
    };
    let executions = state
        .executions()
        .list(account, &filter)
        .map_err(|_| VenueError::Overflow)?;

    let summary = summarize_executions(&executions)?;
    Ok(Json(ExecutionsListResponse {
        executions,
        summary,
    }))
}

/// One execution record of the authenticated account by execution id.
#[utoipa::path(
    get,
    path = "/api/v1/executions/{execution_id}",
    tag = "executions",
    params(("execution_id" = String, Path, description = "The composite execution id")),
    responses(
        (status = 200, description = "The execution record", body = ExecutionRecord),
        (status = 404, description = "No such execution for this account"),
        (status = 401, description = "Missing or invalid token"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn get_execution(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
    Path(execution_id): Path<String>,
) -> Result<Json<ExecutionRecord>, VenueError> {
    let account = auth.claims.account();
    let id = ExecutionId::new(execution_id.clone());
    match state
        .executions()
        .get(&id, account)
        .map_err(|_| VenueError::Overflow)?
    {
        Some(record) => Ok(Json(record)),
        None => Err(VenueError::NotFound(execution_id)),
    }
}

/// Folds the execution aggregate summary.
fn summarize_executions(records: &[ExecutionRecord]) -> Result<ExecutionSummary, VenueError> {
    let total_executions = records.len() as u64;
    let mut total_volume: u64 = 0;
    for record in records {
        total_volume = total_volume
            .checked_add(record.quantity)
            .ok_or(VenueError::Overflow)?;
    }
    let total_edge = sum_signed(records.iter().map(|r| r.edge_cents.get()))?;
    let maker_count = records
        .iter()
        .filter(|r| r.liquidity == LiquidityFlag::Maker)
        .count();
    let maker_ratio = if records.is_empty() {
        0.0
    } else {
        (maker_count as f64) / (records.len() as f64)
    };

    Ok(ExecutionSummary {
        total_executions,
        total_volume,
        total_edge,
        maker_ratio,
    })
}
