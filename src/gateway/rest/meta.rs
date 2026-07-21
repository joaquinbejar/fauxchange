//! Health, venue statistics, and JWT token issuance handlers.
//!
//! `GET /health` is the sole auth-exempt route; `GET /api/v1/stats` is a
//! `Permission::Read` introspection read; `POST /api/v1/auth/token` is
//! JWT-exempt (a caller without a token requests one) but bootstrap-gated and
//! peer-rate-limited — a **replay exclusion** (credential-plane, never venue
//! state, [03 §10](../../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Extension, State};

use crate::auth::{Authorized, DEFAULT_TOKEN_TTL_SECS, WS_TICKET_TTL_SECS};
use crate::error::VenueError;
use crate::exchange::SymbolParser;
use crate::gateway::rest::extract::Json;
use crate::gateway::rest::support::format_rfc3339_utc;
use crate::models::{
    GlobalStatsResponse, HealthResponse, TokenRequest, TokenResponse, WsTicketResponse,
};
use crate::state::AppState;

/// Liveness check — the container health probe. The **only** route exempt from
/// both authentication and rate limiting; it must answer unconditionally.
#[utoipa::path(
    get,
    path = "/health",
    tag = "meta",
    responses((status = 200, description = "Service is healthy", body = HealthResponse)),
)]
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Global venue statistics — a `Permission::Read` introspection read.
///
/// `underlying_count` is authoritative (one single-writer actor per hosted
/// underlying); the expiration/strike counts are projected from the shared
/// symbol index (instruments become visible as they are traded).
/// `total_orders` (resting depth) is `0` until the actor exposes a live
/// book-read path — the sequenced books are single-writer-owned and are not
/// yet readable from the gateway.
#[utoipa::path(
    get,
    path = "/api/v1/stats",
    tag = "meta",
    responses(
        (status = 200, description = "Venue statistics", body = GlobalStatsResponse),
        (status = 401, description = "Missing or invalid token"),
        (status = 429, description = "Rate limited"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn stats(State(state): State<Arc<AppState>>) -> Json<GlobalStatsResponse> {
    use std::collections::BTreeSet;

    let mut expirations: BTreeSet<(String, String)> = BTreeSet::new();
    let mut strikes: BTreeSet<(String, String, u64)> = BTreeSet::new();
    for symbol in state.symbol_index().symbols() {
        if let Ok(parsed) = SymbolParser::parse(&symbol) {
            expirations.insert((
                parsed.underlying().to_string(),
                parsed.expiration_str().to_string(),
            ));
            strikes.insert((
                parsed.underlying().to_string(),
                parsed.expiration_str().to_string(),
                parsed.strike(),
            ));
        }
    }

    Json(GlobalStatsResponse {
        underlying_count: state.underlying_count(),
        total_expirations: expirations.len(),
        total_strikes: strikes.len(),
        // Resting-order depth is not yet readable from the single-writer books;
        // reported as `0` rather than fabricated. The book-read path is a
        // matching-expert seam extension.
        total_orders: 0,
    })
}

/// Issues a signed JWT for a **registered** account, gated by the operator
/// bootstrap secret (`AUTH_BOOTSTRAP_SECRET`). JWT-exempt (no token required to
/// obtain one) but peer-rate-limited.
///
/// The account's permissions and revocation epoch are resolved from the
/// registry — the request's `permissions` field is advisory and is **not**
/// trusted; the venue never mints permissions a caller asks for
/// ([ADR-0007](../../../docs/adr/0007-fix-credentials-and-account-model.md)). A
/// wrong secret is a `401`, an unknown account (after the secret clears) a
/// `404`; the secret is never logged or echoed.
#[utoipa::path(
    post,
    path = "/api/v1/auth/token",
    tag = "auth",
    request_body = TokenRequest,
    responses(
        (status = 200, description = "A signed JWT", body = TokenResponse),
        (status = 401, description = "Token issuance disabled or wrong bootstrap secret"),
        (status = 404, description = "Unknown account"),
        (status = 429, description = "Rate limited"),
    ),
)]
pub async fn issue_token(
    State(state): State<Arc<AppState>>,
    Json(request): Json<TokenRequest>,
) -> Result<Json<TokenResponse>, VenueError> {
    let issued_at_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| VenueError::Overflow)?;
    let ttl_secs = request.ttl_secs.unwrap_or(DEFAULT_TOKEN_TTL_SECS);

    let token = state
        .mint_token(&request.account, &request.secret, issued_at_secs, ttl_secs)
        .map_err(map_mint_error)?;

    let expires_at_secs = issued_at_secs
        .checked_add(ttl_secs)
        .ok_or(VenueError::Overflow)?;

    Ok(Json(TokenResponse {
        token,
        expires_at: format_rfc3339_utc(expires_at_secs),
    }))
}

/// Maps a token-issuance [`AuthError`](crate::auth::AuthError) onto the request
/// boundary error, **never leaking the bootstrap secret**: a disabled/mismatched
/// gate is an unauthorized `401`, an unknown OR revoked account a `404` (the two
/// are deliberately indistinguishable, so the route is no revocation-state
/// oracle), and everything else a redacted internal `500`.
fn map_mint_error(error: crate::auth::AuthError) -> VenueError {
    use crate::auth::AuthError;
    match error {
        AuthError::BootstrapDisabled | AuthError::BootstrapMismatch => VenueError::Unauthorized,
        // A revoked account is refused a fresh token (the #042 P1 mint guard). It
        // renders the SAME as an unknown account — a client-side `404`, never a
        // `500` and never an ERROR-level "issuance failed" log — so the route
        // exposes no way to tell "revoked" from "does not exist".
        AuthError::UnknownAccount | AuthError::AccountRevoked => {
            VenueError::NotFound("account".to_string())
        }
        // Signing / lifetime / key-load failures are internal; the cause stays
        // in `tracing`, never on the wire.
        other => {
            tracing::error!(error = %other, "token issuance failed");
            VenueError::Overflow
        }
    }
}

/// Mints a short-lived, single-use WebSocket handshake **ticket** bound to the
/// authenticated caller's permissions — the recommended way to authenticate
/// `GET /ws` without leaking a long-lived bearer JWT into the query string (and
/// thus into access logs, proxies, and browser history).
///
/// Requires the auth baseline (`Read`, the middleware's baseline); the ticket
/// carries the SAME [`Permission`](crate::models::Permission) set the caller's
/// bearer holds, so `/ws` control actions remain gated by `Admin` exactly as they
/// are for a bearer handshake. The ticket is unguessable (CSPRNG), expires within
/// [`WS_TICKET_TTL_SECS`] seconds, and is valid for exactly one upgrade.
#[utoipa::path(
    post,
    path = "/api/v1/auth/ws-ticket",
    tag = "auth",
    responses(
        (status = 200, description = "A short-lived single-use WS handshake ticket", body = WsTicketResponse),
        (status = 401, description = "Missing or invalid token"),
        (status = 429, description = "Rate limited, or too many outstanding tickets"),
    ),
    security(("bearer_jwt" = [])),
)]
pub async fn issue_ws_ticket(
    State(state): State<Arc<AppState>>,
    Extension(auth): Extension<Authorized>,
) -> Result<Json<WsTicketResponse>, VenueError> {
    // Wall-clock seconds: the ticket TTL is a credential-plane concern, off the
    // sequenced path (never the venue clock).
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| VenueError::Overflow)?;
    let ticket = state.auth().mint_ws_ticket(&auth.claims, now_secs)?;
    let expires_at_secs = now_secs
        .checked_add(WS_TICKET_TTL_SECS)
        .ok_or(VenueError::Overflow)?;

    Ok(Json(WsTicketResponse {
        ticket,
        expires_at: format_rfc3339_utc(expires_at_secs),
        expires_in_secs: WS_TICKET_TTL_SECS,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthError;

    #[test]
    fn test_map_mint_error_revoked_account_is_not_found_not_internal() {
        // The #042 P1 mint guard makes `mint_for_account` return `AccountRevoked`
        // for a revoked account. On the REST token route that MUST render as a
        // client-side `404`, never the `other`-arm internal `500` — a revoked
        // account is an expected refusal, not a server failure — and it is the SAME
        // rendering as an unknown account, so the route is no revocation oracle.
        assert!(matches!(
            map_mint_error(AuthError::AccountRevoked),
            VenueError::NotFound(_)
        ));
        assert!(matches!(
            map_mint_error(AuthError::UnknownAccount),
            VenueError::NotFound(_)
        ));
    }

    #[test]
    fn test_map_mint_error_gate_failures_stay_unauthorized() {
        // Regression guard: the new revoked arm does not disturb the gate mapping.
        assert!(matches!(
            map_mint_error(AuthError::BootstrapDisabled),
            VenueError::Unauthorized
        ));
        assert!(matches!(
            map_mint_error(AuthError::BootstrapMismatch),
            VenueError::Unauthorized
        ));
    }
}
