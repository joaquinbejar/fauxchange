//! REST gateway auth + admission layer.
//!
//! Every non-exempt route runs behind [`app_state_auth_middleware`], which
//! delegates to the venue's **one** [`AuthService::admit`](crate::auth::AuthService::admit)
//! ([03 §6](../../../docs/03-protocol-surfaces.md#6-authentication)): it verifies
//! the JWT, enforces the sliding-window rate limit on the resolved key, checks
//! the revocation epoch, and gates a **baseline** [`Permission::Read`] — then
//! attaches the [`Authorized`] identity to the request so each handler can gate
//! its own stronger permission (`require`). `GET /health` is exempt from both
//! auth and rate limiting; `POST /api/v1/auth/token` is JWT-exempt but still
//! peer-rate-limited ([`peer_rate_limit_middleware`]).
//!
//! The middleware reuses `AuthService::admit` rather than the generic
//! [`auth_middleware`](crate::auth::auth_middleware): `AppState` embeds its
//! `AuthService` inside the shared `Arc<AppState>` (it is not separately
//! `Arc`-wrapped, as `AuthGuard` requires), so the REST layer carries the
//! `Arc<AppState>` and calls `admit` through it — the same admission logic,
//! reached through the state the handlers already share.
//!
//! **Peer seam.** [`peer_addr_middleware`] copies the real socket peer from
//! `ConnectInfo<SocketAddr>` into the [`PeerAddr`] extension **before** the auth
//! layer reads it, so every unauthenticated caller rate-limits under its own IP
//! (never one shared bucket) — from the real connection, never an
//! `X-Forwarded-For` header ([08 §5](../../../docs/08-threat-model.md#5-resource-exhaustion)).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::auth::{Admission, Authorized, PeerAddr, RateLimitKey};
use crate::error::VenueError;
use crate::models::Permission;
use crate::state::AppState;

/// The per-route-group middleware state: the shared venue state plus the
/// **baseline** permission the auth layer gates (always [`Permission::Read`] for
/// the REST surface — handlers gate their own stronger permission via
/// `require`).
#[derive(Clone)]
pub struct AppStateAuthGuard {
    state: Arc<AppState>,
    baseline: Permission,
}

impl AppStateAuthGuard {
    /// Builds a guard gating `baseline` for the routes it protects.
    #[must_use]
    pub fn new(state: Arc<AppState>, baseline: Permission) -> Self {
        Self { state, baseline }
    }
}

/// The Axum auth layer for the protected REST routes. Verifies the JWT,
/// enforces the admission rate limit, checks the revocation epoch, and gates the
/// baseline permission; `GET /health` passes through untouched.
///
/// On admission it inserts the [`Authorized`] identity into the request
/// extensions and attaches the `X-RateLimit-*` headers to the response; on
/// rejection it renders the typed [`VenueError`] (`401`/`403`/`429`) with the
/// rate-limit context.
pub async fn app_state_auth_middleware(
    State(guard): State<AppStateAuthGuard>,
    mut request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_owned();
    let bearer = extract_bearer(request.headers());
    let peer = extract_peer(&request);

    match guard
        .state
        .auth()
        .admit(&path, bearer.as_deref(), peer, guard.baseline)
    {
        Admission::Exempt => next.run(request).await,
        Admission::Admitted {
            identity,
            rate_limit,
        } => {
            request.extensions_mut().insert(*identity);
            let mut response = next.run(request).await;
            rate_limit.apply_headers(response.headers_mut());
            response
        }
        Admission::Rejected { error, rate_limit } => {
            let mut response = error.into_response();
            if let Some(decision) = rate_limit {
                decision.apply_headers(response.headers_mut());
            }
            response
        }
    }
}

/// The Axum layer for `POST /api/v1/auth/token`: the token route cannot require
/// a JWT (a caller without a token requests one), so it is JWT-exempt but still
/// **peer-rate-limited** — a wrong bootstrap secret counts against the peer's
/// budget, so the operator secret cannot be brute-forced without hitting the
/// `429` limit ([08 §5](../../../docs/08-threat-model.md#5-resource-exhaustion)).
pub async fn peer_rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let key = RateLimitKey::Peer(extract_peer(&request));
    let decision = state.auth().rate_limiter().check_and_record_status(&key);
    if !decision.allowed {
        let mut response = VenueError::RateLimited.into_response();
        decision.apply_headers(response.headers_mut());
        return response;
    }
    let mut response = next.run(request).await;
    decision.apply_headers(response.headers_mut());
    response
}

/// Copies the **real socket peer** from the `ConnectInfo<SocketAddr>` extension
/// (populated by `into_make_service_with_connect_info::<SocketAddr>()`) into the
/// [`PeerAddr`] extension the auth layer reads, so each unauthenticated client
/// rate-limits under its own IP. Never trusts an `X-Forwarded-For` header. When
/// no connect-info is present (unit tests via `oneshot`), it leaves the
/// extension absent and the auth layer falls back to the unspecified address.
pub async fn peer_addr_middleware(mut request: Request, next: Next) -> Response {
    if request.extensions().get::<PeerAddr>().is_none()
        && let Some(ConnectInfo(addr)) = request.extensions().get::<ConnectInfo<SocketAddr>>()
    {
        let peer = PeerAddr(addr.ip());
        request.extensions_mut().insert(peer);
    }
    next.run(request).await
}

/// Gates a handler's own required permission against the admitted identity,
/// applying the `Admin ⇒ Trade ⇒ Read` implication ([`Permission::grants`]).
/// The baseline [`Permission::Read`] is already enforced by
/// [`app_state_auth_middleware`]; a handler calls this for a stronger
/// `Trade`/`Admin` requirement.
///
/// # Errors
///
/// [`VenueError::Forbidden`] carrying the missing permission (`403`).
pub(crate) fn require(auth: &Authorized, required: Permission) -> Result<(), VenueError> {
    if auth.claims.has_permission(required) {
        Ok(())
    } else {
        Err(VenueError::Forbidden(required))
    }
}

/// Extracts a bearer token from the `Authorization` header, if present and
/// well-formed. (A REST-local copy of the auth-module helper, which is private.)
fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))?
        .trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_owned())
    }
}

/// Resolves the peer IP from the [`PeerAddr`] extension, falling back to the
/// unspecified address when none is attached.
fn extract_peer(request: &Request) -> IpAddr {
    request
        .extensions()
        .get::<PeerAddr>()
        .map(|peer| peer.0)
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}
