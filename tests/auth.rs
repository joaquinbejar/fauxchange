//! Integration tests for the #011 auth surface: the request flow **through** the
//! Axum `auth_middleware` (missing token → `401`, insufficient permission →
//! `403`, over-limit → `429` with `X-RateLimit-*` headers, `/health` exempt), and
//! the `rate_limiter_window_bound` property (at most `N` admissions per 60 s
//! window on the venue clock).
//!
//! Driven with `tower::ServiceExt::oneshot` so the middleware runs against a real
//! `axum::Router` without binding a TCP listener.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::middleware::from_fn_with_state;
use axum::routing::get;
use proptest::prelude::*;
use tower::ServiceExt;

use fauxchange::auth::{
    AccountProvision, AccountRegistry, AccountStore, Argon2Hasher, AuthGuard, AuthService,
    BootstrapGate, Claims, FixLoginOutcome, JwtAuth, RATE_LIMIT_WINDOW_MS, RateLimitClock,
    RateLimitKey, RateLimiter, RevocationOracle, auth_middleware,
};
use fauxchange::exchange::Hash32;
use fauxchange::models::{AccountId, Permission};

const BOOTSTRAP_SECRET: &str = "operator-secret";

// ---- test collaborators --------------------------------------------------

/// A controllable venue clock — advanceable so the sliding window is exercised
/// deterministically (never `SystemTime`).
#[derive(Clone)]
struct TestClock(Arc<AtomicU64>);

impl TestClock {
    fn new(start_ms: u64) -> Self {
        Self(Arc::new(AtomicU64::new(start_ms)))
    }
}

impl RateLimitClock for TestClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// A map-backed revocation oracle: an account maps to its current epoch.
struct MapRevocation(HashMap<AccountId, u64>);

impl RevocationOracle for MapRevocation {
    fn current_revocation_epoch(&self, account: &AccountId) -> Option<u64> {
        self.0.get(account).copied()
    }
}

fn now_secs() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(error) => panic!("system clock before the Unix epoch: {error}"),
    }
}

fn dev_auth() -> JwtAuth {
    match JwtAuth::dev() {
        Ok(auth) => auth,
        Err(error) => panic!("embedded dev fixtures must parse: {error}"),
    }
}

/// Builds an `AuthService` that knows `account` at epoch `0`, with `limit`
/// requests per window on a clock fixed at `1_000` ms.
fn service(account: &str, limit: u32) -> AuthService<TestClock> {
    let mut epochs = HashMap::new();
    epochs.insert(AccountId::new(account), 0);
    AuthService::new(
        dev_auth(),
        RateLimiter::new(TestClock::new(1_000), limit),
        Arc::new(MapRevocation(epochs)),
    )
}

fn mint(auth: &JwtAuth, account: &str, permissions: Vec<Permission>) -> String {
    let now = now_secs();
    let claims = Claims::new(AccountId::new(account), permissions, now, now + 3_600, 0);
    match auth.mint_token(
        &BootstrapGate::new(Some(BOOTSTRAP_SECRET.to_string())),
        BOOTSTRAP_SECRET,
        &claims,
    ) {
        Ok(token) => token,
        Err(error) => panic!("minting must succeed with the right secret: {error}"),
    }
}

/// A router whose `/api/v1/orders` route requires `required`, with `/health`
/// mounted behind the same layer (the middleware exempts it internally).
fn app(service: Arc<AuthService<TestClock>>, required: Permission) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/api/v1/orders", get(|| async { "orders" }))
        .layer(from_fn_with_state(
            AuthGuard::new(service, required),
            auth_middleware::<TestClock>,
        ))
}

fn request(uri: &str, bearer: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(uri);
    if let Some(token) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    match builder.body(Body::empty()) {
        Ok(req) => req,
        Err(error) => panic!("building the test request must succeed: {error}"),
    }
}

async fn send(router: Router, req: Request<Body>) -> axum::response::Response {
    match router.oneshot(req).await {
        Ok(response) => response,
        Err(error) => panic!("the router service call must not fail: {error}"),
    }
}

// ---- request flow through the middleware ---------------------------------

#[tokio::test]
async fn test_auth_middleware_missing_token_is_401() {
    let service = Arc::new(service("acct-1", 100));
    let response = send(
        app(service, Permission::Read),
        request("/api/v1/orders", None),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_auth_middleware_insufficient_permission_is_403() {
    let service = Arc::new(service("acct-1", 100));
    // A Read token against a route requiring Trade.
    let token = mint(service.jwt(), "acct-1", vec![Permission::Read]);
    let response = send(
        app(Arc::clone(&service), Permission::Trade),
        request("/api/v1/orders", Some(&token)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_auth_middleware_admitted_request_is_200_with_ratelimit_headers() {
    let service = Arc::new(service("acct-1", 100));
    let token = mint(service.jwt(), "acct-1", vec![Permission::Trade]);
    let response = send(
        app(Arc::clone(&service), Permission::Trade),
        request("/api/v1/orders", Some(&token)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    assert!(headers.get("x-ratelimit-limit").is_some());
    assert!(headers.get("x-ratelimit-remaining").is_some());
    assert!(headers.get("x-ratelimit-reset").is_some());
}

#[tokio::test]
async fn test_auth_middleware_over_limit_is_429_with_headers() {
    // Budget of 1: the second request in the window is throttled.
    let service = Arc::new(service("acct-1", 1));
    let token = mint(service.jwt(), "acct-1", vec![Permission::Trade]);

    let first = send(
        app(Arc::clone(&service), Permission::Trade),
        request("/api/v1/orders", Some(&token)),
    )
    .await;
    assert_eq!(first.status(), StatusCode::OK);

    let second = send(
        app(Arc::clone(&service), Permission::Trade),
        request("/api/v1/orders", Some(&token)),
    )
    .await;
    assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    let headers = second.headers();
    assert_eq!(
        headers
            .get("x-ratelimit-limit")
            .and_then(|v| v.to_str().ok()),
        Some("1")
    );
    assert_eq!(
        headers
            .get("x-ratelimit-remaining")
            .and_then(|v| v.to_str().ok()),
        Some("0")
    );
    assert!(headers.get(header::RETRY_AFTER).is_some());
    assert!(headers.get("x-ratelimit-reset").is_some());
}

#[tokio::test]
async fn test_auth_middleware_health_is_exempt_and_answers_unconditionally() {
    // A budget of 0 would throttle any counted request; /health must still answer
    // with no auth header at all.
    let service = Arc::new(service("acct-1", 0));
    let response = send(app(service, Permission::Admin), request("/health", None)).await;
    assert_eq!(response.status(), StatusCode::OK);
    // No rate-limit accounting on the exempt path.
    assert!(response.headers().get("x-ratelimit-limit").is_none());
}

// ---- property: at most N admissions per 60 s window ----------------------

proptest! {
    /// Across any burst of requests inside a single 60 s window, the limiter admits
    /// **exactly** `min(limit, requests)` — i.e. at most `limit` per window.
    #[test]
    fn rate_limiter_window_bound(limit in 1u32..=10, requests in 1usize..=50) {
        let limiter = RateLimiter::with_window(TestClock::new(0), limit, RATE_LIMIT_WINDOW_MS);
        let key = RateLimitKey::Account { account: AccountId::new("acct"), revocation_epoch: 0 };
        let mut allowed = 0u32;
        for _ in 0..requests {
            if limiter.check_and_record_status(&key).allowed {
                allowed += 1;
            }
        }
        let expected = limit.min(u32::try_from(requests).unwrap_or(u32::MAX));
        prop_assert_eq!(allowed, expected);
    }
}

// ==========================================================================
// #012 — account-registry lifecycle through the middleware (REST/WS scope)
// ==========================================================================

/// Provisions the #012 registry (a `Trade` account that can also log in over FIX,
/// and a `Read`-only account) at the pinned Argon2id parameters, then wires it
/// into an `AuthService` as the revocation oracle behind an advanceable clock.
fn registry_backed_service() -> (Arc<AccountRegistry>, Arc<AuthService<TestClock>>) {
    let provisions = vec![
        AccountProvision::new(
            AccountId::new("trader"),
            Hash32([0x11; 32]),
            vec![Permission::Trade],
        )
        .with_fix_login("trader-fix", "sw0rdf1sh"),
        AccountProvision::new(
            AccountId::new("viewer"),
            Hash32([0x22; 32]),
            vec![Permission::Read],
        ),
    ];
    let registry = match AccountRegistry::provision(Argon2Hasher::new(None), provisions) {
        Ok(registry) => Arc::new(registry),
        Err(error) => panic!("provisioning must succeed: {error}"),
    };
    let service = Arc::new(AuthService::new(
        dev_auth(),
        RateLimiter::new(TestClock::new(1_000), 100),
        Arc::clone(&registry) as Arc<dyn RevocationOracle>,
    ));
    (registry, service)
}

/// The full #012 lifecycle over the REST middleware: provision → account-resolved
/// mint → a `Read` account is refused order entry (403) → revoke → its tokens are
/// refused (401). One registry row backs both the mint and the revocation, so the
/// identity and permissions are the same the JWT path resolves.
#[tokio::test]
async fn test_account_registry_lifecycle_mint_refuse_revoke() {
    let (registry, service) = registry_backed_service();
    let gate = BootstrapGate::new(Some(BOOTSTRAP_SECRET.to_string()));

    // The FIX username resolves the SAME AccountId as the JWT path (one identity).
    match registry.account_by_fix_username("trader-fix") {
        Some(account) => {
            assert_eq!(account.id, AccountId::new("trader"));
            assert_eq!(account.permissions, vec![Permission::Trade]);
        }
        None => panic!("the FIX username must resolve the trader account"),
    }
    // And its Argon2id password verifies (schema-ready FIX login path).
    assert!(matches!(
        registry.verify_fix_password("trader-fix", "sw0rdf1sh"),
        FixLoginOutcome::Authenticated { .. }
    ));

    // Account-resolved mint for the `viewer` (Read) — a registry AccountId with the
    // account's REGISTERED permissions, never a fresh subject.
    let viewer_token = match registry.mint_for_account(
        service.jwt(),
        &gate,
        &AccountId::new("viewer"),
        BOOTSTRAP_SECRET,
        now_secs(),
        3_600,
    ) {
        Ok(token) => token,
        Err(error) => panic!("resolved mint must succeed: {error}"),
    };
    // A Read account is refused order entry (a Trade route) → 403.
    let refused = send(
        app(Arc::clone(&service), Permission::Trade),
        request("/api/v1/orders", Some(&viewer_token)),
    )
    .await;
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);

    // A Trade account IS admitted on the same route → 200.
    let trader_token = match registry.mint_for_account(
        service.jwt(),
        &gate,
        &AccountId::new("trader"),
        BOOTSTRAP_SECRET,
        now_secs(),
        3_600,
    ) {
        Ok(token) => token,
        Err(error) => panic!("resolved mint must succeed: {error}"),
    };
    let admitted = send(
        app(Arc::clone(&service), Permission::Trade),
        request("/api/v1/orders", Some(&trader_token)),
    )
    .await;
    assert_eq!(admitted.status(), StatusCode::OK);

    // Revoke the trader: the SAME (now stale) token is refused on the next request.
    assert_eq!(registry.revoke(&AccountId::new("trader")), Some(1));
    let after_revoke = send(
        app(Arc::clone(&service), Permission::Trade),
        request("/api/v1/orders", Some(&trader_token)),
    )
    .await;
    assert_eq!(after_revoke.status(), StatusCode::UNAUTHORIZED);
}

// ---- determinism: venue-clock rate-limit decision is replay-stable -------

#[test]
fn test_rate_limit_decision_is_replay_stable_across_fresh_venues() {
    // Two independently constructed limiters, driven over the SAME venue-clock
    // timeline, must yield identical admit/deny sequences — the venue-clock keying
    // (never SystemTime) makes rate-limit decisions reproducible on replay.
    let timeline = [0u64, 0, 0, 30_000, 30_000, 61_000, 61_000];
    let run = || {
        let clock = TestClock::new(0);
        let limiter = RateLimiter::with_window(clock.clone(), 2, RATE_LIMIT_WINDOW_MS);
        let key = RateLimitKey::Account {
            account: AccountId::new("acct-1"),
            revocation_epoch: 0,
        };
        timeline
            .iter()
            .map(|&tick| {
                clock.0.store(tick, Ordering::SeqCst);
                limiter.check_and_record_status(&key).allowed
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(run(), run());
}
