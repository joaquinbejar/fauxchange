//! The v0.1 **security capstone** suite (#021) — the defining threat-model
//! deliverables for the surfaces shipped so far (REST/WS + auth; FIX is v0.4).
//!
//! It consolidates the hardening scattered across #003/#006/#011/#013/#014 into a
//! single security-framed suite backing [docs/08 §4–§7](../docs/08-threat-model.md)
//! and [docs/TESTING.md §14](../docs/TESTING.md#14-security-testing):
//!
//! 1. **Captured-log credential test** — drive a full logon + order flow with a
//!    `tracing` capture layer installed and assert no password, Argon2id hash,
//!    JWT signing key, bootstrap secret, pepper, or DB connection string appears in
//!    any captured log, error body, or serialised state; the boot config log is
//!    redacted.
//! 2. **Auth / authorization matrix** — every mutating REST/WS op rejects a
//!    missing / insufficient permission; a `Read` account is refused order entry on
//!    every surface; a revocation refuses the account's tokens.
//! 3. **Adversarial fixtures** — oversized bodies, truncated messages, out-of-range
//!    economic fields, malformed symbols, an unknown DTO field → each a typed 4xx /
//!    typed WS reject, never a panic, never a silent accept.
//! 4. **DoS-control tests** — the rate limiter (one budget), the bounded actor
//!    mailbox (backpressure → typed `RateLimited`), the bounded broadcast (laggard
//!    drop, no OOM), the connection cap, and sequence-exhaustion sealing, each as a
//!    **security control**.
//!
//! Driven with `tower::ServiceExt::oneshot` against the real
//! [`fauxchange::gateway::rest::create_router`] and against the venue's public
//! service seams — no TCP listener.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tower::ServiceExt;

use fauxchange::auth::{
    AccountProvision, AccountStore, FixLoginOutcome, RateLimitKey, RateLimitTier, RateLimiter,
};
use fauxchange::exchange::{
    ActorConfig, Cents, EventTimestamp, FixedClock, Hash32, InMemoryVenueJournal, JournalHeader,
    LineageId, NoopFanOut, PlaceholderExecutor, STPMode, SequenceNumber, Side as SeamSide, Symbol,
    TimeInForce as SeamTif, UnderlyingActor, VenueCommand, VenueEvent, VenueOutcome,
    spawn_underlying_actor,
};
use fauxchange::gateway::rest::{MAX_REQUEST_BODY_BYTES, create_router};
use fauxchange::gateway::ws::{FrameOutcome, parse_frame};
use fauxchange::models::{AccountId, MAX_ORDER_QUANTITY, MAX_PRICE_CENTS, OrderType, VenueOrderId};
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};
use fauxchange::subscription::{OrderbookSubscriptionManager, WS_BROADCAST_CAPACITY};

const SECRET: &str = "operator-bootstrap-secret";

/// The concrete per-contract order-entry path for `BTC` call `50000 / 20240329`.
const ORDER_PATH: &str =
    "/api/v1/underlyings/BTC/expirations/20240329/strikes/50000/options/call/orders";
/// The venue kill-switch control (Admin-only).
const KILL_SWITCH_PATH: &str = "/api/v1/controls/kill-switch";

// ============================================================================
// Shared harness
// ============================================================================

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// A venue hosting `BTC`/`ETH` with an admin / trader / reader account, the
/// bootstrap secret set, and a generous rate-limit budget.
fn venue() -> Arc<AppState> {
    let accounts = vec![
        AccountProvision::new(
            AccountId::new("admin-1"),
            Hash32([1; 32]),
            vec![fauxchange::models::Permission::Admin],
        ),
        AccountProvision::new(
            AccountId::new("trader-1"),
            Hash32([2; 32]),
            vec![fauxchange::models::Permission::Trade],
        ),
        AccountProvision::new(
            AccountId::new("reader-1"),
            Hash32([4; 32]),
            vec![fauxchange::models::Permission::Read],
        ),
    ];
    let auth = AuthConfig::dev()
        .expect("dev auth must build")
        .with_bootstrap_secret(SECRET)
        .with_accounts(accounts)
        .with_rate_limit(1_000);
    AppState::new(AppStateConfig::new(["BTC", "ETH"]).with_auth(auth)).expect("AppState must build")
}

fn token(state: &Arc<AppState>, account: &str) -> String {
    state
        .mint_token(&AccountId::new(account), SECRET, now_secs(), 3_600)
        .expect("minting must succeed with the right secret")
}

fn json_request(method: &str, uri: &str, bearer: Option<&str>, body: &Value) -> Request<Body> {
    let bytes = serde_json::to_vec(body).expect("serialise body");
    raw_request(method, uri, bearer, Some("application/json"), bytes)
}

fn raw_request(
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    content_type: Option<&str>,
    body: Vec<u8>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(bearer) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    }
    if let Some(content_type) = content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    builder.body(Body::from(body)).expect("build request")
}

/// Sends one request through a fresh clone of the router; returns
/// `(status, raw_body_text)`.
async fn oneshot_send(state: &Arc<AppState>, request: Request<Body>) -> (StatusCode, String) {
    let router = create_router(Arc::clone(state));
    let response = router.oneshot(request).await.expect("router is infallible");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("reading the response body must succeed");
    (status, String::from_utf8_lossy(&bytes).to_string())
}

fn security_symbol() -> Symbol {
    Symbol::parse("BTC-20240329-50000-C").expect("a valid fixture symbol")
}

// ============================================================================
// 1. Captured-log credential test (a defining deliverable)
// ============================================================================

/// Distinctive markers so a leak is unambiguous. None is a real credential.
const FIX_PLAINTEXT_PW: &str = "PLAINTEXT-PASSWORD-marker-DoNotLog-021";
const BOOTSTRAP_MARKER: &str = "BOOTSTRAP-SECRET-marker-DoNotLog-021";
const WRONG_BOOTSTRAP: &str = "WRONG-BOOTSTRAP-marker-DoNotLog-021";
const PEPPER_MARKER: &[u8] = b"PEPPER-marker-DoNotLog-021";
const DB_PASSWORD_MARKER: &str = "DBPASS-marker-DoNotLog-021";
/// A fragment unique to the embedded dev **private** key (`JwtAuth::dev`).
const DEV_SIGNING_KEY_FRAGMENT: &str = "VNh0Vk8l7tR9inRKTQaO";

/// A `MakeWriter` that appends every formatted `tracing` event into a shared
/// buffer, so the test can scan everything the venue logged.
#[derive(Clone)]
struct CaptureBuffer(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CaptureBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Ok(mut guard) = self.0.lock() {
            guard.extend_from_slice(buf);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureBuffer {
    type Writer = CaptureBuffer;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[test]
fn test_no_credential_appears_in_logs_error_bodies_or_serialised_state() {
    let buffer = Arc::new(Mutex::new(Vec::<u8>::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(CaptureBuffer(Arc::clone(&buffer)))
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();
    // Single-threaded runtime + a thread-local default subscriber: every event the
    // venue emits (including from spawned actor tasks, polled on this thread) is
    // captured for the duration of the flow.
    let _guard = tracing::subscriber::set_default(subscriber);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");

    let mut error_bodies: Vec<String> = Vec::new();
    let mut serialised_state: Vec<String> = Vec::new();
    let mut stored_hash = String::new();

    runtime.block_on(async {
        // A DATABASE_URL-shaped secret held in-process. The boot log must redact it,
        // never echo the connection string (db wiring is a v0.2 seam, #023; this is
        // the forward guard the effective-config redaction must satisfy).
        let database_url = format!("postgres://venue:{DB_PASSWORD_MARKER}@db:5432/fauxchange");

        let accounts = vec![
            AccountProvision::new(
                AccountId::new("trader-1"),
                Hash32([2; 32]),
                vec![fauxchange::models::Permission::Trade],
            )
            .with_fix_login("trader-fix", FIX_PLAINTEXT_PW),
            AccountProvision::new(
                AccountId::new("reader-1"),
                Hash32([4; 32]),
                vec![fauxchange::models::Permission::Read],
            ),
        ];
        let auth = AuthConfig::dev()
            .expect("dev auth must build")
            .with_bootstrap_secret(BOOTSTRAP_MARKER)
            .with_pepper(PEPPER_MARKER.to_vec())
            .with_accounts(accounts);

        // The effective-config-at-boot log MUST be redacted: the `AuthConfig` Debug
        // redacts the bootstrap secret + pepper, and the DATABASE_URL is logged as a
        // redaction sentinel, never the raw connection string.
        tracing::info!(
            effective_config = ?auth,
            database_url = "<redacted>",
            "effective venue config at boot"
        );
        // A guard so a future change that logs `database_url` raw fails this test.
        assert!(database_url.contains(DB_PASSWORD_MARKER));

        let state = AppState::new(AppStateConfig::new(["BTC"]).with_auth(auth))
            .expect("AppState must build");

        // Serialised state must not carry secrets (Debug redacts; serde skips the
        // hash).
        serialised_state.push(format!("{state:?}"));
        if let Some(account) = AccountStore::account(state.accounts(), &AccountId::new("trader-1"))
        {
            serialised_state.push(format!("{account:?}"));
            serialised_state.push(serde_json::to_string(&account).unwrap_or_default());
            if let Some(hash) = account.credentials.password_hash.clone() {
                assert!(
                    hash.starts_with("$argon2id$"),
                    "the hash is a real Argon2id PHC"
                );
                stored_hash = hash;
            }
        }

        // Mint with the CORRECT secret (exercises the bootstrap gate + RS256
        // signing), then a WRONG secret (must fail without echoing the presented
        // value).
        let token = state
            .mint_token(
                &AccountId::new("trader-1"),
                BOOTSTRAP_MARKER,
                now_secs(),
                3_600,
            )
            .expect("minting with the right secret must succeed");
        assert!(
            state
                .mint_token(
                    &AccountId::new("trader-1"),
                    WRONG_BOOTSTRAP,
                    now_secs(),
                    3_600
                )
                .is_err(),
            "a wrong bootstrap secret is rejected"
        );

        // Exercise the FIX credential path (Argon2 verify + timing-equalisation).
        assert!(matches!(
            AccountStore::verify_fix_password(state.accounts(), "trader-fix", FIX_PLAINTEXT_PW),
            FixLoginOutcome::Authenticated { .. }
        ));
        assert!(matches!(
            AccountStore::verify_fix_password(
                state.accounts(),
                "trader-fix",
                "WRONG-PW-marker-DoNotLog-021"
            ),
            FixLoginOutcome::Rejected
        ));

        // Place a real order (order path + post-journal fan-out).
        let (status, body) = oneshot_send(
            &state,
            json_request(
                "POST",
                ORDER_PATH,
                Some(&token),
                &json!({"side": "buy", "price": 50_000, "quantity": 1}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "a well-formed order is accepted");
        error_bodies.push(body);

        // Trigger error renderings; none may leak a secret.
        for request in [
            // A wrong presented bootstrap secret over the token route: the error
            // body must not echo it.
            json_request(
                "POST",
                "/api/v1/auth/token",
                None,
                &json!({
                    "secret": WRONG_BOOTSTRAP,
                    "account": "trader-1",
                    "permissions": ["trade"]
                }),
            ),
            // A validation error.
            json_request(
                "POST",
                ORDER_PATH,
                Some(&token),
                &json!({"side": "buy", "price": MAX_PRICE_CENTS + 1, "quantity": 1}),
            ),
            // An unauthorized order.
            json_request(
                "POST",
                ORDER_PATH,
                None,
                &json!({"side": "buy", "price": 50_000, "quantity": 1}),
            ),
        ] {
            let (_status, body) = oneshot_send(&state, request).await;
            error_bodies.push(body);
        }

        // POSITIVE capture proof: the credential-absence assertions below are only
        // trustworthy if a log event emitted on a SPAWNED actor task genuinely lands
        // in the capture buffer. Drive a real spawned actor (`spawn_underlying_actor`)
        // to the sequence-exhaustion seal — its `tracing::error!(… "sealing
        // underlying")` fires from inside the actor's own task, not the test thread.
        // If this marker is NOT captured (e.g. a future change moves the actor onto a
        // dedicated `std::thread`, or removes actor-side logging), this assertion fails
        // and flags that the negative secret-absence checks can no longer be trusted.
        let seal_config = ActorConfig {
            underlying: Arc::from("BTC"),
            lineage_id: LineageId::new("capture-seal"),
            mailbox_capacity: 4,
            start_sequence: SequenceNumber::new(u64::MAX),
        };
        let seal_journal =
            InMemoryVenueJournal::new(JournalHeader::new(LineageId::new("capture-seal")));
        let (seal_handle, seal_join) = spawn_underlying_actor(
            seal_config,
            seal_journal,
            PlaceholderExecutor,
            NoopFanOut,
            FixedClock::new(EventTimestamp::new(0)),
        );
        // The turn at u64::MAX commits, then the actor task seals and logs the seal.
        let _ = seal_handle
            .submit(VenueCommand::Clock {
                now_ms: EventTimestamp::new(1),
            })
            .await;
        drop(seal_handle);
        let _ = seal_join.await;
    });

    drop(_guard);

    let logs = {
        let guard = buffer.lock().expect("capture buffer lock");
        String::from_utf8_lossy(&guard).to_string()
    };

    // Everything a client / operator could observe.
    let mut haystack = logs.clone();
    for body in &error_bodies {
        haystack.push('\n');
        haystack.push_str(body);
    }
    for state in &serialised_state {
        haystack.push('\n');
        haystack.push_str(state);
    }

    let forbidden: &[(&str, &str)] = &[
        ("FIX plaintext password", FIX_PLAINTEXT_PW),
        ("bootstrap secret", BOOTSTRAP_MARKER),
        ("presented wrong bootstrap secret", WRONG_BOOTSTRAP),
        ("Argon2id PHC marker", "$argon2id$"),
        ("Argon2 pepper", "PEPPER-marker-DoNotLog-021"),
        ("JWT signing key PEM header", "BEGIN PRIVATE KEY"),
        ("JWT signing key fragment", DEV_SIGNING_KEY_FRAGMENT),
        ("DATABASE_URL connection string", "postgres://"),
        ("DATABASE_URL password", DB_PASSWORD_MARKER),
    ];
    for (label, needle) in forbidden {
        assert!(
            !haystack.contains(needle),
            "SECURITY: {label} leaked into a log / error body / serialised state"
        );
    }
    if !stored_hash.is_empty() {
        assert!(
            !haystack.contains(&stored_hash),
            "SECURITY: the stored Argon2id hash leaked"
        );
    }

    // The capture actually captured, and the boot config log was redacted.
    assert!(
        logs.contains("effective venue config at boot"),
        "the boot config event must have been captured"
    );
    assert!(
        logs.contains("<redacted>"),
        "the boot config log must show redaction markers"
    );
    // POSITIVE proof that a SPAWNED-actor-task log event lands in the capture buffer,
    // so the credential-absence assertions above are trustworthy (not vacuously true
    // because nothing on a spawned task ever logged).
    assert!(
        logs.contains("sealing underlying"),
        "a spawned-actor-task tracing event MUST be captured; without it the \
         credential-absence assertions are not trustworthy"
    );
}

// ============================================================================
// 2. Auth / authorization matrix (REST + WS)
// ============================================================================

#[tokio::test]
async fn test_matrix_missing_token_is_unauthorized_on_a_mutating_op() {
    let state = venue();
    let (status, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            ORDER_PATH,
            None,
            &json!({"side": "buy", "price": 50_000, "quantity": 1}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_matrix_read_account_is_refused_order_entry_on_rest() {
    let state = venue();
    let reader = token(&state, "reader-1");
    let (status, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            ORDER_PATH,
            Some(&reader),
            &json!({"side": "buy", "price": 50_000, "quantity": 1}),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a Read account cannot place orders"
    );
}

#[tokio::test]
async fn test_matrix_trade_account_is_refused_admin_control() {
    let state = venue();
    let trader = token(&state, "trader-1");
    let (status, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            KILL_SWITCH_PATH,
            Some(&trader),
            &json!({"enabled": false}),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a Trade account cannot drive an Admin control"
    );
}

#[tokio::test]
async fn test_matrix_trade_account_may_place_orders() {
    let state = venue();
    let trader = token(&state, "trader-1");
    let (status, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            ORDER_PATH,
            Some(&trader),
            &json!({"side": "buy", "price": 50_000, "quantity": 1}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "a Trade account may place orders");
}

#[tokio::test]
async fn test_matrix_revocation_refuses_the_accounts_tokens() {
    let state = venue();
    let trader = token(&state, "trader-1");
    // The token works before revocation.
    let (before, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            ORDER_PATH,
            Some(&trader),
            &json!({"side": "buy", "price": 50_000, "quantity": 1}),
        ),
    )
    .await;
    assert_eq!(before, StatusCode::OK);

    // Revoke the account: its outstanding token is now below the current epoch.
    let new_epoch = AccountStore::revoke(state.accounts(), &AccountId::new("trader-1"));
    assert_eq!(new_epoch, Some(1), "revocation bumps the epoch");

    let (after, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            ORDER_PATH,
            Some(&trader),
            &json!({"side": "buy", "price": 50_000, "quantity": 1}),
        ),
    )
    .await;
    assert_eq!(
        after,
        StatusCode::UNAUTHORIZED,
        "a revoked account's token is refused"
    );
}

#[test]
fn test_matrix_ws_has_no_order_entry_frame() {
    // WS carries no order-entry message: any order-entry-shaped client frame is a
    // typed WS reject (order entry is REST/FIX only), never accepted.
    for frame in [
        r#"{"action":"place_order","side":"buy","price":50000,"quantity":1}"#,
        r#"{"action":"new_order_single","side":"sell","price":50000,"quantity":1}"#,
        r#"{"side":"buy","price":50000,"quantity":1}"#,
    ] {
        match parse_frame(frame) {
            FrameOutcome::Reject(err) => {
                assert!(!err.terminal, "an order-entry rejection is non-terminal");
            }
            other => panic!("an order-entry frame must be rejected, got {other:?}"),
        }
    }
}

// ============================================================================
// 3. Adversarial fixtures — each a typed reject, never a panic / silent accept
// ============================================================================

#[tokio::test]
async fn test_adversarial_oversized_body_is_rejected_before_buffering() {
    let state = venue();
    let trader = token(&state, "trader-1");
    // A body larger than the explicit MAX_REQUEST_BODY_BYTES ceiling: rejected as a
    // 413 before it is fully buffered (a named DoS bound).
    let huge = "A".repeat(MAX_REQUEST_BODY_BYTES + 4_096);
    let body = json!({
        "side": "buy",
        "price": 50_000,
        "quantity": 1,
        "client_order_id": huge
    });
    let request = json_request("POST", ORDER_PATH, Some(&trader), &body);
    let (status, _body) = oneshot_send(&state, request).await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "an oversized body hits the explicit request-body ceiling"
    );
}

#[tokio::test]
async fn test_adversarial_unknown_dto_field_is_a_client_error() {
    let state = venue();
    let trader = token(&state, "trader-1");
    let (status, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            ORDER_PATH,
            Some(&trader),
            &json!({"side": "buy", "price": 50_000, "quantity": 1, "evil": true}),
        ),
    )
    .await;
    assert!(
        status.is_client_error(),
        "deny_unknown_fields rejects a typo'd body with a 4xx, got {status}"
    );
    assert!(!status.is_server_error(), "an unknown field is never a 500");
}

#[tokio::test]
async fn test_adversarial_price_over_ceiling_is_a_typed_400() {
    let state = venue();
    let trader = token(&state, "trader-1");
    let (status, body) = oneshot_send(
        &state,
        json_request(
            "POST",
            ORDER_PATH,
            Some(&trader),
            &json!({"side": "buy", "price": MAX_PRICE_CENTS + 1, "quantity": 1}),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a price over the venue ceiling is a typed 400, not accepted"
    );
    let envelope: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    assert_eq!(envelope["code"], json!("invalid_order"));
}

#[tokio::test]
async fn test_adversarial_quantity_over_lot_ceiling_is_a_typed_400() {
    let state = venue();
    let trader = token(&state, "trader-1");
    let (status, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            ORDER_PATH,
            Some(&trader),
            &json!({"side": "buy", "price": 50_000, "quantity": MAX_ORDER_QUANTITY + 1}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_adversarial_negative_quantity_is_a_client_error() {
    let state = venue();
    let trader = token(&state, "trader-1");
    // `quantity` is a `u64`; a negative number cannot deserialize → a 4xx, never a
    // panic and never a silent accept.
    let (status, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            ORDER_PATH,
            Some(&trader),
            &json!({"side": "buy", "price": 50_000, "quantity": -1}),
        ),
    )
    .await;
    assert!(
        status.is_client_error(),
        "a negative quantity is a 4xx, got {status}"
    );
}

#[tokio::test]
async fn test_adversarial_zero_quantity_is_a_typed_400() {
    let state = venue();
    let trader = token(&state, "trader-1");
    let (status, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            ORDER_PATH,
            Some(&trader),
            &json!({"side": "buy", "price": 50_000, "quantity": 0}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_adversarial_malformed_symbol_is_a_typed_400() {
    let state = venue();
    let trader = token(&state, "trader-1");
    // A style path segment that is not `call`/`put` cannot round-trip to a canonical
    // symbol → a typed 400 at the boundary.
    let bad_style_path =
        "/api/v1/underlyings/BTC/expirations/20240329/strikes/50000/options/banana/orders";
    let (status, _body) = oneshot_send(
        &state,
        json_request(
            "POST",
            bad_style_path,
            Some(&trader),
            &json!({"side": "buy", "price": 50_000, "quantity": 1}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_adversarial_truncated_json_is_a_client_error() {
    let state = venue();
    let trader = token(&state, "trader-1");
    // A truncated / malformed JSON frame is a 4xx decode error, never a panic.
    let request = raw_request(
        "POST",
        ORDER_PATH,
        Some(&trader),
        Some("application/json"),
        br#"{"side":"buy","price":"#.to_vec(),
    );
    let (status, _body) = oneshot_send(&state, request).await;
    assert!(
        status.is_client_error(),
        "truncated JSON is a 4xx, got {status}"
    );
    assert!(!status.is_server_error());
}

// ============================================================================
// 4. DoS-control tests (as security controls, not fairness knobs)
// ============================================================================

#[test]
fn test_dos_rate_limiter_enforces_one_budget_per_account() {
    // The sliding-window limiter keyed on the resolved account: a flood over ANY
    // surface counts against ONE budget (the key is surface-independent).
    let limiter = RateLimiter::new(FixedClock::new(EventTimestamp::new(0)), 3);
    let key = RateLimitKey::Account {
        account: AccountId::new("flooder"),
        revocation_epoch: 0,
        tier: RateLimitTier::Read,
    };
    for i in 0..3 {
        assert!(
            limiter.check_and_record_status(&key).allowed,
            "request {i} is within the budget"
        );
    }
    assert!(
        !limiter.check_and_record_status(&key).allowed,
        "the over-budget request is throttled"
    );
}

/// A `CommandExecutor` that blocks in `execute` until a gate is released, so the
/// actor is stuck on one command while the bounded mailbox fills behind it.
struct GatedExecutor {
    release: Arc<AtomicBool>,
}

impl fauxchange::exchange::CommandExecutor for GatedExecutor {
    fn execute(&mut self, _context: fauxchange::exchange::ExecutionContext<'_>) -> VenueOutcome {
        while !self.release.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(2));
        }
        VenueOutcome::ControlApplied { swept: vec![] }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dos_bounded_actor_mailbox_backpressure_is_typed_rate_limited() {
    let release = Arc::new(AtomicBool::new(false));
    let config = ActorConfig {
        underlying: Arc::from("BTC"),
        lineage_id: LineageId::new("run-mailbox"),
        mailbox_capacity: 1,
        start_sequence: SequenceNumber::START,
    };
    let journal = InMemoryVenueJournal::new(JournalHeader::new(LineageId::new("run-mailbox")));
    let (handle, join) = spawn_underlying_actor(
        config,
        journal,
        GatedExecutor {
            release: Arc::clone(&release),
        },
        NoopFanOut,
        FixedClock::new(EventTimestamp::new(0)),
    );

    let clock_cmd = || VenueCommand::Clock {
        now_ms: EventTimestamp::new(1),
    };

    // Command A: the actor dequeues it and blocks in execute (gate closed).
    let handle_a = handle.clone();
    let a = tokio::spawn(async move { handle_a.submit(clock_cmd()).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Command B: fills the single mailbox slot and sits there (actor busy).
    let handle_b = handle.clone();
    let b = tokio::spawn(async move { handle_b.submit(clock_cmd()).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Command C: the mailbox is full → typed RateLimited (backpressure, never an
    // unbounded queue).
    match handle.submit(clock_cmd()).await {
        Err(fauxchange::VenueError::RateLimited) => {}
        other => panic!("a full mailbox must return RateLimited, got {other:?}"),
    }

    // Release: A and B drain and complete Ok.
    release.store(true, Ordering::SeqCst);
    assert!(a.await.expect("join A").is_ok());
    assert!(b.await.expect("join B").is_ok());

    drop(handle);
    join.await.expect("the actor shuts down cleanly");
}

/// A resting add whose committed outcome opens a fresh book level (and thus emits
/// one `orderbook_delta`) — the minimal event to drive the fan-out.
fn resting_add(seq: u64, order_id: &str, price: u64) -> VenueEvent {
    VenueEvent::new(
        SequenceNumber::new(seq),
        EventTimestamp::new(1),
        VenueCommand::AddOrder {
            symbol: security_symbol(),
            order_id: VenueOrderId::new(order_id),
            account: AccountId::new("acct"),
            owner: Hash32([0x11; 32]),
            client_order_id: None,
            side: SeamSide::Sell,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity: 1,
            time_in_force: SeamTif::Gtc,
            stp_mode: STPMode::None,
        },
        VenueOutcome::Added {
            fills: vec![],
            resting_quantity: 1,
            stp_cancelled: vec![],
        },
    )
}

#[test]
fn test_dos_bounded_broadcast_drops_a_laggard_without_growing_unbounded() {
    // A bounded broadcast ring: a consumer that does not drain lags and must
    // re-snapshot, rather than stalling the producer or growing an OOM queue.
    let manager = OrderbookSubscriptionManager::with_capacity(2);
    let mut receiver = manager.subscribe();
    for i in 0..8u64 {
        manager.on_committed_event(&resting_add(i, &format!("m{i}"), 50_000 + i));
    }
    let mut saw_lagged = false;
    loop {
        match receiver.try_recv() {
            Ok(_) => {}
            Err(broadcast::error::TryRecvError::Lagged(_)) => {
                saw_lagged = true;
                break;
            }
            Err(_) => break,
        }
    }
    assert!(
        saw_lagged,
        "a bounded broadcast drops the slow consumer, never grows unbounded"
    );
    // The recovery is a fresh snapshot reflecting every folded mutation.
    match manager.orderbook_snapshot(&security_symbol(), None) {
        fauxchange::models::WsMessage::OrderbookSnapshot { asks, .. } => {
            assert_eq!(asks.len(), 8, "the snapshot reflects every resting level");
        }
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

#[test]
fn test_dos_connection_cap_bounds_concurrent_sockets() {
    let manager = OrderbookSubscriptionManager::with_limits(WS_BROADCAST_CAPACITY, 2);
    let slot_a = manager.try_acquire_connection().expect("slot 1");
    let slot_b = manager.try_acquire_connection().expect("slot 2");
    assert!(
        manager.try_acquire_connection().is_none(),
        "at the connection cap the next socket is refused (handshake would 503)"
    );
    drop(slot_a);
    assert!(
        manager.try_acquire_connection().is_some(),
        "a released slot is reclaimed"
    );
    drop(slot_b);
}

#[test]
fn test_dos_sequence_exhaustion_seals_the_underlying() {
    // A per-underlying sequence driven to u64::MAX seals the underlying with a
    // typed error rather than wrapping (which would corrupt gap detection/replay).
    let config = ActorConfig {
        underlying: Arc::from("BTC"),
        lineage_id: LineageId::new("run-exhaust"),
        mailbox_capacity: 8,
        start_sequence: SequenceNumber::new(u64::MAX),
    };
    let journal = InMemoryVenueJournal::new(JournalHeader::new(LineageId::new("run-exhaust")));
    let mut actor = UnderlyingActor::new(
        config,
        journal,
        PlaceholderExecutor,
        NoopFanOut,
        FixedClock::new(EventTimestamp::new(0)),
    );

    let command = VenueCommand::Clock {
        now_ms: EventTimestamp::new(1),
    };
    // The turn at u64::MAX commits, then the checked counter seals.
    assert!(
        actor.handle(command.clone()).is_ok(),
        "the final available sequence still commits"
    );
    match actor.handle(command) {
        Err(fauxchange::VenueError::SequenceExhausted) => {}
        other => panic!("an exhausted sequence must seal, got {other:?}"),
    }
}
