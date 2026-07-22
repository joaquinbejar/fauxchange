//! Integration conformance tests for the #013 REST surface.
//!
//! Driven with `tower::ServiceExt::oneshot` against the real
//! [`fauxchange::gateway::rest::create_router`], so the auth/rate-limit layer,
//! the operation-class routing, and the sequenced order path all run without
//! binding a TCP listener. Covers: `/health` reachable without a token; every
//! mutating route rejects a missing/insufficient permission and honours the rate
//! limit; order mutations enter the sequenced path and journal the right
//! `VenueCommand`; `POST /prices` is a journaled `SimStep`; runtime hierarchy
//! create is refused (manifest input); the OpenAPI doc + Swagger UI serve; and a
//! determinism check on the sequenced path.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::Value;
use tower::ServiceExt;

use fauxchange::auth::{AccountProvision, DEFAULT_RATE_LIMIT_PER_WINDOW, RateLimitBudgets};
use fauxchange::exchange::{
    Cents, ExecutionsStore, Hash32, JournalRecord, SequenceNumber, Symbol, VenueCommand,
};
use fauxchange::gateway::rest::create_router;
use fauxchange::models::{
    AccountId, MAX_BULK_CANCEL_ITEMS, MAX_BULK_ORDER_ITEMS, Permission, VenueOrderId,
};
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};

const SECRET: &str = "op-secret";

/// Builds a venue hosting `BTC`/`ETH` with three provisioned accounts (one per
/// permission tier), the bootstrap secret set, and `limit` requests/window.
fn venue(limit: u32) -> Arc<AppState> {
    let accounts = vec![
        AccountProvision::new(
            AccountId::new("admin-1"),
            Hash32([1; 32]),
            vec![Permission::Admin],
        ),
        AccountProvision::new(
            AccountId::new("trader-1"),
            Hash32([2; 32]),
            vec![Permission::Trade],
        ),
        AccountProvision::new(
            AccountId::new("trader-2"),
            Hash32([3; 32]),
            vec![Permission::Trade],
        ),
        AccountProvision::new(
            AccountId::new("reader-1"),
            Hash32([4; 32]),
            vec![Permission::Read],
        ),
    ];
    let auth = match AuthConfig::dev() {
        Ok(auth) => auth
            .with_bootstrap_secret(SECRET)
            .with_accounts(accounts)
            .with_rate_limit(limit),
        Err(error) => panic!("dev auth must build: {error}"),
    };
    match AppState::new(AppStateConfig::new(["BTC", "ETH"]).with_auth(auth)) {
        Ok(state) => state,
        Err(error) => panic!("AppState must build: {error}"),
    }
}

/// Builds the same four-account venue but with explicit **per-tier** rate-limit
/// budgets (#046), so a Read caller and an Admin caller get distinct budgets.
fn venue_with_budgets(budgets: RateLimitBudgets) -> Arc<AppState> {
    let accounts = vec![
        AccountProvision::new(
            AccountId::new("admin-1"),
            Hash32([1; 32]),
            vec![Permission::Admin],
        ),
        AccountProvision::new(
            AccountId::new("reader-1"),
            Hash32([4; 32]),
            vec![Permission::Read],
        ),
    ];
    let auth = match AuthConfig::dev() {
        Ok(auth) => auth
            .with_bootstrap_secret(SECRET)
            .with_accounts(accounts)
            .with_rate_limit_budgets(budgets),
        Err(error) => panic!("dev auth must build: {error}"),
    };
    match AppState::new(AppStateConfig::new(["BTC", "ETH"]).with_auth(auth)) {
        Ok(state) => state,
        Err(error) => panic!("AppState must build: {error}"),
    }
}

fn now_secs() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(e) => panic!("system clock before epoch: {e}"),
    }
}

/// Mints a JWT for `account` via the bootstrap-gated path.
fn token(state: &Arc<AppState>, account: &str) -> String {
    match state.mint_token(&AccountId::new(account), SECRET, now_secs(), 3_600) {
        Ok(token) => token,
        Err(error) => panic!("minting must succeed: {error}"),
    }
}

fn build_request(
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: Option<Value>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            match serde_json::to_vec(&value) {
                Ok(bytes) => Body::from(bytes),
                Err(e) => panic!("serialising the request body must succeed: {e}"),
            }
        }
        None => Body::empty(),
    };
    match builder.body(body) {
        Ok(request) => request,
        Err(e) => panic!("building the request must succeed: {e}"),
    }
}

/// Sends one request through a fresh clone of the router and returns
/// `(status, body_json)`.
async fn send(state: &Arc<AppState>, request: Request<Body>) -> (StatusCode, Value) {
    let router: Router = create_router(Arc::clone(state));
    let response = match router.oneshot(request).await {
        Ok(response) => response,
        Err(e) => panic!("router must be infallible: {e}"),
    };
    let status = response.status();
    let bytes = match to_bytes(response.into_body(), usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => panic!("reading the body must succeed: {e}"),
    };
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

const CONTRACT: &str = "/api/v1/underlyings/BTC/expirations/20240329/strikes/50000/options/call";

fn limit_body(side: &str, price: u64, qty: u64) -> Value {
    serde_json::json!({ "side": side, "price": price, "quantity": qty })
}

/// A limit-order body carrying the `client_order_id` idempotency key an
/// idempotent resend reuses (#099).
fn keyed_limit_body(side: &str, price: u64, qty: u64, client_order_id: &str) -> Value {
    serde_json::json!({
        "side": side,
        "price": price,
        "quantity": qty,
        "client_order_id": client_order_id,
    })
}

// ---- /health is exempt ----------------------------------------------------

#[tokio::test]
async fn test_health_reachable_without_token() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let (status, body) = send(&state, build_request("GET", "/health", None, None)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

// ---- reads require a token ------------------------------------------------

#[tokio::test]
async fn test_stats_requires_token() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let (status, _) = send(&state, build_request("GET", "/api/v1/stats", None, None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_stats_ok_with_read_token() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "reader-1");
    let (status, body) = send(
        &state,
        build_request("GET", "/api/v1/stats", Some(&bearer), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["underlying_count"], 2);
}

// ---- order entry: auth + permission gating --------------------------------

#[tokio::test]
async fn test_place_limit_order_without_token_is_401() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let uri = format!("{CONTRACT}/orders");
    let (status, _) = send(
        &state,
        build_request("POST", &uri, None, Some(limit_body("buy", 50_000, 10))),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_place_limit_order_with_read_token_is_403_and_matches_error_golden() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "reader-1");
    let uri = format!("{CONTRACT}/orders");
    let (status, body) = send(
        &state,
        build_request(
            "POST",
            &uri,
            Some(&bearer),
            Some(limit_body("buy", 50_000, 10)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Re-assert the #003/#008 error_envelope golden through the live handler.
    assert_eq!(body["schema"], "rest-error.v1");
    assert_eq!(body["code"], "forbidden");
    assert_eq!(body["message"], "missing permission Trade");
}

#[tokio::test]
async fn test_place_limit_order_with_trade_token_returns_sequence() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "trader-1");
    let uri = format!("{CONTRACT}/orders");
    let (status, body) = send(
        &state,
        build_request(
            "POST",
            &uri,
            Some(&bearer),
            Some(limit_body("buy", 50_000, 10)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "accepted");
    assert_eq!(body["filled_quantity"], 0);
    assert_eq!(body["remaining_quantity"], 10);
    // The first sequenced command on a fresh BTC actor is at sequence 0.
    assert_eq!(body["sequence"], 0);
    assert!(body["order_id"].as_str().is_some());
}

// ---- order mutation → correct sequenced VenueCommand ----------------------

#[tokio::test]
async fn test_limit_order_journals_an_add_order_command() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "trader-1");
    let uri = format!("{CONTRACT}/orders");
    let (status, _) = send(
        &state,
        build_request(
            "POST",
            &uri,
            Some(&bearer),
            Some(limit_body("sell", 50_000, 4)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // The order entered the sequenced path: the BTC journal holds an AddOrder
    // command (proving it was NOT a direct book call).
    let snapshot = match state.journal_snapshot("BTC").await {
        Ok(snapshot) => snapshot,
        Err(e) => panic!("journal snapshot must succeed: {e}"),
    };
    let has_add = snapshot.records.iter().any(|record| {
        matches!(record, JournalRecord::Command(jc) if matches!(jc.command, VenueCommand::AddOrder { .. }))
    });
    assert!(
        has_add,
        "the limit order must be journaled as an AddOrder command"
    );
}

// ---- cancel-all (owner-scoped mass cancel, #097) --------------------------

/// The cancel-all endpoint URI.
const CANCEL_ALL: &str = "/api/v1/orders/cancel-all";

/// Rests `count` non-crossing buy limits for `account` on the fixture contract,
/// each at a distinct price so all rest (an empty book has no ask to cross).
async fn rest_n_buys(state: &Arc<AppState>, account: &str, count: u64) {
    let bearer = token(state, account);
    let uri = format!("{CONTRACT}/orders");
    for i in 0..count {
        let (status, body) = send(
            state,
            build_request(
                "POST",
                &uri,
                Some(&bearer),
                // Prices well below any ask, distinct per order → all rest, none fill.
                Some(limit_body("buy", 40_000 + i, 1)),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "resting buy must be accepted");
        assert_eq!(body["status"], "accepted", "the buy must rest, not fill");
    }
}

#[tokio::test]
async fn test_cancel_all_cancels_the_accounts_resting_orders_and_reports_count() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    rest_n_buys(&state, "trader-1", 3).await;

    let bearer = token(&state, "trader-1");
    let (status, body) = send(
        &state,
        build_request("DELETE", CANCEL_ALL, Some(&bearer), None),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an owner-scoped cancel-all succeeds"
    );
    assert_eq!(
        body["canceled_count"], 3,
        "cancel-all cancels every one of the account's resting orders"
    );
    assert_eq!(body["failed_count"], 0);

    // A second cancel-all has nothing left to sweep.
    let (status, body) = send(
        &state,
        build_request("DELETE", CANCEL_ALL, Some(&bearer), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["canceled_count"], 0,
        "a repeat cancel-all reports zero — the orders are already gone"
    );
}

#[tokio::test]
async fn test_cancel_all_is_owner_scoped_and_never_touches_another_account() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    rest_n_buys(&state, "trader-1", 2).await;
    rest_n_buys(&state, "trader-2", 3).await;

    // trader-2 cancels all of ITS OWN orders — trader-1's must be untouched.
    let (status, body) = send(
        &state,
        build_request("DELETE", CANCEL_ALL, Some(&token(&state, "trader-2")), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["canceled_count"], 3,
        "trader-2 sweeps exactly its own three orders"
    );

    // trader-1's two orders survive: its own cancel-all still finds two.
    let (status, body) = send(
        &state,
        build_request("DELETE", CANCEL_ALL, Some(&token(&state, "trader-1")), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["canceled_count"], 2,
        "trader-1's orders were never touched by trader-2's cancel-all"
    );
}

#[tokio::test]
async fn test_cancel_all_requires_trade_permission() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let (status, _) = send(
        &state,
        build_request("DELETE", CANCEL_ALL, Some(&token(&state, "reader-1")), None),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "cancel-all is a mutating op — a Read-only caller is 403"
    );
}

#[tokio::test]
async fn test_cancel_all_refuses_a_filtered_request() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "trader-1");
    let (status, _) = send(
        &state,
        build_request(
            "DELETE",
            &format!("{CANCEL_ALL}?side=buy"),
            Some(&bearer),
            None,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a filtered cancel-all is refused, never a silent over-broad sweep"
    );
}

#[tokio::test]
async fn test_crossing_order_reports_fills_from_the_sequenced_path() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let maker = token(&state, "trader-1");
    let taker = token(&state, "trader-2");
    let uri = format!("{CONTRACT}/orders");

    // Maker rests a sell; taker crosses it fully.
    let (maker_status, _) = send(
        &state,
        build_request(
            "POST",
            &uri,
            Some(&maker),
            Some(limit_body("sell", 50_000, 5)),
        ),
    )
    .await;
    assert_eq!(maker_status, StatusCode::OK);

    let (taker_status, body) = send(
        &state,
        build_request(
            "POST",
            &uri,
            Some(&taker),
            Some(limit_body("buy", 50_000, 5)),
        ),
    )
    .await;
    assert_eq!(taker_status, StatusCode::OK);
    // The taker's fills are projected from the receipt's captured outcome.
    assert_eq!(body["status"], "filled");
    assert_eq!(body["filled_quantity"], 5);
    assert_eq!(body["remaining_quantity"], 0);
    assert_eq!(body["sequence"], 1);
}

#[tokio::test]
async fn test_idempotent_resend_renders_the_stored_terminal_report_not_a_fresh_readback() {
    // #099: an idempotent resend (same account + `client_order_id`) must render the
    // STORED terminal report — the ORIGINAL fills, projected from the receipt's
    // captured outcome — NOT an empty fresh store read-back keyed on the resend's
    // freshly-minted order id (which would falsely report `accepted` / 0 filled).
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let maker = token(&state, "trader-1");
    let taker = token(&state, "trader-2");
    let uri = format!("{CONTRACT}/orders");

    // Maker rests a sell 5; the taker crosses it fully, KEYED with a ClOrdID.
    let (maker_status, _) = send(
        &state,
        build_request(
            "POST",
            &uri,
            Some(&maker),
            Some(limit_body("sell", 50_000, 5)),
        ),
    )
    .await;
    assert_eq!(maker_status, StatusCode::OK);

    let (taker_status, first) = send(
        &state,
        build_request(
            "POST",
            &uri,
            Some(&taker),
            Some(keyed_limit_body("buy", 50_000, 5, "dup")),
        ),
    )
    .await;
    assert_eq!(taker_status, StatusCode::OK);
    assert_eq!(first["status"], "filled");
    assert_eq!(first["filled_quantity"], 5);
    assert_eq!(
        state.executions().len(),
        2,
        "one crossing match records two execution legs"
    );

    // Resend the byte-identical taker (same ClOrdID, the standard retry after a
    // dropped ack). The executor dedups: no second order, no phantom fill — and the
    // response renders the ORIGINAL terminal report, never a fresh accepted/0 read-back.
    let (resend_status, resend) = send(
        &state,
        build_request(
            "POST",
            &uri,
            Some(&taker),
            Some(keyed_limit_body("buy", 50_000, 5, "dup")),
        ),
    )
    .await;
    assert_eq!(resend_status, StatusCode::OK);
    assert_eq!(
        resend["status"], "filled",
        "the resend renders the STORED filled terminal, not a fresh `accepted`"
    );
    assert_eq!(
        resend["filled_quantity"], 5,
        "the resend shows the ORIGINAL filled 5, not a read-back 0 (#099)"
    );
    assert_eq!(resend["remaining_quantity"], 0);
    assert_eq!(
        state.executions().len(),
        2,
        "the resend opened no second order (no phantom fill in the store)"
    );
}

// ---- POST /prices is Admin-gated and a journaled SimStep ------------------

#[tokio::test]
async fn test_post_prices_requires_admin() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "trader-1"); // Trade, not Admin
    let body = serde_json::json!({ "symbol": "BTC", "price": 4_200_000 });
    let (status, _) = send(
        &state,
        build_request("POST", "/api/v1/prices", Some(&bearer), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_post_prices_is_journaled_as_a_simstep() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "admin-1");
    let body = serde_json::json!({ "symbol": "BTC", "price": 4_200_000 });
    let (status, response) = send(
        &state,
        build_request("POST", "/api/v1/prices", Some(&bearer), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["success"], true);
    assert_eq!(response["price_cents"], 4_200_000);

    // The price write went through the actor as a SimStep, not a bare write.
    let snapshot = match state.journal_snapshot("BTC").await {
        Ok(snapshot) => snapshot,
        Err(e) => panic!("journal snapshot must succeed: {e}"),
    };
    let has_simstep = snapshot.records.iter().any(|record| {
        matches!(record, JournalRecord::Command(jc) if matches!(jc.command, VenueCommand::SimStep { .. }))
    });
    assert!(has_simstep, "POST /prices must journal a SimStep command");
}

// ---- runtime hierarchy create/delete is refused (manifest input) ----------

#[tokio::test]
async fn test_create_underlying_refused_once_serving() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "admin-1");
    let (status, body) = send(
        &state,
        build_request("POST", "/api/v1/underlyings/SOL", Some(&bearer), None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_order");
    assert!(
        body["message"].as_str().unwrap_or("").contains("manifest"),
        "the refusal must name the manifest-input reason"
    );
}

#[tokio::test]
async fn test_create_underlying_still_requires_admin() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "trader-1"); // Trade, not Admin
    let (status, _) = send(
        &state,
        build_request("POST", "/api/v1/underlyings/SOL", Some(&bearer), None),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

// ---- rate limiting --------------------------------------------------------

#[tokio::test]
async fn test_rate_limit_returns_429_over_budget() {
    // Budget of 2/window on the fixed venue clock: the 3rd request is throttled.
    let state = venue(2);
    let bearer = token(&state, "reader-1");
    for _ in 0..2 {
        let (status, _) = send(
            &state,
            build_request("GET", "/api/v1/stats", Some(&bearer), None),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    let (status, _) = send(
        &state,
        build_request("GET", "/api/v1/stats", Some(&bearer), None),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
}

/// Sends one request and returns `(status, body_json, [ratelimit_limit,
/// ratelimit_remaining, ratelimit_reset, retry_after])` header strings.
async fn send_with_headers(
    state: &Arc<AppState>,
    request: Request<Body>,
) -> (StatusCode, Value, [Option<String>; 4]) {
    let router: Router = create_router(Arc::clone(state));
    let response = match router.oneshot(request).await {
        Ok(response) => response,
        Err(e) => panic!("router must be infallible: {e}"),
    };
    let status = response.status();
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    };
    let headers = [
        header("x-ratelimit-limit"),
        header("x-ratelimit-remaining"),
        header("x-ratelimit-reset"),
        header("retry-after"),
    ];
    let bytes = match to_bytes(response.into_body(), usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => panic!("reading the body must succeed: {e}"),
    };
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json, headers)
}

#[tokio::test]
async fn test_rate_limit_429_envelope_and_headers() {
    // Golden #046: a REST overflow is `429` with the stable error envelope and the
    // `X-RateLimit-*` header shape (limit, remaining=0, reset) plus `Retry-After`.
    let state = venue(1);
    let bearer = token(&state, "reader-1");

    // First request is admitted and already carries the rate-limit headers.
    let (ok_status, _, ok_headers) = send_with_headers(
        &state,
        build_request("GET", "/api/v1/stats", Some(&bearer), None),
    )
    .await;
    assert_eq!(ok_status, StatusCode::OK);
    assert_eq!(ok_headers[0].as_deref(), Some("1")); // X-RateLimit-Limit
    assert_eq!(ok_headers[1].as_deref(), Some("0")); // X-RateLimit-Remaining
    assert!(
        ok_headers[2].is_some(),
        "X-RateLimit-Reset present on admit"
    );

    // The over-budget request is throttled with the full envelope + headers.
    let (status, body, headers) = send_with_headers(
        &state,
        build_request("GET", "/api/v1/stats", Some(&bearer), None),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    // The stable error envelope (schema/code/message).
    assert_eq!(body["code"], "throttled");
    assert_eq!(body["message"], "rate limited");
    assert!(body["schema"].is_string(), "envelope carries a schema tag");
    // The `X-RateLimit-*` header shape.
    assert_eq!(headers[0].as_deref(), Some("1"), "X-RateLimit-Limit");
    assert_eq!(headers[1].as_deref(), Some("0"), "X-RateLimit-Remaining");
    assert!(headers[2].is_some(), "X-RateLimit-Reset present");
    assert!(headers[3].is_some(), "Retry-After present on a 429");
}

#[tokio::test]
async fn test_rate_limit_per_tier_budgets_over_rest() {
    // Integration #046: distinct per-tier budgets applied over REST — a Read caller
    // gets 1/window, an Admin caller 3/window, each keyed on its own account.
    let budgets = RateLimitBudgets::new(60_000, 1, 2, 3);
    let state = venue_with_budgets(budgets);

    // The reader is throttled after a single request (Read budget = 1).
    let reader = token(&state, "reader-1");
    let (first, _) = send(
        &state,
        build_request("GET", "/api/v1/stats", Some(&reader), None),
    )
    .await;
    assert_eq!(first, StatusCode::OK);
    let (second, _) = send(
        &state,
        build_request("GET", "/api/v1/stats", Some(&reader), None),
    )
    .await;
    assert_eq!(
        second,
        StatusCode::TOO_MANY_REQUESTS,
        "the Read tier is throttled after its 1-request budget"
    );

    // The admin gets three requests before throttling (Admin budget = 3), and its
    // 429 reports the Admin limit in the header.
    let admin = token(&state, "admin-1");
    for _ in 0..3 {
        let (status, _, _) = send_with_headers(
            &state,
            build_request("GET", "/api/v1/stats", Some(&admin), None),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    let (status, _, headers) = send_with_headers(
        &state,
        build_request("GET", "/api/v1/stats", Some(&admin), None),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        headers[0].as_deref(),
        Some("3"),
        "the Admin tier's budget is reflected in X-RateLimit-Limit"
    );
}

// ---- OpenAPI doc + Swagger UI served --------------------------------------

#[tokio::test]
async fn test_openapi_json_is_served_and_lists_paths() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let (status, body) = send(
        &state,
        build_request("GET", "/api-docs/openapi.json", None, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["paths"]["/health"].is_object());
    assert!(body["paths"]["/api/v1/orders/bulk"].is_object());
    // The bearer security scheme is registered.
    assert!(body["components"]["securitySchemes"]["bearer_jwt"].is_object());
}

#[tokio::test]
async fn test_swagger_ui_is_served() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let router: Router = create_router(Arc::clone(&state));
    let response = match router
        .oneshot(build_request("GET", "/swagger-ui", None, None))
        .await
    {
        Ok(response) => response,
        Err(e) => panic!("router must be infallible: {e}"),
    };
    // Swagger UI serves the index (200) or redirects to it (3xx).
    assert!(
        response.status().is_success() || response.status().is_redirection(),
        "swagger-ui must be reachable, got {}",
        response.status()
    );
}

// ---- bulk endpoints are bounded (DoS control) -----------------------------

#[tokio::test]
async fn test_bulk_place_over_limit_is_rejected_400() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "trader-1");
    let items: Vec<Value> = (0..(MAX_BULK_ORDER_ITEMS + 1))
        .map(|_| {
            serde_json::json!({
                "symbol": "BTC-20240329-50000-C",
                "side": "buy",
                "price": 50_000,
                "quantity": 1
            })
        })
        .collect();
    let body = serde_json::json!({ "orders": items });
    let (status, body) = send(
        &state,
        build_request("POST", "/api/v1/orders/bulk", Some(&bearer), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["message"]
            .as_str()
            .unwrap_or("")
            .contains("MAX_BULK_ORDER_ITEMS"),
        "the rejection must name the bound"
    );
}

#[tokio::test]
async fn test_bulk_cancel_over_limit_is_rejected_400() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "trader-1");
    let ids: Vec<Value> = (0..(MAX_BULK_CANCEL_ITEMS + 1))
        .map(|i| Value::String(format!("fauxchange:BTC:g{i}:0")))
        .collect();
    let body = serde_json::json!({ "order_ids": ids });
    let (status, body) = send(
        &state,
        build_request("DELETE", "/api/v1/orders/bulk", Some(&bearer), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["message"]
            .as_str()
            .unwrap_or("")
            .contains("MAX_BULK_CANCEL_ITEMS")
    );
}

// ---- typed underlying_sequence on cancel / bulk / toggle (FIX 4) -----------

#[tokio::test]
async fn test_cancel_response_carries_typed_sequence() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "trader-1");
    let place_uri = format!("{CONTRACT}/orders");
    let (status, place) = send(
        &state,
        build_request(
            "POST",
            &place_uri,
            Some(&bearer),
            Some(limit_body("buy", 50_000, 3)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let order_id = match place["order_id"].as_str() {
        Some(id) => id.to_string(),
        None => panic!("place response must carry an order_id"),
    };

    let cancel_uri = format!("{CONTRACT}/orders/{order_id}");
    let (status, cancel) = send(
        &state,
        build_request("DELETE", &cancel_uri, Some(&bearer), None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // A typed sequence field, not just prose (so #018 can parse it).
    assert!(cancel["sequence"].is_number());
    assert_eq!(cancel["success"], true);
}

#[tokio::test]
async fn test_cross_account_cancel_is_masked_identically_to_a_nonexistent_id() {
    // #132 BOLA/IDOR mask: an authenticated account cancelling ANOTHER account's
    // resting order must get a response BYTE-IDENTICAL to cancelling a nonexistent
    // id — so a distinct not-owner reply can never be a cross-account
    // existence/ownership enumeration oracle over the deterministically-minted
    // order ids. The victim's order must survive untouched.
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let victim = token(&state, "trader-1");
    let attacker = token(&state, "trader-2");

    // trader-1 rests a real order and learns its venue order id.
    let (status, place) = send(
        &state,
        build_request(
            "POST",
            &format!("{CONTRACT}/orders"),
            Some(&victim),
            Some(limit_body("buy", 50_000, 3)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let victim_order_id = match place["order_id"].as_str() {
        Some(id) => id.to_string(),
        None => panic!("place response must carry an order_id"),
    };

    // trader-2 (a DIFFERENT account) cancels trader-1's order by its id → NotOwner.
    let (owned_status, owned_body) = send(
        &state,
        build_request(
            "DELETE",
            &format!("{CONTRACT}/orders/{victim_order_id}"),
            Some(&attacker),
            None,
        ),
    )
    .await;
    // trader-2 cancels a genuinely nonexistent id → NotFound.
    let (missing_status, missing_body) = send(
        &state,
        build_request(
            "DELETE",
            &format!("{CONTRACT}/orders/definitely-not-a-real-order-id"),
            Some(&attacker),
            None,
        ),
    )
    .await;

    // Byte-identical: same status, same success flag, same message — no oracle.
    assert_eq!(owned_status, missing_status, "same HTTP status");
    assert_eq!(
        owned_body["success"],
        serde_json::json!(false),
        "a cross-account cancel reports success:false, never a false success"
    );
    assert_eq!(
        owned_body["success"], missing_body["success"],
        "not-owner and not-found share the success flag"
    );
    assert_eq!(
        owned_body["message"], missing_body["message"],
        "not-owner and not-found render a BYTE-IDENTICAL message (the mask)"
    );

    // The victim's order SURVIVED — trader-1 cancels it for real (proves the
    // attacker's attempt never mutated the book).
    let (status, own_cancel) = send(
        &state,
        build_request(
            "DELETE",
            &format!("{CONTRACT}/orders/{victim_order_id}"),
            Some(&victim),
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        own_cancel["success"],
        serde_json::json!(true),
        "the owner's cancel succeeds — the order was untouched by the cross-account attempt"
    );
}

#[tokio::test]
async fn test_bulk_place_item_carries_sequence() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "trader-1");
    let body = serde_json::json!({
        "orders": [
            { "symbol": "BTC-20240329-50000-C", "side": "buy", "price": 50_000, "quantity": 2 }
        ]
    });
    let (status, body) = send(
        &state,
        build_request("POST", "/api/v1/orders/bulk", Some(&bearer), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success_count"], 1);
    assert_eq!(body["results"][0]["status"], "accepted");
    assert!(body["results"][0]["sequence"].is_number());
}

#[tokio::test]
async fn test_toggle_reports_accepted_and_sequenced_with_sequence() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "admin-1");
    let body = serde_json::json!({ "enabled": false });
    let (status, body) = send(
        &state,
        build_request(
            "POST",
            "/api/v1/controls/instrument/BTC-20240329-50000-C/toggle",
            Some(&bearer),
            Some(body),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    assert_eq!(body["enabled"], false);
    // The typed sequence proves it reached the actor and was sequenced.
    assert!(body["sequence"].is_number());
}

// ---- FOK honesty: a killed order is Rejected, never a false Accepted -------

#[tokio::test]
async fn test_fok_order_against_empty_book_is_rejected_not_accepted() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "trader-1");
    let uri = format!("{CONTRACT}/orders");
    let body = serde_json::json!({
        "side": "buy", "price": 50_000, "quantity": 10, "time_in_force": "FOK"
    });
    let (status, body) = send(
        &state,
        build_request("POST", &uri, Some(&bearer), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // A fill-or-kill with no liquidity was KILLED — must not claim "accepted".
    assert_eq!(body["status"], "rejected");
    assert_eq!(body["filled_quantity"], 0);
}

// ---- #118: a place into a halted instrument surfaces Rejected over REST -------

#[tokio::test]
async fn test_place_into_halted_instrument_reports_rejected_not_accepted() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let admin = token(&state, "admin-1");
    let trader = token(&state, "trader-1");

    // Halt the contract via the admin toggle (enabled:false = halt) — a LEGAL
    // Active→Halted transition, so the toggle itself reports success.
    let (status, toggle) = send(
        &state,
        build_request(
            "POST",
            "/api/v1/controls/instrument/BTC-20240329-50000-C/toggle",
            Some(&admin),
            Some(serde_json::json!({ "enabled": false })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(toggle["success"], true);

    // A resting-TIF (GTC) limit into the halted book is a journaled Rejected — the
    // REST response must report that reject, NOT a false "accepted; resting" (#118).
    let uri = format!("{CONTRACT}/orders");
    let (status, body) = send(
        &state,
        build_request(
            "POST",
            &uri,
            Some(&trader),
            Some(limit_body("buy", 50_000, 3)),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "rejected");
    assert_eq!(body["filled_quantity"], 0);
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|m| m.contains("Halted")),
        "the reject reason names the refusing status: {}",
        body["message"]
    );
}

// ---- #118: resume-an-Expired is an illegal transition → typed 409 over REST ---

#[tokio::test]
async fn test_illegal_toggle_transition_reports_409_not_false_success() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let admin = token(&state, "admin-1");
    let symbol = "BTC-20240329-50000-C";

    // Drive the instrument to the terminal Expired state on the sequenced path.
    for status in ["Settling", "Expired"] {
        state
            .submit(VenueCommand::SetInstrumentStatus {
                symbol: match Symbol::parse(symbol) {
                    Ok(s) => s,
                    Err(e) => panic!("symbol: {e:?}"),
                },
                status: match status {
                    "Settling" => fauxchange::exchange::InstrumentStatus::Settling,
                    _ => fauxchange::exchange::InstrumentStatus::Expired,
                },
            })
            .await
            .expect("lifecycle submit");
    }

    // Resume (enabled:true → Active) an Expired instrument is illegal: the registry
    // rejects it, and the toggle handler surfaces a typed 409, never success:true.
    let (status, body) = send(
        &state,
        build_request(
            "POST",
            &format!("/api/v1/controls/instrument/{symbol}/toggle"),
            Some(&admin),
            Some(serde_json::json!({ "enabled": true })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_ne!(
        body["success"], true,
        "an illegal transition is not a success"
    );
}

// ---- determinism of the sequenced path the handlers use -------------------

#[tokio::test]
async fn test_sequenced_path_is_deterministic_across_runs() {
    // The same logical order sequence submitted to two fresh venues assigns the
    // same underlying_sequence and produces the same fills — the determinism
    // property the REST order-entry handlers rely on.
    fn maker() -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: match Symbol::parse("BTC-20240329-50000-C") {
                Ok(s) => s,
                Err(e) => panic!("symbol: {e:?}"),
            },
            order_id: VenueOrderId::new("m"),
            account: AccountId::new("trader-1"),
            owner: Hash32([2; 32]),
            client_order_id: None,
            side: fauxchange::exchange::Side::Sell,
            order_type: fauxchange::models::OrderType::Limit,
            limit_price: Some(Cents::new(50_000)),
            quantity: 5,
            time_in_force: fauxchange::exchange::TimeInForce::Gtc,
            stp_mode: fauxchange::exchange::STPMode::None,
        }
    }
    fn taker() -> VenueCommand {
        VenueCommand::AddOrder {
            symbol: match Symbol::parse("BTC-20240329-50000-C") {
                Ok(s) => s,
                Err(e) => panic!("symbol: {e:?}"),
            },
            order_id: VenueOrderId::new("t"),
            account: AccountId::new("trader-2"),
            owner: Hash32([3; 32]),
            client_order_id: None,
            side: fauxchange::exchange::Side::Buy,
            order_type: fauxchange::models::OrderType::Limit,
            limit_price: Some(Cents::new(50_000)),
            quantity: 5,
            time_in_force: fauxchange::exchange::TimeInForce::Gtc,
            stp_mode: fauxchange::exchange::STPMode::None,
        }
    }

    async fn run() -> (SequenceNumber, usize) {
        let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
        let _ = state.submit(maker()).await;
        let receipt = match state.submit(taker()).await {
            Ok(receipt) => receipt,
            Err(e) => panic!("submit must succeed: {e}"),
        };
        (receipt.underlying_sequence, state.executions().len())
    }

    let first = run().await;
    let second = run().await;
    assert_eq!(first, second, "same journal ⇒ same sequence + executions");
    assert_eq!(first.0, SequenceNumber::new(1));
    assert_eq!(first.1, 2, "one crossing match records two legs");
}
