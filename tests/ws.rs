//! Integration conformance tests for the #014 WebSocket surface.
//!
//! Two layers:
//!
//! - **The `GET /ws` handshake** is driven end-to-end against a real bound
//!   [`create_router`](fauxchange::gateway::rest::create_router) server with a raw
//!   TCP client (no WebSocket client crate): a missing / invalid token **refuses
//!   the upgrade** (`401`, the socket never opens — close-on-auth-error), a valid
//!   bearer (header **or** `?token=` query param) upgrades (`101`), and an
//!   exhausted rate-limit budget is `429`.
//! - **The market-data conformance** is driven in-process through the public
//!   [`OrderbookSubscriptionManager`](fauxchange::gateway::ws::OrderbookSubscriptionManager):
//!   a subscribe yields a snapshot then strictly-increasing deltas, the
//!   `instrument_sequence` never goes backward, a laggard re-snapshots, `fill`
//!   prints are anonymised, a control event emits no delta, and the typed WS error
//!   envelope keeps the socket open on a command error while closing on an auth
//!   error.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;

use fauxchange::auth::{AccountProvision, AccountStore, DEFAULT_RATE_LIMIT_PER_WINDOW};
use fauxchange::exchange::{
    Cents, EventTimestamp, Fill as SeamFill, Hash32, LineageId, STPMode, SequenceNumber,
    Side as SeamSide, SignedCents, Symbol, TimeInForce, VenueCommand, VenueEvent, VenueOutcome,
};
use fauxchange::gateway::rest::create_router;
use fauxchange::gateway::ws::{FrameOutcome, parse_frame};
use fauxchange::models::{
    AccountId, LiquidityFlag, OrderType, Permission, SubscriptionChannel, VenueOrderId, WsMessage,
};
use fauxchange::state::{AppState, AppStateConfig, AuthConfig};
use fauxchange::subscription::OrderbookSubscriptionManager;
use fauxchange::{VenueError, WS_ERROR_SCHEMA, WsErrorCategory, WsErrorCode};

const SECRET: &str = "op-secret";

/// A venue hosting `BTC` with a `Read` and an `Admin` account, the bootstrap
/// secret set, and `limit` requests/window.
fn venue(limit: u32) -> Arc<AppState> {
    let accounts = vec![
        AccountProvision::new(
            AccountId::new("reader-1"),
            Hash32([1; 32]),
            vec![Permission::Read],
        ),
        AccountProvision::new(
            AccountId::new("admin-1"),
            Hash32([2; 32]),
            vec![Permission::Admin],
        ),
    ];
    let auth = match AuthConfig::dev() {
        Ok(auth) => auth
            .with_bootstrap_secret(SECRET)
            .with_accounts(accounts)
            .with_rate_limit(limit),
        Err(error) => panic!("dev auth must build: {error}"),
    };
    match AppState::new(AppStateConfig::new(["BTC"]).with_auth(auth)) {
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

fn token(state: &Arc<AppState>, account: &str) -> String {
    match state.mint_token(&AccountId::new(account), SECRET, now_secs(), 3_600) {
        Ok(token) => token,
        Err(error) => panic!("minting must succeed: {error}"),
    }
}

fn sym() -> Symbol {
    match Symbol::parse("BTC-20240329-50000-C") {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
    }
}

// ============================================================================
// Handshake (real bound server + raw TCP client)
// ============================================================================

/// Binds an ephemeral port and serves the router, returning its address. The
/// spawned task lives until the runtime tears down at the end of the test.
async fn spawn_server(state: Arc<AppState>) -> SocketAddr {
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(e) => panic!("bind must succeed: {e}"),
    };
    let addr = match listener.local_addr() {
        Ok(addr) => addr,
        Err(e) => panic!("local_addr must succeed: {e}"),
    };
    let router = create_router(state);
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    });
    addr
}

/// Sends one raw WebSocket upgrade request and returns the HTTP status code of
/// the response line. Runs on a blocking thread (a plain `std::net::TcpStream`),
/// so no WebSocket client crate and no extra tokio IO feature are needed.
fn raw_handshake(addr: SocketAddr, path: &str, bearer: Option<&str>) -> u16 {
    let mut stream = match std::net::TcpStream::connect(addr) {
        Ok(stream) => stream,
        Err(e) => panic!("connect must succeed: {e}"),
    };
    let mut request = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: Upgrade\r\n\
         Upgrade: websocket\r\n\
         Sec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    );
    if let Some(token) = bearer {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str("\r\n");
    if let Err(e) = stream.write_all(request.as_bytes()) {
        panic!("write must succeed: {e}");
    }
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buffer = [0u8; 512];
    let read = stream.read(&mut buffer).unwrap_or(0);
    let response = String::from_utf8_lossy(&buffer[..read]);
    response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or(0)
}

/// Sends one raw HTTP `GET` with an optional bearer and returns the HTTP status
/// code — the REST sibling of [`raw_handshake`], driven over the **same** bound
/// server so a REST call and a WS handshake share one venue (and one rate-limit
/// budget). Runs on a blocking thread (a plain `std::net::TcpStream`).
fn raw_get(addr: SocketAddr, path: &str, bearer: Option<&str>) -> u16 {
    let mut stream = match std::net::TcpStream::connect(addr) {
        Ok(stream) => stream,
        Err(e) => panic!("connect must succeed: {e}"),
    };
    let mut request = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: close\r\n"
    );
    if let Some(token) = bearer {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str("\r\n");
    if let Err(e) = stream.write_all(request.as_bytes()) {
        panic!("write must succeed: {e}");
    }
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buffer = [0u8; 512];
    let read = stream.read(&mut buffer).unwrap_or(0);
    let response = String::from_utf8_lossy(&buffer[..read]);
    response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or(0)
}

#[tokio::test]
async fn test_throttling_parity_one_budget_across_rest_and_ws() {
    // Control/observation parity of throttling (#046): an authenticated account has
    // ONE budget across surfaces. Budget = 2 on the fixed venue clock → two REST
    // reads consume it, then the WS handshake for the SAME account is throttled
    // (`429`) from the same shared budget. Both hit one bound server.
    let state = venue(2);
    let bearer = token(&state, "reader-1");
    let addr = spawn_server(Arc::clone(&state)).await;

    // Two REST reads exhaust the reader's whole per-window budget.
    for i in 0..2u8 {
        let bearer = bearer.clone();
        let status = match tokio::task::spawn_blocking(move || {
            raw_get(addr, "/api/v1/stats", Some(&bearer))
        })
        .await
        {
            Ok(status) => status,
            Err(e) => panic!("client task panicked: {e}"),
        };
        assert_eq!(status, 200, "REST read {i} is within the shared budget");
    }

    // The WS handshake for the SAME account draws the SAME (now-exhausted) budget.
    let status = match tokio::task::spawn_blocking(move || {
        raw_handshake(addr, "/ws", Some(&bearer))
    })
    .await
    {
        Ok(status) => status,
        Err(e) => panic!("client task panicked: {e}"),
    };
    assert_eq!(
        status, 429,
        "the WS handshake shares the account's one REST/WS budget (throttling parity)"
    );
}

#[tokio::test]
async fn test_ws_handshake_without_token_refuses_upgrade_401() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let addr = spawn_server(state).await;
    let status = match tokio::task::spawn_blocking(move || raw_handshake(addr, "/ws", None)).await {
        Ok(status) => status,
        Err(e) => panic!("client task panicked: {e}"),
    };
    assert_eq!(
        status, 401,
        "an unauthenticated handshake never opens the socket"
    );
}

#[tokio::test]
async fn test_ws_handshake_with_header_bearer_upgrades_101() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "reader-1");
    let addr = spawn_server(state).await;
    let status = match tokio::task::spawn_blocking(move || {
        raw_handshake(addr, "/ws", Some(&bearer))
    })
    .await
    {
        Ok(status) => status,
        Err(e) => panic!("client task panicked: {e}"),
    };
    assert_eq!(status, 101, "a valid header bearer upgrades the socket");
}

#[tokio::test]
async fn test_ws_handshake_with_query_param_token_upgrades_101() {
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    let bearer = token(&state, "reader-1");
    let addr = spawn_server(state).await;
    // A browser WebSocket cannot set headers — the token rides the query string.
    let path = format!("/ws?token={bearer}");
    let status = match tokio::task::spawn_blocking(move || raw_handshake(addr, &path, None)).await {
        Ok(status) => status,
        Err(e) => panic!("client task panicked: {e}"),
    };
    assert_eq!(status, 101, "a valid query-param token upgrades the socket");
}

#[tokio::test]
async fn test_ws_handshake_over_budget_is_throttled_429() {
    // Budget of 1/window on the fixed venue clock: the second handshake for the
    // same account is throttled before the upgrade.
    let state = venue(1);
    let bearer = token(&state, "reader-1");
    let addr = spawn_server(state).await;

    let first = {
        let bearer = bearer.clone();
        match tokio::task::spawn_blocking(move || raw_handshake(addr, "/ws", Some(&bearer))).await {
            Ok(status) => status,
            Err(e) => panic!("client task panicked: {e}"),
        }
    };
    assert_eq!(first, 101, "the first handshake is within budget");

    let second = match tokio::task::spawn_blocking(move || {
        raw_handshake(addr, "/ws", Some(&bearer))
    })
    .await
    {
        Ok(status) => status,
        Err(e) => panic!("client task panicked: {e}"),
    };
    assert_eq!(second, 429, "the second handshake is over budget");
}

// ============================================================================
// Market-data conformance (in-process, via the public manager)
// ============================================================================

/// A resting limit add (no fills) at `(side, price, qty)`.
fn resting_add(order_id: &str, side: SeamSide, price: u64, qty: u64) -> VenueEvent {
    let command = VenueCommand::AddOrder {
        symbol: sym(),
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new("acct"),
        owner: Hash32([1; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity: qty,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    };
    VenueEvent::new(
        SequenceNumber::new(0),
        EventTimestamp::new(1_700_000_000_000),
        command,
        VenueOutcome::Added {
            fills: vec![],
            resting_quantity: qty,
            stp_cancelled: vec![],
        },
    )
}

/// A crossing taker buy that fully consumes the resting maker `maker-1`.
fn crossing_buy(price: u64, qty: u64) -> VenueEvent {
    let lineage = LineageId::new("run-1");
    let execution_id = lineage.execution_id("BTC", SequenceNumber::new(1), 0);
    let maker = SeamFill {
        execution_id: execution_id.clone(),
        order_id: VenueOrderId::new("maker-1"),
        account: AccountId::new("maker"),
        owner: Hash32([0x11; 32]),
        side: SeamSide::Sell,
        liquidity: LiquidityFlag::Maker,
        price: Cents::new(price),
        quantity: qty,
        fee: SignedCents::new(-10),
    };
    let taker = SeamFill {
        execution_id,
        order_id: VenueOrderId::new("taker-1"),
        account: AccountId::new("taker"),
        owner: Hash32([0x22; 32]),
        side: SeamSide::Buy,
        liquidity: LiquidityFlag::Taker,
        price: Cents::new(price),
        quantity: qty,
        fee: SignedCents::new(15),
    };
    let command = VenueCommand::AddOrder {
        symbol: sym(),
        order_id: VenueOrderId::new("taker-1"),
        account: AccountId::new("taker"),
        owner: Hash32([0x22; 32]),
        client_order_id: None,
        side: SeamSide::Buy,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity: qty,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    };
    VenueEvent::new(
        SequenceNumber::new(1),
        EventTimestamp::new(1_700_000_000_000),
        command,
        VenueOutcome::Added {
            fills: vec![maker, taker],
            resting_quantity: 0,
            stp_cancelled: vec![],
        },
    )
}

fn drain(rx: &mut broadcast::Receiver<WsMessage>) -> Vec<WsMessage> {
    let mut out = Vec::new();
    while let Ok(message) = rx.try_recv() {
        out.push(message);
    }
    out
}

#[test]
fn test_subscribe_snapshot_then_sequenced_deltas_never_backward() {
    let manager = OrderbookSubscriptionManager::new();
    let mut rx = manager.subscribe();

    // Rest several orders — each user-driven mutation emits a delta.
    manager.on_committed_event(&resting_add("m1", SeamSide::Sell, 50_100, 8));
    manager.on_committed_event(&resting_add("m2", SeamSide::Buy, 49_900, 12));
    manager.on_committed_event(&resting_add("m3", SeamSide::Sell, 50_200, 3));

    // A subscribe delivers exactly one snapshot at the baseline sequence.
    let snapshot = manager.orderbook_snapshot(&sym(), None);
    let baseline = match &snapshot {
        WsMessage::OrderbookSnapshot {
            sequence,
            bids,
            asks,
            channel,
            ..
        } => {
            assert_eq!(*channel, SubscriptionChannel::Orderbook);
            assert_eq!(*sequence, 3);
            assert_eq!(bids.len(), 1);
            assert_eq!(asks.len(), 2);
            *sequence
        }
        other => panic!("expected a snapshot, got {other:?}"),
    };

    // The deltas that flowed are strictly increasing and never below the baseline
    // after it (the client drops any delta <= baseline; the manager never emits a
    // decreasing sequence).
    let deltas: Vec<u64> = drain(&mut rx)
        .into_iter()
        .filter_map(|m| match m {
            WsMessage::OrderbookDelta { sequence, .. } => Some(sequence),
            _ => None,
        })
        .collect();
    assert_eq!(
        deltas,
        vec![1, 2, 3],
        "deltas are strictly increasing 1,2,3"
    );
    for window in deltas.windows(2) {
        assert!(window[1] > window[0], "sequence never goes backward");
    }
    assert_eq!(baseline, 3);
}

#[test]
fn test_laggard_receiver_re_snapshots_to_recover() {
    // A bounded broadcast: a slow consumer lags (drops backlog) rather than
    // stalling the producer; recovery is a fresh snapshot, never a resend.
    let manager = OrderbookSubscriptionManager::with_capacity(2);
    let mut rx = manager.subscribe();
    for i in 0..6u64 {
        manager.on_committed_event(&resting_add(
            &format!("m{i}"),
            SeamSide::Sell,
            50_000 + i,
            1,
        ));
    }
    let mut lagged = false;
    loop {
        match rx.try_recv() {
            Ok(_) => {}
            Err(broadcast::error::TryRecvError::Lagged(_)) => {
                lagged = true;
                break;
            }
            Err(_) => break,
        }
    }
    assert!(lagged, "a slow consumer lags on a bounded broadcast");
    // The recovery snapshot reflects every folded mutation at its current sequence.
    match manager.orderbook_snapshot(&sym(), None) {
        WsMessage::OrderbookSnapshot { asks, sequence, .. } => {
            assert_eq!(asks.len(), 6);
            assert_eq!(sequence, 6);
        }
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

#[test]
fn test_fill_prints_are_public_and_anonymised() {
    let manager = OrderbookSubscriptionManager::new();
    let mut rx = manager.subscribe();
    manager.on_committed_event(&resting_add("maker-1", SeamSide::Sell, 50_000, 2));
    manager.on_committed_event(&crossing_buy(50_000, 2));

    let messages = drain(&mut rx);
    let fills: Vec<&WsMessage> = messages
        .iter()
        .filter(|m| matches!(m, WsMessage::Fill { .. }))
        .collect();
    assert_eq!(fills.len(), 2, "one match, two anonymised fill legs");
    for fill in fills {
        let value = match serde_json::to_value(fill) {
            Ok(v) => v,
            Err(e) => panic!("serialise failed: {e}"),
        };
        assert_eq!(value["type"], serde_json::json!("fill"));
        // No account/fee leak…
        assert!(value["data"].get("account").is_none());
        assert!(value["data"].get("fee").is_none());
        // …but the four join keys are present, and money is integer cents.
        assert!(value["data"]["execution_id"].is_string());
        assert!(value["data"]["underlying_sequence"].is_u64());
        assert!(value["data"]["venue_ts"].is_u64());
        assert!(value["data"]["liquidity"].is_string());
        assert!(value["data"]["price"].is_u64());
    }
}

#[test]
fn test_control_event_never_emits_orderbook_delta() {
    let manager = OrderbookSubscriptionManager::new();
    let mut rx = manager.subscribe();
    let control = VenueEvent::new(
        SequenceNumber::new(0),
        EventTimestamp::new(1),
        VenueCommand::MarketMakerControl {
            spread_multiplier: Some(2.0),
            size_scalar: None,
            directional_skew: None,
            enabled: Some(false),
        },
        // No market-maker orders rest, so the coupled kill sweep is empty — the
        // control emits no orderbook delta.
        VenueOutcome::ControlApplied { swept: vec![] },
    );
    assert_eq!(manager.on_committed_event(&control), None);
    let deltas = drain(&mut rx)
        .into_iter()
        .filter(|m| matches!(m, WsMessage::OrderbookDelta { .. }))
        .count();
    assert_eq!(
        deltas, 0,
        "a control event (requote knobs) is never a book delta"
    );
}

// ============================================================================
// WS golden shapes re-asserted through the live manager (reuse #004 goldens)
// ============================================================================

#[test]
fn test_live_orderbook_snapshot_and_delta_shapes() {
    let manager = OrderbookSubscriptionManager::new();
    let mut rx = manager.subscribe();
    manager.on_committed_event(&resting_add("m1", SeamSide::Buy, 49_900, 12));

    let snapshot = manager.orderbook_snapshot(&sym(), None);
    let snap_value = serde_json::to_value(&snapshot).expect("serialise snapshot");
    assert_eq!(snap_value["type"], serde_json::json!("orderbook_snapshot"));
    assert_eq!(
        snap_value["data"]["channel"],
        serde_json::json!("orderbook")
    );
    assert!(snap_value["data"]["sequence"].is_u64());
    assert!(snap_value["data"]["bids"][0]["price"].is_u64());

    let delta = drain(&mut rx)
        .into_iter()
        .find(|m| matches!(m, WsMessage::OrderbookDelta { .. }))
        .expect("a delta was emitted");
    let delta_value = serde_json::to_value(&delta).expect("serialise delta");
    assert_eq!(delta_value["type"], serde_json::json!("orderbook_delta"));
    assert!(delta_value["data"]["sequence"].is_u64());
    assert_eq!(
        delta_value["data"]["changes"][0]["side"],
        serde_json::json!("bid")
    );
    assert!(delta_value["data"]["changes"][0]["price"].is_u64());
}

// ============================================================================
// Typed WS error envelope: maps VenueError, close-on-auth vs open-on-command
// ============================================================================

#[test]
fn test_ws_error_envelope_maps_every_venue_error() {
    let cases = [
        (VenueError::NotFound("x".to_string()), WsErrorCode::NotFound),
        (
            VenueError::InvalidOrder("x".to_string()),
            WsErrorCode::InvalidOrder,
        ),
        (VenueError::Unauthorized, WsErrorCode::Unauthorized),
        (
            VenueError::Forbidden(Permission::Trade),
            WsErrorCode::Forbidden,
        ),
        (VenueError::RateLimited, WsErrorCode::Throttled),
        (VenueError::Overflow, WsErrorCode::Internal),
    ];
    for (error, expected) in cases {
        let envelope = error.ws_error(Some("req-1".to_string()));
        assert_eq!(envelope.schema, WS_ERROR_SCHEMA);
        assert_eq!(envelope.code, expected);
        assert_eq!(envelope.request_id.as_deref(), Some("req-1"));
    }
}

#[test]
fn test_auth_error_is_terminal_command_error_is_not() {
    // An authentication failure closes the socket…
    assert!(VenueError::Unauthorized.ws_error(None).terminal);
    // …every command error (forbidden control, not-found, throttle, invalid,
    // internal) leaves the connection open.
    for error in [
        VenueError::Forbidden(Permission::Admin),
        VenueError::NotFound("x".to_string()),
        VenueError::RateLimited,
        VenueError::InvalidOrder("x".to_string()),
        VenueError::Overflow,
    ] {
        assert!(
            !error.ws_error(None).terminal,
            "{error:?} must be non-terminal"
        );
    }
}

// ============================================================================
// Session liveness re-check and connection cap (DoS controls)
// ============================================================================

#[tokio::test]
async fn test_live_session_revalidation_closes_a_revoked_socket() {
    // The heartbeat tick re-checks the session the WS loop holds: after the
    // account is revoked, the same claims fail revalidation with the terminal
    // Unauthorized error that closes the socket.
    let state = venue(DEFAULT_RATE_LIMIT_PER_WINDOW);
    // Mint a token, decode its claims (via a fresh verify through the JWT service).
    let bearer = token(&state, "reader-1");
    let claims = state
        .auth()
        .jwt()
        .verify_token(&bearer)
        .expect("a freshly minted token verifies");
    // While unrevoked, the live session revalidates.
    let now = now_secs();
    assert!(state.auth().revalidate_session(&claims, now).is_ok());
    // Revoke the account; the still-held session now fails (socket would close).
    state.accounts().revoke(&AccountId::new("reader-1"));
    match state.auth().revalidate_session(&claims, now) {
        Err(error) => {
            assert!(
                error.ws_error(None).terminal,
                "a revoked session close is terminal"
            );
        }
        Ok(()) => panic!("a revoked session must fail revalidation"),
    }
}

#[test]
fn test_connection_cap_refuses_at_the_ceiling() {
    let manager = OrderbookSubscriptionManager::with_limits(16, 2);
    let a = manager.try_acquire_connection().expect("slot 1");
    let _b = manager.try_acquire_connection().expect("slot 2");
    assert!(
        manager.try_acquire_connection().is_none(),
        "at the venue-wide cap the next handshake is refused (503)"
    );
    drop(a);
    assert!(
        manager.try_acquire_connection().is_some(),
        "a closed socket frees its slot"
    );
}

#[test]
fn test_order_entry_frame_is_rejected_and_non_terminal() {
    // WS has no order-entry message — a place/cancel/replace frame is rejected
    // with a non-terminal command error (the socket stays open).
    for frame in [
        r#"{"action":"place_order","side":"buy","price":50000,"quantity":1}"#,
        r#"{"action":"cancel_order","order_id":"x"}"#,
        r#"{"side":"buy","price":50000,"quantity":10}"#,
    ] {
        match parse_frame(frame) {
            FrameOutcome::Reject(error) => {
                assert!(!error.terminal, "an order-entry rejection is non-terminal");
                assert_eq!(error.code, WsErrorCode::BadRequest);
                assert!(matches!(
                    error.category,
                    WsErrorCategory::Validation | WsErrorCategory::Decode
                ));
            }
            other => panic!("order-entry frame must be rejected, got {other:?}"),
        }
    }
}
