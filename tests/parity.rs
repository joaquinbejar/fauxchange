//! The **v0.1 protocol-parity suite** — the milestone's primary acceptance test
//! ([018](../milestones/v0.1-backend-core/018-parity-fixtures-rest-ws.md),
//! [03 §7](../docs/03-protocol-surfaces.md#7-protocol-parity-guarantees),
//! [TESTING.md §6–§7](../docs/TESTING.md#6-conformance--parity-rest--ws--fix)).
//!
//! Parity is the contract that makes `fauxchange` trustworthy as a test venue:
//! **the surface an order arrives on must not change what the venue does.** The
//! obligation is milestone-scoped — v0.1 covers **REST + WS**; FIX joins at v0.4
//! (#041), extending this suite through the reusable [`conformance`] helpers, not
//! rewriting it.
//!
//! Sections:
//!
//! 1. **Reachability** — every documented REST route is served with its OpenAPI
//!    shape, and every documented WS message round-trips to its #004 golden.
//! 2. **Observation parity (REST ≡ WS)** — one committed fill renders identically
//!    as a REST `ExecutionRecord` and a WS `fill` on the four join keys plus
//!    price/quantity/side; the WS `fill` omits `account` / `fee`.
//! 3. **Market-data parity** — `orderbook_snapshot` / `orderbook_delta` carry the
//!    per-instrument `instrument_sequence` and resulting-quantity semantics; a gap
//!    recovers by a fresh snapshot, never a resend.
//! 4. **Control parity (REST ≡ WS)** — a WS control action and its REST
//!    equivalent build the *same* `MarketMakerControl` and surface the *same*
//!    sequenced outcome (the venue-global control fans out to every underlying's
//!    actor and is journaled, #47).
//! 5. **REST order-entry base** — place / partial-fill / cancel-replace over the
//!    live REST surface against identically-seeded fresh venues, compared under
//!    the documented normalization rule; the base the v0.4 FIX arm extends.
//! 6. **Normalization-rule unit tests** — which fields are stripped vs compared
//!    verbatim.

mod conformance;

use axum::http::StatusCode;
use serde_json::Value;

use conformance::fix as cfix;
use conformance::{
    AMPLE_RATE_LIMIT, CALL, CONTRACT, NORMALIZED_PLACEHOLDER, NORMALIZED_TS, STRIPPED_KEYS, Step,
    TRANSPORT_TS_KEY, add_order, assert_streams_parity, build_request, drain, drive_rest_orders,
    execution_record_join_keys, journaled_events, normalize_event, normalize_stream, send, token,
    values_for_key, venue, ws_fill_data, ws_fill_join_keys,
};

use fauxchange::exchange::{
    CancelReason, CancelledLeg, Cents, EventTimestamp, Fill as SeamFill, Hash32, InstrumentStatus,
    LineageId, STPMode, SequenceNumber, Side as SeamSide, SignedCents, Symbol, TimeInForce,
    VenueCommand, VenueEvent, VenueOutcome,
};
use fauxchange::exchange::{ExecutionFilter, ExecutionsStore};
use fauxchange::gateway::fix::md_projection::{self, RequestedSides};
use fauxchange::gateway::ws::{ClientAction, FrameOutcome, parse_frame};
use fauxchange::models::{
    AccountId, LiquidityFlag, OrderType, Permission, ReplayReportResponse, VenueOrderId, WsMessage,
};
use fauxchange::{VenueError, WsErrorCode};

// ============================================================================
// Shared fixtures for the hand-built normalizer / market-data events
// ============================================================================

fn sym() -> Symbol {
    match Symbol::parse(CALL) {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
    }
}

/// A resting limit add (no fills) — a committed `VenueEvent` at `sequence`.
fn resting_add(sequence: u64, order_id: &str, side: SeamSide, price: u64, qty: u64) -> VenueEvent {
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
        SequenceNumber::new(sequence),
        EventTimestamp::new(1_700_000_000_000),
        command,
        VenueOutcome::Added {
            fills: vec![],
            resting_quantity: qty,
            stp_cancelled: vec![],
        },
    )
}

/// A crossing taker buy that fully consumes a resting maker — a committed
/// `VenueEvent` carrying two linked fill legs (shared `execution_id`).
fn crossing_buy(sequence: u64, taker_order_id: &str, price: u64, qty: u64) -> VenueEvent {
    let lineage = LineageId::new("fauxchange");
    let execution_id = lineage.execution_id("BTC", SequenceNumber::new(sequence), 0);
    let maker = SeamFill {
        execution_id: execution_id.clone(),
        order_id: VenueOrderId::new("maker-resting"),
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
        order_id: VenueOrderId::new(taker_order_id),
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
        order_id: VenueOrderId::new(taker_order_id),
        account: AccountId::new("taker"),
        owner: Hash32([0x22; 32]),
        client_order_id: Some(fauxchange::models::ClientOrderId::new("cl-42")),
        side: SeamSide::Buy,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity: qty,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    };
    VenueEvent::new(
        SequenceNumber::new(sequence),
        EventTimestamp::new(1_700_000_000_000),
        command,
        VenueOutcome::Added {
            fills: vec![maker, taker],
            resting_quantity: 0,
            stp_cancelled: vec![],
        },
    )
}

// ============================================================================
// 1. Reachability — the milestone acceptance item
// ============================================================================

/// The documented Backend REST route inventory (#013): `(path, methods)` with the
/// `{param}` placeholders the OpenAPI document uses. `/ws` is the WS handshake
/// (not a REST route); it is covered by the WS message set below and by #014.
fn rest_route_inventory() -> Vec<(String, Vec<&'static str>)> {
    let mut routes: Vec<(&str, Vec<&str>)> = vec![
        ("/health", vec!["get"]),
        ("/api/v1/stats", vec!["get"]),
        ("/api/v1/auth/token", vec!["post"]),
        ("/api/v1/controls", vec!["get"]),
        ("/api/v1/controls/kill-switch", vec!["post"]),
        ("/api/v1/controls/enable", vec!["post"]),
        ("/api/v1/controls/parameters", vec!["post"]),
        ("/api/v1/controls/instruments", vec!["get"]),
        ("/api/v1/controls/instrument/{symbol}/toggle", vec!["post"]),
        ("/api/v1/replay/record", vec!["get", "post"]),
        ("/api/v1/replay/export", vec!["get"]),
        ("/api/v1/replay/bundle", vec!["post"]),
        ("/api/v1/prices", vec!["get", "post"]),
        ("/api/v1/prices/{symbol}", vec!["get"]),
        ("/api/v1/underlyings", vec!["get"]),
        (
            "/api/v1/underlyings/{underlying}",
            vec!["get", "post", "delete"],
        ),
        ("/api/v1/underlyings/{underlying}/expirations", vec!["get"]),
        (
            "/api/v1/underlyings/{underlying}/expirations/{expiration}",
            vec!["get", "post"],
        ),
        (
            "/api/v1/underlyings/{underlying}/volatility-surface",
            vec!["get"],
        ),
        (
            "/api/v1/underlyings/{underlying}/expirations/{expiration}/chain",
            vec!["get"],
        ),
        (
            "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes",
            vec!["get"],
        ),
        (
            "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}",
            vec!["get", "post"],
        ),
        ("/api/v1/orders", vec!["get"]),
        ("/api/v1/orders/bulk", vec!["post", "delete"]),
        ("/api/v1/orders/cancel-all", vec!["delete"]),
        ("/api/v1/orders/{order_id}", vec!["get"]),
        ("/api/v1/positions", vec!["get"]),
        ("/api/v1/positions/{symbol}", vec!["get"]),
        ("/api/v1/executions", vec!["get"]),
        ("/api/v1/executions/{execution_id}", vec!["get"]),
        ("/api/v1/admin/snapshot", vec!["post"]),
        ("/api/v1/admin/snapshots", vec!["get"]),
        ("/api/v1/admin/snapshots/{snapshot_id}", vec!["get"]),
        (
            "/api/v1/admin/snapshots/{snapshot_id}/restore",
            vec!["post"],
        ),
    ];
    // The per-contract routes share the CONTRACT prefix.
    let contract: Vec<(&str, Vec<&str>)> = vec![
        ("", vec!["get"]),
        ("/orders", vec!["post"]),
        ("/orders/market", vec!["post"]),
        ("/orders/{order_id}", vec!["delete", "patch"]),
        ("/quote", vec!["get"]),
        ("/greeks", vec!["get"]),
        ("/snapshot", vec!["get"]),
        ("/last-trade", vec!["get"]),
        ("/ohlc", vec!["get"]),
        ("/metrics", vec!["get"]),
    ];
    const CONTRACT_TEMPLATE: &str = "/api/v1/underlyings/{underlying}/expirations/{expiration}/strikes/{strike}/options/{style}";
    let mut out: Vec<(String, Vec<&'static str>)> = routes
        .drain(..)
        .map(|(path, methods)| (path.to_string(), methods))
        .collect();
    for (suffix, methods) in contract {
        out.push((format!("{CONTRACT_TEMPLATE}{suffix}"), methods));
    }
    out
}

#[tokio::test]
async fn test_every_documented_rest_route_is_served_with_its_openapi_shape() {
    // The served OpenAPI document is the public wire contract (#013): every
    // documented route + method must appear in it with its declared shape.
    let state = venue(AMPLE_RATE_LIMIT);
    let (status, doc) = send(
        &state,
        build_request("GET", "/api-docs/openapi.json", None, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "the OpenAPI doc must serve");
    let paths = match doc.get("paths").and_then(Value::as_object) {
        Some(paths) => paths,
        None => panic!("the OpenAPI doc must carry a paths object"),
    };

    for (path, methods) in rest_route_inventory() {
        let entry = match paths.get(&path) {
            Some(entry) => entry,
            None => panic!("documented route {path} is missing from the served OpenAPI doc"),
        };
        for method in methods {
            assert!(
                entry.get(method).is_some(),
                "route {path} must document the {method} operation in its OpenAPI shape"
            );
        }
    }
    // The bearer security scheme the protected paths reference is registered.
    assert!(
        doc["components"]["securitySchemes"]["bearer_jwt"].is_object(),
        "the bearer_jwt security scheme must be registered"
    );
}

#[tokio::test]
async fn test_representative_rest_routes_are_live_reachable() {
    // Reachability in practice: a matched route runs its handler (any typed
    // response — 200 / 400 / 403 / resource-404-with-body). Only a bare
    // route-404 (empty body) means the route is not mounted. An Admin token
    // (Admin implies all) keeps a permission gate from masking reachability.
    let state = venue(AMPLE_RATE_LIMIT);
    let admin = token(&state, "admin-1");

    let cases: &[(&str, String)] = &[
        ("GET", "/health".to_string()),
        ("GET", "/api/v1/stats".to_string()),
        ("GET", "/api/v1/controls".to_string()),
        ("GET", "/api/v1/controls/instruments".to_string()),
        ("GET", "/api/v1/prices".to_string()),
        ("GET", "/api/v1/underlyings".to_string()),
        ("GET", "/api/v1/orders".to_string()),
        ("GET", "/api/v1/orders/does-not-exist".to_string()),
        ("GET", "/api/v1/positions".to_string()),
        ("GET", "/api/v1/executions".to_string()),
        ("GET", "/api/v1/executions/missing".to_string()),
        ("GET", "/api/v1/admin/snapshots".to_string()),
    ];
    for (method, path) in cases {
        let bearer = if *path == "/health" {
            None
        } else {
            Some(admin.as_str())
        };
        let (status, body) = send(&state, build_request(method, path, bearer, None)).await;
        let bare_route_404 = status == StatusCode::NOT_FOUND && body.is_null();
        assert!(
            !bare_route_404,
            "{method} {path} must be a mounted route, got a bare route-404"
        );
    }

    // A mutating route is reachable too: a place through the live router returns a
    // typed accepted response (not a route-404).
    let trader = token(&state, "trader-1");
    let (status, body) = send(
        &state,
        build_request(
            "POST",
            &format!("{CONTRACT}/orders"),
            Some(&trader),
            Some(serde_json::json!({ "side": "buy", "price": 50_000, "quantity": 1 })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "accepted");
}

#[test]
fn test_every_documented_ws_message_round_trips_to_its_golden() {
    // Reuse the #004 WS goldens as the shape oracle: every documented WS message
    // deserializes into `WsMessage` and re-serializes to the identical golden —
    // i.e. every documented WS message is reachable with the same shape.
    let dir = format!("{}/tests/golden/ws", env!("CARGO_MANIFEST_DIR"));
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) => panic!("failed to read {dir}: {e}"),
    };
    let mut covered_types: Vec<String> = Vec::new();
    for entry in entries {
        let path = match entry {
            Ok(entry) => entry.path(),
            Err(e) => panic!("failed to read a ws golden entry: {e}"),
        };
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let raw = match std::fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(e) => panic!("failed to read {}: {e}", path.display()),
        };
        let golden: Value = match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(e) => panic!("failed to parse {}: {e}", path.display()),
        };
        // `error.json` is the bare inner `WsError` (no `type`), not a WsMessage.
        let Some(type_tag) = golden.get("type").and_then(Value::as_str) else {
            continue;
        };
        let message: WsMessage = match serde_json::from_value(golden.clone()) {
            Ok(message) => message,
            Err(e) => panic!("golden {} is not a valid WsMessage: {e}", path.display()),
        };
        let produced = match serde_json::to_value(&message) {
            Ok(produced) => produced,
            Err(e) => panic!("failed to re-serialise {}: {e}", path.display()),
        };
        assert_eq!(
            produced,
            golden,
            "WsMessage {} must round-trip to its golden shape",
            path.display()
        );
        covered_types.push(type_tag.to_string());
    }

    // Every documented server → client message type (03 §4) is covered.
    let documented = [
        "connected",
        "heartbeat",
        "quote",
        "price",
        "config",
        "fill",
        "orderbook_snapshot",
        "orderbook_delta",
        "trade",
        "subscribed",
        "unsubscribed",
        "batch_subscribed",
        "batch_unsubscribed",
        "subscriptions",
        "recording_state",
        "replay_complete",
        "error",
    ];
    for wanted in documented {
        assert!(
            covered_types.iter().any(|t| t == wanted),
            "documented WS message type `{wanted}` must have a golden that round-trips"
        );
    }
}

#[test]
fn test_ws_client_action_set_parses_and_order_entry_is_rejected() {
    // The #014 client → server action set is reachable: subscription + control
    // frames parse to their action; every order-entry-shaped frame is rejected
    // (WS is not an order-entry surface).
    let ok_frames = [
        r#"{"action":"subscribe","channel":"orderbook","symbol":"BTC-20240329-50000-C","depth":5}"#,
        r#"{"action":"unsubscribe","channel":"trades","symbol":"BTC-20240329-50000-C"}"#,
        r#"{"action":"list_subscriptions"}"#,
        r#"{"action":"set_spread","value":1.5}"#,
        r#"{"action":"set_size","value":0.5}"#,
        r#"{"action":"set_skew","value":-0.2}"#,
        r#"{"action":"kill"}"#,
        r#"{"action":"enable"}"#,
    ];
    for frame in ok_frames {
        match parse_frame(frame) {
            FrameOutcome::Action(_, _) => {}
            other => panic!("frame {frame} must parse to an action, got {other:?}"),
        }
    }

    // Control actions map to the market-maker knobs (a spot check the enum is the
    // one #015/control-parity uses).
    match parse_frame(r#"{"action":"kill"}"#) {
        FrameOutcome::Action(ClientAction::Kill, _) => {}
        other => panic!("kill must parse to ClientAction::Kill, got {other:?}"),
    }

    for frame in [
        r#"{"action":"place_order","side":"buy","price":50000,"quantity":1}"#,
        r#"{"action":"cancel_order","order_id":"x"}"#,
        r#"{"side":"buy","price":50000,"quantity":10}"#,
    ] {
        match parse_frame(frame) {
            FrameOutcome::Reject(error) => assert!(!error.terminal),
            other => panic!("order-entry frame {frame} must be rejected, got {other:?}"),
        }
    }
}

/// The sequenced `MarketMakerControl` a WS control action / a REST parameters or
/// kill-switch request builds — the SINGLE control-plane command both surfaces route,
/// so a control cannot diverge across REST and WS by construction (#047).
fn control_command(
    spread_multiplier: Option<f64>,
    size_scalar: Option<f64>,
    directional_skew: Option<f64>,
    enabled: Option<bool>,
) -> VenueCommand {
    VenueCommand::MarketMakerControl {
        spread_multiplier,
        size_scalar,
        directional_skew,
        enabled,
    }
}

#[tokio::test]
async fn test_control_parity_rest_and_ws_apply_the_same_knob() {
    // Control parity (REST ≡ WS): the WS `set_spread` / `set_size` / `set_skew` /
    // `kill` / `enable` actions and the REST `POST /controls/{parameters,kill-switch}`
    // build the SAME sequenced `MarketMakerControl`, so applying it on two identical
    // venues yields identical engine state — the control plane has one command, no
    // per-surface divergence.
    let rest_venue = venue(AMPLE_RATE_LIMIT);
    let ws_venue = venue(AMPLE_RATE_LIMIT);

    // The five control actions, each as the (spread, size, skew, enabled) knobs the
    // two surfaces derive identically.
    let actions = [
        // REST /controls/parameters == WS set_spread / set_size / set_skew batch.
        (Some(2.5), Some(0.4), Some(-0.3), None),
        // WS set_spread.
        (Some(1.5), None, None, None),
        // WS kill == REST /controls/kill-switch {enabled:false}.
        (None, None, None, Some(false)),
        // WS enable == REST /controls/enable {enabled:true}.
        (None, None, None, Some(true)),
    ];

    for (spread, size, skew, enabled) in actions {
        rest_venue
            .submit(control_command(spread, size, skew, enabled))
            .await
            .expect("REST-surface control fans out and applies");
        ws_venue
            .submit(control_command(spread, size, skew, enabled))
            .await
            .expect("WS-surface control fans out and applies");
    }

    // Both engines end in the identical persona-substrate config — REST and WS parity.
    assert_eq!(
        rest_venue.market_maker().get_config(),
        ws_venue.market_maker().get_config(),
        "REST and WS controls apply the identical sequenced knob"
    );
    // The final kill/enable/spread landed (enable was last → enabled).
    let config = rest_venue.market_maker().get_config();
    assert!(config.enabled, "the final enable control took effect");
    assert_eq!(
        config.spread_multiplier, 1.5,
        "the spread control took effect"
    );
    assert_eq!(
        config.directional_skew, -0.3,
        "the skew control took effect"
    );
}

// ============================================================================
// 2. Observation parity (REST ≡ WS) — THE core parity test
// ============================================================================

/// Finds the taker-leg WS `fill` (side `buy`, liquidity `taker`) among a drained
/// batch of broadcast messages.
fn find_taker_fill(messages: &[WsMessage]) -> Option<WsMessage> {
    messages
        .iter()
        .find(|message| {
            matches!(ws_fill_data(message), Some(data)
            if data.get("liquidity").and_then(Value::as_str) == Some("taker")
                && data.get("side").and_then(Value::as_str) == Some("buy"))
        })
        .cloned()
}

#[tokio::test]
async fn test_one_committed_fill_renders_identically_on_rest_and_ws() {
    // ONE committed fill; assert its REST `ExecutionRecord` and WS `fill` agree on
    // the four join keys plus price/quantity/side. Both are projections of the
    // SAME committed event, fed by the same post-journal fan-out.
    let state = venue(AMPLE_RATE_LIMIT);
    let mut rx = state.subscriptions().subscribe();

    // Maker (trader-1) rests a sell; taker (trader-2) fully crosses it at seq 1.
    match state
        .submit(add_order(
            "maker",
            "trader-1",
            2,
            SeamSide::Sell,
            50_000,
            5,
            TimeInForce::Gtc,
        ))
        .await
    {
        Ok(_) => {}
        Err(e) => panic!("maker submit must succeed: {e}"),
    }
    match state
        .submit(add_order(
            "taker",
            "trader-2",
            3,
            SeamSide::Buy,
            50_000,
            5,
            TimeInForce::Gtc,
        ))
        .await
    {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence, SequenceNumber::new(1)),
        Err(e) => panic!("taker submit must succeed: {e}"),
    }

    // The WS projection: the anonymised taker fill print.
    let messages = drain(&mut rx);
    let taker_fill = match find_taker_fill(&messages) {
        Some(fill) => fill,
        None => panic!("the crossing must emit a taker WS fill"),
    };
    let ws_keys = match ws_fill_join_keys(&taker_fill) {
        Some(keys) => keys,
        None => panic!("the taker fill must yield join keys"),
    };

    // The REST projection: the account-scoped ExecutionRecord for the taker leg,
    // read through the live `GET /executions/{id}` route with the taker's token.
    let taker_token = token(&state, "trader-2");
    let uri = format!("/api/v1/executions/{}", ws_keys.execution_id);
    let (status, record) = send(&state, build_request("GET", &uri, Some(&taker_token), None)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the taker ExecutionRecord must be readable: {record}"
    );
    let rest_keys = match execution_record_join_keys(&record) {
        Some(keys) => keys,
        None => panic!("the ExecutionRecord must yield join keys: {record}"),
    };

    // The parity contract: identical join keys + price/quantity/side across REST
    // and WS.
    assert_eq!(
        ws_keys, rest_keys,
        "one fill must render identically on REST and WS"
    );
    // Sanity: the shared values are the ones we drove.
    assert_eq!(rest_keys.underlying_sequence, 1);
    assert_eq!(rest_keys.price, 50_000);
    assert_eq!(rest_keys.quantity, 5);
    assert_eq!(rest_keys.side, "buy");
    assert_eq!(rest_keys.liquidity, "taker");
}

#[tokio::test]
async fn test_ws_fill_is_anonymised_rest_execution_record_is_account_scoped() {
    // The WS `fill` is the PUBLIC projection: it omits `account` and `fee`. The
    // REST `ExecutionRecord` is the AUTHORITATIVE account-scoped projection: it
    // carries both. This asymmetry is the parity contract, not a divergence.
    let state = venue(AMPLE_RATE_LIMIT);
    let mut rx = state.subscriptions().subscribe();
    match state
        .submit(add_order(
            "maker",
            "trader-1",
            2,
            SeamSide::Sell,
            50_000,
            2,
            TimeInForce::Gtc,
        ))
        .await
    {
        Ok(_) => {}
        Err(e) => panic!("maker submit must succeed: {e}"),
    }
    match state
        .submit(add_order(
            "taker",
            "trader-2",
            3,
            SeamSide::Buy,
            50_000,
            2,
            TimeInForce::Gtc,
        ))
        .await
    {
        Ok(_) => {}
        Err(e) => panic!("taker submit must succeed: {e}"),
    }

    let messages = drain(&mut rx);
    let taker_fill = match find_taker_fill(&messages) {
        Some(fill) => fill,
        None => panic!("expected a taker WS fill"),
    };
    let data = match ws_fill_data(&taker_fill) {
        Some(data) => data,
        None => panic!("expected the taker fill data object"),
    };
    // No account / fee leak on the public print…
    assert!(data.get("account").is_none(), "WS fill must omit account");
    assert!(data.get("fee").is_none(), "WS fill must omit fee");
    // …but the four join keys are present.
    assert!(data.get("execution_id").is_some());
    assert!(data.get("underlying_sequence").is_some());
    assert!(data.get("venue_ts").is_some());
    assert!(data.get("liquidity").is_some());

    // The REST ExecutionRecord for the same leg DOES carry account + fee.
    let execution_id = match data.get("execution_id").and_then(Value::as_str) {
        Some(id) => id.to_string(),
        None => panic!("the WS fill must carry an execution_id"),
    };
    let taker_token = token(&state, "trader-2");
    let (status, record) = send(
        &state,
        build_request(
            "GET",
            &format!("/api/v1/executions/{execution_id}"),
            Some(&taker_token),
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(record["account"], "trader-2");
    assert!(record.get("fee_cents").is_some(), "REST record carries fee");
}

// ============================================================================
// 3. Market-data parity — instrument_sequence + resulting-quantity + fresh-snapshot recovery
// ============================================================================

#[tokio::test]
async fn test_orderbook_deltas_are_sequenced_and_resulting_quantity() {
    // Deltas carry a strictly-increasing per-instrument sequence and
    // RESULTING-quantity semantics (the change's quantity is the level's new
    // total, not the increment).
    let state = venue(AMPLE_RATE_LIMIT);
    let mut rx = state.subscriptions().subscribe();

    // Two sells at the SAME ask level: 8 then +4 → resulting totals 8 then 12.
    match state
        .submit(add_order(
            "r1",
            "trader-1",
            2,
            SeamSide::Sell,
            50_100,
            8,
            TimeInForce::Gtc,
        ))
        .await
    {
        Ok(_) => {}
        Err(e) => panic!("first rest must succeed: {e}"),
    }
    match state
        .submit(add_order(
            "r2",
            "trader-1",
            2,
            SeamSide::Sell,
            50_100,
            4,
            TimeInForce::Gtc,
        ))
        .await
    {
        Ok(_) => {}
        Err(e) => panic!("second rest must succeed: {e}"),
    }

    let deltas: Vec<(u64, u64)> = drain(&mut rx)
        .into_iter()
        .filter_map(|message| match serde_json::to_value(&message) {
            Ok(value) if value.get("type").and_then(Value::as_str) == Some("orderbook_delta") => {
                let sequence = value["data"]["sequence"].as_u64()?;
                // The single touched ask level's resulting quantity.
                let change = value["data"]["changes"].get(0)?;
                assert_eq!(change["side"], "ask");
                assert_eq!(change["price"], 50_100);
                Some((sequence, change["quantity"].as_u64()?))
            }
            _ => None,
        })
        .collect();

    assert_eq!(deltas.len(), 2, "each user-driven rest emits one delta");
    // Strictly increasing instrument_sequence.
    assert!(
        deltas[1].0 > deltas[0].0,
        "instrument_sequence must strictly increase, got {deltas:?}"
    );
    // Resulting-quantity: 8 then the cumulative 12 (not the 4 increment).
    assert_eq!(deltas[0].1, 8, "first delta shows the resulting total 8");
    assert_eq!(deltas[1].1, 12, "second delta shows the resulting total 12");

    // A fresh snapshot reflects the folded state at the current sequence — the
    // gap-recovery projection is a snapshot, never a resend.
    match state.subscriptions().orderbook_snapshot(&sym(), None) {
        WsMessage::OrderbookSnapshot { asks, sequence, .. } => {
            assert_eq!(
                sequence, deltas[1].0,
                "the snapshot baselines at the last seq"
            );
            assert_eq!(asks.len(), 1, "one ask level after two rests at one price");
            assert_eq!(asks[0].quantity, 12, "the folded resulting total is 12");
        }
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

#[test]
fn test_market_data_gap_recovers_by_fresh_snapshot_not_resend() {
    // A bounded broadcast: a slow consumer LAGS (drops backlog) rather than
    // stalling the producer, and recovery is a FRESH snapshot — never a resend of
    // the dropped deltas (the market-data namespace is not journaled).
    use fauxchange::subscription::OrderbookSubscriptionManager;
    use tokio::sync::broadcast::error::TryRecvError;

    let manager = OrderbookSubscriptionManager::with_capacity(2);
    let mut rx = manager.subscribe();
    for i in 0..6u64 {
        manager.on_committed_event(&resting_add(
            i,
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
            Err(TryRecvError::Lagged(_)) => {
                lagged = true;
                break;
            }
            Err(_) => break,
        }
    }
    assert!(lagged, "a slow consumer lags on a bounded broadcast");

    // Recovery: a fresh snapshot reflects every folded mutation at its current
    // sequence (the dropped deltas are NOT replayed).
    match manager.orderbook_snapshot(&sym(), None) {
        WsMessage::OrderbookSnapshot { asks, sequence, .. } => {
            assert_eq!(asks.len(), 6, "the fresh snapshot has every folded level");
            assert_eq!(sequence, 6, "the snapshot re-baselines at the current seq");
        }
        other => panic!("expected a fresh snapshot, got {other:?}"),
    }
}

// ============================================================================
// 3b. FIX market-data observation parity (#040) — W/X are the WS twin by construction
// ============================================================================

const BOTH_SIDES: RequestedSides = RequestedSides {
    bids: true,
    asks: true,
};

/// A resting add by the venue-reserved market-maker account — a requote that must
/// NOT emit an `orderbook_delta` (and therefore no FIX `X`).
fn mm_resting_add(
    sequence: u64,
    order_id: &str,
    side: SeamSide,
    price: u64,
    qty: u64,
) -> VenueEvent {
    let command = VenueCommand::AddOrder {
        symbol: sym(),
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new(fauxchange::exchange::MARKET_MAKER_ACCOUNT),
        owner: fauxchange::exchange::MARKET_MAKER_OWNER,
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity: qty,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    };
    VenueEvent::new(
        SequenceNumber::new(sequence),
        EventTimestamp::new(1_700_000_000_000),
        command,
        VenueOutcome::Added {
            fills: vec![],
            resting_quantity: qty,
            stp_cancelled: vec![],
        },
    )
}

#[test]
fn test_fix_market_data_w_and_x_agree_with_ws_on_sequence_and_quantity() {
    // The same-book agreement property: the FIX `W`/`X` are a pure projection of the
    // exact `WsMessage` the manager produced, so `RptSeq (83)` equals the WS
    // `instrument_sequence` and the resulting quantities match — observation parity
    // by construction, no parallel market-data path to drift.
    use fauxchange::subscription::OrderbookSubscriptionManager;

    let manager = OrderbookSubscriptionManager::with_capacity(64);
    let mut rx = manager.subscribe();

    // Two user-driven rests at the same ask level: resulting totals 8 then 12.
    manager.on_committed_event(&resting_add(1, "r1", SeamSide::Sell, 50_100, 8));
    manager.on_committed_event(&resting_add(2, "r2", SeamSide::Sell, 50_100, 4));

    let mut projected = Vec::new();
    while let Ok(message) = rx.try_recv() {
        if let Some((rpt_seq, entries)) =
            md_projection::incremental_projection(&message, BOTH_SIDES)
        {
            let WsMessage::OrderbookDelta {
                sequence, changes, ..
            } = &message
            else {
                unreachable!("incremental_projection only returns Some for a delta")
            };
            // RptSeq (83) == the WS instrument_sequence for the SAME message.
            assert_eq!(rpt_seq, *sequence, "RptSeq(83) == WS instrument_sequence");
            // The X entry's MDEntrySize == the WS change's resulting quantity.
            assert_eq!(
                entries[0].size, changes[0].quantity,
                "MDEntrySize == WS resulting quantity"
            );
            projected.push(rpt_seq);
        }
    }
    assert_eq!(
        projected.len(),
        2,
        "two user-driven rests → two X-eligible deltas"
    );
    assert!(
        projected[1] > projected[0],
        "RptSeq is strictly increasing per instrument"
    );

    // The FIX `W` snapshot projects the same book at the same sequence.
    let snapshot = manager.orderbook_snapshot(&sym(), None);
    let (w_seq, w_entries) =
        md_projection::snapshot_projection(&snapshot, BOTH_SIDES).expect("a W projection");
    let WsMessage::OrderbookSnapshot {
        sequence: ws_seq,
        asks,
        ..
    } = &snapshot
    else {
        unreachable!("orderbook_snapshot returns a snapshot")
    };
    assert_eq!(
        w_seq, *ws_seq,
        "W RptSeq == WS snapshot instrument_sequence"
    );
    assert_eq!(w_entries.len(), asks.len(), "one W entry per ask level");
    assert_eq!(w_entries[0].size, 12, "the folded resulting total is 12");
}

#[test]
fn test_fix_market_data_mm_requote_never_produces_an_incremental() {
    // A market-maker requote does not emit an `orderbook_delta`, so the FIX
    // projection produces NO `X` (inherited from the manager); the requote is
    // instead reflected in the next fresh `W` snapshot.
    use fauxchange::subscription::OrderbookSubscriptionManager;

    let manager = OrderbookSubscriptionManager::with_capacity(64);
    let mut rx = manager.subscribe();
    manager.on_committed_event(&mm_resting_add(1, "mm1", SeamSide::Sell, 50_100, 5));

    let mut incrementals = 0usize;
    while let Ok(message) = rx.try_recv() {
        if md_projection::incremental_projection(&message, BOTH_SIDES).is_some() {
            incrementals += 1;
        }
    }
    assert_eq!(incrementals, 0, "an MM requote never streams an X");

    // The requote IS present in the next fresh W snapshot.
    let snapshot = manager.orderbook_snapshot(&sym(), None);
    let (_, w_entries) =
        md_projection::snapshot_projection(&snapshot, BOTH_SIDES).expect("a W projection");
    assert_eq!(w_entries.len(), 1, "the requote appears in the snapshot");
    assert_eq!(w_entries[0].size, 5);
}

#[test]
fn test_fix_market_data_gap_recovers_by_fresh_w_not_resend() {
    // A market-data `RptSeq` gap (a lagged broadcast) recovers by a FRESH `W`
    // snapshot at the re-baselined `instrument_sequence` — NOT a `ResendRequest (2)`
    // of the dropped deltas. The two sequence namespaces stay distinct: session
    // `MsgSeqNum` resend cannot backfill an application market-data stream.
    use fauxchange::subscription::OrderbookSubscriptionManager;
    use tokio::sync::broadcast::error::TryRecvError;

    let manager = OrderbookSubscriptionManager::with_capacity(2);
    let mut rx = manager.subscribe();
    for i in 0..6u64 {
        manager.on_committed_event(&resting_add(
            i + 1,
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
            Err(TryRecvError::Lagged(_)) => {
                lagged = true;
                break;
            }
            Err(_) => break,
        }
    }
    assert!(lagged, "the RptSeq stream has a gap (the receiver lagged)");

    let snapshot = manager.orderbook_snapshot(&sym(), None);
    let (w_seq, w_entries) =
        md_projection::snapshot_projection(&snapshot, BOTH_SIDES).expect("a fresh W");
    assert_eq!(
        w_seq, 6,
        "the fresh W re-baselines at the current instrument_sequence"
    );
    assert_eq!(
        w_entries.len(),
        6,
        "every folded ask level is in the fresh W"
    );
}

// ============================================================================
// 4. Control parity (REST ≡ WS) — same command, same sequenced fan-out outcome
// ============================================================================

#[tokio::test]
async fn test_control_parity_rest_and_ws_build_same_command_and_surface_same_outcome() {
    // The REST kill-switch and the WS `kill` action both build the IDENTICAL
    // `MarketMakerControl { enabled: Some(false) }` command. It is venue-global and
    // now **routes** — fanned to every underlying's actor and journaled (#47) — so
    // BOTH surfaces report the sequenced success, parity of behaviour.
    let state = venue(AMPLE_RATE_LIMIT);
    let admin = token(&state, "admin-1");

    // REST: the kill-switch handler builds MarketMakerControl and submits it.
    let (rest_status, rest_body) = send(
        &state,
        build_request(
            "POST",
            "/api/v1/controls/kill-switch",
            Some(&admin),
            Some(serde_json::json!({ "enabled": false })),
        ),
    )
    .await;
    assert_eq!(
        rest_status,
        StatusCode::OK,
        "REST kill-switch routes the venue-global control and reports success"
    );
    assert_eq!(rest_body["success"], serde_json::json!(true));
    assert_eq!(rest_body["master_enabled"], serde_json::json!(false));

    // The command both surfaces construct (WS `control()` builds the same value).
    let command = VenueCommand::MarketMakerControl {
        spread_multiplier: None,
        size_scalar: None,
        directional_skew: None,
        enabled: Some(false),
    };
    // Submitting it — the exact command the WS `kill` action routes — now fans out
    // to the venue's underlyings and commits a receipt.
    match state.submit(command).await {
        Ok(_receipt) => {}
        other => panic!("MarketMakerControl must now route (a committed receipt), got {other:?}"),
    }
}

#[tokio::test]
async fn test_control_parity_permission_gate_is_identical_on_rest_and_ws() {
    // The permission required for a control action is the same regardless of
    // surface: Admin. A Trade token is forbidden on the REST kill-switch, exactly
    // as the WS control path forbids a non-Admin caller.
    let state = venue(AMPLE_RATE_LIMIT);
    let trader = token(&state, "trader-1");
    let (status, body) = send(
        &state,
        build_request(
            "POST",
            "/api/v1/controls/enable",
            Some(&trader),
            Some(serde_json::json!({ "enabled": true })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "forbidden");

    // The WS rendering of the same Forbidden(Admin) is a non-terminal envelope.
    let ws_error = VenueError::Forbidden(fauxchange::models::Permission::Admin).ws_error(None);
    assert_eq!(ws_error.code, WsErrorCode::Forbidden);
    assert!(!ws_error.terminal);
}

// ============================================================================
// 5. REST order-entry base — identically-seeded fresh venue per surface
// ============================================================================

/// Runs a scenario over TWO identically-seeded fresh REST venues and returns
/// `(events_a, events_b, ids_a, ids_b)` — the per-surface topology the FIX arm
/// (#041) extends by replacing the second venue with a FIX-driven one.
async fn run_rest_pair(
    steps: &[Step],
) -> (
    Vec<VenueEvent>,
    Vec<VenueEvent>,
    Vec<Option<String>>,
    Vec<Option<String>>,
) {
    let venue_a = venue(AMPLE_RATE_LIMIT);
    let ids_a = drive_rest_orders(&venue_a, steps).await;
    let events_a = journaled_events(&venue_a, "BTC").await;

    let venue_b = venue(AMPLE_RATE_LIMIT);
    let ids_b = drive_rest_orders(&venue_b, steps).await;
    let events_b = journaled_events(&venue_b, "BTC").await;

    (events_a, events_b, ids_a, ids_b)
}

#[tokio::test]
async fn test_rest_place_order_entry_base_normalizes_equal() {
    // A single resting place over two fresh venues: the normalized VenueEvent
    // streams are equal.
    let steps = [Step::Place {
        account: "trader-1",
        side: "sell",
        price: 50_000,
        qty: 5,
        tif: None,
    }];
    let (events_a, events_b, ids_a, ids_b) = run_rest_pair(&steps).await;

    assert_eq!(events_a.len(), 1, "one place is one committed event");
    assert_streams_parity("rest-venue-a", &events_a, "rest-venue-b", &events_b);

    // The normalizer is doing real work: the raw gateway-minted order ids DIFFER
    // across the two fresh venues (the global g-counter), yet the streams
    // normalize equal — proving order_id is a stripped protocol placeholder.
    assert_ne!(
        ids_a[0], ids_b[0],
        "the per-surface order ids differ (a stripped placeholder)"
    );
}

#[tokio::test]
async fn test_rest_partial_fill_order_entry_base_normalizes_equal() {
    // Maker rests 5; taker takes 2 → a partial fill of the maker (remainder 3
    // rests). Two fresh venues normalize equal, and the VERBATIM fields
    // (underlying_sequence, execution_id, fills) are already identical raw.
    let steps = [
        Step::Place {
            account: "trader-1",
            side: "sell",
            price: 50_000,
            qty: 5,
            tif: None,
        },
        Step::Place {
            account: "trader-2",
            side: "buy",
            price: 50_000,
            qty: 2,
            tif: None,
        },
    ];
    let (events_a, events_b, _, _) = run_rest_pair(&steps).await;

    // The crossing event carries fills (non-vacuous).
    let crossing = &events_a[1];
    let fills = match &crossing.outcome {
        VenueOutcome::Added { fills, .. } if !fills.is_empty() => fills,
        other => panic!("expected the taker event to carry fills, got {other:?}"),
    };
    assert_eq!(fills.len(), 2, "one match, two linked legs");

    assert_streams_parity("rest-venue-a", &events_a, "rest-venue-b", &events_b);

    // The compared-verbatim join keys are already identical raw (no normalization
    // needed): same execution_id + underlying_sequence across the two venues.
    let exec_a = normalize_stream(&events_a);
    let exec_b = normalize_stream(&events_b);
    assert_eq!(exec_a, exec_b);
    for (ea, eb) in events_a.iter().zip(events_b.iter()) {
        assert_eq!(
            ea.underlying_sequence, eb.underlying_sequence,
            "underlying_sequence is compared verbatim and must match raw"
        );
    }
}

#[tokio::test]
async fn test_rest_cancel_replace_order_entry_base_normalizes_equal() {
    // Cancel-replace over REST is the documented idiom: place → cancel → re-place
    // (there is no atomic REST modify; `PATCH` directs the client to cancel and
    // re-place). Three committed commands; two fresh venues normalize equal.
    let steps = [
        Step::Place {
            account: "trader-1",
            side: "sell",
            price: 50_000,
            qty: 4,
            tif: None,
        },
        Step::Cancel {
            account: "trader-1",
            target: 0,
        },
        Step::Place {
            account: "trader-1",
            side: "sell",
            price: 50_500,
            qty: 4,
            tif: None,
        },
    ];
    let (events_a, events_b, _, _) = run_rest_pair(&steps).await;

    assert_eq!(events_a.len(), 3, "place + cancel + re-place = 3 events");
    // The middle command is the cancel.
    assert!(
        matches!(events_a[1].command, VenueCommand::CancelOrder { .. }),
        "the second command must be a CancelOrder"
    );
    assert_streams_parity("rest-venue-a", &events_a, "rest-venue-b", &events_b);
}

// ============================================================================
// 6. Normalization-rule unit tests (which fields stripped vs verbatim)
// ============================================================================

#[test]
fn test_normalize_strips_protocol_only_fields_and_keeps_venue_identity_verbatim() {
    // A crossing event carries every field class: command ids, a client_order_id,
    // fills (with per-leg order ids + execution_id), and resting-book state.
    let event = crossing_buy(7, "taker-order-xyz", 50_000, 2);
    let raw = match serde_json::to_value(&event) {
        Ok(value) => value,
        Err(e) => panic!("serialise failed: {e}"),
    };
    let normalized = normalize_event(&event);

    // STRIPPED: every order_id / new_order_id / client_order_id becomes the
    // placeholder (they appear in the command AND in the fills).
    for key in STRIPPED_KEYS {
        let raw_values = values_for_key(&raw, key);
        let norm_values = values_for_key(&normalized, key);
        assert_eq!(
            raw_values.len(),
            norm_values.len(),
            "normalization must not add/remove `{key}` occurrences"
        );
        for value in &norm_values {
            assert_eq!(
                value,
                &Value::String(NORMALIZED_PLACEHOLDER.to_string()),
                "`{key}` must be stripped to the placeholder"
            );
        }
    }
    // At least one order_id actually existed to strip (non-vacuous).
    assert!(!values_for_key(&raw, "order_id").is_empty());

    // STRIPPED: venue_ts becomes the canonical 0.
    assert_eq!(
        normalized[TRANSPORT_TS_KEY],
        Value::Number(NORMALIZED_TS.into())
    );

    // VERBATIM: underlying_sequence, execution_id, and the fill economics are
    // untouched.
    assert_eq!(
        normalized["underlying_sequence"],
        raw["underlying_sequence"]
    );
    let raw_exec = values_for_key(&raw, "execution_id");
    let norm_exec = values_for_key(&normalized, "execution_id");
    assert_eq!(raw_exec, norm_exec, "execution_id is compared verbatim");
    assert!(
        !norm_exec.is_empty(),
        "the crossing carries an execution_id"
    );
    for key in [
        "price",
        "quantity",
        "side",
        "liquidity",
        "fee",
        "account",
        "owner",
    ] {
        assert_eq!(
            values_for_key(&raw, key),
            values_for_key(&normalized, key),
            "`{key}` is compared verbatim and must be untouched"
        );
    }
    // VERBATIM: resting-book state (resting_quantity, stp_cancelled).
    assert_eq!(
        values_for_key(&raw, "resting_quantity"),
        values_for_key(&normalized, "resting_quantity"),
    );
}

#[test]
fn test_normalize_equal_when_only_protocol_only_fields_differ() {
    // Two events identical except for the stripped fields (order id, client id,
    // venue_ts) normalize EQUAL.
    let a = crossing_buy(7, "taker-a", 50_000, 2);
    let mut b = crossing_buy(7, "taker-b", 50_000, 2);
    // Perturb only the transport timestamp (a stripped field).
    b = VenueEvent::new(
        b.underlying_sequence,
        EventTimestamp::new(1_888_000_000_000),
        b.command.clone(),
        b.outcome.clone(),
    );
    assert_eq!(
        normalize_event(&a),
        normalize_event(&b),
        "only protocol-only fields differ, so the normalized events are equal"
    );
    // But raw they DIFFER (proving the difference was real, not absent).
    assert_ne!(
        serde_json::to_value(&a).ok(),
        serde_json::to_value(&b).ok(),
        "the raw events differ in the stripped fields"
    );
}

#[test]
fn test_normalize_unequal_when_a_verbatim_field_differs() {
    // A difference in a compared-verbatim field (a fill price) survives
    // normalization — parity would correctly FAIL.
    let a = crossing_buy(7, "taker-a", 50_000, 2);
    let b = crossing_buy(7, "taker-a", 50_100, 2); // different fill price
    assert_ne!(
        normalize_event(&a),
        normalize_event(&b),
        "a verbatim-field difference must survive normalization"
    );
}

#[test]
fn test_normalize_strips_stp_cancelled_order_id_but_keeps_owner_and_reason() {
    // The STP outcome carries resting-book state: a cancelled leg's order_id is a
    // protocol placeholder (stripped), while its owner + reason are venue identity
    // (verbatim). This covers the STP-rejection shape the v0.4 FIX arm drives
    // against an STP-configured book (see the module report: the REST DTO and the
    // default AppState book cannot express a live STP rejection at v0.1).
    let event = VenueEvent::new(
        SequenceNumber::new(9),
        EventTimestamp::new(1_700_000_000_000),
        VenueCommand::AddOrder {
            symbol: sym(),
            order_id: VenueOrderId::new("aggressor"),
            account: AccountId::new("a"),
            owner: Hash32([0x22; 32]),
            client_order_id: None,
            side: SeamSide::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(50_000)),
            quantity: 2,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::CancelMaker,
        },
        VenueOutcome::Added {
            fills: vec![],
            resting_quantity: 0,
            stp_cancelled: vec![CancelledLeg {
                order_id: VenueOrderId::new("resting-self"),
                owner: Hash32([0x22; 32]),
                symbol: sym(),
                side: SeamSide::Sell,
                reason: CancelReason::SelfTradePrevention,
            }],
        },
    );
    let normalized = normalize_event(&event);

    // The cancelled leg's order_id is stripped…
    let stp = &normalized["outcome"]["Added"]["stp_cancelled"][0];
    assert_eq!(
        stp["order_id"],
        Value::String(NORMALIZED_PLACEHOLDER.to_string())
    );
    // …but its owner + reason are verbatim.
    assert_eq!(
        stp["owner"],
        "2222222222222222222222222222222222222222222222222222222222222222"
    );
    assert_eq!(stp["reason"], "SelfTradePrevention");
}

// ============================================================================
// 7. Record / replay control parity (REST ≡ WS) + observation parity (#030)
// ============================================================================

/// A limit add command targeting the parity fixture `CALL` contract.
fn add_cmd(
    seq: u64,
    account: &str,
    owner: u8,
    side: SeamSide,
    price: u64,
    qty: u64,
) -> VenueCommand {
    let lineage = LineageId::new("fauxchange");
    VenueCommand::AddOrder {
        symbol: sym(),
        order_id: lineage.venue_order_id("BTC", SequenceNumber::new(seq), 0),
        account: AccountId::new(account),
        owner: Hash32([owner; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity: qty,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}

/// Seeds a crossing session (maker sell + crossing taker buy) onto the sequenced
/// path so the venue's journal + executions store carry one fill.
async fn seed_crossing(state: &std::sync::Arc<fauxchange::state::AppState>) {
    for command in [
        add_cmd(0, "maker", 0x11, SeamSide::Sell, 50_000, 2),
        add_cmd(1, "taker", 0x22, SeamSide::Buy, 50_000, 2),
    ] {
        state.submit(command).await.expect("submit must commit");
    }
}

#[tokio::test]
async fn test_control_parity_record_on_off_rest_and_ws_same_effect() {
    // The REST record route and the WS `record` action flip the SAME venue flag —
    // control parity by construction (both call `AppState::set_recording`).
    let state = venue(AMPLE_RATE_LIMIT);
    let admin = token(&state, "admin-1");
    assert!(state.is_recording(), "the venue records by default");

    // REST: POST /replay/record { enabled: false } flips the flag off.
    let (status, body) = send(
        &state,
        build_request(
            "POST",
            "/api/v1/replay/record",
            Some(&admin),
            Some(serde_json::json!({ "enabled": false })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["recording"], serde_json::json!(false));
    assert!(
        !state.is_recording(),
        "REST flipped the shared recording flag"
    );

    // The WS `record` action parses to the same intent…
    match parse_frame(r#"{"action":"record","enabled":true}"#) {
        FrameOutcome::Action(ClientAction::Record(p), _) => assert!(p.enabled),
        other => panic!("expected a Record action, got {other:?}"),
    }
    // …and flips the SAME `AppState::set_recording` the WS handler calls — same
    // effect on the shared flag, from either surface.
    state.set_recording(true);
    assert!(
        state.is_recording(),
        "the WS-invoked method flips the same flag"
    );
}

#[tokio::test]
async fn test_control_parity_record_permission_gate_is_identical_on_rest_and_ws() {
    // Admin-gated on both surfaces: a Trade token is forbidden on the REST record
    // route, exactly as the WS record path forbids a non-Admin caller.
    let state = venue(AMPLE_RATE_LIMIT);
    let trader = token(&state, "trader-1");
    let (status, body) = send(
        &state,
        build_request(
            "POST",
            "/api/v1/replay/record",
            Some(&trader),
            Some(serde_json::json!({ "enabled": false })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "forbidden");
    // The WS rendering of the same Forbidden(Admin) is a non-terminal envelope.
    let ws_error = VenueError::Forbidden(Permission::Admin).ws_error(None);
    assert_eq!(ws_error.code, WsErrorCode::Forbidden);
    assert!(!ws_error.terminal);
}

#[tokio::test]
async fn test_control_parity_replay_bundle_rest_and_ws_same_report() {
    // The REST replay-bundle route and the WS `replay_bundle` action run the SAME
    // offline replay (`AppState::replay_bundle`) and surface the SAME reconstructed
    // report — control parity.
    let state = venue(AMPLE_RATE_LIMIT);
    let admin = token(&state, "admin-1");
    seed_crossing(&state).await;
    let bundle = state
        .export_bundle()
        .await
        .expect("export the recorded scenario");

    // REST replays the bundle and returns the reconstructed summary.
    let (status, body) = send(
        &state,
        build_request(
            "POST",
            "/api/v1/replay/bundle",
            Some(&admin),
            Some(serde_json::to_value(&bundle).expect("serialize bundle")),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "REST replay of a valid bundle succeeds"
    );
    let rest_report: ReplayReportResponse =
        serde_json::from_value(body).expect("REST body is a ReplayReportResponse");

    // The WS-invoked path (`AppState::replay_bundle`) yields the identical report.
    let ws_report = state
        .replay_bundle(&bundle)
        .await
        .expect("offline replay succeeds")
        .to_response();
    assert_eq!(
        rest_report, ws_report,
        "REST and WS surface the identical reconstructed replay report"
    );
    assert_eq!(
        rest_report.executions, 2,
        "the crossing's two legs are reconstructed"
    );
}

#[tokio::test]
async fn test_control_parity_replay_bundle_version_mismatch_is_rejected_on_both() {
    // A bundle whose manifest pins a wrong version is a typed reject on REST (400),
    // and `AppState::replay_bundle` (the WS-invoked path) rejects it identically.
    let state = venue(AMPLE_RATE_LIMIT);
    let admin = token(&state, "admin-1");
    seed_crossing(&state).await;
    let mut bundle = state.export_bundle().await.expect("export bundle");
    // A differing MINOR at the current 0.x base is a genuine load incompatibility
    // (a benign patch bump would instead replay — see the replay unit tests).
    bundle.manifest.versions.fauxchange = "0.1.0-mismatch".to_string();

    let (status, body) = send(
        &state,
        build_request(
            "POST",
            "/api/v1/replay/bundle",
            Some(&admin),
            Some(serde_json::to_value(&bundle).expect("serialize bundle")),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a version mismatch is a typed 400"
    );
    assert_eq!(body["code"], "invalid_order");

    match state.replay_bundle(&bundle).await {
        Err(fauxchange::simulation::ReplayError::VersionMismatch { kind, .. }) => {
            assert_eq!(kind, "fauxchange");
        }
        other => panic!("the WS-invoked path must reject the same mismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn test_observation_parity_replayed_fill_matches_recorded_execution() {
    // A recorded fill and its REPLAYED reconstruction render IDENTICALLY on the
    // REST observation surface — the reconstructed `ExecutionRecord`s equal the live
    // ones the REST `/executions` read returns.
    let state = venue(AMPLE_RATE_LIMIT);
    seed_crossing(&state).await;
    let bundle = state.export_bundle().await.expect("export bundle");
    let report = state.replay_bundle(&bundle).await.expect("replay bundle");

    for account in ["maker", "taker"] {
        let account = AccountId::new(account);
        let live = state
            .executions()
            .list(&account, &ExecutionFilter::default())
            .expect("live executions list");
        let replayed = report
            .executions
            .list(&account, &ExecutionFilter::default())
            .expect("replayed executions list");
        assert!(!live.is_empty(), "the account has a recorded fill leg");
        assert_eq!(
            replayed, live,
            "the replayed fill renders identically to the recorded one on the observation surface"
        );
    }
}

// ============================================================================
// 8. Order-entry parity (REST ≡ FIX) — #041, the milestone's core acceptance
// ============================================================================
//
// The per-surface topology (03 §7): one identically-seeded fresh venue per
// surface (`cfix::rest_parity_venue` and `cfix::FixParityHarness`, both seeded
// from `cfix::parity_accounts` — same account ids, same owner hashes, same default
// lineage, same fixed clock), the SAME logical order submitted over each, then the
// journaled `VenueEvent` streams compared under the SAME normalization rule
// (`assert_streams_parity`) — protocol-only fields (FIX `MsgSeqNum`, transport
// `venue_ts`, the per-surface `order_id`/`new_order_id`, and the FIX `ClOrdID`
// echo, which normalizes as `client_order_id`) stripped; the venue identifiers
// (`underlying_sequence`, `execution_id`, fills incl. per-leg `fee`, resting-book
// state) compared verbatim.

/// Runs a `Step` scenario over an identically-seeded REST venue and an
/// identically-seeded FIX venue, returning `(rest_events, fix_events)` — the two
/// surfaces' journaled `VenueEvent` streams for the same logical orders.
async fn run_rest_fix_pair(steps: &[Step]) -> (Vec<VenueEvent>, Vec<VenueEvent>) {
    let rest = cfix::rest_parity_venue();
    drive_rest_orders(&rest, steps).await;
    let rest_events = journaled_events(&rest, "BTC").await;

    let harness = cfix::FixParityHarness::start().await;
    let fix_events = cfix::drive_fix_orders(&harness, steps).await;

    (rest_events, fix_events)
}

#[tokio::test]
async fn test_order_entry_parity_place_rest_and_fix_normalize_equal() {
    // A single resting place over one REST and one FIX venue: the normalized
    // VenueEvent streams are equal, and the compared-verbatim `underlying_sequence`
    // is already identical raw.
    let steps = [Step::Place {
        account: "trader-1",
        side: "sell",
        price: 50_000,
        qty: 5,
        tif: None,
    }];
    let (rest, fix) = run_rest_fix_pair(&steps).await;

    assert_eq!(rest.len(), 1, "one REST place is one committed event");
    assert_eq!(fix.len(), 1, "one FIX place is one committed event");
    assert_streams_parity("rest", &rest, "fix", &fix);
    assert_eq!(
        rest[0].underlying_sequence, fix[0].underlying_sequence,
        "underlying_sequence is compared verbatim and identical raw"
    );
}

#[tokio::test]
async fn test_order_entry_parity_partial_fill_and_per_leg_fees_normalize_equal() {
    // Maker trader-1 rests 5; taker trader-2 crosses 2 → a partial fill (remainder 3
    // rests). The crossing event carries two linked fill legs sharing `execution_id`,
    // each with its own per-leg `fee`. Both surfaces normalize equal, so the fills —
    // including the signed per-leg fees — agree verbatim across REST and FIX.
    let steps = [
        Step::Place {
            account: "trader-1",
            side: "sell",
            price: 50_000,
            qty: 5,
            tif: None,
        },
        Step::Place {
            account: "trader-2",
            side: "buy",
            price: 50_000,
            qty: 2,
            tif: None,
        },
    ];
    let (rest, fix) = run_rest_fix_pair(&steps).await;

    assert_eq!(rest.len(), 2, "place + crossing = two events");
    assert_eq!(fix.len(), 2);
    // The crossing carries the two linked legs (non-vacuous per-leg-fee coverage).
    let fix_fills = match &fix[1].outcome {
        VenueOutcome::Added { fills, .. } if !fills.is_empty() => fills,
        other => panic!("the FIX crossing must carry fills, got {other:?}"),
    };
    assert_eq!(fix_fills.len(), 2, "one match, two linked legs");

    assert_streams_parity("rest", &rest, "fix", &fix);
    // The fills — including the signed per-leg `fee` — are in the compared-verbatim
    // set, so streams-parity already proves REST and FIX render the identical fee for
    // each leg (whatever the venue fee schedule; the default venue's is 0). Assert the
    // per-leg fees agree across surfaces directly for clarity.
    let rest_fills = match &rest[1].outcome {
        VenueOutcome::Added { fills, .. } => fills,
        other => panic!("the REST crossing must carry fills, got {other:?}"),
    };
    let rest_fees: Vec<_> = rest_fills.iter().map(|f| f.fee).collect();
    let fix_fees: Vec<_> = fix_fills.iter().map(|f| f.fee).collect();
    assert_eq!(
        rest_fees, fix_fees,
        "the per-leg fees agree verbatim across REST and FIX"
    );
    // The compared-verbatim join keys already agree raw across the two surfaces.
    for (r, f) in rest.iter().zip(fix.iter()) {
        assert_eq!(r.underlying_sequence, f.underlying_sequence);
    }
}

#[tokio::test]
async fn test_order_entry_parity_cancel_replace_idiom_normalizes_equal() {
    // Cancel-replace as the documented cross-surface idiom — place → cancel →
    // re-place — driven identically over REST (`POST`/`DELETE`) and FIX (`D`/`F`/`D`).
    // Both journal [AddOrder, CancelOrder, AddOrder] and normalize equal. (A FIX `G`
    // maps to a single `Replace` command REST cannot express, so the stream-parity
    // comparison uses the cancel-then-replace idiom both surfaces share; `G`'s report
    // shape is covered by the conformance script.)
    let steps = [
        Step::Place {
            account: "trader-1",
            side: "sell",
            price: 50_000,
            qty: 4,
            tif: None,
        },
        Step::Cancel {
            account: "trader-1",
            target: 0,
        },
        Step::Place {
            account: "trader-1",
            side: "sell",
            price: 50_500,
            qty: 4,
            tif: None,
        },
    ];
    let (rest, fix) = run_rest_fix_pair(&steps).await;

    assert_eq!(rest.len(), 3, "place + cancel + re-place = 3 events");
    assert_eq!(fix.len(), 3);
    assert!(
        matches!(fix[1].command, VenueCommand::CancelOrder { .. }),
        "the middle FIX command is a CancelOrder"
    );
    assert_streams_parity("rest", &rest, "fix", &fix);
}

#[tokio::test]
async fn test_order_entry_parity_rejected_order_journals_nothing_on_both() {
    // A Read-permission account's order is rejected on BOTH surfaces with the
    // surface-appropriate rendering — REST `403 forbidden`, FIX `ExecutionReport (8)`
    // `Rejected` (OrdRejReason authorization) — and NEITHER journals a command
    // (identical authorization, §7 item 4). The empty journaled streams normalize
    // equal: rejection parity.
    let rest = cfix::rest_parity_venue();
    let reader = token(&rest, "reader-1");
    let (rest_status, rest_body) = send(
        &rest,
        build_request(
            "POST",
            &format!("{CONTRACT}/orders"),
            Some(&reader),
            Some(serde_json::json!({ "side": "buy", "price": 50_000, "quantity": 1 })),
        ),
    )
    .await;
    assert_eq!(
        rest_status,
        StatusCode::FORBIDDEN,
        "REST refuses a Read order"
    );
    assert_eq!(rest_body["code"], "forbidden");
    let rest_events = journaled_events(&rest, "BTC").await;
    assert!(
        rest_events.is_empty(),
        "a REST-rejected order journals no command"
    );

    let harness = cfix::FixParityHarness::start().await;
    let mut reader_fix = cfix::FixClient::logon(harness.addr(), cfix::READER).await;
    let reply = reader_fix.place_limit("rej-1", "1", 50_000, 1, "1").await;
    let rejected = match cfix::find_msg(&reply, "8") {
        Some(frame) => frame,
        None => panic!("a Read FIX order must be an ExecutionReport(8) Rejected, got {reply:?}"),
    };
    assert_eq!(
        cfix::field(rejected, "150").as_deref(),
        Some("8"),
        "ExecType Rejected"
    );
    assert_eq!(
        cfix::field(rejected, "39").as_deref(),
        Some("8"),
        "OrdStatus Rejected"
    );
    assert!(
        !cfix::any_msg_type(&reply, "3"),
        "an application-order rejection is never a session Reject(3)"
    );
    let fix_events = journaled_events(harness.state(), "BTC").await;
    assert!(
        fix_events.is_empty(),
        "a FIX-rejected order journals no command"
    );

    assert_streams_parity("rest", &rest_events, "fix", &fix_events);
}

#[tokio::test]
async fn test_order_entry_parity_same_payload_retry_is_idempotent_on_both() {
    // The shared idempotency key `(account, client_order_id)` / `(account, ClOrdID)`:
    // a byte-identical retry after an ambiguous ack returns the STORED TERMINAL RESULT
    // and opens NO second order, on BOTH surfaces. With #103 the two surfaces now
    // dedup at the SAME layer — BEFORE the sequencer — so the retry consumes no
    // `underlying_sequence` and neither surface journals a second event:
    //   * FIX dedups at the gateway (the session `ClOrdID → order_id` correlation).
    //   * REST dedups at the shared cross-protocol pre-submit index
    //     (`AppState::resolve_client_order_id` over the same `(account, ClOrdID)`
    //     index #098), so the sequential resend never reaches the actor.
    // The executor's post-sequencer `add_with_idempotency` (`VenueOutcome::Duplicate`)
    // remains the backstop for the genuinely-concurrent race; here the sequential
    // retry short-circuits earlier.
    let rest = cfix::rest_parity_venue();
    let trader = token(&rest, "trader-1");
    let body = serde_json::json!({
        "side": "sell", "price": 50_000, "quantity": 3, "client_order_id": "idem-key-1"
    });
    let mut rest_responses = Vec::new();
    for attempt in 0..2 {
        let (status, response) = send(
            &rest,
            build_request(
                "POST",
                &format!("{CONTRACT}/orders"),
                Some(&trader),
                Some(body.clone()),
            ),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "REST submit #{attempt} is accepted (the retry returns the stored result)"
        );
        rest_responses.push(response);
    }
    // The deduped retry echoes the ORIGINAL placement's FULL canonical identity —
    // both order_id AND sequence — rendered from the shared `(account, ClOrdID)` index
    // record, never a freshly-minted retry id or a fabricated sequence.
    assert_eq!(
        rest_responses[1]["order_id"], rest_responses[0]["order_id"],
        "the retry echoes the ORIGINAL order id"
    );
    assert_eq!(
        rest_responses[1]["sequence"], rest_responses[0]["sequence"],
        "the retry echoes the ORIGINAL placement sequence"
    );
    let rest_events = journaled_events(&rest, "BTC").await;

    let harness = cfix::FixParityHarness::start().await;
    let mut trader_fix = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    // Same ClOrdID, new MsgSeqNum both times — the standard retry after a dropped ack.
    let _ = trader_fix
        .place_limit("idem-key-1", "2", 50_000, 3, "1")
        .await;
    let _ = trader_fix
        .place_limit("idem-key-1", "2", 50_000, 3, "1")
        .await;
    let fix_events = journaled_events(harness.state(), "BTC").await;

    // BOTH surfaces dedup the retry BEFORE the sequencer (#103): exactly ONE journaled
    // event on each, the resend never reaching the actor and consuming no sequence.
    assert_eq!(
        rest_events.len(),
        1,
        "REST dedups the retry before the sequencer (pre-submit index)"
    );
    // FIX dedups before the sequencer (gateway ClOrdID correlation): one journaled
    // event, the resend never reaching the actor.
    assert_eq!(
        fix_events.len(),
        1,
        "FIX dedups the retry before the sequencer"
    );
    // The one opened order is IDENTICAL across surfaces, at the SAME sequence — the
    // shared pre-submit idempotency key aligns the surfaces exactly.
    assert_eq!(
        rest_events[0].outcome, fix_events[0].outcome,
        "the one opened order is identical across REST and FIX (idempotency parity)"
    );
    assert_eq!(
        rest_events[0].underlying_sequence, fix_events[0].underlying_sequence,
        "the one opened order carries the same underlying_sequence on both surfaces"
    );
    assert_streams_parity("rest", &rest_events, "fix", &fix_events);
}

#[tokio::test]
async fn test_order_entry_parity_interleaved_duplicate_retry_same_sequence_progression() {
    // The core #103 acceptance: a duplicate retry INTERLEAVED with new same-underlying
    // orders yields the SAME `underlying_sequence` progression on REST and FIX. The
    // scenario is A(k1), B(k2), retry-A(k1, byte-identical), C(k3) — all on `BTC`, all
    // by trader-1. The retry must consume NO sequence on either surface, so both
    // journal exactly [A@0, B@1, C@2] and the subsequent order C lands at the SAME
    // sequence per surface (03 §7 item 1). Before #103 the REST retry consumed seq 2
    // (a post-journal no-op) and pushed C to seq 3, diverging from FIX.
    //
    // A/B/C differ in price so none crosses (all resting sells) — the outcomes are
    // three plain resting adds, identical across surfaces under normalization.

    // --- REST arm ---------------------------------------------------------
    let rest = cfix::rest_parity_venue();
    let trader = token(&rest, "trader-1");
    let place = |cl: &str, price: u64, qty: u64| {
        serde_json::json!({
            "side": "sell", "price": price, "quantity": qty, "client_order_id": cl
        })
    };
    for body in [
        place("k1", 50_000, 3), // A
        place("k2", 50_100, 2), // B
        place("k1", 50_000, 3), // retry A (byte-identical) — dedups pre-submit
        place("k3", 50_200, 1), // C
    ] {
        let (status, response) = send(
            &rest,
            build_request(
                "POST",
                &format!("{CONTRACT}/orders"),
                Some(&trader),
                Some(body),
            ),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "every REST place (incl the deduped retry) is accepted, got {response}"
        );
    }
    let rest_events = journaled_events(&rest, "BTC").await;

    // --- FIX arm ----------------------------------------------------------
    let harness = cfix::FixParityHarness::start().await;
    let mut fix = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let _ = fix.place_limit("k1", "2", 50_000, 3, "1").await; // A
    let _ = fix.place_limit("k2", "2", 50_100, 2, "1").await; // B
    let _ = fix.place_limit("k1", "2", 50_000, 3, "1").await; // retry A — dedups pre-submit
    let _ = fix.place_limit("k3", "2", 50_200, 1, "1").await; // C
    let fix_events = journaled_events(harness.state(), "BTC").await;

    // The interleaved duplicate consumed NO sequence on either surface: three
    // journaled events, sequences [0, 1, 2], identical progression across surfaces.
    assert_eq!(
        rest_events.len(),
        3,
        "REST journals A, B, C — the retry consumed no sequence"
    );
    assert_eq!(
        fix_events.len(),
        3,
        "FIX journals A, B, C — the retry consumed no sequence"
    );
    let rest_seqs: Vec<u64> = rest_events
        .iter()
        .map(|e| e.underlying_sequence.get())
        .collect();
    let fix_seqs: Vec<u64> = fix_events
        .iter()
        .map(|e| e.underlying_sequence.get())
        .collect();
    assert_eq!(
        rest_seqs,
        vec![0, 1, 2],
        "the retry consumed no sequence; C lands at seq 2"
    );
    assert_eq!(
        rest_seqs, fix_seqs,
        "REST and FIX share the SAME underlying_sequence progression under interleaving"
    );
    assert_streams_parity("rest", &rest_events, "fix", &fix_events);
}

#[tokio::test]
async fn test_resting_retry_response_echoes_the_original_nonzero_sequence() {
    // A purely-RESTING (zero-fill) deduped retry must render the ORIGINAL placement's
    // NON-ZERO `underlying_sequence`, not a fabricated 0. The response `sequence` for a
    // resting order cannot be recovered from fills (there are none), so it comes from
    // the `(account, ClOrdID)` index record's stored placement sequence — the full
    // canonical identity, matching the post-submit `VenueOutcome::Duplicate` (#142).
    let rest = cfix::rest_parity_venue();
    let trader = token(&rest, "trader-1");
    let uri = format!("{CONTRACT}/orders");

    // A warm-up resting order (no client_order_id) at seq 0, so the keyed order A below
    // commits at a NON-ZERO sequence.
    let (status, _) = send(
        &rest,
        build_request(
            "POST",
            &uri,
            Some(&trader),
            Some(serde_json::json!({"side":"sell","price":49_000,"quantity":1})),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // A: a resting sell carrying a client_order_id — commits at seq 1.
    let body = serde_json::json!({
        "side":"sell","price":50_000,"quantity":3,"client_order_id":"resting-k"
    });
    let (status, first) = send(
        &rest,
        build_request("POST", &uri, Some(&trader), Some(body.clone())),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["status"], "accepted", "A rests, got {first}");
    assert_eq!(
        first["sequence"], 1,
        "A commits at a NON-ZERO sequence (behind the warm-up), got {first}"
    );

    // The byte-identical retry dedups pre-submit and echoes A's FULL identity —
    // order_id AND the non-zero sequence 1, never a fabricated 0.
    let (status, retry) = send(
        &rest,
        build_request("POST", &uri, Some(&trader), Some(body)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        retry["order_id"], first["order_id"],
        "the resting retry echoes the ORIGINAL order id"
    );
    assert_eq!(
        retry["sequence"], first["sequence"],
        "the resting retry echoes the ORIGINAL non-zero sequence (1), never a fabricated 0"
    );
    assert_eq!(retry["sequence"], 1);

    // The retry consumed no sequence: the journal holds exactly the warm-up + A.
    let events = journaled_events(&rest, "BTC").await;
    assert_eq!(
        events.len(),
        2,
        "the deduped resting retry journaled nothing (no sequence consumed)"
    );
}

#[tokio::test]
async fn test_concurrent_identical_retries_open_one_order_with_the_canonical_identity() {
    // The #103 race safety net (the reviewer's fix, #142): two BYTE-IDENTICAL retries
    // fired CONCURRENTLY can both miss the (post-journal-published) pre-submit index
    // and reach the actor. Exactly ONE order must open, BOTH responses must carry the
    // ORIGINAL (canonical) order id — never a freshly-minted, never-added id — and the
    // executions store must hold EXACTLY the original legs (no double fan-out). It is
    // fine if one request is pre-submit-deduped and the other is the actor's
    // `VenueOutcome::Duplicate`; both paths render the same canonical identity and the
    // Duplicate is a projection no-op, so the invariants hold under any interleaving.
    let taker_account = AccountId::new("trader-2");
    let rest = cfix::rest_parity_venue();
    let maker = token(&rest, "trader-1");
    let taker = token(&rest, "trader-2");
    let uri = format!("{CONTRACT}/orders");

    // A resting maker sell of 10 @ 50_000 — ample depth, so a DOUBLE fill (both
    // retries entering the book) would leave TWO taker legs, not one.
    let (status, _) = send(
        &rest,
        build_request(
            "POST",
            &uri,
            Some(&maker),
            Some(serde_json::json!({"side":"sell","price":50_000,"quantity":10})),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Fire two byte-identical keyed taker buys concurrently.
    let body = serde_json::json!({
        "side":"buy","price":50_000,"quantity":2,"client_order_id":"race-dup"
    });
    let (a, b) = tokio::join!(
        send(
            &rest,
            build_request("POST", &uri, Some(&taker), Some(body.clone())),
        ),
        send(
            &rest,
            build_request("POST", &uri, Some(&taker), Some(body.clone())),
        ),
    );
    let (status_a, resp_a) = a;
    let (status_b, resp_b) = b;
    assert_eq!(status_a, StatusCode::OK, "first concurrent retry: {resp_a}");
    assert_eq!(
        status_b,
        StatusCode::OK,
        "second concurrent retry: {resp_b}"
    );

    // BOTH responses carry the SAME canonical order id — the winner's, never a
    // never-added phantom id (the loser rendered `Duplicate`/pre-submit, not its own
    // freshly-minted id).
    assert_eq!(
        resp_a["order_id"], resp_b["order_id"],
        "both concurrent retries render the ONE canonical order id, never a phantom"
    );
    // Each renders the ONE order's stored terminal — filled 2 (against the 10 maker).
    assert_eq!(
        resp_a["filled_quantity"], 2,
        "resp_a renders the one order's fills"
    );
    assert_eq!(
        resp_b["filled_quantity"], 2,
        "resp_b renders the one order's fills"
    );

    // No double fan-out: the executions store holds EXACTLY the one taker leg (a
    // second entering order would have folded a second leg / a 4-lot fill).
    let taker_legs = rest
        .executions()
        .list(&taker_account, &ExecutionFilter::default())
        .expect("taker legs");
    assert_eq!(
        taker_legs.len(),
        1,
        "exactly one order opened; no double fan-out under the concurrent race"
    );
    assert_eq!(
        taker_legs[0].quantity, 2,
        "the one taker leg filled 2, not 4"
    );
}

#[tokio::test]
async fn test_conflicting_reuse_of_client_order_id_is_a_rejected_outcome_not_a_false_accept() {
    // #103 conflicting-reuse: the same `(account, ClOrdID)` reused with DIFFERENT
    // economics is NOT a fast-path hit and NOT a false accept — the pre-submit index
    // holds `symbol`/`side`/`quantity`, so a differing quantity misses the fast path
    // and submits, where the actor's full-fingerprint idempotency map rejects it as a
    // conflicting reuse. The handler renders the OBSERVED reject (200 body,
    // `status: rejected`), never a fabricated accept and never a phantom resting order.
    let rest = cfix::rest_parity_venue();
    let trader = token(&rest, "trader-1");
    let uri = format!("{CONTRACT}/orders");

    // A: a resting sell 3 @ 50_000 under key `reuse-k` — accepted, indexed.
    let (status, first) = send(
        &rest,
        build_request(
            "POST",
            &uri,
            Some(&trader),
            Some(serde_json::json!({
                "side":"sell","price":50_000,"quantity":3,"client_order_id":"reuse-k"
            })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["status"], "accepted", "A rests, got {first}");

    // Reuse `reuse-k` with a DIFFERENT quantity (5): a conflicting reuse. It misses
    // the pre-submit fast path (quantity differs from the indexed 3) and the actor
    // rejects it.
    let (status, reuse) = send(
        &rest,
        build_request(
            "POST",
            &uri,
            Some(&trader),
            Some(serde_json::json!({
                "side":"sell","price":50_000,"quantity":5,"client_order_id":"reuse-k"
            })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        reuse["status"], "rejected",
        "a conflicting reuse is the OBSERVED reject, never a false accept: {reuse}"
    );
    assert_eq!(reuse["filled_quantity"], 0);
    assert!(
        reuse["message"]
            .as_str()
            .is_some_and(|m| m.contains("client_order_id")),
        "the reject names the conflicting-reuse reason, got {reuse}"
    );

    // Exactly ONE order rests: A (Added) and the conflicting reuse (Rejected) — the
    // reuse opened no second resting order.
    let events = journaled_events(&rest, "BTC").await;
    let added = events
        .iter()
        .filter(|event| matches!(event.outcome, VenueOutcome::Added { .. }))
        .count();
    assert_eq!(
        added, 1,
        "only A rests; the conflicting reuse opened no order"
    );
}

#[tokio::test]
async fn test_idempotent_resend_after_fill_renders_stored_terminal_report_on_rest_and_fix() {
    // #099: an idempotent resend AFTER A FILL renders the STORED terminal report —
    // the ORIGINAL fills — on BOTH surfaces, and opens no second order. REST projects
    // it from the receipt's captured `VenueOutcome` (never a fresh read-back keyed on
    // the resend's fresh order id); FIX re-renders it from the shared executions store
    // keyed on the canonical order id. The resend creates NO new execution, so both
    // surfaces surface the identical ORIGINAL leg — byte-identical in its join keys
    // (execution_id, underlying_sequence, liquidity) across surfaces.
    let taker_account = AccountId::new("trader-2");

    // ---- REST arm: maker sell 2, taker buy 3 keyed "dup" (fills 2, rests 1) ----
    let rest = cfix::rest_parity_venue();
    let maker = token(&rest, "trader-1");
    let taker = token(&rest, "trader-2");
    let uri = format!("{CONTRACT}/orders");
    let (status, _) = send(
        &rest,
        build_request(
            "POST",
            &uri,
            Some(&maker),
            Some(serde_json::json!({"side":"sell","price":50_000,"quantity":2})),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, first) = send(
        &rest,
        build_request(
            "POST",
            &uri,
            Some(&taker),
            Some(serde_json::json!({"side":"buy","price":50_000,"quantity":3,"client_order_id":"dup"})),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["status"], "partial");
    assert_eq!(first["filled_quantity"], 2);
    let rest_taker_legs = rest
        .executions()
        .list(&taker_account, &ExecutionFilter::default())
        .expect("rest taker legs");
    assert_eq!(
        rest_taker_legs.len(),
        1,
        "the original fill records one taker leg"
    );

    // Resend the byte-identical keyed taker.
    let (status, resend) = send(
        &rest,
        build_request(
            "POST",
            &uri,
            Some(&taker),
            Some(serde_json::json!({"side":"buy","price":50_000,"quantity":3,"client_order_id":"dup"})),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        resend["status"], "partial",
        "REST resend renders the STORED partial terminal, not a fresh `accepted`"
    );
    assert_eq!(
        resend["filled_quantity"], 2,
        "REST resend shows the ORIGINAL filled 2, not a read-back 0 (#099)"
    );
    // The phantom-identity fix (#099): the resend echoes the ORIGINAL placement's
    // order id + terminal sequence — the id that actually entered the book — never
    // the freshly-minted retry id (which never rested) or this turn's sequence.
    assert_eq!(
        resend["order_id"], first["order_id"],
        "REST resend echoes the ORIGINAL order id, not a freshly-minted retry id (#099)"
    );
    assert_eq!(
        resend["sequence"], first["sequence"],
        "REST resend echoes the ORIGINAL terminal sequence, not the retry turn's (#099)"
    );
    assert_eq!(
        rest.executions()
            .list(&taker_account, &ExecutionFilter::default())
            .expect("rest taker legs after resend")
            .len(),
        1,
        "the REST resend opened no second execution"
    );

    // ---- FIX arm: the same scenario over D messages ----
    let harness = cfix::FixParityHarness::start().await;
    let mut maker_fix = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let mut taker_fix = cfix::FixClient::logon(harness.addr(), cfix::TRADER2).await;
    let _ = maker_fix.place_limit("maker-p", "2", 50_000, 2, "1").await;
    let first_fix = taker_fix.place_limit("dup", "1", 50_000, 3, "1").await;
    assert!(
        first_fix
            .iter()
            .any(|f| cfix::msg_type(f).as_deref() == Some("8")
                && cfix::field(f, "150").as_deref() == Some("F")),
        "the FIX taker partially fills (a Trade), got {first_fix:?}"
    );
    let fix_taker_legs = harness
        .state()
        .executions()
        .list(&taker_account, &ExecutionFilter::default())
        .expect("fix taker legs");
    assert_eq!(fix_taker_legs.len(), 1);

    // Resend the byte-identical taker (same ClOrdID, new MsgSeqNum).
    let resend_fix = taker_fix.place_limit("dup", "1", 50_000, 3, "1").await;
    let report = resend_fix
        .iter()
        .find(|f| cfix::msg_type(f).as_deref() == Some("8"))
        .unwrap_or_else(|| panic!("expected a status ExecutionReport, got {resend_fix:?}"));
    assert_eq!(
        cfix::field(report, "14").as_deref(),
        Some("2"),
        "FIX resend CumQty is the ORIGINAL 2, not a fabricated 0"
    );
    assert_ne!(
        cfix::field(report, "150").as_deref(),
        Some("0"),
        "FIX resend is not a fabricated ExecType=New"
    );
    assert_eq!(
        harness
            .state()
            .executions()
            .list(&taker_account, &ExecutionFilter::default())
            .expect("fix taker legs after resend")
            .len(),
        1,
        "the FIX resend opened no second execution"
    );

    // ---- cross-surface parity: the resend's terminal report join keys ----
    let rest_leg = &rest_taker_legs[0];
    let fix_leg = &fix_taker_legs[0];
    assert_eq!(
        rest_leg.execution_id, fix_leg.execution_id,
        "execution_id is byte-identical across REST and FIX resend"
    );
    assert_eq!(
        rest_leg.underlying_sequence, fix_leg.underlying_sequence,
        "underlying_sequence is byte-identical across REST and FIX resend"
    );
    assert_eq!(
        rest_leg.liquidity, fix_leg.liquidity,
        "liquidity is byte-identical across REST and FIX resend"
    );
    assert_eq!(rest_leg.price_cents, fix_leg.price_cents);
    assert_eq!(rest_leg.quantity, fix_leg.quantity);
    assert_eq!(rest_leg.side, fix_leg.side);
}

#[tokio::test]
async fn test_order_entry_parity_place_into_halted_rejects_on_rest_and_fix() {
    // #118: a place into a HALTED instrument is a journaled `VenueOutcome::Rejected`,
    // surfaced as the OBSERVED reject on BOTH surfaces — REST `status: rejected` (a 200
    // body, never a false `accepted`) ≡ FIX `ExecutionReport(8)` `ExecType=Rejected`
    // (`150=8` / `39=8`), never a false `New`. REST ≡ FIX order entry.
    let contract = sym(); // BTC-20240329-50000-C

    // REST arm: halt the instrument on the domain, then POST a resting limit order.
    let rest = cfix::rest_parity_venue();
    rest.submit_set_instrument_status(contract.clone(), InstrumentStatus::Halted)
        .await
        .expect("halting the instrument must be sequenced");
    let trader = token(&rest, "trader-1");
    let (rest_status, rest_body) = send(
        &rest,
        build_request(
            "POST",
            &format!("{CONTRACT}/orders"),
            Some(&trader),
            Some(serde_json::json!({ "side": "buy", "price": 50_000, "quantity": 1 })),
        ),
    )
    .await;
    assert_eq!(
        rest_status,
        StatusCode::OK,
        "an observed reject is an Ok(Receipt) rendered 200 with status=rejected, not an HTTP error"
    );
    assert_eq!(
        rest_body["status"], "rejected",
        "REST renders the OBSERVED reject, never a false accepted"
    );

    // FIX arm: an identically-seeded venue, the same instrument halted, then a `D`.
    let harness = cfix::FixParityHarness::start().await;
    harness
        .state()
        .submit_set_instrument_status(contract, InstrumentStatus::Halted)
        .await
        .expect("halting the instrument must be sequenced");
    let mut trader_fix = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let reply = trader_fix.place_limit("halt-1", "1", 50_000, 1, "1").await;
    assert!(
        !cfix::any_msg_type(&reply, "3"),
        "an observed place reject is never a session Reject(3)"
    );
    let rejected = match cfix::find_msg(&reply, "8") {
        Some(frame) => frame,
        None => panic!(
            "a place into a halted instrument must be an ExecutionReport(8) Rejected, got {reply:?}"
        ),
    };
    assert_eq!(
        cfix::field(rejected, "150").as_deref(),
        Some("8"),
        "ExecType Rejected (never a false New)"
    );
    assert_eq!(
        cfix::field(rejected, "39").as_deref(),
        Some("8"),
        "OrdStatus Rejected"
    );
}

#[tokio::test]
async fn test_fix_partial_replace_failure_is_a_cancel_reject_never_a_false_replaced() {
    // #118: a non-atomic replace whose cancel leg commits but whose replacement add is
    // rejected (`Replace { cancelled: true, add: Rejected }`) MUST NOT render a false
    // `8 Replaced`, and MUST NOT track the never-resting replacement (a phantom F/G
    // correlation). It renders an `OrderCancelReject (9)` naming the add-leg reason.
    let harness = cfix::FixParityHarness::start().await;
    let mut trader = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;

    // Rest a buy on the empty book (no cross), so the original is genuinely resting.
    let placed = trader.place_limit("pr-orig", "1", 50_000, 5, "1").await;
    let new = match cfix::find_msg(&placed, "8") {
        Some(new) => new,
        None => panic!("the initial D must yield an ExecutionReport(8), got {placed:?}"),
    };
    assert_eq!(
        cfix::field(new, "39").as_deref(),
        Some("0"),
        "OrdStatus New"
    );

    // A MARKET replace (OrdType=1, no price): the cancel leg removes the resting order,
    // then the add leg is rejected — a market order does not rest.
    let reply = trader.market_replace("pr-orig", "pr-new", "1", 5).await;
    assert!(
        cfix::find_msg(&reply, "8")
            .filter(|f| cfix::field(f, "150").as_deref() == Some("5"))
            .is_none(),
        "a partial-replace failure must NEVER emit a false ExecutionReport(8) Replaced, got {reply:?}"
    );
    let reject = match cfix::find_msg(&reply, "9") {
        Some(reject) => reject,
        None => panic!("a partial-replace failure must be an OrderCancelReject(9), got {reply:?}"),
    };
    assert_eq!(
        cfix::field(reject, "102").as_deref(),
        Some("2"),
        "CxlRejReason Broker/Exchange Option — the named add-leg reason, not the unknown-order mask (1)"
    );
    assert_eq!(
        cfix::field(reject, "39").as_deref(),
        Some("4"),
        "OrdStatus Canceled — the cancel leg committed, so the original is gone, not Rejected (8)"
    );

    // The rejected replacement was never tracked: a cancel of its ClOrdID is the uniform
    // masked reject (unknown order), proving no phantom correlation was created.
    let phantom_cancel = trader.cancel("pr-new", "pr-cxl", "1").await;
    let masked = match cfix::find_msg(&phantom_cancel, "9") {
        Some(masked) => masked,
        None => {
            panic!("cancelling the never-tracked replacement must be a 9, got {phantom_cancel:?}")
        }
    };
    assert_eq!(
        cfix::field(masked, "102").as_deref(),
        Some("1"),
        "the never-tracked replacement is an unknown order (masked reject), not a live order"
    );
}

#[tokio::test]
async fn test_order_entry_parity_uncancellable_cancel_masks_reason_on_rest_and_fix() {
    // #118: a cancel the order path refuses is the OBSERVED reject on BOTH surfaces —
    // REST `success:false` (never a false success), FIX `OrderCancelReject(9)` (never a
    // false `Canceled`) — and the FIX reject `Text(58)` is UNIFORM: it never reveals
    // not-found vs not-owner vs already-gone (a cross-account enumeration oracle).

    // REST arm: a cancel of an order id the venue never issued → observed Rejected,
    // reported `success:false` (a 200 body, never a false success).
    let rest = cfix::rest_parity_venue();
    let trader = token(&rest, "trader-1");
    let (rest_status, rest_body) = send(
        &rest,
        build_request(
            "DELETE",
            &format!("{CONTRACT}/orders/never-issued-id"),
            Some(&trader),
            None,
        ),
    )
    .await;
    assert_eq!(
        rest_status,
        StatusCode::OK,
        "an observed cancel reject is a 200 body, not an HTTP error"
    );
    assert_eq!(
        rest_body["success"], false,
        "REST reports the observed reject as success:false, never a false success"
    );

    // FIX arm: a session cancels its OWN order that has already fully filled (gone from
    // the book) — the order path rejects it, so the masked-reject branch emits a `9`.
    let harness = cfix::FixParityHarness::start().await;
    let mut maker = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let mut taker = cfix::FixClient::logon(harness.addr(), cfix::TRADER2).await;
    // trader-1 rests a sell; trader-2 fully crosses it, so trader-1's order is gone.
    let _ = maker.place_limit("mask-maker", "2", 50_000, 2, "1").await;
    let _ = taker.place_limit("mask-taker", "1", 50_000, 2, "1").await;
    // trader-1 cancels its now-filled order → observed Rejected → masked OrderCancelReject(9).
    let reply = maker.cancel("mask-maker", "mask-cxl", "2").await;
    assert!(
        !cfix::any_msg_type(&reply, "3"),
        "a cancel reject is never a session Reject(3)"
    );
    let r9 = match cfix::find_msg(&reply, "9") {
        Some(r9) => r9,
        None => panic!(
            "a cancel of an uncancellable order must be an OrderCancelReject(9), got {reply:?}"
        ),
    };
    assert_eq!(
        cfix::field(r9, "102").as_deref(),
        Some("1"),
        "CxlRejReason is the uniform Unknown order (not a per-reason code)"
    );
    // The masking contract: the Text(58) is the uniform masked reason (identical to a
    // never-placed reject), and NEVER leaks the specific journaled cause — the not-owner
    // reason ("requesting account does not own the order") or the already-gone reason
    // ("order is not resting").
    if let Some(text) = cfix::field(r9, "58") {
        let lowered = text.to_lowercase();
        assert!(
            lowered.contains("unknown order"),
            "the reject carries the uniform masked reason, got {text:?}"
        );
        assert!(
            !lowered.contains("does not own") && !lowered.contains("not resting"),
            "the reject must not reveal not-owner / already-gone, got {text:?}"
        );
    }
}

#[tokio::test]
async fn test_order_entry_parity_uncancellable_replace_masks_reason_on_fix() {
    // #132: the masking extends to the REPLACE (`G`) path, not just cancel. A replace
    // the order path refuses (here an already-gone original) is an observed
    // `VenueOutcome::Rejected` rendered as a masked `OrderCancelReject (9)` — keyed on
    // the TYPED RejectKind — with the UNIFORM `Text (58)` + `CxlRejReason (102) = 1`,
    // never revealing not-found vs not-owner vs already-gone (a cross-account oracle).
    let harness = cfix::FixParityHarness::start().await;
    let mut maker = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let mut taker = cfix::FixClient::logon(harness.addr(), cfix::TRADER2).await;
    // trader-1 rests a sell; trader-2 fully crosses it, so trader-1's order is gone.
    let _ = maker.place_limit("rmask-maker", "2", 50_000, 2, "1").await;
    let _ = taker.place_limit("rmask-taker", "1", 50_000, 2, "1").await;
    // trader-1 REPLACES its now-filled (gone) order → observed Rejected → masked `9`.
    let reply = maker
        .replace("rmask-maker", "rmask-rpl", "2", 50_100, 2)
        .await;
    assert!(
        !cfix::any_msg_type(&reply, "3"),
        "a replace reject is never a session Reject(3)"
    );
    let r9 = match cfix::find_msg(&reply, "9") {
        Some(r9) => r9,
        None => panic!(
            "a replace of an uncancellable order must be an OrderCancelReject(9), got {reply:?}"
        ),
    };
    assert_eq!(
        cfix::field(r9, "434").as_deref(),
        Some("2"),
        "CxlRejResponseTo is OrderCancelReplaceRequest (2)"
    );
    assert_eq!(
        cfix::field(r9, "102").as_deref(),
        Some("1"),
        "CxlRejReason is the uniform Unknown order (masked), never a per-reason code"
    );
    if let Some(text) = cfix::field(r9, "58") {
        let lowered = text.to_lowercase();
        assert!(
            lowered.contains("unknown order"),
            "the replace reject carries the uniform masked reason, got {text:?}"
        );
        assert!(
            !lowered.contains("does not own") && !lowered.contains("not resting"),
            "the replace reject must not reveal not-owner / already-gone, got {text:?}"
        );
    }
}

/// An `AddOrder` whose STP-configured book cancels one resting leg — the STP-outcome
/// shape, parameterised by the aggressor / resting order ids so two per-surface
/// events can be built that differ only in the stripped ids.
fn stp_event(aggressor: &str, resting: &str) -> VenueEvent {
    VenueEvent::new(
        SequenceNumber::new(9),
        EventTimestamp::new(1_700_000_000_000),
        VenueCommand::AddOrder {
            symbol: sym(),
            order_id: VenueOrderId::new(aggressor),
            account: AccountId::new("trader-1"),
            owner: Hash32([0x22; 32]),
            client_order_id: None,
            side: SeamSide::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(50_000)),
            quantity: 2,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::CancelMaker,
        },
        VenueOutcome::Added {
            fills: vec![],
            resting_quantity: 0,
            stp_cancelled: vec![CancelledLeg {
                order_id: VenueOrderId::new(resting),
                owner: Hash32([0x22; 32]),
                symbol: sym(),
                side: SeamSide::Sell,
                reason: CancelReason::SelfTradePrevention,
            }],
        },
    )
}

#[test]
fn test_order_entry_parity_stp_cancelled_outcome_normalizes_equal_across_surfaces() {
    // A LIVE STP rejection is not wire-expressible at v0.1: neither the REST place
    // DTO nor the FIX `NewOrderSingle (D)` carries an STP mode — per-account STP is
    // venue config, and the ONE shared `add_order_command` builder stamps
    // `stp_mode: None` for both surfaces (src/gateway/rest/support.rs). So an STP-mode
    // order is *identically inexpressible*. What #041 asserts is that the STP-cancelled
    // OUTCOME normalizes IDENTICALLY across surfaces: the cancelled leg's `order_id` is
    // a stripped protocol placeholder while its `owner` + `reason` are compared
    // verbatim, so two per-surface events that differ only in the stripped ids
    // normalize equal.
    let rest_like = stp_event("rest-aggressor", "rest-resting");
    let fix_like = stp_event("fix-aggressor", "fix-resting");
    assert_eq!(
        normalize_event(&rest_like),
        normalize_event(&fix_like),
        "the STP-cancelled outcome normalizes identically across surfaces"
    );
    assert_ne!(
        serde_json::to_value(&rest_like).ok(),
        serde_json::to_value(&fix_like).ok(),
        "raw, the two events differ in the stripped ids (the difference was real)"
    );
}

#[test]
fn test_ws_is_excluded_from_order_entry_parity() {
    // Order-entry parity is REST ≡ FIX only. WS has NO order-entry client message, so
    // every order-entry-shaped WS frame is rejected (non-terminal — the socket stays
    // open). WS remains an OBSERVATION surface (its `fill` renders the same committed
    // event, asserted in section 9), so its exclusion here is scope, not a gap.
    for frame in [
        r#"{"action":"place_order","side":"buy","price":50000,"quantity":1}"#,
        r#"{"action":"cancel_order","order_id":"x"}"#,
        r#"{"action":"replace_order","order_id":"x","price":50100,"quantity":2}"#,
        r#"{"side":"buy","price":50000,"quantity":10}"#,
    ] {
        match parse_frame(frame) {
            FrameOutcome::Reject(error) => assert!(
                !error.terminal,
                "an order-entry WS frame is a non-terminal reject: {frame}"
            ),
            other => panic!("WS order-entry frame {frame} must be rejected, got {other:?}"),
        }
    }
}

// ============================================================================
// 9. Observation parity (REST/WS/FIX) — one committed fill, three projections
// ============================================================================

/// Collects reply frames from `client` until a `Trade` `ExecutionReport (8)` is
/// seen or a bounded number of drains elapse (the New + Trade reports may arrive in
/// separate reads).
async fn collect_until_trade(
    client: &mut cfix::FixClient,
    mut frames: Vec<Vec<u8>>,
) -> Vec<Vec<u8>> {
    for _ in 0..5 {
        if frames
            .iter()
            .any(|f| cfix::fix_report_projection(f).is_some())
        {
            break;
        }
        frames.extend(client.drain().await);
    }
    frames
}

#[tokio::test]
async fn test_one_committed_fill_renders_identically_on_rest_ws_and_fix() {
    // ONE committed fill; assert its REST `ExecutionRecord`, WS `fill`, and FIX
    // `ExecutionReport (8)` agree on the join keys. All three are projections of the
    // SAME committed event, driven over the live FIX order path and observed on all
    // three surfaces of the one serving venue.
    let harness = cfix::FixParityHarness::start().await;
    let state = harness.state();
    let mut rx = state.subscriptions().subscribe();

    // Maker trader-1 rests a sell; taker trader-2 fully crosses it.
    let mut maker = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let _ = maker.place_limit("obs-maker", "2", 50_000, 5, "1").await;
    let mut taker = cfix::FixClient::logon(harness.addr(), cfix::TRADER2).await;
    let taker_reports = taker.place_limit("obs-taker", "1", 50_000, 5, "1").await;
    let taker_reports = collect_until_trade(&mut taker, taker_reports).await;

    // The FIX projection: the taker's Trade ExecutionReport(8).
    let fix_keys = match taker_reports
        .iter()
        .find_map(|f| cfix::fix_report_projection(f))
    {
        Some(keys) => keys,
        None => panic!("the crossing must emit a taker FIX Trade report: {taker_reports:?}"),
    };

    // The WS projection: the anonymised taker fill print.
    let messages = drain(&mut rx);
    let taker_fill = match find_taker_fill(&messages) {
        Some(fill) => fill,
        None => panic!("the crossing must emit a taker WS fill"),
    };
    let ws_keys = match ws_fill_join_keys(&taker_fill) {
        Some(keys) => keys,
        None => panic!("the taker fill must yield join keys"),
    };

    // The REST projection: the account-scoped ExecutionRecord for the taker leg.
    let taker_token = token(state, "trader-2");
    let uri = format!("/api/v1/executions/{}", ws_keys.execution_id);
    let (status, record) = send(state, build_request("GET", &uri, Some(&taker_token), None)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the taker ExecutionRecord must be readable: {record}"
    );
    let rest_keys = match execution_record_join_keys(&record) {
        Some(keys) => keys,
        None => panic!("the ExecutionRecord must yield join keys: {record}"),
    };

    // REST ≡ WS on ALL join keys (including `venue_ts`).
    assert_eq!(
        ws_keys, rest_keys,
        "one fill renders identically on REST and WS (all join keys)"
    );
    // FIX carries `execution_id`, `liquidity`, `underlying_sequence`, `venue_ts`
    // (`TransactTime 60`, #104), `side`, `quantity`, `price` — ALL FOUR fill
    // observation join keys (4-of-4 REST≡WS≡FIX observation parity).
    assert_eq!(
        fix_keys.execution_id, ws_keys.execution_id,
        "FIX ExecID(17) == the shared execution_id"
    );
    assert_eq!(
        fix_keys.liquidity, ws_keys.liquidity,
        "FIX LastLiquidityInd(851) == liquidity"
    );
    assert_eq!(
        fix_keys.venue_ts, ws_keys.venue_ts,
        "FIX TransactTime(60) == venue_ts (the 4th observation join key, #104)"
    );
    assert_eq!(
        fix_keys.underlying_sequence, ws_keys.underlying_sequence,
        "FIX SecondaryExecID(527) == underlying_sequence"
    );
    assert_eq!(fix_keys.side, ws_keys.side, "FIX Side(54) == side");
    assert_eq!(
        fix_keys.quantity, ws_keys.quantity,
        "FIX LastQty(32) == quantity"
    );
    assert_eq!(
        fix_keys.price, ws_keys.price,
        "FIX LastPx(31) == price cents"
    );

    // Sanity: the values we drove.
    assert_eq!(rest_keys.underlying_sequence, 1);
    assert_eq!(rest_keys.price, 50_000);
    assert_eq!(rest_keys.quantity, 5);
    assert_eq!(rest_keys.side, "buy");
    assert_eq!(rest_keys.liquidity, "taker");
}

#[tokio::test]
async fn test_one_committed_fill_renders_fee_consistently_on_rest_fix_and_omits_it_on_ws() {
    // #114 item 4: the SAME committed fill's fee/commission renders with consistent
    // SEMANTICS and UNITS (integer cents) on every present surface — the REST
    // `ExecutionRecord.fee_cents` and the FIX `ExecutionReport(8)` `Commission(12)` +
    // `CommType(13)=3` (absolute) carry the SAME value; the anonymised WS `fill`
    // INTENTIONALLY omits the account-private fee. Driven over the live FIX order
    // path into a fee-configured venue and observed on all three surfaces.
    let harness =
        cfix::FixParityHarness::start_with_microstructure(cfix::fee_microstructure()).await;
    let state = harness.state();
    let mut rx = state.subscriptions().subscribe();

    // Maker trader-1 rests a sell; taker trader-2 fully crosses it. taker_bps = 25 on
    // a 50_000c × 5 notional (250_000) → a 625c (= $6.25) taker fee.
    let mut maker = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let _ = maker.place_limit("fee-maker", "2", 50_000, 5, "1").await;
    let mut taker = cfix::FixClient::logon(harness.addr(), cfix::TRADER2).await;
    let taker_reports = taker.place_limit("fee-taker", "1", 50_000, 5, "1").await;
    let taker_reports = collect_until_trade(&mut taker, taker_reports).await;

    // FIX: the taker's Trade ExecutionReport(8) carries Commission(12) + CommType(13).
    let trade = match taker_reports
        .iter()
        .find(|f| cfix::fix_report_projection(f).is_some())
    {
        Some(frame) => frame,
        None => panic!("the crossing must emit a taker FIX Trade report: {taker_reports:?}"),
    };
    let fix_commission = match cfix::field(trade, "12") {
        Some(value) => value,
        None => panic!("the Trade report must carry Commission(12)"),
    };
    let fix_commission_cents =
        match fauxchange::gateway::fix::price::parse_signed_decimal_to_cents(&fix_commission) {
            Ok(cents) => cents.get(),
            Err(error) => panic!("Commission(12) must parse to signed cents: {error}"),
        };
    assert_eq!(
        cfix::field(trade, "13").as_deref(),
        Some("3"),
        "CommType(13) is Absolute (a value in cents, not bps/percent)"
    );
    let execution_id = match cfix::field(trade, "17") {
        Some(id) => id,
        None => panic!("the Trade report must carry ExecID(17)"),
    };

    // WS: the anonymised taker fill OMITS the account-private fee/commission.
    let messages = drain(&mut rx);
    let taker_fill = match find_taker_fill(&messages) {
        Some(fill) => fill,
        None => panic!("the crossing must emit a taker WS fill"),
    };
    let ws_data = match ws_fill_data(&taker_fill) {
        Some(data) => data,
        None => panic!("the taker fill must yield a data object"),
    };
    assert!(ws_data.get("fee").is_none(), "WS fill must omit fee");
    assert!(
        ws_data.get("fee_cents").is_none(),
        "WS fill must omit fee_cents"
    );
    assert!(
        ws_data.get("commission").is_none(),
        "WS fill must omit commission"
    );
    // The public `edge` must NOT leak the account-private fee: the net-of-fee edge is
    // a REST/FIX ExecutionRecord-only analytic, so the anonymised WS fill's edge is 0
    // (the public gross projection), never `-fee`.
    assert_eq!(
        ws_data.get("edge").and_then(Value::as_i64),
        Some(0),
        "WS fill edge must not leak the account-private net-of-fee edge"
    );

    // REST: the account-scoped ExecutionRecord for the taker leg carries fee_cents.
    let taker_token = token(state, "trader-2");
    let uri = format!("/api/v1/executions/{execution_id}");
    let (status, record) = send(state, build_request("GET", &uri, Some(&taker_token), None)).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the taker ExecutionRecord must be readable: {record}"
    );
    let rest_fee_cents = match record["fee_cents"].as_i64() {
        Some(fee) => fee,
        None => panic!("the REST ExecutionRecord must carry an integer fee_cents: {record}"),
    };

    // The parity contract: the fee is a non-zero integer number of cents, IDENTICAL
    // on REST and FIX, and absent on WS.
    assert_eq!(
        rest_fee_cents, 625,
        "the taker fee is 25 bps of the 250_000c notional"
    );
    assert_eq!(
        fix_commission_cents, rest_fee_cents,
        "FIX Commission(12) and REST fee_cents are the same value in the same unit (cents)"
    );
}

// ============================================================================
// 10. FIX conformance script (#041) — session admin + order + MD happy path
//     AND every context-sensitive reject row of 03 §8, with reason tags +
//     Text(58) redaction.
// ============================================================================

/// Collects reply frames from `client` until an `ExecutionReport (8)` with the given
/// `ExecType (150)` is seen or a bounded number of drains elapse.
async fn collect_reports(
    client: &mut cfix::FixClient,
    mut frames: Vec<Vec<u8>>,
    exec_type: &str,
) -> Vec<Vec<u8>> {
    for _ in 0..5 {
        if frames.iter().any(|f| {
            cfix::msg_type(f).as_deref() == Some("8")
                && cfix::field(f, "150").as_deref() == Some(exec_type)
        }) {
            break;
        }
        frames.extend(client.drain().await);
    }
    frames
}

#[tokio::test]
async fn test_fix_conformance_script_session_admin_order_and_market_data_happy_path() {
    // The coherent happy-path script on one serving venue: session admin (A / 0 / 1 /
    // 2 / 5) + order entry (D / G / F → 8) + market data (V → W). Each concurrent
    // session is a DISTINCT account, so no session hosts two connections and the
    // per-(account, comp_id) sequence store is never contended.
    let harness = cfix::FixParityHarness::start().await;
    let addr = harness.addr();

    // Session admin (A): a raw logon so the ack fields are asserted (credential-free).
    let logon =
        cfix::attempt_logon(addr, cfix::ADMIN.sender, cfix::ADMIN.user, cfix::ADMIN.pw).await;
    let ack = match cfix::find_msg(&logon, "A") {
        Some(ack) => ack,
        None => panic!("Logon(A) must be acked, got {logon:?}"),
    };
    assert_eq!(
        cfix::field(ack, "108").as_deref(),
        Some("30"),
        "HeartBtInt echoed"
    );
    assert!(
        cfix::field(ack, "553").is_none() && cfix::field(ack, "554").is_none(),
        "the Logon(A) ack carries NO credential"
    );

    // TRADER1 session: TestRequest(1) → Heartbeat(0), then D → 8 New, G → 8 Replaced,
    // F → 8 Canceled — the order-entry happy path.
    let mut trader = cfix::FixClient::logon(addr, cfix::TRADER1).await;

    let hb = trader.test_request("PING-CONF").await;
    let hb0 = match cfix::find_msg(&hb, "0") {
        Some(hb0) => hb0,
        None => panic!("TestRequest(1) must yield a Heartbeat(0), got {hb:?}"),
    };
    assert_eq!(
        cfix::field(hb0, "112").as_deref(),
        Some("PING-CONF"),
        "the Heartbeat(0) echoes the TestReqID"
    );

    let d = trader.place_limit("conf-rest", "2", 50_000, 5, "1").await;
    let new = match cfix::find_msg(&d, "8") {
        Some(new) => new,
        None => panic!("D must yield an ExecutionReport(8), got {d:?}"),
    };
    assert_eq!(
        cfix::field(new, "150").as_deref(),
        Some("0"),
        "ExecType New"
    );
    assert_eq!(
        cfix::field(new, "39").as_deref(),
        Some("0"),
        "OrdStatus New"
    );

    let g = trader
        .replace("conf-rest", "conf-repl", "2", 50_500, 5)
        .await;
    let g = collect_reports(&mut trader, g, "5").await;
    let replaced = match g.iter().find(|f| {
        cfix::msg_type(f).as_deref() == Some("8") && cfix::field(f, "150").as_deref() == Some("5")
    }) {
        Some(replaced) => replaced,
        None => panic!("G must yield an ExecutionReport(8) Replaced, got {g:?}"),
    };
    assert_eq!(
        cfix::field(replaced, "39").as_deref(),
        Some("5"),
        "OrdStatus Replaced"
    );

    let f = trader.cancel("conf-repl", "conf-cxl", "2").await;
    let canceled = match cfix::find_msg(&f, "8") {
        Some(canceled) => canceled,
        None => panic!("F must yield an ExecutionReport(8), got {f:?}"),
    };
    assert_eq!(
        cfix::field(canceled, "150").as_deref(),
        Some("4"),
        "ExecType Canceled"
    );

    // READER session: market data V (Bid+Offer) → W (the empty-book baseline still
    // carries RptSeq(83)).
    let mut reader = cfix::FixClient::logon(addr, cfix::READER).await;
    let v = reader.market_data("MDR-CONF", &["0", "1"]).await;
    let w = match cfix::find_msg(&v, "W") {
        Some(w) => w,
        None => panic!("V(Bid+Offer) must yield a W snapshot, got {v:?}"),
    };
    assert_eq!(
        cfix::field(w, "262").as_deref(),
        Some("MDR-CONF"),
        "W echoes MDReqID"
    );
    assert!(cfix::field(w, "83").is_some(), "W carries RptSeq(83)");

    // Session admin (2): a deliberate inbound MsgSeqNum gap → ResendRequest(2), on a
    // dedicated TRADER2 session (so the gap never disturbs the order session).
    let mut gapper = cfix::FixClient::logon(addr, cfix::TRADER2).await;
    let gap_reply = gapper.send_out_of_order().await;
    assert!(
        cfix::any_msg_type(&gap_reply, "2"),
        "an inbound MsgSeqNum gap yields a ResendRequest(2), got {gap_reply:?}"
    );

    // Session admin (5): a clean client Logout is acked with a Logout(5).
    let logout_reply = trader.logout().await;
    assert!(
        cfix::any_msg_type(&logout_reply, "5"),
        "a client Logout(5) is acked with a Logout(5), got {logout_reply:?}"
    );
}

#[tokio::test]
async fn test_fix_conformance_script_reject_3_malformed_frame() {
    // (Reject 3) A malformed application frame (a `NewOrderSingle (D)` missing the
    // required `Side (54)`) is a SESSION-level Reject(3) with SessionRejectReason(373)
    // and RefTagID(371) pointing at the missing tag — never an order-level 8/9. On a
    // fresh venue so the session-level reject never entangles another row's sequence.
    let harness = cfix::FixParityHarness::start().await;
    let mut client = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let reply = client.order_missing_side("conf-bad").await;
    let r3 = match cfix::find_msg(&reply, "3") {
        Some(r3) => r3,
        None => panic!("a D missing Side(54) must be a session Reject(3), got {reply:?}"),
    };
    assert!(
        cfix::field(r3, "373").is_some(),
        "Reject(3) carries a SessionRejectReason(373)"
    );
    assert_eq!(
        cfix::field(r3, "371").as_deref(),
        Some("54"),
        "RefTagID(371) points at the missing Side(54)"
    );
}

#[tokio::test]
async fn test_fix_conformance_script_reject_8_conflicting_clordid_reuse() {
    // (8 Rejected) A conflicting `ClOrdID` reuse (same key, different economics) is an
    // ExecutionReport(8) Rejected with OrdRejReason(103)=6 (Duplicate Order) — the
    // order-level idempotency-conflict reject, never a session Reject(3).
    let harness = cfix::FixParityHarness::start().await;
    let mut client = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let _ = client.place_limit("conf-reuse", "2", 40_000, 3, "1").await;
    let conflict = client.place_limit("conf-reuse", "2", 40_000, 7, "1").await;
    assert!(
        !cfix::any_msg_type(&conflict, "3"),
        "an idempotency conflict is never a session Reject(3)"
    );
    let rejected = match conflict.iter().find(|f| {
        cfix::msg_type(f).as_deref() == Some("8") && cfix::field(f, "150").as_deref() == Some("8")
    }) {
        Some(rejected) => rejected,
        None => panic!("a conflicting ClOrdID reuse must be an 8 Rejected, got {conflict:?}"),
    };
    assert_eq!(
        cfix::field(rejected, "103").as_deref(),
        Some("6"),
        "OrdRejReason Duplicate Order"
    );
}

#[tokio::test]
async fn test_fix_conformance_script_reject_9_cancel_unknown_order() {
    // (9) A cancel of an order the session never placed is an OrderCancelReject(9) with
    // CxlRejReason(102)=1 (Unknown order), CxlRejResponseTo(434)=1, OrigClOrdID echoed.
    let harness = cfix::FixParityHarness::start().await;
    let mut client = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let reply = client.cancel("never-placed", "conf-cxl-unknown", "1").await;
    assert!(
        !cfix::any_msg_type(&reply, "3"),
        "a cancel failure is never a session Reject(3)"
    );
    let r9 = match cfix::find_msg(&reply, "9") {
        Some(r9) => r9,
        None => {
            panic!("a cancel of an unknown order must be an OrderCancelReject(9), got {reply:?}")
        }
    };
    assert_eq!(
        cfix::field(r9, "102").as_deref(),
        Some("1"),
        "CxlRejReason Unknown order"
    );
    assert_eq!(
        cfix::field(r9, "434").as_deref(),
        Some("1"),
        "CxlRejResponseTo Order Cancel Request"
    );
    assert_eq!(
        cfix::field(r9, "41").as_deref(),
        Some("never-placed"),
        "the OrigClOrdID is echoed"
    );
}

#[tokio::test]
async fn test_fix_conformance_script_reject_y_unsupported_market_data() {
    // (Y) A trade-only `V` (no book side) is a MarketDataRequestReject(Y) with
    // MDReqRejReason(281)=8 (Unsupported MDEntryType), never a bare session Reject(3),
    // and its Text(58) leaks no internal state.
    let harness = cfix::FixParityHarness::start().await;
    let mut client = cfix::FixClient::logon(harness.addr(), cfix::READER).await;
    let reply = client.market_data("MDR-TRADE", &["2"]).await;
    assert!(
        !cfix::any_msg_type(&reply, "3"),
        "a MD reject is never a bare Reject(3)"
    );
    let y = match cfix::find_msg(&reply, "Y") {
        Some(y) => y,
        None => panic!("a trade-only V must be a MarketDataRequestReject(Y), got {reply:?}"),
    };
    assert_eq!(
        cfix::field(y, "281").as_deref(),
        Some("8"),
        "MDReqRejReason Unsupported MDEntryType"
    );
    if let Some(text) = cfix::field(y, "58") {
        // A safe, human-readable reason — never a panic string, an internal source
        // path, or an unbounded dump of internal state.
        assert!(
            !text.contains("panic") && !text.contains("src/") && text.len() < 200,
            "the Text(58) must be a safe, redacted reason, got {text:?}"
        );
    }
}

#[tokio::test]
async fn test_fix_conformance_script_reject_j_unsupported_application_message() {
    // (j) A well-formed application MsgType the venue has no handler for (R,
    // QuoteRequest) is a BusinessMessageReject(j) with BusinessRejectReason(380)=3 and
    // RefMsgType(372)=R, never a bare session Reject(3).
    let harness = cfix::FixParityHarness::start().await;
    let mut client = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let reply = client.unsupported().await;
    let j = match cfix::find_msg(&reply, "j") {
        Some(j) => j,
        None => {
            panic!("an unsupported app MsgType must be a BusinessMessageReject(j), got {reply:?}")
        }
    };
    assert_eq!(
        cfix::field(j, "380").as_deref(),
        Some("3"),
        "BusinessRejectReason"
    );
    assert_eq!(
        cfix::field(j, "372").as_deref(),
        Some("R"),
        "RefMsgType echoed"
    );
}

#[tokio::test]
async fn test_fix_conformance_script_logout_5_on_credential_failure_redacts_text() {
    // (Logout 5) A logon-credential failure is refused with a Logout(5); the presented
    // credential NEVER appears anywhere in the reply — Text(58) and every field are
    // redacted. A ghost identity (unknown username / unbound CompID) so the row is
    // fully isolated.
    const BAD_USER: &str = "ghost-nonexistent-user";
    const BAD_PW: &str = "totally-wrong-secret-DoNotLog";
    let harness = cfix::FixParityHarness::start().await;
    let reply = cfix::attempt_logon(harness.addr(), "GHOSTCLIENT", BAD_USER, BAD_PW).await;
    assert!(
        cfix::any_msg_type(&reply, "5"),
        "a bad-credential logon is refused with a Logout(5), got {reply:?}"
    );
    for frame in &reply {
        let text = String::from_utf8_lossy(frame);
        assert!(
            !text.contains(BAD_PW),
            "the presented password must never appear in a reply frame"
        );
    }
}

/// Every `ExecutionReport (8)` `Canceled (150=4)` in `frames`, in wire order.
fn canceled_reports(frames: &[Vec<u8>]) -> Vec<&Vec<u8>> {
    frames
        .iter()
        .filter(|f| {
            cfix::msg_type(f).as_deref() == Some("8")
                && cfix::field(f, "150").as_deref() == Some("4")
        })
        .collect()
}

#[tokio::test]
async fn test_fix_mass_cancel_q_accepts_and_reports_one_8_per_swept_order() {
    // (#97) A committed `OrderMassCancelRequest (q)` renders `OrderMassCancelReport (r)`
    // ACCEPTED (not the old honest Rejected) plus one `ExecutionReport (8) Canceled`
    // per swept resting order (03 §5.3). trader-1 rests three GTC buys, then mass-cancels.
    let harness = cfix::FixParityHarness::start().await;
    let mut client = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    for i in 0..3 {
        let reply = client
            .place_limit(&format!("mc-a-{i}"), "1", 40_000 + i, 1, "1")
            .await;
        assert!(
            cfix::any_msg_type(&reply, "8"),
            "each resting buy is acked with an ExecutionReport(8), got {reply:?}"
        );
    }

    let reply = client.mass_cancel("mc-a-q").await;
    assert!(
        !cfix::any_msg_type(&reply, "3"),
        "a mass cancel is never a session Reject(3), got {reply:?}"
    );
    let r = match cfix::find_msg(&reply, "r") {
        Some(r) => r,
        None => panic!("a committed q must render an OrderMassCancelReport(r), got {reply:?}"),
    };
    assert_eq!(
        cfix::field(r, "531").as_deref(),
        Some("7"),
        "MassCancelResponse is ACCEPTED (All=7), never the honest Rejected(0)"
    );
    assert_eq!(
        cfix::field(r, "533").as_deref(),
        Some("3"),
        "TotalAffectedOrders is the true swept count"
    );

    let canceled = canceled_reports(&reply);
    assert_eq!(
        canceled.len(),
        3,
        "exactly one ExecutionReport(8) Canceled per swept order, got {reply:?}"
    );
    for report in &canceled {
        assert_eq!(
            cfix::field(report, "55").as_deref(),
            Some("BTC-20240329-50000-C"),
            "each per-order 8 carries the swept order's Symbol"
        );
        assert_eq!(
            cfix::field(report, "39").as_deref(),
            Some("4"),
            "OrdStatus is Canceled"
        );
        assert!(
            cfix::field(report, "37").is_some(),
            "each per-order 8 carries the venue OrderID(37)"
        );
    }

    // A second mass cancel has nothing left to sweep: r accepted, zero affected, no 8s.
    let reply = client.mass_cancel("mc-a-q2").await;
    let r = cfix::find_msg(&reply, "r").expect("a repeat q still renders an r");
    assert_eq!(
        cfix::field(r, "533").as_deref(),
        Some("0"),
        "a repeat mass cancel reports zero affected — the orders are already gone"
    );
    assert_eq!(
        canceled_reports(&reply).len(),
        0,
        "a repeat mass cancel emits no per-order 8"
    );
}

#[tokio::test]
async fn test_fix_mass_cancel_q_is_owner_scoped_across_accounts() {
    // (#97) Owner scoping: trader-2's `q` sweeps ONLY trader-2's orders; trader-1's
    // resting orders are untouched and are still cancellable by trader-1's own `q`.
    let harness = cfix::FixParityHarness::start().await;
    let mut trader1 = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let mut trader2 = cfix::FixClient::logon(harness.addr(), cfix::TRADER2).await;

    // trader-1 rests two, trader-2 rests three — all resting GTC buys on one book.
    let mut trader1_ids = Vec::new();
    for i in 0..2 {
        let reply = trader1
            .place_limit(&format!("iso-1-{i}"), "1", 41_000 + i, 1, "1")
            .await;
        let new = cfix::find_msg(&reply, "8").expect("trader-1 buy is acked with an 8");
        trader1_ids.push(cfix::field(new, "37").expect("the 8 carries an OrderID(37)"));
    }
    for i in 0..3 {
        let _ = trader2
            .place_limit(&format!("iso-2-{i}"), "1", 42_000 + i, 1, "1")
            .await;
    }

    // trader-2 mass-cancels: exactly its own three, none of trader-1's.
    let reply = trader2.mass_cancel("iso-2-q").await;
    let r = cfix::find_msg(&reply, "r").expect("trader-2's q renders an r");
    assert_eq!(
        cfix::field(r, "533").as_deref(),
        Some("3"),
        "trader-2 sweeps exactly its own three orders"
    );
    assert_eq!(canceled_reports(&reply).len(), 3);
    // The swept ids must NOT disclose or include any of trader-1's orders.
    let swept_wire: String = reply
        .iter()
        .map(|f| String::from_utf8_lossy(f).into_owned())
        .collect();
    for id in &trader1_ids {
        assert!(
            !swept_wire.contains(id.as_str()),
            "trader-2's mass cancel must not touch or disclose trader-1's order {id}"
        );
    }

    // trader-1's two orders survive: its OWN q still finds exactly two.
    let reply = trader1.mass_cancel("iso-1-q").await;
    let r = cfix::find_msg(&reply, "r").expect("trader-1's q renders an r");
    assert_eq!(
        cfix::field(r, "533").as_deref(),
        Some("2"),
        "trader-1's orders were never touched by trader-2's cancel-all"
    );
    assert_eq!(canceled_reports(&reply).len(), 2);
}

#[tokio::test]
async fn test_fix_mass_cancel_q_reports_8_for_an_order_not_placed_this_session() {
    // (#97 finding 3) A resting order the current FIX session did NOT place — here a
    // placement submitted straight onto the sequenced path (exactly like a REST
    // client or a prior FIX session) — is swept by a FIX `q` and MUST still receive
    // its own `ExecutionReport(8) Canceled`. The swept leg now carries the resting
    // order's own Symbol/Side (journaled in the outcome), so the render no longer
    // depends on this session's placement tracking, and the `r` 533 count equals the
    // number of `8`s.
    let harness = cfix::FixParityHarness::start().await;

    // A REST-equivalent placement for trader-1, NOT tracked by any FIX session.
    let symbol = match Symbol::parse(CALL) {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol parses: {e:?}"),
    };
    let placed_id = VenueOrderId::new("rest-side-mc-1");
    let receipt = harness
        .state()
        .submit(VenueCommand::AddOrder {
            symbol,
            order_id: placed_id.clone(),
            account: AccountId::new(cfix::TRADER1.account),
            owner: Hash32([cfix::TRADER1.owner_byte; 32]),
            client_order_id: None,
            side: SeamSide::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(39_000)),
            quantity: 1,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        })
        .await;
    assert!(
        receipt.is_ok(),
        "the sequenced (REST-equivalent) placement must commit: {receipt:?}"
    );

    // trader-1 logs on fresh and mass-cancels: the sweep hits the order this session
    // never tracked in `placed`.
    let mut client = cfix::FixClient::logon(harness.addr(), cfix::TRADER1).await;
    let reply = client.mass_cancel("mc-untracked-q").await;

    let r = match cfix::find_msg(&reply, "r") {
        Some(r) => r,
        None => panic!("a committed q must render an r, got {reply:?}"),
    };
    assert_eq!(
        cfix::field(r, "533").as_deref(),
        Some("1"),
        "TotalAffectedOrders counts the untracked order"
    );
    let canceled = canceled_reports(&reply);
    assert_eq!(
        canceled.len(),
        1,
        "the untracked order still gets its own 8 — count matches r's 533, got {reply:?}"
    );
    let report = canceled[0];
    assert_eq!(
        cfix::field(report, "55").as_deref(),
        Some(CALL),
        "the 8 carries the swept order's Symbol from the journaled leg, not session tracking"
    );
    assert_eq!(
        cfix::field(report, "54").as_deref(),
        Some("1"),
        "the 8 carries the swept order's Side (Buy) from the journaled leg"
    );
    assert_eq!(
        cfix::field(report, "37").as_deref(),
        Some(placed_id.as_str()),
        "the 8 references the REST-placed venue OrderID(37)"
    );
}
