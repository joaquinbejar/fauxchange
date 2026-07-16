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
//!    honest not-routable outcome (not a fabricated success — the command is not
//!    yet routable, #015).
//! 5. **REST order-entry base** — place / partial-fill / cancel-replace over the
//!    live REST surface against identically-seeded fresh venues, compared under
//!    the documented normalization rule; the base the v0.4 FIX arm extends.
//! 6. **Normalization-rule unit tests** — which fields are stripped vs compared
//!    verbatim.

mod conformance;

use axum::http::StatusCode;
use serde_json::Value;

use conformance::{
    AMPLE_RATE_LIMIT, CALL, CONTRACT, NORMALIZED_PLACEHOLDER, NORMALIZED_TS, STRIPPED_KEYS, Step,
    TRANSPORT_TS_KEY, add_order, assert_streams_parity, build_request, drain, drive_rest_orders,
    execution_record_join_keys, journaled_events, normalize_event, normalize_stream, send, token,
    values_for_key, venue, ws_fill_data, ws_fill_join_keys,
};

use fauxchange::exchange::{
    CancelReason, CancelledLeg, Cents, EventTimestamp, Fill as SeamFill, Hash32, LineageId,
    STPMode, SequenceNumber, Side as SeamSide, SignedCents, Symbol, TimeInForce, VenueCommand,
    VenueEvent, VenueOutcome,
};
use fauxchange::gateway::ws::{ClientAction, FrameOutcome, parse_frame};
use fauxchange::models::{AccountId, LiquidityFlag, OrderType, VenueOrderId, WsMessage};
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
// 4. Control parity (REST ≡ WS) — same command, same honest not-routable outcome
// ============================================================================

#[tokio::test]
async fn test_control_parity_rest_and_ws_build_same_command_and_surface_same_outcome() {
    // The REST kill-switch and the WS `kill` action both build the IDENTICAL
    // `MarketMakerControl { enabled: Some(false) }` command. It is not yet
    // routable on the per-underlying submit path (#015), so BOTH surfaces surface
    // the SAME honest not-routable error — parity of behaviour, not a fabricated
    // success.
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
        StatusCode::BAD_REQUEST,
        "REST kill-switch surfaces the honest not-routable error, not a fabricated 200"
    );
    assert_eq!(rest_body["code"], "invalid_order");

    // The command both surfaces construct (WS `control()` builds the same value).
    let command = VenueCommand::MarketMakerControl {
        spread_multiplier: None,
        size_scalar: None,
        directional_skew: None,
        enabled: Some(false),
    };
    // Submitting it — the exact command the WS `kill` action routes — yields the
    // SAME typed not-routable error.
    match state.submit(command).await {
        Err(VenueError::InvalidOrder(_)) => {}
        other => panic!("MarketMakerControl must be not-routable (InvalidOrder), got {other:?}"),
    }
    // …and its WS rendering is the non-terminal InvalidOrder envelope the WS
    // control path returns (its close-vs-continue behaviour is unit-tested in
    // `src/gateway/ws/mod.rs`).
    let ws_error = VenueError::InvalidOrder("x".to_string()).ws_error(None);
    assert_eq!(ws_error.code, WsErrorCode::InvalidOrder);
    assert!(
        !ws_error.terminal,
        "a control command error keeps the socket open"
    );
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
