//! Golden wire-format tests for the REST DTOs and `WsMessage` variants
//! ([TESTING.md §4](../docs/TESTING.md)).
//!
//! The wire shape — casing, the `WsMessage` `type` discriminant, and **money as
//! bare integer cents** — is part of the contract, so representative DTOs and
//! every `WsMessage` variant are pinned against a committed golden. A change to
//! any shape must update its golden in the **same commit** as the code change
//! ([docs/01 §10](../docs/01-domain-model.md), [docs/03 §4](../docs/03-protocol-surfaces.md),
//! [SEMVER.md](../docs/SEMVER.md)).
//!
//! Fixtures are compared as parsed JSON values so key order and whitespace do
//! not make the assertion brittle. Run `UPDATE_GOLDEN=1 cargo test --test
//! golden` to (re)generate the fixtures after an intentional shape change, then
//! review the diff.

use fauxchange::exchange::FanOut;
use fauxchange::exchange::{
    AddOutcome, CancelReason, CancelledLeg, Cents, EventTimestamp, ExecutionsStore,
    Fill as VenueFill, Hash32, InMemoryExecutionsStore, InMemoryPositionsStore, JournalRecord,
    LineageId, MarkPriceBook, PositionsStore, RejectKind, SequenceNumber, Side as SeamSide,
    SignedCents, SnapshotRestored, StoreFanOut, Symbol, TimeInForce as SeamTif, VenueCommand,
    VenueEvent, VenueOutcome,
};
use fauxchange::{
    AccountId, BookSide, BulkOrderResponse, BulkOrderResultItem, BulkOrderStatus, ClientOrderId,
    CreateSnapshotResponse, ExecutionId, ExecutionRecord, ExecutionSummary, ExecutionsListResponse,
    FillPrint, InstrumentLifecycle, InstrumentView, LimitOrderStatus, LiquidityFlag,
    MarketOrderStatus, OhlcBar, OptionStyle, Order, OrderStatus, OrderType, Permission,
    PlaceLimitOrderRequest, PlaceLimitOrderResponse, PlaceMarketOrderResponse, Position,
    PriceLevelChange, PriceLevelData, QuoteResponse, ReplayReportResponse, RestoreSnapshotResponse,
    Side, SnapshotSummary, SnapshotsListResponse, SubscriptionChannel, SubscriptionResult,
    SystemControlResponse, TimeInForce, TokenResponse, UnderlyingReplaySummary, VenueError,
    VenueOrderId, WsMessage,
};
use serde::Serialize;
use std::sync::Arc;

/// Parses a canonical symbol for a fixture, panicking (never `unwrap`) with a
/// clear message on an unexpected parse failure.
fn sym(raw: &str) -> Symbol {
    match Symbol::parse(raw) {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
    }
}

/// Loads and parses a golden fixture under `tests/golden/`.
fn load_golden(relative: &str) -> serde_json::Value {
    let path = format!("{}/tests/golden/{}", env!("CARGO_MANIFEST_DIR"), relative);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) => panic!("failed to read golden {path}: {e}"),
    };
    match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(e) => panic!("failed to parse golden {path}: {e}"),
    }
}

/// Asserts `value` serialises to the committed golden at `relative`, or, under
/// `UPDATE_GOLDEN`, (re)writes the golden.
fn assert_golden<T: Serialize>(relative: &str, value: &T) {
    let produced = match serde_json::to_value(value) {
        Ok(v) => v,
        Err(e) => panic!("failed to serialise {relative}: {e}"),
    };
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        let path = format!("{}/tests/golden/{}", env!("CARGO_MANIFEST_DIR"), relative);
        let mut pretty = match serde_json::to_string_pretty(&produced) {
            Ok(s) => s,
            Err(e) => panic!("failed to pretty-print {relative}: {e}"),
        };
        pretty.push('\n');
        if let Err(e) = std::fs::write(&path, pretty) {
            panic!("failed to write golden {path}: {e}");
        }
        return;
    }
    assert_eq!(
        produced,
        load_golden(relative),
        "golden mismatch for {relative}"
    );
}

/// Asserts every money field in a golden JSON tree is an integer (never a
/// float), the core money-on-the-wire contract.
fn assert_no_float_money(value: &serde_json::Value, money_keys: &[&str]) {
    if let serde_json::Value::Object(map) = value {
        for key in money_keys {
            if let Some(field) = map.get(*key)
                && !field.is_null()
            {
                assert!(
                    field.is_i64() || field.is_u64(),
                    "money field `{key}` must be an integer, got {field}"
                );
            }
        }
    }
}

// ============================================================================
// Error envelopes (from #003 — reused verbatim)
// ============================================================================

#[test]
fn test_golden_rest_error_envelope_matches_forbidden_shape() {
    let envelope = VenueError::Forbidden(Permission::Trade).error_envelope();
    assert_golden("rest/error_envelope.json", &envelope);
}

#[test]
fn test_golden_ws_error_envelope_matches_forbidden_shape() {
    let envelope = VenueError::Forbidden(Permission::Trade).ws_error(Some("req-1".to_string()));
    assert_golden("ws/error.json", &envelope);
}

// ============================================================================
// REST DTO goldens
// ============================================================================

#[test]
fn test_golden_rest_order() {
    let order = Order {
        id: VenueOrderId::new("lin:BTC:7:0"),
        client_order_id: Some(ClientOrderId::new("client-42")),
        account: AccountId::new("acct-1"),
        symbol: sym("BTC-20240329-50000-C"),
        side: Side::Buy,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(50_000)),
        quantity: 10,
        filled_quantity: 4,
        remaining_quantity: 6,
        time_in_force: TimeInForce::Gtc,
        status: OrderStatus::Partial,
        submitted_at: EventTimestamp::new(1_700_000_000_000),
        sequence: SequenceNumber::new(7),
    };
    assert_golden("rest/order.json", &order);
    assert_no_float_money(&load_golden("rest/order.json"), &["limit_price"]);
}

#[test]
fn test_golden_rest_place_limit_order_request() {
    let req = PlaceLimitOrderRequest {
        side: Side::Buy,
        price: Cents::new(50_000),
        quantity: 10,
        time_in_force: Some(TimeInForce::Gtc),
        gtd_expires_at: None,
        client_order_id: Some(ClientOrderId::new("client-42")),
    };
    assert_golden("rest/place_limit_order_request.json", &req);
    assert_no_float_money(
        &load_golden("rest/place_limit_order_request.json"),
        &["price"],
    );
}

#[test]
fn test_golden_rest_place_limit_order_response() {
    let resp = PlaceLimitOrderResponse {
        order_id: VenueOrderId::new("lin:BTC:7:0"),
        status: LimitOrderStatus::Accepted,
        filled_quantity: 0,
        remaining_quantity: 10,
        sequence: SequenceNumber::new(7),
        message: "order accepted".to_string(),
    };
    assert_golden("rest/place_limit_order_response.json", &resp);
}

#[test]
fn test_golden_rest_place_market_order_response() {
    let resp = PlaceMarketOrderResponse {
        order_id: VenueOrderId::new("lin:BTC:8:0"),
        status: MarketOrderStatus::Filled,
        filled_quantity: 10,
        remaining_quantity: 0,
        // Volume-weighted average of the two fills, in integer cents:
        // (50_000·6 + 50_025·4) / 10 = 500_100 / 10 = 50_010 (exact; the
        // truncate-toward-zero rounding rule leaves it unchanged here).
        average_price: Some(Cents::new(50_010)),
        sequence: SequenceNumber::new(8),
        fills: vec![
            FillPrint {
                price: Cents::new(50_000),
                quantity: 6,
            },
            FillPrint {
                price: Cents::new(50_025),
                quantity: 4,
            },
        ],
    };
    assert_golden("rest/place_market_order_response.json", &resp);
    assert_no_float_money(
        &load_golden("rest/place_market_order_response.json"),
        &["average_price"],
    );
}

#[test]
fn test_golden_rest_execution_record() {
    let record = ExecutionRecord {
        execution_id: ExecutionId::new("lin:BTC:7:0"),
        order_id: VenueOrderId::new("lin:BTC:7:0"),
        account: AccountId::new("acct-1"),
        symbol: "BTC".to_string(),
        instrument: sym("BTC-20240329-50000-C"),
        side: Side::Buy,
        liquidity: LiquidityFlag::Taker,
        quantity: 2,
        price_cents: Cents::new(50_000),
        fee_cents: SignedCents::new(15),
        theo_value_cents: Cents::new(49_950),
        edge_cents: SignedCents::new(-50),
        underlying_sequence: SequenceNumber::new(7),
        latency_us: 250,
        executed_at: EventTimestamp::new(1_700_000_000_000),
    };
    assert_golden("rest/execution_record.json", &record);
    assert_no_float_money(
        &load_golden("rest/execution_record.json"),
        &["price_cents", "fee_cents", "theo_value_cents", "edge_cents"],
    );
}

#[test]
fn test_golden_rest_executions_list() {
    let list = ExecutionsListResponse {
        executions: vec![ExecutionRecord {
            execution_id: ExecutionId::new("lin:BTC:7:0"),
            order_id: VenueOrderId::new("lin:BTC:7:0"),
            account: AccountId::new("acct-1"),
            symbol: "BTC".to_string(),
            instrument: sym("BTC-20240329-50000-C"),
            side: Side::Buy,
            liquidity: LiquidityFlag::Maker,
            quantity: 2,
            price_cents: Cents::new(50_000),
            fee_cents: SignedCents::new(-10),
            theo_value_cents: Cents::new(49_950),
            edge_cents: SignedCents::new(50),
            underlying_sequence: SequenceNumber::new(7),
            latency_us: 250,
            executed_at: EventTimestamp::new(1_700_000_000_000),
        }],
        summary: ExecutionSummary {
            total_executions: 1,
            total_volume: 2,
            total_edge: SignedCents::new(50),
            maker_ratio: 1.0,
        },
    };
    assert_golden("rest/executions_list.json", &list);
}

/// Builds one crossing match's `venue.v1` event with two linked fill legs
/// (maker rebate, taker fee) — the fan-out input the executions/positions store
/// goldens are projected from.
fn store_match_event() -> VenueEvent {
    let lineage = LineageId::new("run-1");
    let seq = SequenceNumber::new(7);
    let execution_id = lineage.execution_id("BTC", seq, 0);
    let taker_id = lineage.venue_order_id("BTC", seq, 0);
    let maker_id = lineage.venue_order_id("BTC", SequenceNumber::new(1), 0);

    let command = VenueCommand::AddOrder {
        symbol: sym("BTC-20240329-50000-C"),
        order_id: taker_id.clone(),
        account: AccountId::new("taker-acct"),
        owner: Hash32([0x22; 32]),
        client_order_id: None,
        side: SeamSide::Buy,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(50_000)),
        quantity: 2,
        time_in_force: SeamTif::Gtc,
        stp_mode: fauxchange::exchange::STPMode::None,
    };
    let outcome = VenueOutcome::Added {
        fills: vec![
            VenueFill {
                execution_id: execution_id.clone(),
                order_id: maker_id,
                account: AccountId::new("maker-acct"),
                owner: Hash32([0x11; 32]),
                side: SeamSide::Sell,
                liquidity: LiquidityFlag::Maker,
                price: Cents::new(50_000),
                quantity: 2,
                fee: SignedCents::new(-10),
            },
            VenueFill {
                execution_id,
                order_id: taker_id,
                account: AccountId::new("taker-acct"),
                owner: Hash32([0x22; 32]),
                side: SeamSide::Buy,
                liquidity: LiquidityFlag::Taker,
                price: Cents::new(50_000),
                quantity: 2,
                fee: SignedCents::new(15),
            },
        ],
        resting_quantity: 0,
        stp_cancelled: vec![],
    };
    VenueEvent::new(
        SequenceNumber::new(7),
        EventTimestamp::new(1_700_000_000_000),
        command,
        outcome,
    )
}

#[test]
fn test_golden_rest_execution_report() {
    // The executions store's projection of both legs of one match — the
    // authoritative fill log's wire shape: two `ExecutionRecord`s sharing one
    // `execution_id`, each with its own account, side, liquidity, and fee (a maker
    // rebate is negative), cents as bare integers. No pricer / latency is wired in
    // #008, so `theo_value_cents` defaults to the fill price (edge 0) and
    // `latency_us` is 0.
    let executions = Arc::new(InMemoryExecutionsStore::new());
    let mut fan = StoreFanOut::new(
        Arc::clone(&executions),
        Arc::new(InMemoryPositionsStore::new()),
        Arc::new(MarkPriceBook::new()),
    );
    let _ = fan.emit(&store_match_event());

    let execution_id = ExecutionId::new("run-1:BTC:7:0");
    let maker = executions
        .get(&execution_id, &AccountId::new("maker-acct"))
        .expect("get maker leg")
        .expect("a recorded maker leg");
    let taker = executions
        .get(&execution_id, &AccountId::new("taker-acct"))
        .expect("get taker leg")
        .expect("a recorded taker leg");
    let report = vec![maker, taker];

    assert_golden("rest/execution_report.json", &report);
    let golden = load_golden("rest/execution_report.json");
    if let Some(legs) = golden.as_array() {
        assert_eq!(legs.len(), 2, "both legs of the match are recorded");
        // The two legs share one execution id but differ in account and fee.
        assert_eq!(legs[0]["execution_id"], legs[1]["execution_id"]);
        assert_ne!(legs[0]["account"], legs[1]["account"]);
        for leg in legs {
            assert_no_float_money(
                leg,
                &["price_cents", "fee_cents", "theo_value_cents", "edge_cents"],
            );
        }
    }
}

#[test]
fn test_golden_rest_positions() {
    // The positions store's fold of both accounts of one match, marked live at a
    // spot — the `Position` wire shape: signed `net_quantity`, volume-weighted
    // `avg_price`, and integer-cents realized / unrealized P&L. `delta_exposure`
    // is 0.0 (Greeks are not wired in #008).
    let positions = Arc::new(InMemoryPositionsStore::new());
    let mut fan = StoreFanOut::new(
        Arc::new(InMemoryExecutionsStore::new()),
        Arc::clone(&positions),
        Arc::new(MarkPriceBook::new()),
    );
    let _ = fan.emit(&store_match_event());

    let symbol = sym("BTC-20240329-50000-C");
    let mark = Some(Cents::new(50_500));
    let maker = positions
        .get(&AccountId::new("maker-acct"), &symbol, mark)
        .expect("get maker position")
        .expect("a maker position");
    let taker = positions
        .get(&AccountId::new("taker-acct"), &symbol, mark)
        .expect("get taker position")
        .expect("a taker position");
    let report = vec![maker, taker];

    assert_golden("rest/positions.json", &report);
    let golden = load_golden("rest/positions.json");
    if let Some(rows) = golden.as_array() {
        assert_eq!(rows.len(), 2);
        // The maker is short, the taker is long — opposite signed net quantities.
        assert_eq!(rows[0]["net_quantity"], serde_json::json!(-2));
        assert_eq!(rows[1]["net_quantity"], serde_json::json!(2));
        for row in rows {
            assert_no_float_money(
                row,
                &[
                    "avg_price",
                    "current_price",
                    "realized_pnl",
                    "unrealized_pnl",
                ],
            );
        }
    }
}

#[test]
fn test_golden_rest_position() {
    let position = Position {
        account: AccountId::new("acct-1"),
        symbol: sym("BTC-20240329-50000-C"),
        underlying: "BTC".to_string(),
        net_quantity: -5,
        avg_price: Cents::new(50_000),
        current_price: Some(Cents::new(50_500)),
        realized_pnl: SignedCents::new(1_200),
        unrealized_pnl: Some(SignedCents::new(-2_500)),
        delta_exposure: -2.5,
    };
    assert_golden("rest/position.json", &position);
    assert_no_float_money(
        &load_golden("rest/position.json"),
        &[
            "avg_price",
            "current_price",
            "realized_pnl",
            "unrealized_pnl",
        ],
    );
}

#[test]
fn test_golden_rest_instrument_view() {
    let view = InstrumentView {
        symbol: sym("BTC-20240329-50000-C"),
        underlying: "BTC".to_string(),
        expiration: "20240329".to_string(),
        strike: 50_000,
        style: OptionStyle::Call,
        status: InstrumentLifecycle::Active,
    };
    assert_golden("rest/instrument_view.json", &view);
}

#[test]
fn test_golden_rest_quote() {
    let quote = QuoteResponse {
        bid_price: Some(Cents::new(49_900)),
        bid_size: 12,
        ask_price: Some(Cents::new(50_100)),
        ask_size: 8,
        timestamp: EventTimestamp::new(1_700_000_000_000),
    };
    assert_golden("rest/quote.json", &quote);
    assert_no_float_money(&load_golden("rest/quote.json"), &["bid_price", "ask_price"]);
}

#[test]
fn test_golden_rest_ohlc_bar() {
    let bar = OhlcBar {
        timestamp: 1_700_000_000,
        open: Cents::new(50_000),
        high: Cents::new(50_300),
        low: Cents::new(49_800),
        close: Cents::new(50_100),
        volume: 420,
        trade_count: 37,
    };
    assert_golden("rest/ohlc_bar.json", &bar);
    assert_no_float_money(
        &load_golden("rest/ohlc_bar.json"),
        &["open", "high", "low", "close"],
    );
}

#[test]
fn test_golden_rest_token_response() {
    let resp = TokenResponse {
        token: "<jwt>".to_string(),
        expires_at: "2026-07-15T00:00:00Z".to_string(),
    };
    assert_golden("rest/token_response.json", &resp);
}

#[test]
fn test_golden_rest_controls() {
    let resp = SystemControlResponse {
        master_enabled: true,
        spread_multiplier: 1.5,
        size_scalar: 0.5,
        directional_skew: -0.25,
    };
    assert_golden("rest/controls.json", &resp);
}

#[test]
fn test_golden_rest_create_snapshot() {
    let resp = CreateSnapshotResponse {
        success: true,
        snapshot_id: "snap-1".to_string(),
        orderbooks_saved: 12,
        orders_saved: 340,
        orderbooks_failed: 0,
        timestamp: EventTimestamp::new(1_700_000_000_000),
    };
    assert_golden("rest/create_snapshot.json", &resp);
}

#[test]
fn test_golden_rest_snapshot_summary() {
    let summary = SnapshotSummary {
        snapshot_id: "snap-1".to_string(),
        orderbook_count: 12,
        total_orders: 340,
        created_at: EventTimestamp::new(1_700_000_000_000),
    };
    assert_golden("rest/snapshot_summary.json", &summary);
}

#[test]
fn test_golden_rest_snapshots_list() {
    let list = SnapshotsListResponse {
        snapshots: vec![SnapshotSummary {
            snapshot_id: "snap-1".to_string(),
            orderbook_count: 12,
            total_orders: 340,
            created_at: EventTimestamp::new(1_700_000_000_000),
        }],
        total: 1,
    };
    assert_golden("rest/snapshots_list.json", &list);
}

#[test]
fn test_golden_rest_restore_snapshot() {
    let resp = RestoreSnapshotResponse {
        success: true,
        snapshot_id: "snap-1".to_string(),
        orderbooks_restored: 12,
        orders_restored: 340,
        orderbooks_failed: 0,
        timestamp: EventTimestamp::new(1_700_000_000_000),
    };
    assert_golden("rest/restore_snapshot.json", &resp);
}

#[test]
fn test_golden_rest_bulk_order_response() {
    let resp = BulkOrderResponse {
        success_count: 1,
        failure_count: 1,
        results: vec![
            BulkOrderResultItem {
                index: 0,
                order_id: Some(VenueOrderId::new("lin:BTC:7:0")),
                sequence: Some(SequenceNumber::new(7)),
                status: BulkOrderStatus::Accepted,
                error: None,
            },
            BulkOrderResultItem {
                index: 1,
                order_id: None,
                sequence: None,
                status: BulkOrderStatus::Rejected,
                error: Some("invalid order: limit price must be positive".to_string()),
            },
        ],
        rolled_back: false,
        rollback_warnings: vec![],
    };
    assert_golden("rest/bulk_order_response.json", &resp);
}

// ============================================================================
// WsMessage goldens (one per variant — the `type` discriminant is pinned)
// ============================================================================

#[test]
fn test_golden_ws_connected() {
    assert_golden(
        "ws/connected.json",
        &WsMessage::Connected {
            message: "welcome".to_string(),
        },
    );
}

#[test]
fn test_golden_ws_heartbeat() {
    assert_golden(
        "ws/heartbeat.json",
        &WsMessage::Heartbeat {
            timestamp: EventTimestamp::new(1_700_000_000_000),
        },
    );
}

#[test]
fn test_golden_ws_quote() {
    let msg = WsMessage::Quote {
        symbol: sym("BTC-20240329-50000-C"),
        expiration: "20240329".to_string(),
        strike: 50_000,
        style: OptionStyle::Call,
        bid_price: Some(Cents::new(49_900)),
        ask_price: Some(Cents::new(50_100)),
        bid_size: 12,
        ask_size: 8,
    };
    assert_golden("ws/quote.json", &msg);
}

#[test]
fn test_golden_ws_price() {
    let msg = WsMessage::Price {
        symbol: "BTC".to_string(),
        price_cents: Cents::new(4_200_000),
    };
    assert_golden("ws/price.json", &msg);
}

#[test]
fn test_golden_ws_config() {
    let msg = WsMessage::Config {
        enabled: true,
        spread_multiplier: 1.5,
        size_scalar: 0.5,
        directional_skew: -0.25,
        // A plain market-maker config broadcast (not a control ack) omits the
        // venue-global fan-out delivery fields, so the wire shape is unchanged (#118).
        ok_count: None,
        total: None,
        fully_applied: None,
    };
    assert_golden("ws/config.json", &msg);
}

#[test]
fn test_golden_ws_fill_is_anonymised() {
    let msg = WsMessage::Fill {
        execution_id: ExecutionId::new("lin:BTC:7:0"),
        underlying_sequence: SequenceNumber::new(7),
        venue_ts: EventTimestamp::new(1_700_000_000_000),
        liquidity: LiquidityFlag::Taker,
        symbol: "BTC".to_string(),
        instrument: sym("BTC-20240329-50000-C"),
        side: Side::Buy,
        quantity: 2,
        price: Cents::new(50_000),
        edge: SignedCents::new(-50),
    };
    assert_golden("ws/fill.json", &msg);
    // The public print carries no account-scoped detail.
    let golden = load_golden("ws/fill.json");
    assert!(golden["data"].get("account").is_none());
    assert!(golden["data"].get("fee").is_none());
}

#[test]
fn test_golden_ws_orderbook_snapshot() {
    let msg = WsMessage::OrderbookSnapshot {
        channel: SubscriptionChannel::Orderbook,
        symbol: sym("BTC-20240329-50000-C"),
        sequence: 42,
        bids: vec![PriceLevelData {
            price: Cents::new(49_900),
            quantity: 12,
        }],
        asks: vec![PriceLevelData {
            price: Cents::new(50_100),
            quantity: 8,
        }],
    };
    assert_golden("ws/orderbook_snapshot.json", &msg);
}

#[test]
fn test_golden_ws_orderbook_delta() {
    let msg = WsMessage::OrderbookDelta {
        symbol: sym("BTC-20240329-50000-C"),
        sequence: 43,
        changes: vec![
            PriceLevelChange {
                side: BookSide::Bid,
                price: Cents::new(49_900),
                quantity: 20,
            },
            PriceLevelChange {
                side: BookSide::Ask,
                price: Cents::new(50_100),
                quantity: 0,
            },
        ],
    };
    assert_golden("ws/orderbook_delta.json", &msg);
}

#[test]
fn test_golden_ws_trade() {
    let msg = WsMessage::Trade {
        trade_id: "trade-1".to_string(),
        symbol: sym("BTC-20240329-50000-C"),
        price: Cents::new(50_000),
        quantity: 2,
        timestamp: EventTimestamp::new(1_700_000_000_000),
        maker_order_id: VenueOrderId::new("lin:BTC:7:0"),
        taker_order_id: VenueOrderId::new("lin:BTC:8:0"),
    };
    assert_golden("ws/trade.json", &msg);
}

#[test]
fn test_golden_ws_subscribed() {
    let msg = WsMessage::Subscribed {
        channel: SubscriptionChannel::Orderbook,
        symbol: "BTC-20240329-50000-C".to_string(),
    };
    assert_golden("ws/subscribed.json", &msg);
}

#[test]
fn test_golden_ws_unsubscribed() {
    let msg = WsMessage::Unsubscribed {
        channel: SubscriptionChannel::Orderbook,
        symbol: "BTC-20240329-50000-C".to_string(),
    };
    assert_golden("ws/unsubscribed.json", &msg);
}

#[test]
fn test_golden_ws_batch_subscribed() {
    let msg = WsMessage::BatchSubscribed {
        request_id: Some("req-9".to_string()),
        subscriptions: vec![SubscriptionResult {
            channel: SubscriptionChannel::Trades,
            symbol: Some("BTC-20240329-50000-C".to_string()),
            underlying: None,
            status: "ok".to_string(),
        }],
    };
    assert_golden("ws/batch_subscribed.json", &msg);
}

#[test]
fn test_golden_ws_batch_unsubscribed() {
    let msg = WsMessage::BatchUnsubscribed {
        request_id: Some("req-10".to_string()),
        subscriptions: vec![SubscriptionResult {
            channel: SubscriptionChannel::Trades,
            symbol: Some("BTC-20240329-50000-C".to_string()),
            underlying: None,
            status: "ok".to_string(),
        }],
    };
    assert_golden("ws/batch_unsubscribed.json", &msg);
}

#[test]
fn test_golden_ws_subscriptions_list() {
    let msg = WsMessage::SubscriptionList {
        active: vec![fauxchange::ActiveSubscription {
            channel: SubscriptionChannel::Orderbook,
            symbol: Some("BTC-20240329-50000-C".to_string()),
            underlying: None,
            depth: Some(10),
        }],
    };
    assert_golden("ws/subscriptions.json", &msg);
}

#[test]
fn test_golden_ws_recording_state() {
    let msg = WsMessage::RecordingState { recording: true };
    assert_golden("ws/recording_state.json", &msg);
}

#[test]
fn test_golden_ws_replay_complete() {
    let msg = WsMessage::ReplayComplete {
        report: ReplayReportResponse {
            per_underlying: vec![UnderlyingReplaySummary {
                underlying: "BTC".to_string(),
                event_count: 3,
                last_sequence: Some(2),
            }],
            executions: 2,
        },
    };
    assert_golden("ws/replay_complete.json", &msg);
}

#[test]
fn test_golden_ws_error_message() {
    let msg = WsMessage::Error(
        VenueError::Forbidden(Permission::Trade).ws_error(Some("req-1".to_string())),
    );
    assert_golden("ws/error_message.json", &msg);
}

// ============================================================================
// Venue envelope golden (venue.v1 — the durable journal record)
// ============================================================================

#[test]
fn test_golden_venue_add_order_event() {
    // A representative `venue.v1` VenueEvent: an AddOrder carrying the identity
    // the upstream command drops (account/owner/TIF/STP) with a captured
    // two-leg-per-match `Added` outcome. Pins the mandatory `schema` tag, the
    // PascalCase variant tags, the seam Side (`BUY`/`SELL`) / TimeInForce (`GTC`)
    // / STPMode (`None`) wire forms, the Hash32 hex owner, and money as integer
    // cents — so a field rename or a casing drift is a hard decode error.
    let lineage = LineageId::new("run-1");
    let seq = SequenceNumber::new(7);
    let execution_id = lineage.execution_id("BTC", seq, 0);
    let taker_id = lineage.venue_order_id("BTC", seq, 0);
    let maker_id = lineage.venue_order_id("BTC", SequenceNumber::new(1), 0);
    let taker_owner = Hash32([0x22; 32]);
    let maker_owner = Hash32([0x11; 32]);

    let command = VenueCommand::AddOrder {
        symbol: sym("BTC-20240329-50000-C"),
        order_id: taker_id.clone(),
        account: AccountId::new("taker-acct"),
        owner: taker_owner,
        client_order_id: Some(ClientOrderId::new("client-42")),
        side: SeamSide::Buy,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(50_000)),
        quantity: 2,
        time_in_force: SeamTif::Gtc,
        stp_mode: fauxchange::exchange::STPMode::None,
    };
    let outcome = VenueOutcome::Added {
        fills: vec![
            VenueFill {
                execution_id: execution_id.clone(),
                order_id: maker_id,
                account: AccountId::new("maker-acct"),
                owner: maker_owner,
                side: SeamSide::Sell,
                liquidity: LiquidityFlag::Maker,
                price: Cents::new(50_000),
                quantity: 2,
                fee: SignedCents::new(-10),
            },
            VenueFill {
                execution_id,
                order_id: taker_id,
                account: AccountId::new("taker-acct"),
                owner: taker_owner,
                side: SeamSide::Buy,
                liquidity: LiquidityFlag::Taker,
                price: Cents::new(50_000),
                quantity: 2,
                fee: SignedCents::new(15),
            },
        ],
        resting_quantity: 0,
        // No STP fired on this add: the always-present vec serialises as `[]`.
        stp_cancelled: vec![],
    };
    let event = VenueEvent::new(
        seq,
        EventTimestamp::new(1_700_000_000_000),
        command,
        outcome,
    );

    assert_golden("venue/add_order_event.json", &event);
    // The schema tag is present and the money fields are integer cents.
    let golden = load_golden("venue/add_order_event.json");
    assert_eq!(golden["schema"], serde_json::json!("venue.v1"));
    // The empty STP vec is present on the wire, not elided.
    assert_eq!(
        golden["outcome"]["Added"]["stp_cancelled"],
        serde_json::json!([])
    );
    if let Some(fills) = golden["outcome"]["Added"]["fills"].as_array() {
        for fill in fills {
            assert_no_float_money(fill, &["price", "fee"]);
        }
    }
}

#[test]
fn test_golden_venue_add_order_stp_cancelled_event() {
    // A cancel-maker AddOrder whose incoming aggressor removed a same-owner
    // resting leg via self-trade prevention in the one add turn — pins the
    // `stp_cancelled` branch (there is no separate cancel command / sequence) so
    // the lossless STP-cancellation record is frozen into the venue.v1 shape.
    let lineage = LineageId::new("run-1");
    let seq = SequenceNumber::new(9);
    let execution_id = lineage.execution_id("BTC", seq, 0);
    let taker_id = lineage.venue_order_id("BTC", seq, 0);
    let taker_owner = Hash32([0x22; 32]);
    // A different-owner resting maker that actually filled.
    let counterparty_owner = Hash32([0x33; 32]);
    // The same-owner resting maker the aggressor removed via STP.
    let self_maker_id = lineage.venue_order_id("BTC", SequenceNumber::new(3), 0);

    let command = VenueCommand::AddOrder {
        symbol: sym("BTC-20240329-50000-C"),
        order_id: taker_id.clone(),
        account: AccountId::new("taker-acct"),
        owner: taker_owner,
        client_order_id: None,
        side: SeamSide::Buy,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(50_000)),
        quantity: 2,
        time_in_force: SeamTif::Gtc,
        stp_mode: fauxchange::exchange::STPMode::CancelMaker,
    };
    let outcome = VenueOutcome::Added {
        fills: vec![
            VenueFill {
                execution_id: execution_id.clone(),
                order_id: lineage.venue_order_id("BTC", SequenceNumber::new(2), 0),
                account: AccountId::new("counterparty-acct"),
                owner: counterparty_owner,
                side: SeamSide::Sell,
                liquidity: LiquidityFlag::Maker,
                price: Cents::new(50_000),
                quantity: 1,
                fee: SignedCents::new(-5),
            },
            VenueFill {
                execution_id,
                order_id: taker_id,
                account: AccountId::new("taker-acct"),
                owner: taker_owner,
                side: SeamSide::Buy,
                liquidity: LiquidityFlag::Taker,
                price: Cents::new(50_000),
                quantity: 1,
                fee: SignedCents::new(8),
            },
        ],
        resting_quantity: 0,
        stp_cancelled: vec![CancelledLeg {
            order_id: self_maker_id,
            owner: taker_owner,
            symbol: sym("BTC-20240329-50000-C"),
            side: SeamSide::Sell,
            reason: CancelReason::SelfTradePrevention,
        }],
    };
    let event = VenueEvent::new(
        seq,
        EventTimestamp::new(1_700_000_000_000),
        command,
        outcome,
    );

    assert_golden("venue/add_order_stp_cancelled_event.json", &event);
    let golden = load_golden("venue/add_order_stp_cancelled_event.json");
    assert_eq!(
        golden["outcome"]["Added"]["stp_cancelled"][0]["reason"],
        serde_json::json!("SelfTradePrevention")
    );
}

#[test]
fn test_golden_venue_market_order_event() {
    // A representative `venue.v1` VenueEvent for the upstream true non-resting
    // market primitive: a market `AddOrder` (no `limit_price`) with a captured
    // `Market` outcome carrying a two-leg fill and the cancelled unfilled
    // remainder — pins the always-present `fills` / `stp_cancelled` empty-vec
    // convention and the `unfilled_quantity` field into the venue.v1 shape.
    let lineage = LineageId::new("run-1");
    let seq = SequenceNumber::new(11);
    let execution_id = lineage.execution_id("BTC", seq, 0);
    let taker_id = lineage.venue_order_id("BTC", seq, 0);
    let maker_id = lineage.venue_order_id("BTC", SequenceNumber::new(4), 0);
    let taker_owner = Hash32([0x22; 32]);
    let maker_owner = Hash32([0x11; 32]);

    let command = VenueCommand::AddOrder {
        symbol: sym("BTC-20240329-50000-C"),
        order_id: taker_id.clone(),
        account: AccountId::new("taker-acct"),
        owner: taker_owner,
        client_order_id: None,
        side: SeamSide::Buy,
        order_type: OrderType::Market,
        limit_price: None,
        quantity: 5,
        time_in_force: SeamTif::Ioc,
        stp_mode: fauxchange::exchange::STPMode::None,
    };
    let outcome = VenueOutcome::Market {
        fills: vec![
            VenueFill {
                execution_id: execution_id.clone(),
                order_id: maker_id,
                account: AccountId::new("maker-acct"),
                owner: maker_owner,
                side: SeamSide::Sell,
                liquidity: LiquidityFlag::Maker,
                price: Cents::new(50_000),
                quantity: 2,
                fee: SignedCents::new(0),
            },
            VenueFill {
                execution_id,
                order_id: taker_id,
                account: AccountId::new("taker-acct"),
                owner: taker_owner,
                side: SeamSide::Buy,
                liquidity: LiquidityFlag::Taker,
                price: Cents::new(50_000),
                quantity: 2,
                fee: SignedCents::new(0),
            },
        ],
        // 3 of the 5 contracts found no liquidity: cancelled, never rested, never
        // assigned an invented price.
        unfilled_quantity: 3,
        stp_cancelled: vec![],
    };
    let event = VenueEvent::new(
        seq,
        EventTimestamp::new(1_700_000_000_000),
        command,
        outcome,
    );

    assert_golden("venue/market_order_event.json", &event);
    let golden = load_golden("venue/market_order_event.json");
    assert_eq!(golden["schema"], serde_json::json!("venue.v1"));
    // A market command carries no `limit_price`; the unfilled remainder is present.
    assert_eq!(
        golden["command"]["AddOrder"]["limit_price"],
        serde_json::json!(null)
    );
    assert_eq!(
        golden["outcome"]["Market"]["unfilled_quantity"],
        serde_json::json!(3)
    );
    assert_eq!(
        golden["outcome"]["Market"]["stp_cancelled"],
        serde_json::json!([])
    );
}

#[test]
fn test_golden_venue_replace_partial_event() {
    // A representative `venue.v1` VenueEvent for a non-atomic `Replace` whose
    // cancel leg succeeded but whose add leg was rejected — the defined,
    // replayable partial state (old order gone, no new order rests), NOT a silent
    // loss. Pins the explicit `Replace { cancelled, add: Rejected }` shape.
    let lineage = LineageId::new("run-1");
    let seq = SequenceNumber::new(13);
    let command = VenueCommand::Replace {
        symbol: sym("BTC-20240329-50000-C"),
        order_id: lineage.venue_order_id("BTC", SequenceNumber::new(5), 0),
        new_order_id: lineage.venue_order_id("BTC", seq, 0),
        account: AccountId::new("acct-1"),
        client_order_id: Some(ClientOrderId::new("cl-new")),
        orig_client_order_id: Some(ClientOrderId::new("cl-orig")),
        side: SeamSide::Buy,
        limit_price: Some(Cents::new(40_000)),
        quantity: 2,
        time_in_force: SeamTif::Fok,
        stp_mode: fauxchange::exchange::STPMode::None,
    };
    let outcome = VenueOutcome::Replace {
        cancelled: true,
        add: AddOutcome::rejected(
            RejectKind::NotFillable,
            "order was not fillable and did not rest",
        ),
    };
    let event = VenueEvent::new(
        seq,
        EventTimestamp::new(1_700_000_000_000),
        command,
        outcome,
    );

    assert_golden("venue/replace_partial_event.json", &event);
    let golden = load_golden("venue/replace_partial_event.json");
    assert_eq!(golden["schema"], serde_json::json!("venue.v1"));
    assert_eq!(
        golden["outcome"]["Replace"]["cancelled"],
        serde_json::json!(true)
    );
    assert!(golden["outcome"]["Replace"]["add"]["Rejected"].is_object());
    // The #098-fix-4 wire addition: a Replace command carries the replacement and
    // retired client-order ids so #085 recovery rebuilds the cross-session
    // correlation deterministically (both are `#[serde(default)]`, so a legacy record
    // without them still decodes).
    assert_eq!(
        golden["command"]["Replace"]["client_order_id"],
        serde_json::json!("cl-new")
    );
    assert_eq!(
        golden["command"]["Replace"]["orig_client_order_id"],
        serde_json::json!("cl-orig")
    );
}

#[test]
fn test_golden_venue_snapshot_restored_epoch() {
    // The `venue.v1` wire addition of #009: the `SnapshotRestored` epoch marker
    // record a restore writes as the first record of a fresh journal epoch. Pins
    // the `JournalRecord::Epoch` variant tag, the mandatory `schema` tag, the
    // continued `underlying_sequence` (never reset), the epoch counter, and the
    // lineage carried forward — so a field rename or casing drift is a hard decode
    // error.
    let record = JournalRecord::epoch(SnapshotRestored::new(
        SequenceNumber::new(42),
        EventTimestamp::new(1_700_000_000_000),
        "snap-1",
        1,
        LineageId::new("run-1"),
    ));
    assert_golden("venue/snapshot_restored_epoch.json", &record);
    let golden = load_golden("venue/snapshot_restored_epoch.json");
    assert_eq!(golden["Epoch"]["schema"], serde_json::json!("venue.v1"));
    assert_eq!(
        golden["Epoch"]["underlying_sequence"],
        serde_json::json!(42)
    );
    assert_eq!(golden["Epoch"]["epoch"], serde_json::json!(1));
    assert_eq!(golden["Epoch"]["lineage_id"], serde_json::json!("run-1"));
}

#[test]
fn test_golden_venue_persisted_journal_row() {
    // The durable ON-DISK layout of one journal record (#029,
    // `migrations/…_journal.sql`, `src/db/journal.rs`): the routing / unique-key
    // columns `(underlying, underlying_sequence, kind)` plus the VERBATIM `venue.v1`
    // envelope `payload` — the `serde_json` bytes stored as TEXT, so a `venue.v1`
    // record can never be silently mutated by a JSONB key reorder. Pins the row
    // projection the durable `PgVenueJournal` writes so a schema / store drift is a
    // golden mismatch in the same commit.
    let command = VenueCommand::CancelOrder {
        symbol: sym("BTC-20240329-50000-C"),
        order_id: VenueOrderId::new("run-1:BTC:7:0"),
        account: AccountId::new("acct-1"),
    };
    let record = JournalRecord::command(
        SequenceNumber::new(7),
        EventTimestamp::new(1_700_000_000_000),
        command,
    );
    let payload = match serde_json::to_string(&record) {
        Ok(payload) => payload,
        Err(e) => panic!("serialise journal record: {e}"),
    };
    // The persisted row = the projected columns + the verbatim envelope payload.
    let row = serde_json::json!({
        "underlying": "BTC",
        "underlying_sequence": record.sequence().get(),
        "kind": record.kind(),
        "payload": payload,
    });
    assert_golden("venue/persisted_journal_row.json", &row);

    let golden = load_golden("venue/persisted_journal_row.json");
    assert_eq!(golden["kind"], serde_json::json!("command"));
    assert_eq!(golden["underlying_sequence"], serde_json::json!(7));
    // The `payload` column holds the exact `venue.v1` `JournalRecord` JSON and
    // round-trips back to the identical record.
    let payload_str = match golden["payload"].as_str() {
        Some(payload) => payload,
        None => panic!("the payload column must be a JSON string"),
    };
    match serde_json::from_str::<JournalRecord>(payload_str) {
        Ok(reparsed) => assert_eq!(reparsed, record, "the payload reparses to the record"),
        Err(e) => panic!("payload must reparse to a JournalRecord: {e}"),
    }
}

// ============================================================================
// #047 — WS quote / fill shape under a market-maker persona
// ============================================================================

/// A persona-driven quote renders on the WS `quote` channel in the SAME wire shape
/// as any other quote — a wide, skewed persona (wider spread, asymmetric sizes) does
/// not change the schema, only the values. Money stays integer cents.
#[test]
fn test_golden_ws_persona_quote_shape() {
    let quote = WsMessage::Quote {
        symbol: sym("BTC-20351231-50000-C"),
        expiration: "20351231".to_string(),
        strike: 50_000,
        style: OptionStyle::Call,
        // A `wide_skewed` persona: a wide spread, sizes trimmed asymmetrically.
        bid_price: Some(Cents::new(4_900)),
        ask_price: Some(Cents::new(5_100)),
        bid_size: 3,
        ask_size: 2,
    };
    assert_golden("ws/persona_quote.json", &quote);
    let value = serde_json::to_value(&quote).expect("serialise persona quote");
    assert_no_float_money(&value, &["bid_price", "ask_price"]);
}

/// A persona maker's fill renders on the public, anonymised WS `fill` channel with
/// the four cross-surface join keys and its captured `edge`, money as integer cents.
#[test]
fn test_golden_ws_persona_fill_shape() {
    let fill = WsMessage::Fill {
        execution_id: ExecutionId::new("run-1:BTC:7:0"),
        underlying_sequence: SequenceNumber::new(7),
        venue_ts: EventTimestamp::new(1_700_000_000_000),
        liquidity: LiquidityFlag::Maker,
        symbol: "BTC".to_string(),
        instrument: sym("BTC-20351231-50000-C"),
        side: Side::Sell,
        quantity: 2,
        price: Cents::new(5_100),
        // The persona maker sold above its quote-time theo → positive captured edge.
        edge: SignedCents::new(75),
    };
    assert_golden("ws/persona_fill.json", &fill);
    let value = serde_json::to_value(&fill).expect("serialise persona fill");
    assert_no_float_money(&value, &["price", "edge"]);
}
