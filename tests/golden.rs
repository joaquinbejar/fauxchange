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

use fauxchange::exchange::{Cents, EventTimestamp, SequenceNumber, SignedCents, Symbol};
use fauxchange::{
    AccountId, BookSide, BulkOrderResponse, BulkOrderResultItem, BulkOrderStatus, ClientOrderId,
    CreateSnapshotResponse, ExecutionId, ExecutionRecord, ExecutionSummary, ExecutionsListResponse,
    FillPrint, InstrumentLifecycle, InstrumentView, LimitOrderStatus, LiquidityFlag,
    MarketOrderStatus, OhlcBar, OptionStyle, Order, OrderStatus, OrderType, Permission,
    PlaceLimitOrderRequest, PlaceLimitOrderResponse, PlaceMarketOrderResponse, Position,
    PriceLevelChange, PriceLevelData, QuoteResponse, Side, SubscriptionChannel, SubscriptionResult,
    SystemControlResponse, TimeInForce, TokenResponse, VenueError, VenueOrderId, WsMessage,
};
use serde::Serialize;

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
        average_price: Some(50_012.5),
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
fn test_golden_rest_bulk_order_response() {
    let resp = BulkOrderResponse {
        success_count: 1,
        failure_count: 1,
        results: vec![
            BulkOrderResultItem {
                index: 0,
                order_id: Some(VenueOrderId::new("lin:BTC:7:0")),
                status: BulkOrderStatus::Accepted,
                error: None,
            },
            BulkOrderResultItem {
                index: 1,
                order_id: None,
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
fn test_golden_ws_error_message() {
    let msg = WsMessage::Error(
        VenueError::Forbidden(Permission::Trade).ws_error(Some("req-1".to_string())),
    );
    assert_golden("ws/error_message.json", &msg);
}
