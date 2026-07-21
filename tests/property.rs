//! Property tests for the domain boundary newtypes and the DTO layer
//! ([TESTING.md §3](../docs/TESTING.md)).
//!
//! - `cents_never_lossy` — the money newtypes survive a `serde` round-trip and
//!   serialise as bare integers (no float drift on any wire).
//! - `symbol_roundtrip` — a canonical symbol parses, then formats to itself.
//! - `order_dto_serde_identity` — an [`Order`] DTO round-trips through JSON with
//!   its casing and integer-cents money intact.
//! - `ws_message_serde_identity` — every [`WsMessage`] variant round-trips
//!   through its `type`/`data` framing.

use fauxchange::exchange::{Cents, EventTimestamp, Notional, SequenceNumber, SignedCents, Symbol};
use fauxchange::{
    AccountId, ClientOrderId, ExecutionId, LiquidityFlag, Order, OrderStatus, OrderType,
    Permission, Side, TimeInForce, VenueError, VenueOrderId, WsMessage,
};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;

/// Builds a canonical symbol string from validated components (always parseable).
fn symbol_string(
    underlying: &str,
    year: u32,
    month: u32,
    day: u32,
    strike: u64,
    style: &str,
) -> String {
    format!("{underlying}-{year:04}{month:02}{day:02}-{strike}-{style}")
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 1024, max_shrink_iters: 50_000, ..ProptestConfig::default() })]

    /// Every money newtype serialises as a bare integer and round-trips through
    /// JSON without loss.
    #[test]
    fn cents_never_lossy(a in any::<u64>(), b in any::<i64>(), n in any::<u128>()) {
        // Cents (u64).
        let cents = Cents::new(a);
        let cents_json = serde_json::to_string(&cents)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(&cents_json, &a.to_string());
        let cents_back: Cents = serde_json::from_str(&cents_json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(cents_back, cents);

        // SignedCents (i64).
        let signed = SignedCents::new(b);
        let signed_json = serde_json::to_string(&signed)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(&signed_json, &b.to_string());
        let signed_back: SignedCents = serde_json::from_str(&signed_json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(signed_back, signed);

        // Notional (u128).
        let notional = Notional::new(n);
        let notional_json = serde_json::to_string(&notional)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(&notional_json, &n.to_string());
        let notional_back: Notional = serde_json::from_str(&notional_json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(notional_back, notional);
    }

    /// A canonical symbol parses, and the stored canonical form equals the
    /// input string (parse-then-format is the identity).
    #[test]
    fn symbol_roundtrip(
        underlying in "[A-Z]{1,6}",
        year in 1970u32..=2099,
        month in 1u32..=12,
        day in 1u32..=28,
        strike in 1u64..=u64::MAX,
        style in "[CP]",
    ) {
        let raw = symbol_string(&underlying, year, month, day, strike, &style);
        let symbol = match Symbol::parse(&raw) {
            Ok(s) => s,
            Err(e) => return Err(TestCaseError::fail(format!("parse failed for {raw}: {e:?}"))),
        };
        prop_assert_eq!(symbol.as_str(), raw.as_str());

        // The canonical string also survives a serde round-trip as a bare JSON string.
        let json = serde_json::to_string(&symbol)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(&json, &format!("\"{raw}\""));
        let back: Symbol = serde_json::from_str(&json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(back, symbol);
    }

    /// An `Order` DTO round-trips through JSON unchanged — its casing, its
    /// optional-field elision, and its integer-cents `limit_price` all preserved.
    #[test]
    fn order_dto_serde_identity(
        underlying in "[A-Z]{1,6}",
        year in 1970u32..=2099,
        month in 1u32..=12,
        day in 1u32..=28,
        strike in 1u64..=u64::MAX,
        style in "[CP]",
        is_buy in any::<bool>(),
        is_limit in any::<bool>(),
        price in 1u64..=u64::MAX,
        quantity in 1u64..=u64::MAX,
        filled in any::<u64>(),
        remaining in any::<u64>(),
        sequence in any::<u64>(),
        submitted_at in any::<u64>(),
        id in "[a-zA-Z0-9:_-]{1,24}",
        account in "[a-zA-Z0-9_-]{1,16}",
        has_coid in any::<bool>(),
        coid in "[a-zA-Z0-9_-]{1,16}",
        status_pick in 0u8..5,
    ) {
        let raw = symbol_string(&underlying, year, month, day, strike, &style);
        let symbol = match Symbol::parse(&raw) {
            Ok(s) => s,
            Err(e) => return Err(TestCaseError::fail(format!("parse failed for {raw}: {e:?}"))),
        };
        let (order_type, limit_price) = if is_limit {
            (OrderType::Limit, Some(Cents::new(price)))
        } else {
            (OrderType::Market, None)
        };
        let status = match status_pick {
            0 => OrderStatus::Pending,
            1 => OrderStatus::Active,
            2 => OrderStatus::Partial,
            3 => OrderStatus::Filled,
            _ => OrderStatus::Canceled,
        };
        let order = Order {
            id: VenueOrderId::new(id),
            client_order_id: has_coid.then(|| ClientOrderId::new(coid)),
            account: AccountId::new(account),
            symbol,
            side: if is_buy { Side::Buy } else { Side::Sell },
            order_type,
            limit_price,
            quantity,
            filled_quantity: filled,
            remaining_quantity: remaining,
            time_in_force: TimeInForce::Gtc,
            status,
            submitted_at: EventTimestamp::new(submitted_at),
            sequence: SequenceNumber::new(sequence),
        };
        let json = serde_json::to_string(&order)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let back: Order = serde_json::from_str(&json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(back, order);
    }

    /// Every integer/string `WsMessage` variant round-trips through its
    /// `#[serde(tag = "type", content = "data")]` framing unchanged.
    ///
    /// The `Config` variant (documented analytic floats) is excluded here: an
    /// `f64` is not guaranteed to be 1-ULP exact across `serde_json`'s default
    /// parser, so its shape is pinned by a golden instead of a round-trip.
    #[test]
    fn ws_message_serde_identity(
        variant in 0u8..12,
        underlying in "[A-Z]{1,6}",
        year in 1970u32..=2099,
        month in 1u32..=12,
        day in 1u32..=28,
        strike in 1u64..=u64::MAX,
        style in "[CP]",
        price in any::<u64>(),
        quantity in any::<u64>(),
        edge in any::<i64>(),
        sequence in any::<u64>(),
        ts in any::<u64>(),
        text in "[a-zA-Z0-9 ._-]{0,32}",
    ) {
        let raw = symbol_string(&underlying, year, month, day, strike, &style);
        let symbol = match Symbol::parse(&raw) {
            Ok(s) => s,
            Err(e) => return Err(TestCaseError::fail(format!("parse failed for {raw}: {e:?}"))),
        };
        let msg = match variant {
            0 => WsMessage::Connected { message: text },
            1 => WsMessage::Heartbeat { timestamp: EventTimestamp::new(ts) },
            2 => WsMessage::Price { symbol: underlying, price_cents: Cents::new(price) },
            3 => WsMessage::Trade {
                trade_id: text,
                symbol,
                price: Cents::new(price),
                quantity,
                timestamp: EventTimestamp::new(ts),
                maker_order_id: VenueOrderId::new(format!("{underlying}-m")),
                taker_order_id: VenueOrderId::new(format!("{underlying}-t")),
            },
            4 => WsMessage::Fill {
                execution_id: ExecutionId::new(text),
                underlying_sequence: SequenceNumber::new(sequence),
                venue_ts: EventTimestamp::new(ts),
                liquidity: LiquidityFlag::Maker,
                symbol: underlying,
                instrument: symbol,
                side: Side::Sell,
                quantity,
                price: Cents::new(price),
                edge: SignedCents::new(edge),
            },
            5 => WsMessage::OrderbookSnapshot {
                channel: fauxchange::SubscriptionChannel::Orderbook,
                symbol,
                sequence,
                bids: vec![fauxchange::PriceLevelData { price: Cents::new(price), quantity }],
                asks: vec![],
            },
            6 => WsMessage::OrderbookDelta {
                symbol,
                sequence,
                changes: vec![fauxchange::PriceLevelChange {
                    side: fauxchange::BookSide::Ask,
                    price: Cents::new(price),
                    quantity,
                }],
            },
            7 => WsMessage::Subscribed {
                channel: fauxchange::SubscriptionChannel::Trades,
                symbol: raw,
            },
            8 => WsMessage::Unsubscribed {
                channel: fauxchange::SubscriptionChannel::Trades,
                symbol: raw,
            },
            9 => WsMessage::BatchSubscribed {
                request_id: Some(text.clone()),
                subscriptions: vec![fauxchange::SubscriptionResult {
                    channel: fauxchange::SubscriptionChannel::Trades,
                    symbol: Some(raw),
                    underlying: None,
                    status: "ok".to_string(),
                }],
            },
            10 => WsMessage::BatchUnsubscribed {
                request_id: Some(text.clone()),
                subscriptions: vec![fauxchange::SubscriptionResult {
                    channel: fauxchange::SubscriptionChannel::Trades,
                    symbol: Some(raw),
                    underlying: None,
                    status: "ok".to_string(),
                }],
            },
            _ => WsMessage::Error(VenueError::Forbidden(Permission::Trade).ws_error(Some(text))),
        };
        let json = serde_json::to_string(&msg)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let back: WsMessage = serde_json::from_str(&json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(back, msg);
    }
}
