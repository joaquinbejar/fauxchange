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
//! - `venue_envelope_serde_identity` — a generated `venue.v1` [`VenueEvent`]
//!   (`AddOrder` + captured two-leg fills) round-trips through JSON, schema tag
//!   and integer cents intact.
//! - `venue_id_grammar_collision_free` — the composite id grammar is
//!   deterministic and collision-free across `(lineage, underlying, sequence,
//!   index)` tuples (colon-free tokens).

use fauxchange::exchange::{
    CancelReason, CancelledLeg, Cents, EventTimestamp, Fill as VenueFill, LineageId, Notional,
    SequenceNumber, SignedCents, Symbol, VenueCommand, VenueEvent, VenueOutcome,
};
use fauxchange::exchange::{Hash32, STPMode, Side as SeamSide, TimeInForce as SeamTif};
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

    /// A generated `venue.v1` [`VenueEvent`] — an `AddOrder` carrying the dropped
    /// identity (account/owner/TIF/STP) plus a captured two-leg-per-match `Added`
    /// outcome — round-trips through JSON unchanged, with its mandatory `schema`
    /// tag and integer-cents money intact. Seam `Side` / `TimeInForce` / `STPMode`
    /// are the upstream newtypes, so this also pins their round-trip inside the
    /// envelope.
    #[test]
    fn venue_envelope_serde_identity(
        lineage in "[a-zA-Z0-9_-]{1,16}",
        underlying in "[A-Z]{1,6}",
        year in 1970u32..=2099,
        month in 1u32..=12,
        day in 1u32..=28,
        strike in 1u64..=u64::MAX,
        style in "[CP]",
        seq in any::<u64>(),
        side_pick in 0u8..2,
        tif_pick in 0u8..5,
        stp_pick in 0u8..4,
        gtd_ms in any::<u64>(),
        price in 1u64..=u64::MAX,
        quantity in 1u64..=u64::MAX,
        fee in any::<i64>(),
        n_matches in 0u32..=3,
        n_stp in 0u32..=2,
        taker_owner_bytes in proptest::array::uniform32(any::<u8>()),
        maker_owner_bytes in proptest::array::uniform32(any::<u8>()),
        has_coid in any::<bool>(),
        coid in "[a-zA-Z0-9_-]{1,16}",
    ) {
        let raw = symbol_string(&underlying, year, month, day, strike, &style);
        let symbol = match Symbol::parse(&raw) {
            Ok(s) => s,
            Err(e) => return Err(TestCaseError::fail(format!("parse failed for {raw}: {e:?}"))),
        };
        let lineage_id = LineageId::new(lineage);
        let sequence = SequenceNumber::new(seq);

        let taker_side = if side_pick == 0 { SeamSide::Buy } else { SeamSide::Sell };
        let maker_side = if side_pick == 0 { SeamSide::Sell } else { SeamSide::Buy };
        let tif = match tif_pick {
            0 => SeamTif::Gtc,
            1 => SeamTif::Ioc,
            2 => SeamTif::Fok,
            3 => SeamTif::Gtd(gtd_ms),
            _ => SeamTif::Day,
        };
        let stp = match stp_pick {
            0 => STPMode::None,
            1 => STPMode::CancelTaker,
            2 => STPMode::CancelMaker,
            _ => STPMode::CancelBoth,
        };
        let taker_owner = Hash32(taker_owner_bytes);
        let maker_owner = Hash32(maker_owner_bytes);

        let order_id = lineage_id.venue_order_id(underlying.as_str(), sequence, 0);
        let command = VenueCommand::AddOrder {
            symbol,
            order_id: order_id.clone(),
            account: AccountId::new("taker"),
            owner: taker_owner,
            client_order_id: has_coid.then(|| ClientOrderId::new(coid)),
            side: taker_side,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(price)),
            quantity,
            time_in_force: tif,
            stp_mode: stp,
        };

        // Two linked legs per match, sharing one execution id per match.
        let mut fills = Vec::new();
        for fill_index in 0..n_matches {
            let execution_id = lineage_id.execution_id(underlying.as_str(), sequence, fill_index);
            fills.push(VenueFill {
                execution_id: execution_id.clone(),
                order_id: lineage_id.venue_order_id(
                    underlying.as_str(),
                    SequenceNumber::new(1),
                    fill_index,
                ),
                account: AccountId::new("maker"),
                owner: maker_owner,
                side: maker_side,
                liquidity: LiquidityFlag::Maker,
                price: Cents::new(price),
                quantity,
                fee: SignedCents::new(fee),
            });
            fills.push(VenueFill {
                execution_id,
                order_id: order_id.clone(),
                account: AccountId::new("taker"),
                owner: taker_owner,
                side: taker_side,
                liquidity: LiquidityFlag::Taker,
                price: Cents::new(price),
                quantity,
                fee: SignedCents::new(fee),
            });
        }

        // Resting legs the aggressor removed via STP inside this one add turn.
        let stp_cancelled = (0..n_stp)
            .map(|i| CancelledLeg {
                order_id: lineage_id.venue_order_id(
                    underlying.as_str(),
                    SequenceNumber::new(1),
                    i,
                ),
                owner: maker_owner,
                reason: CancelReason::SelfTradePrevention,
            })
            .collect();

        let event = VenueEvent::new(
            sequence,
            EventTimestamp::new(seq),
            command,
            VenueOutcome::Added { fills, resting_quantity: 0, stp_cancelled },
        );

        let json = serde_json::to_string(&event)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let back: VenueEvent = serde_json::from_str(&json)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(&back, &event);
        prop_assert!(back.is_current_schema());
    }

    /// The composite id grammar is **deterministic** (same tuple ⇒ identical id)
    /// and **collision-free** over `(lineage, underlying, sequence, index)`:
    /// distinct tuples mint distinct ids because the colon-delimited grammar is
    /// injective over the colon-free lineage / underlying alphabets (so `BTC`
    /// sequence 1 and `ETH` sequence 1 never collide).
    #[test]
    fn venue_id_grammar_collision_free(
        lin_a in "[a-zA-Z0-9_-]{1,16}",
        lin_b in "[a-zA-Z0-9_-]{1,16}",
        und_a in "[A-Z]{1,6}",
        und_b in "[A-Z]{1,6}",
        seq_a in any::<u64>(),
        seq_b in any::<u64>(),
        idx_a in any::<u32>(),
        idx_b in any::<u32>(),
    ) {
        let id_a = LineageId::new(lin_a.clone())
            .venue_order_id(und_a.as_str(), SequenceNumber::new(seq_a), idx_a);
        let id_b = LineageId::new(lin_b.clone())
            .venue_order_id(und_b.as_str(), SequenceNumber::new(seq_b), idx_b);

        let same_tuple = lin_a == lin_b && und_a == und_b && seq_a == seq_b && idx_a == idx_b;
        if same_tuple {
            prop_assert_eq!(id_a.as_str(), id_b.as_str());
        } else {
            prop_assert_ne!(id_a.as_str(), id_b.as_str());
        }

        // Determinism: rebuilding from the same tuple yields the identical id.
        let id_a_again = LineageId::new(lin_a)
            .venue_order_id(und_a.as_str(), SequenceNumber::new(seq_a), idx_a);
        prop_assert_eq!(id_a.as_str(), id_a_again.as_str());

        // An execution id built from the same tuple shares the grammar exactly.
        let exec_a = LineageId::new(lin_b)
            .execution_id(und_b.as_str(), SequenceNumber::new(seq_b), idx_b);
        prop_assert_eq!(id_b.as_str(), exec_a.as_str());
    }
}
