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
//! - `sequence_monotonic_per_symbol` — the per-underlying single-writer actor
//!   assigns `0, 1, 2, …` gaplessly in its turn order, independently per symbol,
//!   over an arbitrary interleaving of commands across two underlyings.
//! - `ws_instrument_sequence_monotonic_per_symbol` — the WS market-data
//!   `instrument_sequence` (a **separate** namespace from the journaled
//!   `underlying_sequence`) is strictly increasing per instrument across an
//!   arbitrary stream of book mutations folded through the subscription manager
//!   ([03 §4.1](../docs/03-protocol-surfaces.md), [01 §9.1](../docs/01-domain-model.md)).
//! - `position_pnl_stays_consistent_across_fills` — over an arbitrary fill
//!   sequence, a position's `realized + unrealized` P&L equals the net cash flow
//!   plus `net_quantity × mark`, exactly, in integer cents
//!   ([008](../milestones/v0.1-backend-core/008-executions-positions-stores.md)).
//! - `config_validate_rejects_out_of_range` — the layered config validator
//!   accepts no out-of-range value for the v0.2 knobs (clock, log format, seed,
//!   bind address): a value is accepted **iff** it is genuinely valid, otherwise
//!   the load fails with the matching typed [`ConfigError`]. The harness is stood
//!   up here for v0.5 (#44–#47) to extend
//!   ([022](../milestones/v0.2-packaging/022-config-surface.md)).

use fauxchange::config::{ClockMode, Config, ConfigError, LogFormat};
use fauxchange::exchange::{
    ActorConfig, CancelReason, CancelledLeg, Cents, CommandExecutor, EventTimestamp,
    ExecutionContext, Fill as VenueFill, FixedClock, InMemoryPositionsStore, InMemoryVenueJournal,
    JournalHeader, JournalRecord, LineageId, MatchingExecutor, NoopFanOut, Notional,
    PlaceholderExecutor, PositionLeg, PositionsStore, RecordKind, SequenceNumber, SignedCents,
    Symbol, TopOfBook, UnderlyingActor, VenueCommand, VenueEvent, VenueJournal, VenueOutcome,
};
use fauxchange::exchange::{
    Hash32, OptionStyle, STPMode, Side as SeamSide, TimeInForce as SeamTif,
};
use fauxchange::gateway::fix::enums::{
    MdEntryType, OrdType as FixOrdType, OrderSide, TimeInForce as FixTif,
};
use fauxchange::gateway::fix::header::{StandardHeader, UtcTimestamp};
use fauxchange::gateway::fix::marketdata::{MarketDataSnapshotFullRefresh, SnapshotEntry};
use fauxchange::gateway::fix::order::NewOrderSingle;
use fauxchange::gateway::fix::price::{
    PriceScale, parse_decimal_to_cents, render_cents_to_decimal,
};
use fauxchange::gateway::fix::{DecodedMessage, decode};
use fauxchange::market_maker::{QuoteInput, Quoter};
use fauxchange::simulation::{JournalStream, replay_streams};
use fauxchange::subscription::OrderbookSubscriptionManager;
use fauxchange::{
    AccountId, ClientOrderId, ExecutionId, LiquidityFlag, Order, OrderStatus, OrderType,
    Permission, Side, TimeInForce, VenueError, VenueOrderId, WsMessage,
};
use ironfix_core::types::{CompId, SeqNum};
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

    /// The `underlying_sequence` is monotonic and gapless **per symbol**: driving
    /// two independent underlying actors with an arbitrary interleaving of
    /// commands (`true` → BTC, `false` → ETH), each actor assigns `0, 1, 2, …` in
    /// its own turn order via its venue-owned checked counter, and the two
    /// counters never interfere (`BTC` and `ETH` sequence independently). Driven
    /// synchronously through the actor's `handle` turn, which is the same total
    /// order the spawned mailbox produces.
    #[test]
    fn sequence_monotonic_per_symbol(routes in prop::collection::vec(any::<bool>(), 0..96)) {
        // The command payload is irrelevant to sequence assignment (the #006
        // placeholder executor captures a neutral outcome); a cancel suffices.
        let symbol = Symbol::parse("BTC-20240329-50000-C")
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
        let command = VenueCommand::CancelOrder {
            symbol,
            order_id: VenueOrderId::new("order-x"),
            account: AccountId::new("acct-1"),
        };

        let make = |underlying: &str| {
            let lineage = LineageId::new("run-1");
            let journal = InMemoryVenueJournal::new(JournalHeader::new(lineage.clone()));
            UnderlyingActor::new(
                ActorConfig::new(underlying, lineage, 16),
                journal,
                PlaceholderExecutor,
                NoopFanOut,
                FixedClock::new(EventTimestamp::new(1)),
            )
        };
        let mut btc = make("BTC");
        let mut eth = make("ETH");
        let mut next_btc = 0u64;
        let mut next_eth = 0u64;

        for route in routes {
            if route {
                let receipt = btc
                    .handle(command.clone())
                    .map_err(|e| TestCaseError::fail(e.to_string()))?;
                prop_assert_eq!(receipt.underlying_sequence, SequenceNumber::new(next_btc));
                next_btc += 1;
            } else {
                let receipt = eth
                    .handle(command.clone())
                    .map_err(|e| TestCaseError::fail(e.to_string()))?;
                prop_assert_eq!(receipt.underlying_sequence, SequenceNumber::new(next_eth));
                next_eth += 1;
            }
        }

        // Independence: each stream's highest sequence reflects only its own count.
        prop_assert_eq!(
            btc.journal().last_sequence(),
            next_btc.checked_sub(1).map(SequenceNumber::new)
        );
        prop_assert_eq!(
            eth.journal().last_sequence(),
            next_eth.checked_sub(1).map(SequenceNumber::new)
        );
    }

    /// `journal_replay_reconstructs_book`: an arbitrary stream of limit adds
    /// (random side / price / quantity against one contract) captured through a
    /// fresh [`MatchingExecutor`] reconstructs **identical** fills and top-of-book
    /// when the same stream is replayed on a second fresh instance. This is the
    /// bounded determinism oracle scoped to per-underlying order state: the
    /// engine's `Uuid` order ids and wall-clock trade timestamps are excluded, so
    /// two runs of the same command prefix agree on the captured venue artifacts
    /// ([02 §5](../docs/02-matching-architecture.md)). The full harness is #017.
    #[test]
    fn journal_replay_reconstructs_book(
        orders in prop::collection::vec((any::<bool>(), 1u64..=1_000_000, 1u64..=20), 1..12)
    ) {
        let lineage = LineageId::new("run-1");
        let symbol = Symbol::parse("BTC-20240329-50000-C")
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;

        // Build the deterministic command stream: one limit add per element, its
        // venue order id minted from the id grammar at its sequence.
        let commands: Vec<VenueCommand> = orders
            .iter()
            .enumerate()
            .map(|(index, &(is_buy, price, quantity))| {
                let sequence = SequenceNumber::new(index as u64);
                VenueCommand::AddOrder {
                    symbol: symbol.clone(),
                    order_id: lineage.venue_order_id("BTC", sequence, 0),
                    account: AccountId::new(format!("acct-{index}")),
                    owner: Hash32([index as u8; 32]),
                    client_order_id: None,
                    side: if is_buy { SeamSide::Buy } else { SeamSide::Sell },
                    order_type: OrderType::Limit,
                    limit_price: Some(Cents::new(price)),
                    quantity,
                    time_in_force: SeamTif::Gtc,
                    stp_mode: STPMode::None,
                }
            })
            .collect();

        let replay = |lineage: &LineageId| -> (Vec<VenueOutcome>, TopOfBook) {
            let mut executor = MatchingExecutor::new("BTC");
            let outcomes = commands
                .iter()
                .enumerate()
                .map(|(index, command)| {
                    executor.execute(ExecutionContext {
                        underlying: "BTC",
                        lineage_id: lineage,
                        sequence: SequenceNumber::new(index as u64),
                        venue_ts: EventTimestamp::new(1),
                        command,
                    })
                })
                .collect();
            (outcomes, executor.top_of_book(&symbol))
        };

        let (outcomes_a, top_a) = replay(&lineage);
        let (outcomes_b, top_b) = replay(&lineage);
        prop_assert_eq!(outcomes_a, outcomes_b);
        prop_assert_eq!(top_a, top_b);
    }

    /// `journal_driver_replay_reconstructs_book` (#030): a randomly generated
    /// journaled session (limit adds against one contract, driven through the real
    /// single-writer actor into its write-ahead journal) replays through the
    /// **production replay driver** ([`replay_streams`]) into a **fresh** registry
    /// to the **identical** ordered `VenueEvent` stream and top-of-book. This
    /// exercises the same re-execution core recovery uses, over the driver's public
    /// `JournalStream` input, so the driver is not a second apply path
    /// ([04 §4](../docs/04-market-data-and-replay.md#4-historical-replay)).
    #[test]
    fn journal_driver_replay_reconstructs_book(
        orders in prop::collection::vec((any::<bool>(), 1u64..=1_000_000, 1u64..=20), 1..12)
    ) {
        let lineage = LineageId::new("run-1");
        let symbol = Symbol::parse("BTC-20240329-50000-C")
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;

        // Drive the random stream through a REAL single-writer actor so the journal
        // is exactly what the live venue writes.
        let header = JournalHeader::new(lineage.clone());
        let mut actor = UnderlyingActor::new(
            ActorConfig::new("BTC", lineage.clone(), 64),
            InMemoryVenueJournal::new(header.clone()),
            MatchingExecutor::new("BTC"),
            NoopFanOut,
            FixedClock::new(EventTimestamp::new(1_700_000_000_000)),
        );
        for (index, &(is_buy, price, quantity)) in orders.iter().enumerate() {
            let sequence = SequenceNumber::new(index as u64);
            let command = VenueCommand::AddOrder {
                symbol: symbol.clone(),
                order_id: lineage.venue_order_id("BTC", sequence, 0),
                account: AccountId::new(format!("acct-{index}")),
                owner: Hash32([index as u8; 32]),
                client_order_id: None,
                side: if is_buy { SeamSide::Buy } else { SeamSide::Sell },
                order_type: OrderType::Limit,
                limit_price: Some(Cents::new(price)),
                quantity,
                time_in_force: SeamTif::Gtc,
                stp_mode: STPMode::None,
            };
            actor
                .handle(command)
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
        }
        let records = actor
            .journal()
            .read_from(SequenceNumber::START)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let stored_events: Vec<VenueEvent> = records
            .iter()
            .filter_map(|record| match record {
                JournalRecord::Event(event) => Some(event.clone()),
                _ => None,
            })
            .collect();

        // Two independent driver replays into fresh registries agree with each
        // other AND with the recorded stream — the persistent-path oracle.
        let stream = JournalStream::new("BTC", header, records);
        let report_a = replay_streams(std::slice::from_ref(&stream))
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let report_b = replay_streams(std::slice::from_ref(&stream))
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let replay_a = report_a
            .underlying("BTC")
            .ok_or_else(|| TestCaseError::fail("BTC replay missing"))?;
        let replay_b = report_b
            .underlying("BTC")
            .ok_or_else(|| TestCaseError::fail("BTC replay missing"))?;
        prop_assert_eq!(&replay_a.events, &stored_events);
        prop_assert_eq!(&replay_a.events, &replay_b.events);
        prop_assert_eq!(replay_a.top_of_book(&symbol), replay_b.top_of_book(&symbol));
    }

    /// The **store-level** `journal_replay_reconstructs_book` invariant (#029): an
    /// arbitrary sequence of write-ahead `(command, event)` pairs appended to a
    /// fresh journal reads back through `read_from(0)` as **exactly** those records,
    /// in append (`N`) order — the durable substrate the recovery reducer
    /// re-executes over. This is the physical-store contract the durable
    /// `PgVenueJournal` swaps in behind unchanged; end-to-end book reconstruction is
    /// asserted in the determinism + integration suites
    /// ([TESTING.md §3](../docs/TESTING.md#3-property-tests)).
    #[test]
    fn journal_read_from_returns_appended_pairs_in_n_order(count in 0usize..40) {
        let lineage = LineageId::new("run-1");
        let mut journal = InMemoryVenueJournal::new(JournalHeader::new(lineage));
        let symbol = Symbol::parse("BTC-20240329-50000-C")
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;

        let mut expected: Vec<JournalRecord> = Vec::with_capacity(count * 2);
        for n in 0..count as u64 {
            let seq = SequenceNumber::new(n);
            let order_id = VenueOrderId::new(format!("order-{n}"));
            let command = VenueCommand::CancelOrder {
                symbol: symbol.clone(),
                order_id: order_id.clone(),
                account: AccountId::new("acct-1"),
            };
            let cmd = JournalRecord::command(seq, EventTimestamp::new(1), command.clone());
            let evt = JournalRecord::event(VenueEvent::new(
                seq,
                EventTimestamp::new(1),
                command,
                VenueOutcome::Cancelled { order_id },
            ));
            journal
                .append(cmd.clone())
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            journal
                .append(evt.clone())
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            expected.push(cmd);
            expected.push(evt);
        }

        let read = journal
            .read_from(SequenceNumber::START)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert_eq!(&read, &expected);

        // Filtering the command records yields ascending, gapless `N` (0..count).
        let sequences: Vec<u64> = read
            .iter()
            .filter(|record| record.kind() == RecordKind::Command)
            .map(|record| record.sequence().get())
            .collect();
        let expected_sequences: Vec<u64> = (0..count as u64).collect();
        prop_assert_eq!(sequences, expected_sequences);
    }

    /// The WS market-data `instrument_sequence` is strictly increasing **per
    /// instrument** and gapless (`1, 2, 3, …`) across an arbitrary interleaving of
    /// book mutations, independently for two instruments (`true` → the call,
    /// `false` → the put). This is the market-data gap-detection namespace — a
    /// separate counter from the journaled `underlying_sequence` — folded through
    /// the subscription manager exactly as the actor's `WsFanOut` feeds it.
    #[test]
    fn ws_instrument_sequence_monotonic_per_symbol(
        routes in prop::collection::vec(any::<bool>(), 0..96),
    ) {
        let manager = OrderbookSubscriptionManager::new();
        let call = Symbol::parse("BTC-20240329-50000-C")
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
        let put = Symbol::parse("BTC-20240329-50000-P")
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;

        // A resting limit add (no fills) always changes the book, so it always
        // emits a delta and advances that instrument's sequence.
        let resting_add = |symbol: &fauxchange::exchange::Symbol, index: u64| -> VenueEvent {
            let command = VenueCommand::AddOrder {
                symbol: symbol.clone(),
                order_id: VenueOrderId::new(format!("o-{}-{index}", symbol.as_str())),
                account: AccountId::new("acct"),
                owner: Hash32([1; 32]),
                client_order_id: None,
                side: SeamSide::Sell,
                // A unique price per add so each rests at its own level.
                limit_price: Some(Cents::new(50_000 + index)),
                order_type: OrderType::Limit,
                quantity: 1,
                time_in_force: SeamTif::Gtc,
                stp_mode: STPMode::None,
            };
            VenueEvent::new(
                SequenceNumber::new(index),
                EventTimestamp::new(1),
                command,
                VenueOutcome::Added {
                    fills: vec![],
                    resting_quantity: 1,
                    stp_cancelled: vec![],
                },
            )
        };

        let mut next_call = 0u64;
        let mut next_put = 0u64;
        for (index, route) in routes.into_iter().enumerate() {
            let (symbol, expected) = if route {
                next_call += 1;
                (&call, next_call)
            } else {
                next_put += 1;
                (&put, next_put)
            };
            let sequence = manager.on_committed_event(&resting_add(symbol, index as u64));
            prop_assert_eq!(
                sequence,
                Some(expected),
                "each instrument's sequence advances gaplessly and independently"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, max_shrink_iters: 50_000, ..ProptestConfig::default() })]

    /// A position's realized + unrealized P&L is exactly the net cash flow plus
    /// the mark-to-market of the open position, in integer cents, across any fill
    /// sequence. The store's fold computes both halves from one exact cost basis,
    /// so the identity holds bit-for-bit (the truncated `avg_price` is never used
    /// in the P&L). Inputs are bounded so the independent `i128` cross-check
    /// cannot overflow.
    #[test]
    fn position_pnl_stays_consistent_across_fills(
        legs in proptest::collection::vec(
            (any::<bool>(), 1u64..1_000, 1u64..1_000_000, -1_000i64..1_000),
            1..30usize,
        ),
        mark in 1u64..1_000_000,
    ) {
        let store = InMemoryPositionsStore::new();
        let account = AccountId::new("acct");
        let symbol = Symbol::parse("BTC-20240329-50000-C")
            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;

        // Independently accumulate the mark-to-market baseline: net cash flow is
        // `-Σ(signed_qty × price) - Σ(fee)`, net quantity is `Σ(signed_qty)`.
        let mut expected_cash: i128 = 0;
        let mut expected_net: i128 = 0;
        for (is_buy, quantity, price, fee) in &legs {
            let dq: i128 = if *is_buy { i128::from(*quantity) } else { -i128::from(*quantity) };
            expected_net += dq;
            expected_cash -= dq * i128::from(*price);
            expected_cash -= i128::from(*fee);
            store
                .apply(&PositionLeg {
                    account: &account,
                    symbol: &symbol,
                    underlying: "BTC",
                    side: if *is_buy { SeamSide::Buy } else { SeamSide::Sell },
                    quantity: *quantity,
                    price: Cents::new(*price),
                    fee: SignedCents::new(*fee),
                })
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
        }

        let position = store
            .get(&account, &symbol, Some(Cents::new(mark)))
            .map_err(|e| TestCaseError::fail(e.to_string()))?
            .ok_or_else(|| TestCaseError::fail("expected a folded position"))?;

        prop_assert_eq!(i128::from(position.net_quantity), expected_net);
        let realized = i128::from(position.realized_pnl.get());
        let unrealized = i128::from(position.unrealized_pnl.map(SignedCents::get).unwrap_or(0));
        let total_mtm = expected_cash + expected_net * i128::from(mark);
        prop_assert_eq!(realized + unrealized, total_mtm);
    }
}

// ============================================================================
// Market-maker persona quoting (#015)
// ============================================================================
//
// - `mm_persona_spread_widens_with_multiplier` — a wider `spread_multiplier`
//   never narrows the quoted spread (the clamp knob is honoured monotonically).
// - `mm_persona_skew_shifts_symmetric` — directional skew is a same-signed
//   PARALLEL shift of bid and ask (the spread width is preserved), across the
//   clamp range `[-1.0, 1.0]`.

proptest! {
    #![proptest_config(ProptestConfig { cases: 1024, max_shrink_iters: 50_000, ..ProptestConfig::default() })]

    /// A wider spread multiplier (within `[0.1, 10.0]`) must not narrow the
    /// quoted spread — the persona knob is honoured monotonically.
    #[test]
    fn mm_persona_spread_widens_with_multiplier(
        spot in 100_000u64..10_000_000,
        strike_ratio in 70u64..130,
        days in 1.0f64..365.0,
        iv in 0.05f64..1.5,
        m_lo in 0.1f64..5.0,
        m_delta in 0.0f64..4.9,
        is_call in any::<bool>(),
    ) {
        let strike = (spot.saturating_mul(strike_ratio) / 100).max(1);
        let m_hi = (m_lo + m_delta).min(10.0);
        let style = if is_call { OptionStyle::Call } else { OptionStyle::Put };
        let quoter = Quoter::default();
        let input = |mult| QuoteInput {
            spot_cents: spot,
            strike_cents: strike,
            days_to_expiry: days,
            style,
            spread_multiplier: mult,
            size_scalar: 1.0,
            directional_skew: 0.0,
            iv: Some(iv),
        };
        if let (Some(narrow), Some(wide)) =
            (quoter.generate_quote(&input(m_lo)), quoter.generate_quote(&input(m_hi)))
        {
            let narrow_spread = narrow.ask_price.get() - narrow.bid_price.get();
            let wide_spread = wide.ask_price.get() - wide.bid_price.get();
            prop_assert!(
                wide_spread >= narrow_spread,
                "a wider multiplier must not narrow the spread: {wide_spread} < {narrow_spread}"
            );
        }
    }

    /// Directional skew shifts the bid and the ask by the SAME signed amount
    /// (a parallel shift that preserves the spread width), across `[-1.0, 1.0]`.
    #[test]
    fn mm_persona_skew_shifts_symmetric(
        spot in 1_000_000u64..10_000_000,
        strike_ratio in 90u64..110,
        days in 20.0f64..365.0,
        iv in 0.2f64..1.0,
        skew in -1.0f64..1.0,
        is_call in any::<bool>(),
    ) {
        let strike = (spot.saturating_mul(strike_ratio) / 100).max(1);
        let style = if is_call { OptionStyle::Call } else { OptionStyle::Put };
        let quoter = Quoter::default();
        // A wide multiplier + a large near-ATM theo keep both legs comfortably
        // above their floors, so the parallel shift is not clipped.
        let input = |sk| QuoteInput {
            spot_cents: spot,
            strike_cents: strike,
            days_to_expiry: days,
            style,
            spread_multiplier: 10.0,
            size_scalar: 1.0,
            directional_skew: sk,
            iv: Some(iv),
        };
        if let (Some(neutral), Some(skewed)) =
            (quoter.generate_quote(&input(0.0)), quoter.generate_quote(&input(skew)))
        {
            // Only assert when neither floor clipped a leg (large theo case).
            prop_assume!(neutral.bid_price.get() > 1 && skewed.bid_price.get() > 1);
            prop_assume!(
                skewed.ask_price.get() > skewed.bid_price.get() + 1
                    && neutral.ask_price.get() > neutral.bid_price.get() + 1
            );
            let bid_shift = skewed.bid_price.get() as i128 - neutral.bid_price.get() as i128;
            let ask_shift = skewed.ask_price.get() as i128 - neutral.ask_price.get() as i128;
            prop_assert_eq!(
                bid_shift, ask_shift,
                "skew must shift bid and ask by the same signed amount"
            );
            // The spread width is preserved under the parallel shift.
            prop_assert_eq!(
                skewed.ask_price.get() - skewed.bid_price.get(),
                neutral.ask_price.get() - neutral.bid_price.get()
            );
        }
    }
}

// ============================================================================
// Layered config validation (#022)
// ============================================================================
//
// - `config_validate_rejects_out_of_range` — for the v0.2 config knobs (clock
//   mode, log format, run seed, bind address), the layered validator accepts a
//   value IFF it is genuinely valid; every other value fails fast with the
//   matching typed `ConfigError` (never a silent default). Each knob is exercised
//   independently through the public `Config::load_from` CLI seam (the others
//   keep their defaults), so a single knob's out-of-range value is isolated from
//   the fixed validation order. The harness stands up here for v0.5 (#44–#47) to
//   extend with the microstructure ranges.

proptest! {
    #![proptest_config(ProptestConfig { cases: 1024, max_shrink_iters: 50_000, ..ProptestConfig::default() })]

    #[test]
    fn config_validate_rejects_out_of_range(
        clock_token in "[a-zA-Z]{0,12}",
        log_token in "[a-zA-Z]{0,12}",
        seed_token in "[A-Za-z0-9 +-]{0,24}",
        addr_token in "[A-Za-z0-9.:]{0,24}",
    ) {
        // The env layer is always empty; each knob is overridden via one CLI flag
        // so the others keep their valid defaults and cannot mask this knob.
        let empty_env = |_: &str| None;

        // ---- clock mode: valid IFF a known token ----
        let clock_result = Config::load_from(
            vec!["--clock".to_string(), clock_token.clone()],
            empty_env,
        );
        match ClockMode::from_token(&clock_token) {
            Some(mode) => {
                let config = clock_result.map_err(|e| TestCaseError::fail(e.to_string()))?;
                prop_assert_eq!(config.clock.mode, mode);
            }
            None => match clock_result {
                Err(ConfigError::InvalidClock { value }) => prop_assert_eq!(value, clock_token),
                other => {
                    return Err(TestCaseError::fail(format!(
                        "clock '{clock_token}' must be InvalidClock, got {other:?}"
                    )));
                }
            },
        }

        // ---- log format: valid IFF a known token ----
        let log_result = Config::load_from(
            vec!["--log-format".to_string(), log_token.clone()],
            empty_env,
        );
        match LogFormat::from_token(&log_token) {
            Some(format) => {
                let config = log_result.map_err(|e| TestCaseError::fail(e.to_string()))?;
                prop_assert_eq!(config.logging.format, format);
            }
            None => match log_result {
                Err(ConfigError::InvalidLogFormat { value }) => prop_assert_eq!(value, log_token),
                other => {
                    return Err(TestCaseError::fail(format!(
                        "log format '{log_token}' must be InvalidLogFormat, got {other:?}"
                    )));
                }
            },
        }

        // ---- run seed: valid IFF it parses as u64 (the validator trims) ----
        let seed_result = Config::load_from(
            vec!["--seed".to_string(), seed_token.clone()],
            empty_env,
        );
        match seed_token.trim().parse::<u64>() {
            Ok(seed) => {
                let config = seed_result.map_err(|e| TestCaseError::fail(e.to_string()))?;
                prop_assert_eq!(config.determinism.seed, seed);
            }
            Err(_) => match seed_result {
                Err(ConfigError::BadSeed { value }) => prop_assert_eq!(value, seed_token),
                other => {
                    return Err(TestCaseError::fail(format!(
                        "seed '{seed_token}' must be BadSeed, got {other:?}"
                    )));
                }
            },
        }

        // ---- bind address: valid IFF it parses as a SocketAddr ----
        let addr_result = Config::load_from(
            vec!["--http-addr".to_string(), addr_token.clone()],
            empty_env,
        );
        match addr_token.parse::<std::net::SocketAddr>() {
            Ok(addr) => {
                let config = addr_result.map_err(|e| TestCaseError::fail(e.to_string()))?;
                prop_assert_eq!(config.server.http_addr, addr);
            }
            Err(_) => match addr_result {
                Err(ConfigError::BadAddress { field, value, .. }) => {
                    prop_assert_eq!(field, "http_addr");
                    prop_assert_eq!(value, addr_token);
                }
                other => {
                    return Err(TestCaseError::fail(format!(
                        "address '{addr_token}' must be BadAddress, got {other:?}"
                    )));
                }
            },
        }
    }
}

// ============================================================================
// FIX 4.4 vocabulary (#036): the checked Price seam is never lossy and every
// typed message survives an encode∘decode round trip over its input space.
// ============================================================================

/// A canonical symbol drawn from a fixed valid set (always parseable).
fn fix_symbol() -> impl Strategy<Value = Symbol> {
    prop::sample::select(vec![
        "BTC-20240329-50000-C",
        "BTC-20240329-50000-P",
        "ETH-20251222-3000-C",
        "AAPL-20240119-190-P",
    ])
    .prop_map(|raw| Symbol::parse(raw).expect("fixture symbol parses"))
}

/// A valid `CompID` over an uppercase alphabet within the 32-byte limit.
fn fix_comp_id() -> impl Strategy<Value = CompId> {
    "[A-Z]{1,8}".prop_map(|raw| CompId::new(&raw).expect("comp id within limit"))
}

/// A standard header with an arbitrary comp-id pair and sequence number and a
/// fixed, well-formed sending time.
fn fix_header() -> impl Strategy<Value = StandardHeader> {
    (fix_comp_id(), fix_comp_id(), any::<u64>()).prop_map(|(sender, target, seq)| {
        StandardHeader::new(
            sender,
            target,
            SeqNum::new(seq),
            UtcTimestamp::parse(52, "20240329-12:00:00.000").expect("sending time"),
        )
    })
}

/// A client order id over a colon-friendly alphabet (composite ids use `:`).
fn fix_clordid() -> impl Strategy<Value = ClientOrderId> {
    "[A-Za-z0-9:_-]{1,16}".prop_map(ClientOrderId::new)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 1024, ..ProptestConfig::default() })]

    /// The seam renders any `Cents` value to a decimal and parses it back to the
    /// identical cents — no float drift, exact both ways.
    #[test]
    fn fix_price_seam_cents_never_lossy(raw in any::<u64>()) {
        let cents = Cents::new(raw);
        let decimal = render_cents_to_decimal(cents);
        // Exactly two fractional digits (the venue cents scale), one dot.
        prop_assert_eq!(decimal.matches('.').count(), 1);
        let fractional = decimal.split('.').nth(1).unwrap_or("");
        prop_assert_eq!(fractional.len(), 2);
        let back = parse_decimal_to_cents(&decimal)
            .map_err(|e| TestCaseError::fail(format!("parse {decimal} failed: {e:?}")))?;
        prop_assert_eq!(back, cents);
    }

    /// An on-tick price (a whole multiple of the tick) always survives the full
    /// tick-keyed seam, and an off-tick price is always rejected.
    #[test]
    fn fix_price_scale_admits_on_tick_and_rejects_off_tick(
        tick in 2u64..=1000,
        multiple in 0u64..=1_000_000,
        offset in 1u64..1000,
    ) {
        let scale = PriceScale::new(tick)
            .map_err(|e| TestCaseError::fail(format!("scale build failed: {e:?}")))?;
        let on_tick = Cents::new(tick.saturating_mul(multiple));
        let decimal = render_cents_to_decimal(on_tick);
        let parsed = scale.decimal_to_cents(&decimal)
            .map_err(|e| TestCaseError::fail(format!("on-tick {decimal} rejected: {e:?}")))?;
        prop_assert_eq!(parsed, on_tick);

        // An offset strictly between 0 and the tick is off-tick and rejected.
        let off = offset % tick;
        if off != 0 {
            let off_cents = Cents::new(on_tick.get().saturating_add(off));
            let off_decimal = render_cents_to_decimal(off_cents);
            prop_assert!(scale.decimal_to_cents(&off_decimal).is_err());
        }
    }

    /// A `NewOrderSingle (D)` survives an encode∘decode round trip over the full
    /// space of side / type / TIF / price / account / symbol combinations, with
    /// the conditional Price/ExpireTime requiredness satisfied by construction.
    #[test]
    fn fix_new_order_single_encode_decode_round_trip(
        header in fix_header(),
        cl_ord_id in fix_clordid(),
        account in proptest::option::of("[a-z0-9-]{1,12}"),
        symbol in fix_symbol(),
        side in prop::sample::select(vec![OrderSide::Buy, OrderSide::Sell]),
        limit in any::<bool>(),
        price_cents in any::<u32>(),
        order_qty in 1u64..=1_000_000,
        tif_index in 0usize..5,
    ) {
        let (ord_type, price) = if limit {
            (FixOrdType::Limit, Some(Cents::new(u64::from(price_cents))))
        } else {
            (FixOrdType::Market, None)
        };
        let (time_in_force, expire_time) = match tif_index {
            0 => (FixTif::Day, None),
            1 => (FixTif::Gtc, None),
            2 => (FixTif::Ioc, None),
            3 => (FixTif::Fok, None),
            _ => (
                FixTif::Gtd,
                Some(UtcTimestamp::parse(126, "20240329-23:59:59.000").expect("expire time")),
            ),
        };
        let order = NewOrderSingle {
            header,
            cl_ord_id,
            account: account.map(AccountId::new),
            symbol,
            side,
            transact_time: UtcTimestamp::parse(60, "20240329-12:00:00.000").expect("transact time"),
            ord_type,
            price,
            order_qty,
            time_in_force,
            expire_time,
        };
        let bytes = DecodedMessage::NewOrderSingle(order.clone()).encode();
        match decode(&bytes) {
            Ok(DecodedMessage::NewOrderSingle(back)) => prop_assert_eq!(back, order),
            other => return Err(TestCaseError::fail(format!("expected NewOrderSingle, got {other:?}"))),
        }
    }

    /// A `MarketDataSnapshotFullRefresh (W)` with an arbitrary number of book
    /// entries survives an encode∘decode round trip, preserving entry order and
    /// per-level cents exactly.
    #[test]
    fn fix_market_data_snapshot_encode_decode_round_trip(
        header in fix_header(),
        rpt_seq in any::<u64>(),
        symbol in fix_symbol(),
        entries in prop::collection::vec(
            (
                prop::sample::select(vec![MdEntryType::Bid, MdEntryType::Offer, MdEntryType::Trade]),
                any::<u32>(),
                any::<u32>(),
            ),
            0..8,
        ),
    ) {
        let entries: Vec<SnapshotEntry> = entries
            .into_iter()
            .map(|(entry_type, price, size)| SnapshotEntry {
                entry_type,
                price: Cents::new(u64::from(price)),
                size: u64::from(size),
            })
            .collect();
        let snapshot = MarketDataSnapshotFullRefresh {
            header,
            md_req_id: "MDR-1".to_string(),
            symbol,
            rpt_seq: SequenceNumber::new(rpt_seq),
            entries,
        };
        let bytes = DecodedMessage::MarketDataSnapshotFullRefresh(snapshot.clone()).encode();
        match decode(&bytes) {
            Ok(DecodedMessage::MarketDataSnapshotFullRefresh(back)) => prop_assert_eq!(back, snapshot),
            other => return Err(TestCaseError::fail(format!("expected snapshot, got {other:?}"))),
        }
    }
}
