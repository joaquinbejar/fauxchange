//! Integration tests for [`AppState`], the application-layer wiring, exercised
//! through the **public** surface from an external crate
//! ([010](../milestones/v0.1-backend-core/010-appstate-wiring.md)).
//!
//! These stand in for a gateway: they hold an `Arc<AppState>` and reach the order
//! path **only** through [`AppState::submit`] — there is no public book / leaf /
//! sequencer accessor to bypass it — then read the fills back from the same
//! shared stores `AppState` exposes, proving the post-journal fan-out and the
//! read surface point at one set of stores.

use std::sync::Arc;

use fauxchange::exchange::{
    Cents, ExecutionFilter, ExecutionsStore, FanoutSummary, Hash32, InstrumentStatus, LineageId,
    PositionsStore, STPMode, Side, Symbol, TimeInForce, VenueCommand, VenueOutcome,
};
use fauxchange::microstructure::{ContractSpecsConfig, FileMicrostructure, MicrostructureConfig};
use fauxchange::state::{AppState, AppStateConfig, AppStateError};
use fauxchange::{AccountId, ClientOrderId, LiquidityFlag, OrderType, VenueError, VenueOrderId};

fn state(underlyings: &[&str]) -> Arc<AppState> {
    // Auth defaults to the embedded dev key pair (no accounts) when the config
    // carries none; construction is fallible only on that auth build.
    match AppState::new(
        AppStateConfig::new(underlyings.iter().copied()).with_lineage(LineageId::new("run-1")),
    ) {
        Ok(state) => state,
        Err(error) => panic!("AppState::new must succeed with dev auth: {error}"),
    }
}

fn sym(raw: &str) -> Symbol {
    match Symbol::parse(raw) {
        Ok(s) => s,
        Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
    }
}

#[allow(clippy::too_many_arguments)]
fn add(
    symbol: &str,
    order_id: &str,
    account: &str,
    owner: u8,
    side: Side,
    price: u64,
    quantity: u64,
) -> VenueCommand {
    VenueCommand::AddOrder {
        symbol: sym(symbol),
        order_id: VenueOrderId::new(order_id),
        account: AccountId::new(account),
        owner: Hash32([owner; 32]),
        client_order_id: None,
        side,
        order_type: OrderType::Limit,
        limit_price: Some(Cents::new(price)),
        quantity,
        time_in_force: TimeInForce::Gtc,
        stp_mode: STPMode::None,
    }
}

/// #098 REST/FIX parity seam: a client-order-id resolves to a venue order id
/// through the **single** [`AppState::resolve_client_order_id`] both gateways use
/// (FIX's `resolve_order`, and any REST cancel/replace/status-by-client-order-id).
/// A placement lands the account-scoped mapping; a lookup on another account is a
/// masked miss — one account can never resolve another's order.
#[tokio::test]
async fn test_client_order_id_resolves_through_the_shared_account_scoped_seam() {
    let state = state(&["BTC"]);
    let symbol = "BTC-20240329-50000-C";
    let account = AccountId::new("trader-1");
    let clid = ClientOrderId::new("cl-parity-1");

    // Empty before any placement — the ONLY way to populate the index is the
    // sequenced submit path (the same command REST and FIX both build).
    assert!(state.resolve_client_order_id(&account, &clid).is_none());

    let order_id = VenueOrderId::new("order-parity-1");
    state
        .submit(VenueCommand::AddOrder {
            symbol: sym(symbol),
            order_id: order_id.clone(),
            account: account.clone(),
            owner: Hash32([0x33; 32]),
            client_order_id: Some(clid.clone()),
            side: Side::Buy,
            order_type: OrderType::Limit,
            limit_price: Some(Cents::new(50_000)),
            quantity: 2,
            time_in_force: TimeInForce::Gtc,
            stp_mode: STPMode::None,
        })
        .await
        .expect("place");

    // The shared seam maps the client id to the venue order id, on the account.
    let resolved = state
        .resolve_client_order_id(&account, &clid)
        .expect("the placement is resolvable cross-session via the shared seam");
    assert_eq!(resolved.order_id, order_id);
    assert_eq!(resolved.side, Side::Buy);
    assert_eq!(resolved.quantity, 2);

    // Account isolation: another account with the SAME client id gets a masked miss.
    assert!(
        state
            .resolve_client_order_id(&AccountId::new("intruder"), &clid)
            .is_none(),
        "a colliding client id on another account is indistinguishable from unknown"
    );
}

/// #114 item 1: `AppState::new` re-runs `MicrostructureConfig::validate()` on the
/// live path (before spawning any actor), so a config that arrived deserialized
/// (bypassing the `resolve` proof) with an out-of-domain spec knob fails fast with a
/// typed `AppStateError::Microstructure`, never serving a request.
#[tokio::test]
async fn test_appstate_new_rejects_invalid_microstructure_before_spawning_actors() {
    // Start from the neutral default, then poison one persisted spec knob past the
    // durable BIGINT (i64) domain via a serde round-trip — the same path a hostile /
    // legacy bundle takes to reach `AppState` without going through `resolve`.
    let mut json = serde_json::to_value(MicrostructureConfig::default())
        .expect("the default microstructure serializes");
    json["default_specs"]["max_price_cents"] = serde_json::json!(u64::MAX);
    let hostile: MicrostructureConfig =
        serde_json::from_value(json).expect("the poisoned microstructure deserializes");

    let config = AppStateConfig::new(["BTC"])
        .with_lineage(LineageId::new("run-ms-invalid"))
        .with_microstructure(hostile);
    match AppState::new(config) {
        Err(AppStateError::Microstructure(_)) => {}
        Err(other) => panic!("expected AppStateError::Microstructure, got {other:?}"),
        Ok(_) => panic!("an invalid microstructure config must not build an AppState"),
    }
}

/// A gateway stand-in submits a crossing pair through the ONLY path and reads the
/// two linked fill legs back from the shared executions store `AppState` exposes.
#[tokio::test]
async fn test_submit_end_to_end_lands_the_fill_in_the_shared_store() {
    let state = state(&["BTC"]);
    let symbol = "BTC-20240329-50000-C";

    // Before any submit, the shared store the reader sees is empty: the ONLY way
    // to populate it is the sequenced submit path.
    assert!(state.executions().is_empty());

    // Resting maker sell.
    match state
        .submit(add(symbol, "maker-1", "maker", 0x11, Side::Sell, 50_000, 2))
        .await
    {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence.get(), 0),
        Err(e) => panic!("maker submit failed: {e}"),
    }
    // No fill yet — the maker only rests.
    assert!(state.executions().is_empty());

    // Crossing taker buy at the same price.
    match state
        .submit(add(symbol, "taker-1", "taker", 0x22, Side::Buy, 50_000, 2))
        .await
    {
        Ok(receipt) => assert_eq!(receipt.underlying_sequence.get(), 1),
        Err(e) => panic!("taker submit failed: {e}"),
    }

    // The post-journal fan-out recorded BOTH legs into the very store the reader
    // holds — same `Arc` on the write and read side.
    assert_eq!(state.executions().len(), 2);

    let maker_legs = state
        .executions()
        .list(&AccountId::new("maker"), &ExecutionFilter::default())
        .expect("maker executions");
    let taker_legs = state
        .executions()
        .list(&AccountId::new("taker"), &ExecutionFilter::default())
        .expect("taker executions");
    assert_eq!(maker_legs.len(), 1);
    assert_eq!(taker_legs.len(), 1);
    assert_eq!(maker_legs[0].liquidity, LiquidityFlag::Maker);
    assert_eq!(taker_legs[0].liquidity, LiquidityFlag::Taker);
    // The two legs share one execution id (the cross-surface join key).
    assert_eq!(maker_legs[0].execution_id, taker_legs[0].execution_id);
    assert_eq!(taker_legs[0].price_cents, Cents::new(50_000));
    assert_eq!(taker_legs[0].quantity, 2);

    // The same match folded into both accounts' positions in the shared fold.
    let symbol_obj = sym(symbol);
    let maker_pos = state
        .positions()
        .get(&AccountId::new("maker"), &symbol_obj, None)
        .expect("maker position get")
        .expect("a maker position");
    let taker_pos = state
        .positions()
        .get(&AccountId::new("taker"), &symbol_obj, None)
        .expect("taker position get")
        .expect("a taker position");
    assert_eq!(maker_pos.net_quantity, -2); // sold
    assert_eq!(taker_pos.net_quantity, 2); // bought
}

/// Two underlyings sequence independently: a fill on `BTC` never touches `ETH`'s
/// stream, and each underlying's actor assigns its own sequence from 0.
#[tokio::test]
async fn test_two_underlyings_route_and_sequence_independently() {
    let state = state(&["BTC", "ETH"]);

    // A BTC crossing pair.
    for cmd in [
        add(
            "BTC-20240329-50000-C",
            "b-mk",
            "mk",
            0x11,
            Side::Sell,
            50_000,
            1,
        ),
        add(
            "BTC-20240329-50000-C",
            "b-tk",
            "tk",
            0x22,
            Side::Buy,
            50_000,
            1,
        ),
    ] {
        state.submit(cmd).await.expect("BTC submit");
    }
    // An ETH resting order — its own actor, its own sequence starting at 0.
    let eth_receipt = state
        .submit(add(
            "ETH-20240329-3000-C",
            "e-mk",
            "mk",
            0x33,
            Side::Sell,
            3_000,
            1,
        ))
        .await
        .expect("ETH submit");
    assert_eq!(eth_receipt.underlying_sequence.get(), 0);

    // Only the BTC match produced fills (2 legs); the ETH order merely rests.
    assert_eq!(state.executions().len(), 2);
    let btc = state
        .executions()
        .list(&AccountId::new("tk"), &ExecutionFilter::default())
        .expect("list");
    assert_eq!(btc.len(), 1);
    assert_eq!(btc[0].symbol, "BTC");
}

/// A submit for an underlying this venue does not host is a typed `NotFound`,
/// never a silent drop or a panic.
#[tokio::test]
async fn test_submit_unhosted_underlying_is_typed_not_found() {
    let state = state(&["BTC"]);
    match state
        .submit(add(
            "ETH-20240329-3000-C",
            "x",
            "acct",
            0x11,
            Side::Buy,
            3_000,
            1,
        ))
        .await
    {
        Err(VenueError::NotFound(detail)) => assert!(detail.contains("ETH")),
        other => panic!("expected NotFound, got {other:?}"),
    }
    // The unroutable submit never touched the shared store.
    assert!(state.executions().is_empty());
}

/// The per-underlying journal is reachable read-only through `AppState`, routed to
/// the owning actor — the read side of the journal handle.
#[tokio::test]
async fn test_journal_snapshot_is_routed_and_read_only() {
    let state = state(&["BTC"]);
    state
        .submit(add(
            "BTC-20240329-50000-C",
            "o1",
            "acct",
            0x11,
            Side::Sell,
            50_000,
            1,
        ))
        .await
        .expect("submit");
    let snapshot = state.journal_snapshot("BTC").await.expect("snapshot");
    // The committed command + paired event are journaled at sequence 0.
    assert_eq!(snapshot.last_sequence.map(|s| s.get()), Some(0));
    assert_eq!(snapshot.records.len(), 2);

    // An unhosted underlying's journal is a typed NotFound.
    match state.journal_snapshot("ETH").await {
        Err(VenueError::NotFound(_)) => {}
        other => panic!("expected NotFound, got {other:?}"),
    }
}

// ============================================================================
// #118 — the sequenced outcome is surfaced on the receipt through `submit`
// ============================================================================

/// An order into a **halted** instrument is a journaled `Rejected` — and `submit`
/// now surfaces that observed outcome on the receipt, so a caller reading only the
/// live return value can never believe the rejected order took effect (#118 Gap 1).
#[tokio::test]
async fn test_order_into_halted_instrument_surfaces_rejected_through_submit() {
    let state = state(&["BTC"]);
    let symbol = "BTC-20240329-50000-C";

    // Halt the call on the sequenced path; the transition applies.
    let halt = state
        .submit(VenueCommand::SetInstrumentStatus {
            symbol: sym(symbol),
            status: InstrumentStatus::Halted,
        })
        .await
        .expect("halt submit");
    match &halt.outcome {
        Some(VenueOutcome::InstrumentStatusChanged { status, .. }) => {
            assert_eq!(*status, InstrumentStatus::Halted);
        }
        other => panic!("halt must surface InstrumentStatusChanged, got {other:?}"),
    }

    // A GTC order into the halted book is a journaled Rejected — surfaced, not a
    // false accept (a resting TIF would otherwise read as "accepted; resting").
    let rejected = state
        .submit(add(symbol, "o1", "trader-1", 0x22, Side::Buy, 50_000, 3))
        .await
        .expect("the actor turn commits even though the command is a captured Rejected");
    match &rejected.outcome {
        Some(VenueOutcome::Rejected { reason, .. }) => {
            assert!(
                reason.contains("Halted"),
                "reason names the status: {reason}"
            );
        }
        other => panic!("an order into a halted instrument must surface Rejected, got {other:?}"),
    }
    // No fill landed — the reject was a true no-op on the book.
    assert!(state.executions().is_empty());
}

/// A `SetInstrumentStatus` that is an **illegal** lifecycle transition (resume an
/// `Expired` instrument) surfaces a `Rejected` outcome through `submit`, not a
/// false applied confirmation (#118 Gap 3).
#[tokio::test]
async fn test_illegal_status_transition_surfaces_rejected_through_submit() {
    let state = state(&["BTC"]);
    let symbol = "BTC-20240329-50000-C";
    for status in [InstrumentStatus::Settling, InstrumentStatus::Expired] {
        state
            .submit(VenueCommand::SetInstrumentStatus {
                symbol: sym(symbol),
                status,
            })
            .await
            .expect("lifecycle submit");
    }
    // Expired is terminal: resume-an-Expired is rejected by the upstream state
    // machine and the reject is surfaced on the receipt.
    let illegal = state
        .submit(VenueCommand::SetInstrumentStatus {
            symbol: sym(symbol),
            status: InstrumentStatus::Active,
        })
        .await
        .expect("the actor turn commits even though the transition is a captured Rejected");
    match &illegal.outcome {
        Some(VenueOutcome::Rejected { .. }) => {}
        other => panic!("an illegal transition must surface Rejected, got {other:?}"),
    }
}

/// Two fresh venues replay the same command stream to the **same surfaced
/// `VenueOutcome`** on each receipt — the outcome the gateway renders is exactly
/// the replay-stable journaled outcome (#118 determinism).
#[tokio::test]
async fn test_surfaced_outcomes_are_deterministic_across_two_venues() {
    async fn run() -> Vec<Option<VenueOutcome>> {
        let state = state(&["BTC"]);
        let symbol = "BTC-20240329-50000-C";
        let commands = vec![
            VenueCommand::SetInstrumentStatus {
                symbol: sym(symbol),
                status: InstrumentStatus::Halted,
            },
            // Into the halted book → Rejected.
            add(symbol, "h", "trader-1", 0x22, Side::Buy, 50_000, 1),
            VenueCommand::SetInstrumentStatus {
                symbol: sym(symbol),
                status: InstrumentStatus::Active,
            },
            // Resting maker, then a crossing taker → Added-with-fills.
            add(symbol, "m", "maker", 0x11, Side::Sell, 50_000, 2),
            add(symbol, "t", "taker", 0x22, Side::Buy, 50_000, 2),
        ];
        let mut surfaced = Vec::with_capacity(commands.len());
        for command in commands {
            match state.submit(command).await {
                Ok(receipt) => surfaced.push(receipt.outcome),
                Err(e) => panic!("submit failed: {e}"),
            }
        }
        surfaced
    }

    let first = run().await;
    let second = run().await;
    assert_eq!(
        first, second,
        "the same journal surfaces the same VenueOutcome on each receipt"
    );
    // Non-vacuous: the stream really covered a reject and a crossing fill.
    assert!(matches!(&first[1], Some(VenueOutcome::Rejected { .. })));
    assert!(matches!(
        &first[4],
        Some(VenueOutcome::Added { fills, .. }) if !fills.is_empty()
    ));
}

// ============================================================================
// #118 — partial venue-global fan-out visibility on the receipt
// ============================================================================

/// A venue-global `MarketMakerControl` fanned across every underlying reports its
/// delivery on the representative receipt: a healthy fan-out is `ok_count == total`
/// and `fully_applied` (#118 Gap 2 — the summary is real and counts every hosted
/// underlying, so a partial fan-out would read `ok_count < total`).
#[tokio::test]
async fn test_venue_global_fanout_reports_full_delivery_on_the_receipt() {
    let state = state(&["BTC", "ETH", "SOL"]);
    let receipt = state
        .submit(VenueCommand::MarketMakerControl {
            spread_multiplier: None,
            size_scalar: None,
            directional_skew: None,
            enabled: Some(false),
        })
        .await
        .expect("venue-global control fans out");
    match receipt.fanout {
        Some(summary) => {
            assert_eq!(
                summary,
                FanoutSummary {
                    ok_count: 3,
                    total: 3
                }
            );
            assert!(
                summary.fully_applied(),
                "a healthy fan-out is fully applied"
            );
        }
        None => panic!("a venue-global control must carry a fan-out summary"),
    }
    // Every underlying's stream journaled the control (ControlApplied per underlying).
    // No market maker is resting here, so the coupled kill sweep is empty.
    match &receipt.outcome {
        Some(VenueOutcome::ControlApplied { swept }) => {
            assert!(swept.is_empty(), "no MM orders rest, so nothing is swept");
        }
        other => panic!("the representative outcome is ControlApplied, got {other:?}"),
    }
}

/// #114 item 5: the per-symbol contract-spec override — configured TIGHTER than its
/// underlying — genuinely gates ORDER acceptance on the live `AppState::submit` seam.
/// The overridden contract carries a 5-cent tick, 2-lot, 10-contract cap, and a
/// `[100, 200_000]` band; its BTC underlying keeps the wide venue default. An order
/// that satisfies the underlying but violates the per-symbol tick / lot / max-qty /
/// band is rejected with a typed `VenueError::InvalidOrder`; one satisfying the
/// per-symbol profile is accepted; and a sibling contract falls back to the
/// (looser) underlying default.
#[tokio::test]
async fn test_per_symbol_override_gates_order_acceptance_live() {
    let overridden = "BTC-20240329-50000-C";
    let mut instrument_specs = std::collections::BTreeMap::new();
    instrument_specs.insert(
        overridden.to_string(),
        ContractSpecsConfig {
            tick_size_cents: Some(5),
            lot_size: Some(2),
            min_price_cents: Some(100),
            max_price_cents: Some(200_000),
            max_order_qty: Some(10),
        },
    );
    let microstructure =
        MicrostructureConfig::resolve(&FileMicrostructure::default(), &instrument_specs)
            .expect("per-symbol config resolves");
    let state = match AppState::new(
        AppStateConfig::new(["BTC"])
            .with_lineage(LineageId::new("run-per-symbol"))
            .with_microstructure(microstructure),
    ) {
        Ok(state) => state,
        Err(error) => panic!("AppState::new must succeed: {error}"),
    };

    // ACCEPT: price 500 (on the per-symbol 5-tick, in-band), qty 4 (a 2-lot multiple,
    // ≤ the 10 cap) satisfies the per-symbol profile.
    state
        .submit(add(overridden, "ok-1", "trader", 0x11, Side::Buy, 500, 4))
        .await
        .expect("an order satisfying the per-symbol profile is accepted");

    // REJECT off-tick: 503 is on the underlying's 1-cent tick but off the per-symbol
    // 5-cent tick → typed InvalidOrder naming the tick.
    match state
        .submit(add(
            overridden,
            "bad-tick",
            "trader",
            0x11,
            Side::Buy,
            503,
            4,
        ))
        .await
    {
        Err(VenueError::InvalidOrder(detail)) => {
            assert!(
                detail.contains("tick"),
                "the reject names the tick: {detail}"
            );
        }
        other => panic!("an off-per-symbol-tick order must be rejected, got {other:?}"),
    }

    // REJECT off-lot: qty 3 is a valid underlying quantity but off the per-symbol 2-lot.
    match state
        .submit(add(
            overridden,
            "bad-lot",
            "trader",
            0x11,
            Side::Buy,
            500,
            3,
        ))
        .await
    {
        Err(VenueError::InvalidOrder(detail)) => {
            assert!(detail.contains("lot"), "the reject names the lot: {detail}");
        }
        other => panic!("an off-per-symbol-lot order must be rejected, got {other:?}"),
    }

    // REJECT above the per-symbol max quantity: 12 clears the underlying's 1_000_000
    // cap but violates the per-symbol 10 cap.
    match state
        .submit(add(
            overridden,
            "bad-qty",
            "trader",
            0x11,
            Side::Buy,
            500,
            12,
        ))
        .await
    {
        Err(VenueError::InvalidOrder(detail)) => {
            assert!(
                detail.contains("max order quantity"),
                "the reject names the max quantity: {detail}"
            );
        }
        other => panic!("an over-per-symbol-cap order must be rejected, got {other:?}"),
    }

    // REJECT below the per-symbol band: 50 is on the 5-tick and above the underlying's
    // 1-cent floor, but below the per-symbol 100-cent floor.
    match state
        .submit(add(
            overridden,
            "bad-band",
            "trader",
            0x11,
            Side::Buy,
            50,
            4,
        ))
        .await
    {
        Err(VenueError::InvalidOrder(detail)) => {
            assert!(
                detail.contains("min_price_cents"),
                "the reject names the per-symbol band floor: {detail}"
            );
        }
        other => panic!("a below-per-symbol-band order must be rejected, got {other:?}"),
    }

    // FALLBACK: a sibling BTC contract with no per-symbol override falls back to the
    // (looser) BTC underlying default — 503 / qty 3 are admitted there.
    state
        .submit(add(
            "BTC-20240329-60000-C",
            "sibling-ok",
            "trader",
            0x11,
            Side::Buy,
            503,
            3,
        ))
        .await
        .expect("a sibling contract falls back to the looser underlying default");
}
