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
    Cents, ExecutionFilter, ExecutionsStore, Hash32, LineageId, PositionsStore, STPMode, Side,
    Symbol, TimeInForce, VenueCommand,
};
use fauxchange::state::{AppState, AppStateConfig};
use fauxchange::{AccountId, LiquidityFlag, OrderType, VenueError, VenueOrderId};

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
