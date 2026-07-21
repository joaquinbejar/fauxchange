//! #028 integration: the price-walk cadence, the market maker it drives, and the
//! sequenced-path `venue_ts` all run off the **one** injected venue clock — an
//! accelerated run advances the cadence at the configured multiplier
//! ([04 §2](../docs/04-market-data-and-replay.md#2-synthetic-price-generation),
//! [04 §5](../docs/04-market-data-and-replay.md#5-clock-control)).
//!
//! The clock is driven with **controlled wall instants** ([`SimClock::track_wall`]),
//! so these assertions are deterministic rather than racing the real wall clock;
//! the live cadence loop reads `SystemTime` in the off-path driver
//! ([`SimClock::tick`]), which is exercised by the clock unit tests.

use std::sync::Arc;

use fauxchange::exchange::{Cents, EventTimestamp, VenueCommand};
use fauxchange::models::{AccountId, VenueOrderId};
use fauxchange::simulation::{AssetConfig, PriceUpdate, VenueClockConfig, WalkTypeConfig};
use fauxchange::state::{AppState, AppStateConfig};
use tokio::sync::broadcast;

const UNDERLYING: &str = "BTC";
const SYMBOL: &str = "BTC-20240329-50000-C";
const MULTIPLIER: u32 = 60;
const START_MS: u64 = 1_000_000_000_000;

/// An `AppState` over one walked underlying on an **accelerated** venue clock — the
/// one clock the actors, the simulator, and the rate limiter share.
fn accelerated_state() -> Arc<AppState> {
    let config = AppStateConfig::new([UNDERLYING])
        .with_clock(VenueClockConfig::accelerated(START_MS, MULTIPLIER))
        .with_assets(vec![AssetConfig::new(
            UNDERLYING,
            Cents::new(5_000_000),
            0.20,
            WalkTypeConfig::GeometricBrownian,
        )]);
    match AppState::new(config) {
        Ok(state) => state,
        Err(e) => panic!("AppState with dev auth must build: {e}"),
    }
}

/// The `now_ms` of the next broadcast price update.
fn recv_now_ms(rx: &mut broadcast::Receiver<PriceUpdate>) -> u64 {
    match rx.try_recv() {
        Ok(update) => update.now_ms.get(),
        Err(e) => panic!("expected a broadcast price update: {e:?}"),
    }
}

/// A BTC cancel — the cheapest command that returns a receipt carrying `venue_ts`.
fn cancel() -> VenueCommand {
    VenueCommand::CancelOrder {
        symbol: match fauxchange::exchange::Symbol::parse(SYMBOL) {
            Ok(symbol) => symbol,
            Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
        },
        order_id: VenueOrderId::new("order-1"),
        account: AccountId::new("acct-1"),
    }
}

#[tokio::test]
async fn test_accelerated_clock_advances_price_walk_cadence_at_multiplier() {
    let state = accelerated_state();
    let sim = state.simulator();
    let mut prices = sim.subscribe();

    // The price-walk cadence runs off the injected clock: drive it with controlled
    // wall instants and step the walk — each SimStep is stamped from the SAME clock,
    // advancing at the multiplier.
    state.clock().track_wall(10_000); // anchor at the epoch
    sim.step_once();
    let first = recv_now_ms(&mut prices);
    assert_eq!(
        first, START_MS,
        "the first emit is at the anchored virtual epoch"
    );

    state.clock().track_wall(10_100); // +100 ms of wall time
    sim.step_once();
    let second = recv_now_ms(&mut prices);
    // 100 ms of wall time × 60 = 6_000 ms of virtual time advanced.
    assert_eq!(
        second,
        START_MS + 100 * u64::from(MULTIPLIER),
        "the accelerated cadence advanced at the configured multiplier"
    );
    assert_eq!(second - first, 6_000);
}

#[tokio::test]
async fn test_sequenced_venue_ts_and_price_walk_share_the_one_injected_clock() {
    let state = accelerated_state();

    // Advance the injected clock off the sequenced path (accelerated wall-tracking).
    state.clock().track_wall(20_000); // anchor
    state.clock().track_wall(20_500); // +500 ms wall × 60 = 30_000 virtual ms
    let expected = START_MS + 500 * u64::from(MULTIPLIER);
    assert_eq!(state.clock().now_ms().get(), expected);

    // A sequenced order's venue_ts is stamped from that SAME clock — so the actor's
    // venue_ts and the price-walk's now_ms are one injected clock, not two.
    let receipt = match state.submit(cancel()).await {
        Ok(receipt) => receipt,
        Err(e) => panic!("cancel must route to the BTC actor: {e}"),
    };
    assert_eq!(
        receipt.venue_ts,
        EventTimestamp::new(expected),
        "venue_ts is stamped from the same injected clock the price walk reads"
    );

    // And a walk step emitted after the advance carries the identical instant.
    let sim = state.simulator();
    let mut prices = sim.subscribe();
    sim.step_once();
    assert_eq!(
        recv_now_ms(&mut prices),
        expected,
        "the SimStep now_ms matches the advanced venue clock"
    );
}
