//! The seam by which a generated price step reaches the venue — the simulation
//! analogue of the market-maker [`CommandSink`](crate::market_maker::CommandSink)
//! ([016](../../milestones/v0.1-backend-core/016-price-simulator-walks.md),
//! [03 §10](../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
//!
//! ## A price step is two journaled effects, never a bare write
//!
//! A generated (or manually overridden) price is applied through a [`StepSink`],
//! which does **two** journaled things and never touches a book directly:
//!
//! 1. It routes the move onto the per-underlying single-writer actor as a
//!    [`VenueCommand::SimStep`], so the price step is journaled and replays
//!    (a bare price write would silently bypass the actor and break replay,
//!    [03 §10](../../docs/03-protocol-surfaces.md#10-state-changing-operation-classification)).
//! 2. **Only once that `SimStep` is confirmed sequenced**, it drives the market
//!    maker for the same underlying ([`MarketMakerEngine::update_price`]), whose
//!    requotes enter the **same** actor path as their own journaled
//!    [`AddOrder`](VenueCommand::AddOrder)s — so the synthetic liquidity a price
//!    move induces is journaled and replayable exactly like real order flow
//!    ([04 §2](../../docs/04-market-data-and-replay.md#2-synthetic-price-generation)).
//!
//! The sink is the one place that bridges the simulator to both the sequencer and
//! the market maker, so the simulator itself stays free of a direct actor or
//! market-maker dependency.
//!
//! ## Causal order: the SimStep is sequenced before its requotes (rule 3)
//!
//! The market maker is advanced on the **forwarder task**, immediately after the
//! `SimStep`'s `submit` receipt confirms it is journaled — never eagerly on the
//! simulation thread. This closes a causality hole: the requote `AddOrder`s a
//! price move induces flow through the market maker's own (independent) forwarder,
//! so if the drive fired before the `SimStep` was sequenced a requote could be
//! journaled **before** its causing step. Gating the drive on the confirmed
//! `SimStep` guarantees every requote is sequenced after the step that caused it,
//! and a step the sequencer never admitted (dropped by backpressure, unhosted, or
//! rejected by the actor) drives **no** requote at all.
//!
//! ## Off the client path (rule 8)
//!
//! [`StepSink::apply_step`] is synchronous and **non-blocking**: the `SimStep` is
//! handed to a bounded ordered forwarder (`try_send`, drop-and-warn when full) and
//! then returns. The market-maker drive and the `submit` await run on the
//! forwarder task, never on the simulation loop's thread, and no lock is ever held
//! across the enqueue.
//!
//! [`MarketMakerEngine::update_price`]: crate::market_maker::MarketMakerEngine::update_price

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::exchange::{ActorHandle, Cents, EventTimestamp, VenueCommand};
use crate::market_maker::MarketMakerEngine;

/// The seam by which the simulator applies one price step to the venue: it routes
/// a [`VenueCommand::SimStep`] onto the sequenced path **and** drives the market
/// maker's requote for the same underlying, so both the move and its derived
/// liquidity are journaled.
pub trait StepSink: Send + Sync {
    /// Applies one price step for `underlying` at the caller-supplied venue-clock
    /// `now_ms` (never `SystemTime`). Non-blocking; ordering of the `SimStep`s
    /// handed to a single sink is preserved.
    ///
    /// Returns whether the step was **admitted** onto the sequenced path. The
    /// caller MUST journal-before-publish: a step reported dropped (`false` —
    /// backpressure or a closed forwarder) has **no** journal record, so the
    /// caller must NOT publish a price for it, or a subscriber would observe a
    /// price replay cannot reproduce (rule 3). `true` means the `SimStep` was
    /// accepted onto the ordered forwarder that sequences it.
    #[must_use = "a dropped step (false) has no journal record; the caller must not publish its price"]
    fn apply_step(
        &self,
        now_ms: EventTimestamp,
        underlying: &str,
        price: Cents,
        bid: Option<Cents>,
        ask: Option<Cents>,
    ) -> bool;
}

/// The default bounded capacity of the [`VenueStepSink`] forwarding channel — a
/// DoS control, never unbounded. A full channel drops the `SimStep` with a `WARN`
/// (the next tick supersedes it) rather than blocking the simulation loop.
pub const DEFAULT_STEP_SINK_CAPACITY: usize = 4_096;

/// The production [`StepSink`]: routes each `SimStep` to the right per-underlying
/// [`ActorHandle`] through a single ordered forwarder task (off the simulation
/// loop's thread), and drives the [`MarketMakerEngine`] so a price move requotes.
///
/// `apply_step` performs a non-blocking `try_send` of the `SimStep` onto a bounded
/// channel and returns; a dedicated forwarder task drains it **in order**,
/// `submit`s each to the underlying's actor, and — **only after** that submit
/// confirms the step is sequenced — calls [`MarketMakerEngine::set_venue_now_ms`]
/// and [`MarketMakerEngine::update_price`], which cascade the requote `AddOrder`s
/// through the engine's own sink. Because the drive follows the confirmed
/// `SimStep`, a requote is never journaled before its causing step, and a dropped
/// or rejected step drives no requote (rule 3). A slow actor turn delays only
/// synthetic prices, never a client order (rule 8).
///
/// [`MarketMakerEngine::set_venue_now_ms`]: crate::market_maker::MarketMakerEngine::set_venue_now_ms
/// [`MarketMakerEngine::update_price`]: crate::market_maker::MarketMakerEngine::update_price
pub struct VenueStepSink {
    tx: mpsc::Sender<VenueCommand>,
}

impl VenueStepSink {
    /// Builds the sink over the venue's per-underlying actor handles and the
    /// market-maker engine, spawning its forwarder task. Must be called within a
    /// `tokio` runtime.
    #[must_use]
    pub fn new(
        handles: HashMap<Arc<str>, ActorHandle>,
        market_maker: Arc<MarketMakerEngine>,
    ) -> Arc<Self> {
        Self::with_capacity(handles, market_maker, DEFAULT_STEP_SINK_CAPACITY)
    }

    /// Builds the sink with an explicit bounded forwarder capacity.
    #[must_use]
    pub fn with_capacity(
        handles: HashMap<Arc<str>, ActorHandle>,
        market_maker: Arc<MarketMakerEngine>,
        capacity: usize,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        tokio::spawn(
            StepForwarder {
                handles,
                rx,
                market_maker,
            }
            .run(),
        );
        Arc::new(Self { tx })
    }
}

impl StepSink for VenueStepSink {
    fn apply_step(
        &self,
        now_ms: EventTimestamp,
        underlying: &str,
        price: Cents,
        bid: Option<Cents>,
        ask: Option<Cents>,
    ) -> bool {
        // Hand the price move to the ordered forwarder as a `SimStep`. The
        // market-maker drive is NOT done here: it happens on the forwarder task
        // only after this step is confirmed sequenced, so a requote can never be
        // journaled before its causing step and a dropped step drives no requote
        // (rule 3). A full or closed channel drops the step with a WARN; the next
        // tick supersedes it (rule 8) and, because the step is never sequenced, no
        // requote fires for it — and, reported as `false`, the caller publishes no
        // price for it either.
        let command = VenueCommand::SimStep {
            now_ms,
            underlying: underlying.to_string(),
            price,
            bid,
            ask,
        };
        match self.tx.try_send(command) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    underlying,
                    "simulation step sink is full; dropping a SimStep (backpressure)"
                );
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!(
                    underlying,
                    "simulation step sink is closed; dropping a SimStep"
                );
                false
            }
        }
    }
}

/// The forwarder task: drains the bounded channel in order, submits each
/// `SimStep` onto its underlying's actor, and — **only after** that submit
/// confirms the step is sequenced — drives the market maker for the same
/// underlying, so its requotes are journaled strictly after their causing step.
struct StepForwarder {
    handles: HashMap<Arc<str>, ActorHandle>,
    rx: mpsc::Receiver<VenueCommand>,
    market_maker: Arc<MarketMakerEngine>,
}

impl StepForwarder {
    async fn run(mut self) {
        while let Some(command) = self.rx.recv().await {
            // A `SimStep` carries its routing underlying and the market-maker drive
            // inputs directly; anything else is unroutable here and is dropped
            // without driving the maker.
            let VenueCommand::SimStep {
                now_ms,
                underlying,
                price,
                ..
            } = &command
            else {
                tracing::warn!("simulation step carries no routable underlying; dropping");
                continue;
            };
            // Copy the drive inputs out before `submit` consumes the command; the
            // requote cascade fires only once the step is sequenced.
            let now_ms = *now_ms;
            let price = *price;
            let underlying = underlying.clone();

            let Some(handle) = self.handles.get(underlying.as_str()) else {
                tracing::warn!(
                    underlying,
                    "simulation step routes to an unhosted underlying; dropping"
                );
                continue;
            };
            // Sequence the `SimStep` FIRST and confirm it is journaled before
            // driving the market maker. A rejected step (full mailbox, sealed
            // underlying) is dropped and drives no requote; the next tick
            // supersedes it.
            if let Err(error) = handle.submit(command).await {
                tracing::debug!(error = %error, "simulation step command not accepted");
                continue;
            }
            // The step is now journaled. Advance the maker's venue clock to the
            // step's instant (so time-to-expiry stays consistent with the sim
            // clock) and update the price, which cascades the requote `AddOrder`s
            // through the engine's own sink — journaled, replayable, and strictly
            // after the step that caused them (rule 3).
            self.market_maker.set_venue_now_ms(now_ms.get());
            self.market_maker.update_price(&underlying, price.get());
        }
        tracing::debug!("simulation step forwarder stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use crate::exchange::{LineageId, Symbol};
    use crate::market_maker::{CommandSink, Quoter};

    const BTC_CALL: &str = "BTC-20351231-50000-C";

    /// A [`CommandSink`] that records the requote commands the market maker
    /// enqueues, for assertions that no requote fired.
    #[derive(Default)]
    struct CollectingSink {
        commands: Mutex<Vec<VenueCommand>>,
    }

    impl CollectingSink {
        fn count(&self) -> usize {
            self.commands.try_lock().map_or(0, |guard| guard.len())
        }
    }

    impl CommandSink for CollectingSink {
        fn enqueue(&self, command: VenueCommand) {
            if let Ok(mut guard) = self.commands.try_lock() {
                guard.push(command);
            }
        }
    }

    /// A market maker whose venue clock sits well before the fixture's far-future
    /// expiry, with the call registered so a driven price would requote — returned
    /// with the recording sink so a test can assert no requote fired.
    fn market_maker() -> (Arc<MarketMakerEngine>, Arc<CollectingSink>) {
        let sink = Arc::new(CollectingSink::default());
        let engine =
            MarketMakerEngine::new(sink.clone(), LineageId::new("run-1"), Quoter::default());
        engine.set_venue_now_ms(1_735_689_600_000);
        let symbol = Symbol::parse(BTC_CALL).expect("valid fixture symbol");
        engine.register_instrument(&symbol);
        (Arc::new(engine), sink)
    }

    /// A step routed to an underlying no actor hosts is dropped by the forwarder
    /// and drives NO market-maker advance: `update_price` is unreachable without a
    /// hosting handle, so the maker never learns the price and never requotes.
    #[tokio::test]
    async fn test_dropped_step_drives_no_market_maker_advance() {
        let (mm, sink) = market_maker();
        // No handles: every step routes to an "unhosted" underlying and is dropped
        // before any `submit`, so the market-maker drive is unreachable.
        let step_sink = VenueStepSink::new(HashMap::new(), Arc::clone(&mm));

        // `apply_step` admits the step onto the forwarder (the channel is open);
        // the forwarder then drops it because no handle hosts "BTC", so the maker
        // is never advanced. (Admission to the forwarder is not the same as the
        // actor accepting it.)
        let admitted = step_sink.apply_step(
            EventTimestamp::new(1_735_689_600_000),
            "BTC",
            Cents::new(5_000_000),
            None,
            None,
        );
        assert!(
            admitted,
            "the open forwarder admits the step before it drops it"
        );

        // Let the forwarder drain the step (and prove it dropped it): the maker is
        // never advanced, so it holds no price and enqueued no requote.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            mm.get_price("BTC"),
            None,
            "a dropped step never advances the market maker"
        );
        assert_eq!(
            sink.count(),
            0,
            "a dropped step enqueues no requote command"
        );
    }
}
