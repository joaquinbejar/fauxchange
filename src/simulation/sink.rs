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
//! 2. It drives the market maker for the same underlying
//!    ([`MarketMakerEngine::update_price`]), whose requotes enter the **same**
//!    actor path as their own journaled [`AddOrder`](VenueCommand::AddOrder)s — so
//!    the synthetic liquidity a price move induces is journaled and replayable
//!    exactly like real order flow
//!    ([04 §2](../../docs/04-market-data-and-replay.md#2-synthetic-price-generation)).
//!
//! The sink is the one place that bridges the simulator to both the sequencer and
//! the market maker, so the simulator itself stays free of a direct actor or
//! market-maker dependency.
//!
//! ## Off the client path (rule 8)
//!
//! [`StepSink::apply_step`] is synchronous and **non-blocking**: the `SimStep` is
//! handed to a bounded ordered forwarder (`try_send`, drop-and-warn when full),
//! and the market-maker drive is itself non-blocking. No lock is ever held across
//! the enqueue.
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
    /// `now_ms` (never `SystemTime`). Non-blocking, fire-and-forget; ordering of
    /// the `SimStep`s handed to a single sink is preserved.
    fn apply_step(
        &self,
        now_ms: EventTimestamp,
        underlying: &str,
        price: Cents,
        bid: Option<Cents>,
        ask: Option<Cents>,
    );
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
/// channel; a dedicated forwarder task drains it **in order** and `submit`s each
/// to the underlying's actor, dropping the receipt — so a slow actor turn delays
/// only synthetic prices, never a client order (rule 8). It then calls
/// [`MarketMakerEngine::set_venue_now_ms`] and [`MarketMakerEngine::update_price`],
/// which cascade the requote `AddOrder`s through the engine's own sink.
///
/// [`MarketMakerEngine::set_venue_now_ms`]: crate::market_maker::MarketMakerEngine::set_venue_now_ms
/// [`MarketMakerEngine::update_price`]: crate::market_maker::MarketMakerEngine::update_price
pub struct VenueStepSink {
    tx: mpsc::Sender<VenueCommand>,
    market_maker: Arc<MarketMakerEngine>,
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
        tokio::spawn(StepForwarder { handles, rx }.run());
        Arc::new(Self { tx, market_maker })
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
    ) {
        // 1. Journal the price move as a `SimStep` on the underlying's actor.
        let command = VenueCommand::SimStep {
            now_ms,
            underlying: underlying.to_string(),
            price,
            bid,
            ask,
        };
        match self.tx.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    underlying,
                    "simulation step sink is full; dropping a SimStep (backpressure)"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!(
                    underlying,
                    "simulation step sink is closed; dropping a SimStep"
                );
            }
        }

        // 2. Drive the market maker: advance its venue clock to the step's instant
        //    (so time-to-expiry stays consistent with the sim clock) and update the
        //    price, which cascades the requote `AddOrder`s through the engine's own
        //    sink — journaled and replayable, never a direct book mutation.
        self.market_maker.set_venue_now_ms(now_ms.get());
        self.market_maker.update_price(underlying, price.get());
    }
}

/// The forwarder task: drains the bounded channel in order and submits each
/// `SimStep` onto its underlying's actor, dropping the receipt.
struct StepForwarder {
    handles: HashMap<Arc<str>, ActorHandle>,
    rx: mpsc::Receiver<VenueCommand>,
}

impl StepForwarder {
    async fn run(mut self) {
        while let Some(command) = self.rx.recv().await {
            let Some(underlying) = step_underlying(&command) else {
                tracing::warn!("simulation step carries no routable underlying; dropping");
                continue;
            };
            let Some(handle) = self.handles.get(underlying) else {
                tracing::warn!(
                    underlying,
                    "simulation step routes to an unhosted underlying; dropping"
                );
                continue;
            };
            if let Err(error) = handle.submit(command).await {
                // Best-effort: a rejected step (full mailbox, sealed underlying)
                // is dropped; the next tick supersedes it.
                tracing::debug!(error = %error, "simulation step command not accepted");
            }
        }
        tracing::debug!("simulation step forwarder stopped");
    }
}

/// The underlying a `SimStep` routes to (carried directly on the command, so no
/// symbol parse is needed). Every other command yields `None`.
#[inline]
fn step_underlying(command: &VenueCommand) -> Option<&str> {
    match command {
        VenueCommand::SimStep { underlying, .. } => Some(underlying.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_step_underlying_extracts_the_ticker() {
        let command = VenueCommand::SimStep {
            now_ms: EventTimestamp::new(1),
            underlying: "BTC".to_string(),
            price: Cents::new(5_000_000),
            bid: None,
            ask: None,
        };
        assert_eq!(step_underlying(&command), Some("BTC"));
        assert_eq!(
            step_underlying(&VenueCommand::Clock {
                now_ms: EventTimestamp::new(1),
            }),
            None
        );
    }
}
