//! The **late-bound market-maker control hub** — the seam that resolves the
//! "engine needs the actor handles / the actors want the control sink" cycle at
//! wiring time (#047 phase 2).
//!
//! ## The chicken-and-egg
//!
//! The per-underlying single-writer actors are spawned **first**, and each carries
//! an optional [`MarketMakerControlSink`] so a committed
//! [`VenueCommand::MarketMakerControl`](crate::exchange::VenueCommand::MarketMakerControl)
//! takes effect on the sequenced path. But the [`MarketMakerEngine`] that *is* that
//! sink is built **after** the actors — it needs their [`ActorHandle`](crate::exchange::ActorHandle)s
//! to route its requotes. So the engine cannot be handed to the actors at spawn time.
//!
//! [`MarketMakerControlHub`] breaks the cycle: it is created **before** the actors,
//! shared into each executor as the sink, and **bound** to the engine once the engine
//! exists. Until it is bound it is an inert no-op; the venue binds it inside
//! [`AppState::new`](crate::state::AppState) — synchronously, before it ever serves —
//! so no control can arrive at an unbound hub in practice.
//!
//! ## Determinism (rule 3)
//!
//! The hub is installed **only** on the live order path. The replay/recovery
//! reconstruction executors install **no** sink, so re-execution derives the
//! identical [`ControlApplied`](crate::exchange::VenueOutcome::ControlApplied) event
//! without ever driving a live engine — the requotes a control induces are journaled
//! as their own `AddOrder` commands ([02 §5](../../docs/02-matching-architecture.md)).

use std::sync::{Arc, OnceLock};

use crate::exchange::{MarketMakerControlKnobs, MarketMakerControlSink};
use crate::market_maker::engine::MarketMakerEngine;

/// A late-bound [`MarketMakerControlSink`]: shared into every underlying's executor
/// at spawn time, then bound to the [`MarketMakerEngine`] once it is constructed.
///
/// Bind-once via an [`OnceLock`]; a control applied before the bind is dropped with a
/// `WARN` (unreachable in the wired venue, where the bind precedes serving).
#[derive(Default)]
pub struct MarketMakerControlHub {
    engine: OnceLock<Arc<MarketMakerEngine>>,
}

impl std::fmt::Debug for MarketMakerControlHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The engine is large and not `Debug`; report only the bind state.
        f.debug_struct("MarketMakerControlHub")
            .field("bound", &self.is_bound())
            .finish()
    }
}

impl MarketMakerControlHub {
    /// Builds an unbound hub behind an `Arc`, ready to hand to the executors as the
    /// sequenced control sink.
    #[must_use]
    #[inline]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            engine: OnceLock::new(),
        })
    }

    /// Binds the hub to the market-maker `engine` — idempotent-safe: a second bind is
    /// ignored with a `WARN` (the venue binds exactly once, inside `AppState::new`).
    pub fn bind(&self, engine: Arc<MarketMakerEngine>) {
        if self.engine.set(engine).is_err() {
            tracing::warn!("market-maker control hub already bound; ignoring the rebind");
        }
    }

    /// Whether the hub has been bound to an engine.
    #[must_use]
    #[inline]
    pub fn is_bound(&self) -> bool {
        self.engine.get().is_some()
    }
}

impl MarketMakerControlSink for MarketMakerControlHub {
    #[inline]
    fn apply_control(&self, knobs: MarketMakerControlKnobs) {
        match self.engine.get() {
            Some(engine) => engine.apply_sequenced_control(knobs),
            None => tracing::warn!(
                "market-maker control applied before the hub was bound; dropping (no engine)"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::LineageId;
    use crate::market_maker::sink::CommandSink;
    use crate::market_maker::{MarketMakerConfig, Quoter};

    /// A no-op sink so an engine can be constructed without a runtime.
    struct NullSink;
    impl CommandSink for NullSink {
        fn enqueue(&self, _command: crate::exchange::VenueCommand) {}
    }

    fn engine() -> Arc<MarketMakerEngine> {
        Arc::new(MarketMakerEngine::new(
            Arc::new(NullSink),
            LineageId::new("run-1"),
            Quoter::default(),
        ))
    }

    #[test]
    fn test_unbound_hub_drops_control_without_panicking() {
        let hub = MarketMakerControlHub::new();
        assert!(!hub.is_bound());
        // A control before bind is a no-op (dropped), never a panic.
        hub.apply_control(MarketMakerControlKnobs {
            spread_multiplier: Some(2.0),
            size_scalar: None,
            directional_skew: None,
            enabled: None,
        });
    }

    #[test]
    fn test_bound_hub_applies_control_to_the_engine() {
        let engine = engine();
        let hub = MarketMakerControlHub::new();
        hub.bind(Arc::clone(&engine));
        assert!(hub.is_bound());
        assert_eq!(engine.get_config(), MarketMakerConfig::default());

        hub.apply_control(MarketMakerControlKnobs {
            spread_multiplier: Some(2.5),
            size_scalar: Some(0.5),
            directional_skew: Some(-0.25),
            enabled: Some(false),
        });
        let config = engine.get_config();
        assert_eq!(config.spread_multiplier, 2.5);
        assert_eq!(config.size_scalar, 0.5);
        assert_eq!(config.directional_skew, -0.25);
        assert!(!config.enabled);
    }

    #[test]
    fn test_rebind_is_ignored() {
        let hub = MarketMakerControlHub::new();
        let first = engine();
        hub.bind(Arc::clone(&first));
        // A second bind is ignored (the first engine stays bound).
        hub.bind(engine());
        hub.apply_control(MarketMakerControlKnobs {
            spread_multiplier: Some(3.0),
            size_scalar: None,
            directional_skew: None,
            enabled: None,
        });
        assert_eq!(first.get_config().spread_multiplier, 3.0);
    }
}
