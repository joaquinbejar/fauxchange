//! The venue-owned **instrument-status registry** — the sequenced per-instrument
//! lifecycle state the order path consults before admitting an order
//! ([01 §5](../../../docs/01-domain-model.md),
//! [ADR-0006](../../../docs/adr/0006-venue-command-envelope-and-single-writer-journal.md)).
//!
//! ## What it is (and why it is venue-owned)
//!
//! The upstream [`InstrumentStatus`] enum and its `Active → Halted → Settling →
//! Expired` lifecycle (plus the `Halted → Active` resume edge) are the single
//! source of truth for **which transitions are legal** — this registry never
//! reimplements that state machine, it delegates every transition to the upstream
//! [`InstrumentStatus::can_transition`]. What the venue owns is the **sequenced
//! projection**: a per-[`Symbol`] status map that a [`VenueCommand::SetInstrumentStatus`]
//! mutates and a [`VenueCommand::AddOrder`] reads, both **inside the single-writer
//! actor's turn**, so the whole thing is a deterministic function of the journal.
//!
//! ## Determinism ([02 §5](../../../docs/02-matching-architecture.md))
//!
//! The registry is **sequenced state**: it is only ever mutated by executing a
//! journaled `SetInstrumentStatus` command and only ever read by executing an
//! `AddOrder` command, both on the single-writer path. Reconstruction on replay is
//! therefore automatic and exact — the recovery reducer re-executes the same
//! command stream into a fresh [`MatchingExecutor`](crate::exchange::MatchingExecutor),
//! folding the identical status map — and it reads **no wall clock, no RNG, and no
//! map-iteration order** (every lookup is a `HashMap` point read keyed on the
//! canonical [`Symbol`]; an absent instrument defaults to the upstream default
//! [`InstrumentStatus::Active`], matching a freshly vivified leaf).
//!
//! [`VenueCommand::SetInstrumentStatus`]: crate::exchange::VenueCommand::SetInstrumentStatus
//! [`VenueCommand::AddOrder`]: crate::exchange::VenueCommand::AddOrder

use std::collections::HashMap;

use crate::exchange::boundary::InstrumentStatus;
use crate::exchange::symbol::Symbol;

/// An illegal instrument-status transition rejected by the upstream lifecycle
/// state machine ([`InstrumentStatus::can_transition`]).
///
/// Its [`Display`](std::fmt::Display) is the **deterministic** reason string a
/// [`VenueCommand::SetInstrumentStatus`](crate::exchange::VenueCommand::SetInstrumentStatus)
/// rejection carries into the journaled outcome, so a replay reproduces the exact
/// same reject text — it is a pure function of the two upstream statuses, never a
/// wall-clock or dynamic value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InstrumentStatusError {
    /// The requested `from -> to` edge is not legal in the upstream lifecycle
    /// (e.g. a move out of the terminal `Expired`, or `Settling -> Active`).
    #[error("illegal instrument status transition from {from} to {to}")]
    IllegalTransition {
        /// The instrument's current status.
        from: InstrumentStatus,
        /// The rejected target status.
        to: InstrumentStatus,
    },
}

/// The venue-owned, sequenced per-[`Symbol`] instrument-status registry.
///
/// A point-lookup map only; it is never iterated for order-affecting logic, so no
/// map-iteration order leaks into the sequenced path. An **absent** instrument is
/// [`InstrumentStatus::Active`] (the upstream default a freshly vivified leaf
/// carries), so the venue starts every contract accepting orders until a
/// `SetInstrumentStatus` halts, settles, or expires it.
#[derive(Debug, Clone, Default)]
pub struct InstrumentStatusRegistry {
    /// Symbols whose status has been changed away from the default; an absent
    /// symbol is [`InstrumentStatus::Active`].
    statuses: HashMap<Symbol, InstrumentStatus>,
}

impl InstrumentStatusRegistry {
    /// Builds an empty registry — every instrument is [`InstrumentStatus::Active`]
    /// until a transition records otherwise.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            statuses: HashMap::new(),
        }
    }

    /// The current status of `symbol`, defaulting to the upstream default
    /// [`InstrumentStatus::Active`] when no transition has been recorded — the
    /// same status a freshly vivified leaf book carries.
    #[must_use]
    #[inline]
    pub fn status_of(&self, symbol: &Symbol) -> InstrumentStatus {
        self.statuses
            .get(symbol)
            .copied()
            .unwrap_or(InstrumentStatus::Active)
    }

    /// Whether `symbol` is currently accepting new orders — `true` only for an
    /// [`InstrumentStatus::Active`] instrument (the upstream
    /// [`InstrumentStatus::is_accepting_orders`] rule).
    #[must_use]
    #[inline]
    pub fn is_accepting_orders(&self, symbol: &Symbol) -> bool {
        self.status_of(symbol).is_accepting_orders()
    }

    /// Validates and applies a lifecycle transition for `symbol`, delegating the
    /// legality check to the **upstream** [`InstrumentStatus::can_transition`]
    /// state machine (never a venue reimplementation).
    ///
    /// On success the new status is recorded and returned; a legal self-transition
    /// (`X -> X`) is an idempotent no-op that still succeeds. This is the only
    /// mutator, and it runs on the single-writer path, so the map stays a
    /// deterministic function of the journaled `SetInstrumentStatus` stream.
    ///
    /// # Errors
    ///
    /// [`InstrumentStatusError::IllegalTransition`] if the current-to-`to` edge is
    /// not legal — the registry is left unchanged.
    pub fn try_transition(
        &mut self,
        symbol: &Symbol,
        to: InstrumentStatus,
    ) -> Result<InstrumentStatus, InstrumentStatusError> {
        let from = self.status_of(symbol);
        if !from.can_transition(to) {
            return Err(InstrumentStatusError::IllegalTransition { from, to });
        }
        self.statuses.insert(symbol.clone(), to);
        Ok(to)
    }

    /// The number of instruments with a non-default recorded status.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.statuses.len()
    }

    /// Whether no instrument has a non-default recorded status.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.statuses.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sym(raw: &str) -> Symbol {
        match Symbol::parse(raw) {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol {raw} failed to parse: {e:?}"),
        }
    }

    const CALL: &str = "BTC-20240329-50000-C";

    #[test]
    fn test_absent_instrument_defaults_to_active() {
        let registry = InstrumentStatusRegistry::new();
        assert_eq!(registry.status_of(&sym(CALL)), InstrumentStatus::Active);
        assert!(registry.is_accepting_orders(&sym(CALL)));
        assert!(registry.is_empty());
    }

    #[test]
    fn test_halt_stops_accepting_orders() {
        let mut registry = InstrumentStatusRegistry::new();
        let applied = match registry.try_transition(&sym(CALL), InstrumentStatus::Halted) {
            Ok(s) => s,
            Err(e) => panic!("Active -> Halted must be legal: {e}"),
        };
        assert_eq!(applied, InstrumentStatus::Halted);
        assert!(!registry.is_accepting_orders(&sym(CALL)));
        assert_eq!(registry.status_of(&sym(CALL)), InstrumentStatus::Halted);
    }

    #[test]
    fn test_resume_restores_accepting_orders() {
        let mut registry = InstrumentStatusRegistry::new();
        registry
            .try_transition(&sym(CALL), InstrumentStatus::Halted)
            .expect("halt");
        registry
            .try_transition(&sym(CALL), InstrumentStatus::Active)
            .expect("resume Halted -> Active is legal");
        assert!(registry.is_accepting_orders(&sym(CALL)));
    }

    #[test]
    fn test_illegal_transition_is_rejected_and_leaves_state_unchanged() {
        let mut registry = InstrumentStatusRegistry::new();
        registry
            .try_transition(&sym(CALL), InstrumentStatus::Settling)
            .expect("Active -> Settling is legal");
        // Settling -> Active is not a legal edge.
        match registry.try_transition(&sym(CALL), InstrumentStatus::Active) {
            Err(InstrumentStatusError::IllegalTransition { from, to }) => {
                assert_eq!(from, InstrumentStatus::Settling);
                assert_eq!(to, InstrumentStatus::Active);
            }
            other => panic!("expected an illegal-transition rejection, got {other:?}"),
        }
        // Unchanged after the rejected transition.
        assert_eq!(registry.status_of(&sym(CALL)), InstrumentStatus::Settling);
    }

    #[test]
    fn test_expired_is_terminal() {
        let mut registry = InstrumentStatusRegistry::new();
        registry
            .try_transition(&sym(CALL), InstrumentStatus::Expired)
            .expect("Active -> Expired is legal");
        match registry.try_transition(&sym(CALL), InstrumentStatus::Active) {
            Err(InstrumentStatusError::IllegalTransition { .. }) => {}
            other => panic!("Expired is terminal; expected rejection, got {other:?}"),
        }
    }

    #[test]
    fn test_self_transition_is_idempotent_noop() {
        let mut registry = InstrumentStatusRegistry::new();
        registry
            .try_transition(&sym(CALL), InstrumentStatus::Halted)
            .expect("halt");
        // Halted -> Halted is a legal idempotent no-op.
        let applied = registry
            .try_transition(&sym(CALL), InstrumentStatus::Halted)
            .expect("self-transition is a legal no-op");
        assert_eq!(applied, InstrumentStatus::Halted);
    }

    #[test]
    fn test_reject_reason_is_deterministic_and_names_both_statuses() {
        let err = InstrumentStatusError::IllegalTransition {
            from: InstrumentStatus::Expired,
            to: InstrumentStatus::Active,
        };
        assert_eq!(
            err.to_string(),
            "illegal instrument status transition from Expired to Active"
        );
    }
}
