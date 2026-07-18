//! Scheduled **expiry / roll** — the venue-clock-driven lifecycle driver that issues
//! the upstream `InstrumentStatus` transitions as **sequenced commands**
//! ([05 §10](../../docs/05-microstructure-config.md#10-halt-scenarios),
//! [047](../../milestones/v0.5-microstructure/047-personas-liquidity-halt.md)).
//!
//! ## Schedule source, not a book mutator
//!
//! The upstream `ExpiryScheduler` / `ExpiryLifecycleManager` mutate the book
//! **directly** (they set statuses and cancel orders off the sequencer), so the
//! venue does **not** call them on the live path. This module reuses only their
//! *operational times* (default `08:00` expiry / `08:30` settlement UTC) as a
//! schedule source and issues every transition through the **sequenced order path**
//! ([`AppState::submit`](crate::state::AppState)) so it is journaled and replays
//! identically ([02 §5](../../docs/02-matching-architecture.md)).
//!
//! At the operational **expiry** instant an expiration transitions to `Settling`
//! after a scoped [`MassCancel`](crate::exchange::VenueCommand::MassCancel) (cancelling
//! **all** resting orders, including `GTC`); at the operational **settlement** instant
//! it transitions to `Expired`. There is **no** `Settled` status — the verified
//! upstream lifecycle is `Settling → Expired`. The operational times are distinct
//! from the `23:59:59 UTC` symbol-identity instant ([01 §5](../../docs/01-domain-model.md#5-instruments-and-the-symbol-grammar));
//! expiries are always `ExpirationDate::DateTime`.
//!
//! ## Determinism ([02 §5](../../docs/02-matching-architecture.md))
//!
//! [`ExpirySchedule`] is a **pure function** of `(operational offsets, expiration,
//! now_ms)` — pure integer-millisecond arithmetic, **no** wall clock (the instant is
//! the venue clock's), **no** RNG, **no** map-iteration order. The commands it emits
//! carry their own data, so replay re-executes them from the journal without
//! re-running the driver. Intraday `Day` / `Gtd` time-in-force expiry is a separate
//! [`EvictExpiredOrders`](crate::exchange::VenueCommand::EvictExpiredOrders) sweep
//! carrying its `now_ms`.

use optionstratlib::ExpirationDate;

use crate::exchange::{InstrumentStatus, MassCancelScope, MassCancelType, Symbol, VenueCommand};
use crate::models::AccountId;

/// Milliseconds per UTC day — the floor divisor mapping an epoch-ms instant onto its
/// UTC-midnight day boundary (epoch ms aligns to day boundaries at multiples of this).
const MS_PER_DAY: i64 = 86_400_000;

/// The default operational **expiry** offset from UTC midnight — `08:00:00 UTC`, in
/// milliseconds ([05 §10](../../docs/05-microstructure-config.md#10-halt-scenarios)).
pub const DEFAULT_EXPIRY_OFFSET_MS: i64 = 8 * 3_600_000;

/// The default operational **settlement** offset from UTC midnight — `08:30:00 UTC`,
/// in milliseconds.
pub const DEFAULT_SETTLEMENT_OFFSET_MS: i64 = 8 * 3_600_000 + 30 * 60_000;

/// The reserved requester account attributed to a scheduled roll's `MassCancel`
/// (a [`MassCancelType::All`] sweep ignores the owner, so this is attribution only).
const EXPIRY_ROLL_ACCOUNT: &str = "venue-expiry-scheduler";

/// A misconfigured [`ExpirySchedule`] — the operational offsets are outside a day or
/// out of order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ExpiryScheduleError {
    /// An operational offset is negative or `>=` one day.
    #[error("operational offset {offset_ms} ms is outside [0, {MS_PER_DAY})")]
    OffsetOutOfDay {
        /// The offending offset in milliseconds.
        offset_ms: i64,
    },
    /// The settlement offset is before the expiry offset (settlement must be at or
    /// after expiry).
    #[error("settlement offset {settlement_ms} ms is before expiry offset {expiry_ms} ms")]
    SettlementBeforeExpiry {
        /// The expiry offset in milliseconds.
        expiry_ms: i64,
        /// The settlement offset in milliseconds.
        settlement_ms: i64,
    },
}

/// The operational lifecycle **phase** of an expiration at a venue-clock instant.
///
/// A total order (`PreExpiry < Settling < Expired`) so the driver only ever advances
/// an expiration forward, never regresses it into an illegal upstream transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum ExpiryPhase {
    /// Before the operational expiry instant — still accepting orders.
    PreExpiry = 0,
    /// At/after operational expiry, before settlement — resting orders cancelled,
    /// status `Settling`, no new orders.
    Settling = 1,
    /// At/after operational settlement — status `Expired` (terminal).
    Expired = 2,
}

/// The pure, deterministic **expiry schedule**: the operational expiry / settlement
/// offsets from UTC midnight, and the phase computation that drives the sequenced
/// lifecycle transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpirySchedule {
    expiry_offset_ms: i64,
    settlement_offset_ms: i64,
}

impl Default for ExpirySchedule {
    /// The default `08:00` expiry / `08:30` settlement schedule.
    #[inline]
    fn default() -> Self {
        Self {
            expiry_offset_ms: DEFAULT_EXPIRY_OFFSET_MS,
            settlement_offset_ms: DEFAULT_SETTLEMENT_OFFSET_MS,
        }
    }
}

impl ExpirySchedule {
    /// Builds a schedule from explicit operational offsets (ms from UTC midnight).
    ///
    /// # Errors
    ///
    /// [`ExpiryScheduleError`] if an offset is outside `[0, one day)` or settlement is
    /// before expiry.
    pub fn new(
        expiry_offset_ms: i64,
        settlement_offset_ms: i64,
    ) -> Result<Self, ExpiryScheduleError> {
        for offset in [expiry_offset_ms, settlement_offset_ms] {
            if !(0..MS_PER_DAY).contains(&offset) {
                return Err(ExpiryScheduleError::OffsetOutOfDay { offset_ms: offset });
            }
        }
        if settlement_offset_ms < expiry_offset_ms {
            return Err(ExpiryScheduleError::SettlementBeforeExpiry {
                expiry_ms: expiry_offset_ms,
                settlement_ms: settlement_offset_ms,
            });
        }
        Ok(Self {
            expiry_offset_ms,
            settlement_offset_ms,
        })
    }

    /// The operational `(expiry_instant_ms, settlement_instant_ms)` for an
    /// `expiration`, or `None` for a relative `ExpirationDate::Days` (which has no
    /// fixed calendar date and cannot drive a lifecycle transition — it breaks
    /// replay, [01 §4](../../docs/01-domain-model.md)).
    ///
    /// Pure integer-ms arithmetic: floor the expiration's epoch-ms to its UTC-midnight
    /// day boundary, then add the operational offsets.
    #[must_use]
    pub fn operational_instants(&self, expiration: &ExpirationDate) -> Option<(i64, i64)> {
        let expiration_ms = match expiration {
            ExpirationDate::DateTime(dt) => dt.timestamp_millis(),
            // A relative `Days` expiry has no fixed calendar date, so it drives no
            // lifecycle transition and is refused here (never propagated).
            ExpirationDate::Days(_) => return None, // days-expiry-allow: defensive read-arm
        };
        let day_ms = expiration_ms.div_euclid(MS_PER_DAY) * MS_PER_DAY;
        Some((
            day_ms + self.expiry_offset_ms,
            day_ms + self.settlement_offset_ms,
        ))
    }

    /// The operational phase of `expiration` at venue-clock `now_ms`, or `None` for a
    /// relative `Days` expiry. Pure and deterministic (rule 3).
    #[must_use]
    pub fn phase_at(&self, expiration: &ExpirationDate, now_ms: i64) -> Option<ExpiryPhase> {
        let (expiry_instant, settlement_instant) = self.operational_instants(expiration)?;
        Some(if now_ms >= settlement_instant {
            ExpiryPhase::Expired
        } else if now_ms >= expiry_instant {
            ExpiryPhase::Settling
        } else {
            ExpiryPhase::PreExpiry
        })
    }

    /// The **sequenced commands** that advance an expiration's `symbols` from `from`
    /// to `to` (both phases), in the documented order. Empty when `to <= from` (no
    /// forward transition), so re-running the driver at the same phase is a no-op and
    /// never emits an illegal regressive transition.
    ///
    /// - entering `Settling`: a scoped [`MassCancel`] of the whole `expiration`
    ///   (cancelling **all** resting orders, including `GTC`), then a per-symbol
    ///   [`SetInstrumentStatus`](VenueCommand::SetInstrumentStatus)`(Settling)`;
    /// - entering `Expired`: a per-symbol `SetInstrumentStatus(Expired)`.
    ///
    /// Symbols are consumed in the caller's order (the driver sorts them), so the
    /// emitted command sequence is a deterministic function of the inputs.
    #[must_use]
    pub fn transition_commands(
        &self,
        expiration: &ExpirationDate,
        symbols: &[Symbol],
        from: ExpiryPhase,
        to: ExpiryPhase,
    ) -> Vec<VenueCommand> {
        if to <= from {
            return Vec::new();
        }
        let mut commands = Vec::new();
        // If we cross INTO Settling on this step, cancel the expiration then mark
        // Settling. (A jump straight to Expired from PreExpiry still runs the
        // Settling leg first, matching the upstream catch-up order.)
        if from < ExpiryPhase::Settling {
            commands.push(VenueCommand::MassCancel {
                scope: MassCancelScope::Expiration(*expiration),
                cancel_type: MassCancelType::All,
                account: AccountId::new(EXPIRY_ROLL_ACCOUNT),
            });
            for symbol in symbols {
                commands.push(VenueCommand::SetInstrumentStatus {
                    symbol: symbol.clone(),
                    status: InstrumentStatus::Settling,
                });
            }
        }
        if to >= ExpiryPhase::Expired {
            for symbol in symbols {
                commands.push(VenueCommand::SetInstrumentStatus {
                    symbol: symbol.clone(),
                    status: InstrumentStatus::Expired,
                });
            }
        }
        commands
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expiration(symbol: &str) -> ExpirationDate {
        let parsed = crate::exchange::SymbolParser::parse(symbol).expect("valid fixture symbol");
        *parsed.expiration()
    }

    fn sym(raw: &str) -> Symbol {
        Symbol::parse(raw).expect("valid fixture symbol")
    }

    // A canonical Friday expiry: identity instant 2024-03-29T23:59:59Z.
    const CALL: &str = "BTC-20240329-50000-C";
    const PUT: &str = "BTC-20240329-50000-P";
    // Day boundary 2024-03-29T00:00:00Z in epoch ms.
    const DAY_MS: i64 = 1_711_670_400_000;

    #[test]
    fn test_operational_instants_are_08_00_and_08_30_utc() {
        let schedule = ExpirySchedule::default();
        let (expiry, settle) = schedule
            .operational_instants(&expiration(CALL))
            .expect("DateTime expiry");
        assert_eq!(expiry, DAY_MS + DEFAULT_EXPIRY_OFFSET_MS, "08:00 UTC");
        assert_eq!(settle, DAY_MS + DEFAULT_SETTLEMENT_OFFSET_MS, "08:30 UTC");
    }

    #[test]
    fn test_phase_at_crosses_expiry_then_settlement() {
        let schedule = ExpirySchedule::default();
        let exp = expiration(CALL);
        // Just before 08:00 → PreExpiry.
        assert_eq!(
            schedule.phase_at(&exp, DAY_MS + DEFAULT_EXPIRY_OFFSET_MS - 1),
            Some(ExpiryPhase::PreExpiry)
        );
        // Exactly 08:00 → Settling.
        assert_eq!(
            schedule.phase_at(&exp, DAY_MS + DEFAULT_EXPIRY_OFFSET_MS),
            Some(ExpiryPhase::Settling)
        );
        // Just before 08:30 → still Settling.
        assert_eq!(
            schedule.phase_at(&exp, DAY_MS + DEFAULT_SETTLEMENT_OFFSET_MS - 1),
            Some(ExpiryPhase::Settling)
        );
        // Exactly 08:30 → Expired.
        assert_eq!(
            schedule.phase_at(&exp, DAY_MS + DEFAULT_SETTLEMENT_OFFSET_MS),
            Some(ExpiryPhase::Expired)
        );
    }

    #[test]
    fn test_settling_transition_cancels_all_then_sets_settling() {
        let schedule = ExpirySchedule::default();
        let exp = expiration(CALL);
        let symbols = [sym(CALL), sym(PUT)];
        let commands = schedule.transition_commands(
            &exp,
            &symbols,
            ExpiryPhase::PreExpiry,
            ExpiryPhase::Settling,
        );
        // MassCancel(all, incl GTC) first, then SetInstrumentStatus(Settling) per leg.
        assert!(matches!(
            commands.first(),
            Some(VenueCommand::MassCancel {
                cancel_type: MassCancelType::All,
                scope: MassCancelScope::Expiration(_),
                ..
            })
        ));
        let settling = commands
            .iter()
            .filter(|c| {
                matches!(
                    c,
                    VenueCommand::SetInstrumentStatus {
                        status: InstrumentStatus::Settling,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(settling, 2, "one Settling transition per contract leg");
    }

    #[test]
    fn test_expired_transition_from_settling_sets_expired_only() {
        let schedule = ExpirySchedule::default();
        let exp = expiration(CALL);
        let symbols = [sym(CALL), sym(PUT)];
        let commands = schedule.transition_commands(
            &exp,
            &symbols,
            ExpiryPhase::Settling,
            ExpiryPhase::Expired,
        );
        // No further mass cancel — just Expired per leg.
        assert!(commands.iter().all(|c| matches!(
            c,
            VenueCommand::SetInstrumentStatus {
                status: InstrumentStatus::Expired,
                ..
            }
        )));
        assert_eq!(commands.len(), 2);
    }

    #[test]
    fn test_full_catch_up_from_pre_expiry_to_expired_runs_settling_then_expired() {
        let schedule = ExpirySchedule::default();
        let exp = expiration(CALL);
        let symbols = [sym(CALL)];
        let commands = schedule.transition_commands(
            &exp,
            &symbols,
            ExpiryPhase::PreExpiry,
            ExpiryPhase::Expired,
        );
        // Cancel + Settling + Expired, in that order.
        assert!(matches!(commands[0], VenueCommand::MassCancel { .. }));
        assert!(matches!(
            commands[1],
            VenueCommand::SetInstrumentStatus {
                status: InstrumentStatus::Settling,
                ..
            }
        ));
        assert!(matches!(
            commands[2],
            VenueCommand::SetInstrumentStatus {
                status: InstrumentStatus::Expired,
                ..
            }
        ));
    }

    #[test]
    fn test_no_forward_transition_emits_nothing() {
        let schedule = ExpirySchedule::default();
        let exp = expiration(CALL);
        let symbols = [sym(CALL)];
        // Same phase, or a would-be regression, emits no command (never illegal).
        assert!(
            schedule
                .transition_commands(&exp, &symbols, ExpiryPhase::Settling, ExpiryPhase::Settling)
                .is_empty()
        );
        assert!(
            schedule
                .transition_commands(&exp, &symbols, ExpiryPhase::Expired, ExpiryPhase::Settling)
                .is_empty()
        );
    }

    #[test]
    fn test_phase_at_is_none_for_relative_days_expiry() {
        // A relative `Days` expiry has no fixed calendar date, so it drives no roll.
        let schedule = ExpirySchedule::default();
        // days-expiry-allow: test fixture proving a relative `Days` expiry is refused.
        let days = ExpirationDate::Days(optionstratlib::prelude::Positive::ONE);
        assert_eq!(schedule.operational_instants(&days), None);
        assert_eq!(schedule.phase_at(&days, DAY_MS), None);
    }

    #[test]
    fn test_new_rejects_bad_offsets() {
        assert_eq!(
            ExpirySchedule::new(-1, 100),
            Err(ExpiryScheduleError::OffsetOutOfDay { offset_ms: -1 })
        );
        assert_eq!(
            ExpirySchedule::new(MS_PER_DAY, MS_PER_DAY),
            Err(ExpiryScheduleError::OffsetOutOfDay {
                offset_ms: MS_PER_DAY
            })
        );
        assert_eq!(
            ExpirySchedule::new(1000, 500),
            Err(ExpiryScheduleError::SettlementBeforeExpiry {
                expiry_ms: 1000,
                settlement_ms: 500,
            })
        );
        assert!(
            ExpirySchedule::new(0, 0).is_ok(),
            "expiry == settlement is legal"
        );
    }

    #[test]
    fn test_schedule_is_pure_and_deterministic() {
        let schedule = ExpirySchedule::default();
        let exp = expiration(CALL);
        assert_eq!(
            schedule.phase_at(&exp, DAY_MS),
            schedule.phase_at(&exp, DAY_MS)
        );
        assert_eq!(
            schedule.operational_instants(&exp),
            schedule.operational_instants(&exp)
        );
    }
}
