//! The venue **clock service** — time as an injected venue service, never
//! `SystemTime` on the sequenced path
//! ([04 §5](../../docs/04-market-data-and-replay.md#5-clock-control),
//! [ADR-0004](../../docs/adr/0004-deterministic-replay-with-seeded-clock.md),
//! [02 §5.3](../../docs/02-matching-architecture.md#5-determinism)).
//!
//! [`SimClock`] is the one time source the whole venue reads: the per-underlying
//! actors stamp [`VenueEvent::venue_ts`](crate::exchange::VenueEvent) from it, the
//! price-walk cadence stamps its `SimStep` from it, and the auth rate limiter
//! reads it — so a single seeded clock decides every timestamp, and replay reuses
//! the recorded value rather than re-reading a wall clock. It implements the
//! [`VenueClock`] seam (the actor's stamp source) and the
//! [`RateLimitClock`](crate::auth::RateLimitClock) seam (the rate limiter's), and
//! it is owned by [`AppState`](crate::state::AppState) and injected — it is a
//! **service, not a global**.
//!
//! ## The sequenced-path read is allocation-free and wall-clock-free
//!
//! [`SimClock::now_ms`] is a single relaxed-`Acquire` atomic load of the current
//! venue instant — no `SystemTime`, no allocation — so it is safe inside the HP-1
//! order path and can never leak wall-clock non-determinism onto the sequenced
//! path (the acceptance guard in `tests/determinism.rs`). **Who advances** the
//! atomic depends on the mode, and every advance happens **off** the sequenced
//! read:
//!
//! - [`ClockMode::Stepped`] — advanced **only** by an explicit
//!   [`step`](SimClock::step) (the venue-control coordinator's `Clock`
//!   [`VenueCommand`](crate::exchange::VenueCommand) path), by exactly the
//!   configured virtual interval. Deterministic and replayable: the advance's
//!   value is carried in the journaled command, never re-read on replay
//!   ([02 §4.1](../../docs/02-matching-architecture.md#41-venue-wide-commands-marketmakercontrol--clock--simstep)).
//! - [`ClockMode::Accelerated`] — the cadence driver advances the virtual clock
//!   by `multiplier ×` the elapsed wall time each tick
//!   ([`track_wall`](SimClock::track_wall) / [`tick`](SimClock::tick)); the wall
//!   read happens in the off-path driver, never in [`now_ms`](SimClock::now_ms).
//! - [`ClockMode::Realtime`] — the same as accelerated with `multiplier = 1`: the
//!   virtual clock tracks wall time 1:1 (within the cadence tolerance).
//!
//! Live runs stamp from the clock (wall-derived in realtime/accelerated); a replay
//! reuses the journaled value, so `venue_ts` and every carried `now_ms` are the
//! recorded ones — the clock is **excluded from same-seed regeneration** and
//! reproduced from the journal ([04 §6](../../docs/04-market-data-and-replay.md#6-determinism-and-seeding)).
//!
//! ## Named upstream limitation: the leaf clock (Day/GTD TIF admission)
//!
//! Deterministic `Day` / `GTD` time-in-force *admission* wants the venue clock
//! injected **at the leaf book**, so a leaf decides TIF against venue time, not
//! wall time ([02 §5.5b](../../docs/02-matching-architecture.md#5-determinism)).
//! `orderbook-rs` 0.10.5 provides exactly that API (`OrderBook::with_clock` /
//! `Arc<dyn Clock>` / `MonotonicClock` / `StubClock`), **but** the pinned
//! `option-chain-orderbook` 0.7.0 does not thread it through its lazy
//! `get_or_create_*` leaf construction, exposes no `OptionOrderBook::with_clock`,
//! and `OrderBook::set_clock` needs `&mut self` while the venue holds vivified
//! leaves as `Arc<OptionOrderBook>`. So **until that named upstream work lands**
//! (threading `Arc<dyn Clock>` through the managers), the injectable-leaf-clock
//! guarantee covers no hierarchy leaf, and the intraday expiry sweep
//! (`EvictExpiredOrders`) stays a journaled no-op. This is a *named* limitation,
//! not a silent one: it is pinned by
//! `tests/determinism.rs::test_evict_expired_orders_is_a_documented_leaf_clock_limitation`.
//! The [`SimClock`] here is already the `now_millis` source such a leaf would read
//! once the API is threadable — no venue rework is needed to adopt it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::exchange::{EventTimestamp, VenueClock};
use crate::simulation::simulator::{DEFAULT_START_MS, DEFAULT_STEP_MS};

/// The default accelerated multiplier (`60×` — one wall second is one virtual
/// minute) when `[clock] mode = "accelerated"` supplies none.
pub const DEFAULT_ACCEL_MULTIPLIER: u32 = 60;

/// The default stepped virtual interval, in **milliseconds** — one virtual minute
/// per step, aligned with the price-walk step ([`DEFAULT_STEP_MS`]) so the venue
/// clock and the walk's own time base advance together.
pub const DEFAULT_STEP_INTERVAL_MS: u64 = DEFAULT_STEP_MS;

/// The default virtual-clock epoch, in **milliseconds** — the same fixed,
/// deterministic start the price walk uses ([`DEFAULT_START_MS`]), well before the
/// venue's far-dated expiries so time-to-expiry stays positive. Never the wall
/// clock.
pub const DEFAULT_CLOCK_START_MS: u64 = DEFAULT_START_MS;

// ============================================================================
// ClockMode — the parameterised runtime mode
// ============================================================================

/// The venue clock's runtime mode, **carrying its parameters** — the richer
/// sibling of the parameterless config token
/// [`crate::config::ClockMode`](crate::config::ClockMode), which the application
/// layer maps onto this ([04 §5](../../docs/04-market-data-and-replay.md#5-clock-control)).
///
/// The clock module owns this parameterised form (domain does not depend on
/// config); [`AppState`](crate::state::AppState) bridges the config token plus the
/// `[clock]` scalar knobs onto it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClockMode {
    /// The virtual clock tracks wall time 1:1 (within the cadence tolerance).
    Realtime,
    /// The virtual clock advances at a fixed `multiplier ×` wall time.
    Accelerated {
        /// Virtual milliseconds advanced per wall millisecond (clamped to `>= 1`).
        multiplier: u32,
    },
    /// The virtual clock advances **only** on an explicit step, by exactly
    /// `step_ms` each time.
    Stepped {
        /// The virtual interval one [`step`](SimClock::step) advances, in
        /// **milliseconds** (clamped to `>= 1`).
        step_ms: u64,
    },
}

impl ClockMode {
    /// The canonical mode token, matching [`crate::config::ClockMode::as_str`] —
    /// the value recorded in the [`RunManifest`](crate::simulation::RunManifest).
    #[must_use]
    #[inline]
    pub fn as_token(self) -> &'static str {
        match self {
            ClockMode::Realtime => "realtime",
            ClockMode::Accelerated { .. } => "accelerated",
            ClockMode::Stepped { .. } => "stepped",
        }
    }
}

// ============================================================================
// VenueClockConfig — the clock's construction parameters
// ============================================================================

/// The construction parameters for a [`SimClock`]: its mode (with parameters) and
/// its virtual epoch.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VenueClockConfig {
    /// The runtime mode (carrying multiplier / step interval).
    pub mode: ClockMode,
    /// The virtual-clock epoch the clock starts at, in **milliseconds**.
    pub start_ms: u64,
}

impl Default for VenueClockConfig {
    /// The local-dev default: realtime, starting at the deterministic virtual
    /// epoch ([`DEFAULT_CLOCK_START_MS`]).
    fn default() -> Self {
        Self {
            mode: ClockMode::Realtime,
            start_ms: DEFAULT_CLOCK_START_MS,
        }
    }
}

impl VenueClockConfig {
    /// A realtime clock starting at `start_ms`.
    #[must_use]
    #[inline]
    pub const fn realtime(start_ms: u64) -> Self {
        Self {
            mode: ClockMode::Realtime,
            start_ms,
        }
    }

    /// An accelerated clock advancing at `multiplier ×` wall time from `start_ms`.
    #[must_use]
    #[inline]
    pub const fn accelerated(start_ms: u64, multiplier: u32) -> Self {
        Self {
            mode: ClockMode::Accelerated { multiplier },
            start_ms,
        }
    }

    /// A stepped clock advancing by exactly `step_ms` per step from `start_ms`.
    #[must_use]
    #[inline]
    pub const fn stepped(start_ms: u64, step_ms: u64) -> Self {
        Self {
            mode: ClockMode::Stepped { step_ms },
            start_ms,
        }
    }
}

// ============================================================================
// SimClock — the shared venue time service
// ============================================================================

/// The shared, mode-aware venue clock. Cloned as a cheap `Arc` handle into every
/// per-underlying actor, the price simulator, and the auth rate limiter, so they
/// all read the **same** advancing venue instant.
///
/// `now_ms` on the sequenced path is a single atomic load — no wall-clock read, no
/// allocation. Advancing (stepped step, or realtime/accelerated wall-tracking)
/// happens off that read.
#[derive(Clone, Debug)]
pub struct SimClock {
    inner: Arc<ClockInner>,
}

#[derive(Debug)]
struct ClockInner {
    mode: ClockMode,
    /// The virtual epoch the clock started at.
    start_ms: u64,
    /// The current venue instant read on the sequenced path (an atomic load).
    current_ms: AtomicU64,
    /// The wall instant (ms) captured on the first wall-tracking call, mapping
    /// wall → virtual for realtime / accelerated. `0` means "not yet anchored".
    wall_anchor_ms: AtomicU64,
}

impl SimClock {
    /// Builds a clock from its config, starting at `config.start_ms`.
    #[must_use]
    pub fn new(config: VenueClockConfig) -> Self {
        Self {
            inner: Arc::new(ClockInner {
                mode: normalise_mode(config.mode),
                start_ms: config.start_ms,
                current_ms: AtomicU64::new(config.start_ms),
                wall_anchor_ms: AtomicU64::new(0),
            }),
        }
    }

    /// A realtime clock at the default epoch — the local-dev default.
    #[must_use]
    #[inline]
    pub fn realtime() -> Self {
        Self::new(VenueClockConfig::default())
    }

    /// A stepped clock at `start_ms` advancing by `step_ms` per step.
    #[must_use]
    #[inline]
    pub fn stepped(start_ms: u64, step_ms: u64) -> Self {
        Self::new(VenueClockConfig::stepped(start_ms, step_ms))
    }

    /// An accelerated clock at `start_ms` advancing at `multiplier ×` wall time.
    #[must_use]
    #[inline]
    pub fn accelerated(start_ms: u64, multiplier: u32) -> Self {
        Self::new(VenueClockConfig::accelerated(start_ms, multiplier))
    }

    /// The runtime mode this clock was built with (parameters clamped to `>= 1`).
    #[must_use]
    #[inline]
    pub fn mode(&self) -> ClockMode {
        self.inner.mode
    }

    /// The virtual epoch this clock started at, in **milliseconds**.
    #[must_use]
    #[inline]
    pub fn start_ms(&self) -> u64 {
        self.inner.start_ms
    }

    /// The current venue instant, in **milliseconds** — a single atomic load, no
    /// `SystemTime`, no allocation. The inherent form the [`VenueClock`] and
    /// [`RateLimitClock`](crate::auth::RateLimitClock) impls both delegate to, so a
    /// caller reads it without importing either trait and without ambiguity.
    #[must_use]
    #[inline]
    pub fn now_ms(&self) -> EventTimestamp {
        EventTimestamp::new(self.inner.current_ms.load(Ordering::Acquire))
    }

    /// Advances the clock **monotonically** to `target_ms` and returns the new
    /// instant. A `target_ms` at or below the current instant is a no-op (the
    /// clock never regresses) — the invariant every downstream `venue_ts`/`now_ms`
    /// consumer relies on.
    #[inline]
    pub fn advance_to(&self, target_ms: u64) -> EventTimestamp {
        let mut current = self.inner.current_ms.load(Ordering::Acquire);
        loop {
            if target_ms <= current {
                return EventTimestamp::new(current);
            }
            match self.inner.current_ms.compare_exchange_weak(
                current,
                target_ms,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return EventTimestamp::new(target_ms),
                Err(observed) => current = observed,
            }
        }
    }

    /// Advances a **stepped** clock by exactly its configured interval and returns
    /// the new instant. In realtime / accelerated modes this is a no-op read (the
    /// cadence driver advances those via [`tick`](Self::tick)).
    ///
    /// Checked, never wrapping: a `u64::MAX` virtual timeline is unreachable, so
    /// the clamp never fires in practice, but it is the explicit `checked_*(..)
    /// .unwrap_or(u64::MAX)` clamp the repo rules require over a banned
    /// `saturating_*` (rules §arithmetic).
    #[allow(clippy::manual_saturating_arithmetic)]
    #[inline]
    pub fn step(&self) -> EventTimestamp {
        match self.inner.mode {
            ClockMode::Stepped { step_ms } => {
                let current = self.inner.current_ms.load(Ordering::Acquire);
                let target = current.checked_add(step_ms).unwrap_or(u64::MAX);
                self.advance_to(target)
            }
            ClockMode::Realtime | ClockMode::Accelerated { .. } => self.now_ms(),
        }
    }

    /// Advances a realtime / accelerated clock to track wall time at
    /// `wall_now_ms`, returning the new virtual instant. **Deterministic and
    /// testable**: the caller supplies the wall instant, so a unit test drives it
    /// without reading the system clock. In stepped mode it is a no-op read.
    ///
    /// The first call anchors on `wall_now_ms`; later calls advance the virtual
    /// clock by `multiplier ×` the wall elapsed since the anchor. Monotonic (never
    /// regresses). The `checked_*(..).unwrap_or(u64::MAX)` clamps are the explicit
    /// form the repo rules require over a banned `saturating_*`.
    #[allow(clippy::manual_saturating_arithmetic)]
    #[inline]
    pub fn track_wall(&self, wall_now_ms: u64) -> EventTimestamp {
        let multiplier = match self.inner.mode {
            ClockMode::Realtime => 1,
            ClockMode::Accelerated { multiplier } => multiplier,
            ClockMode::Stepped { .. } => return self.now_ms(),
        };
        let anchor = self.wall_anchor(wall_now_ms);
        // A wall instant below the anchor (a backwards NTP step) clamps to zero
        // elapsed — the virtual clock never regresses.
        let wall_elapsed = wall_now_ms.checked_sub(anchor).unwrap_or(0);
        let virtual_elapsed = wall_elapsed
            .checked_mul(u64::from(multiplier))
            .unwrap_or(u64::MAX);
        let target = self
            .inner
            .start_ms
            .checked_add(virtual_elapsed)
            .unwrap_or(u64::MAX);
        self.advance_to(target)
    }

    /// The cadence driver's per-tick advance: reads the wall clock **off the
    /// sequenced path** (realtime / accelerated) and advances the virtual clock,
    /// or is a no-op read (stepped, where the control coordinator advances).
    ///
    /// This is the one method that reads `SystemTime`, and it is invoked only by
    /// the price-walk cadence loop / clock driver — never on the sequenced path,
    /// where [`now_ms`](Self::now_ms) is a pure atomic load.
    #[inline]
    pub fn tick(&self) -> EventTimestamp {
        match self.inner.mode {
            ClockMode::Realtime | ClockMode::Accelerated { .. } => self.track_wall(wall_now_ms()),
            ClockMode::Stepped { .. } => self.now_ms(),
        }
    }

    /// Resolves (installing on first use) the wall anchor. A `wall_now_ms` of `0`
    /// is stored as `1` so the `0` sentinel stays distinct — real epoch
    /// milliseconds are never `0`.
    #[inline]
    fn wall_anchor(&self, wall_now_ms: u64) -> u64 {
        let existing = self.inner.wall_anchor_ms.load(Ordering::Acquire);
        if existing != 0 {
            return existing;
        }
        let anchor = wall_now_ms.max(1);
        match self.inner.wall_anchor_ms.compare_exchange(
            0,
            anchor,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => anchor,
            Err(observed) => observed,
        }
    }
}

impl VenueClock for SimClock {
    /// The sequenced-path read — delegates to the inherent [`SimClock::now_ms`]
    /// (an atomic load; no `SystemTime`, no allocation).
    #[inline]
    fn now_ms(&self) -> EventTimestamp {
        SimClock::now_ms(self)
    }
}

impl crate::auth::RateLimitClock for SimClock {
    /// Bridges the venue clock onto the rate-limiter seam — the rate limiter and
    /// the sequenced path read the **same** advancing venue instant, so
    /// rate-limit decisions replay deterministically
    /// ([03 §6.1](../../docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering)).
    #[inline]
    fn now_ms(&self) -> u64 {
        SimClock::now_ms(self).get()
    }
}

/// Clamps a mode's parameters to their valid floor (`>= 1`) so a `0` multiplier /
/// step can never freeze the clock or divide the timeline by zero.
#[inline]
fn normalise_mode(mode: ClockMode) -> ClockMode {
    match mode {
        ClockMode::Realtime => ClockMode::Realtime,
        ClockMode::Accelerated { multiplier } => ClockMode::Accelerated {
            multiplier: multiplier.max(1),
        },
        ClockMode::Stepped { step_ms } => ClockMode::Stepped {
            step_ms: step_ms.max(1),
        },
    }
}

/// Wall-clock milliseconds since the Unix epoch — the one wall read, used only by
/// [`SimClock::tick`] in the off-path cadence driver, never on the sequenced path.
/// A pre-epoch system clock clamps to `0` (it never panics).
#[inline]
fn wall_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| u64::try_from(since.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

// ============================================================================
// CorrelationId — the venue-control fan-out tag
// ============================================================================

/// A shared tag correlating the per-underlying commands a single venue-wide
/// control advance fans out — so an operator can tell that one `Clock` advance
/// (or, later, `SimStep` / `MarketMakerControl`) produced this set of
/// per-underlying sequenced commands, and detect a partial fan-out
/// ([02 §4.1](../../docs/02-matching-architecture.md#41-venue-wide-commands-marketmakercontrol--clock--simstep)).
///
/// In #028 the id is an **in-memory** coordinator ack surfaced on the advance
/// result; journaling it durably for post-hoc partial-detection queries lands with
/// the durable journal (#029).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CorrelationId(u64);

impl CorrelationId {
    /// Builds a correlation id from a raw counter value.
    #[must_use]
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw counter value.
    #[must_use]
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "corr-{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- stepped: advances by exactly the interval, and not otherwise --------

    #[test]
    fn test_clock_stepped_advances_by_interval() {
        let clock = SimClock::stepped(1_000, 250);
        assert_eq!(clock.now_ms().get(), 1_000);
        assert_eq!(clock.step().get(), 1_250);
        assert_eq!(clock.step().get(), 1_500);
        // Each step advances the shared instant read on the sequenced path.
        assert_eq!(clock.now_ms().get(), 1_500);
    }

    #[test]
    fn test_clock_stepped_does_not_advance_without_a_step() {
        let clock = SimClock::stepped(5_000, 60_000);
        // Wall-tracking is inert in stepped mode: only an explicit step advances.
        assert_eq!(clock.track_wall(9_999_999).get(), 5_000);
        assert_eq!(clock.tick().get(), 5_000);
        assert_eq!(clock.now_ms().get(), 5_000);
        assert_eq!(clock.step().get(), 65_000);
    }

    #[test]
    fn test_clock_stepped_zero_interval_is_clamped_to_one() {
        // A 0 interval would freeze the clock; it is clamped to 1.
        let clock = SimClock::stepped(0, 0);
        assert_eq!(clock.step().get(), 1);
    }

    // ---- accelerated: advances by the multiplier -----------------------------

    #[test]
    fn test_clock_accelerated_advances_by_multiplier() {
        let clock = SimClock::accelerated(1_000, 60);
        // First wall-tracking call anchors; the virtual clock stays at the epoch.
        assert_eq!(clock.track_wall(10_000).get(), 1_000);
        // 100 ms of wall time later → 60 × 100 = 6_000 virtual ms advanced.
        assert_eq!(clock.track_wall(10_100).get(), 7_000);
        // Another 400 ms wall → 500 ms total × 60 = 30_000 virtual ms from anchor.
        assert_eq!(clock.track_wall(10_500).get(), 31_000);
    }

    #[test]
    fn test_clock_accelerated_zero_multiplier_is_clamped_to_one() {
        let clock = SimClock::accelerated(0, 0);
        clock.track_wall(1_000);
        // Clamped to 1×, so it tracks wall 1:1 rather than freezing.
        assert_eq!(clock.track_wall(1_500).get(), 500);
    }

    // ---- realtime: tracks wall 1:1 -------------------------------------------

    #[test]
    fn test_clock_realtime_tracks_wall_one_to_one() {
        let clock = SimClock::realtime();
        let start = clock.start_ms();
        clock.track_wall(1_000_000);
        // 750 ms of wall time → 750 ms of virtual time (1:1), from the anchor.
        assert_eq!(clock.track_wall(1_000_750).get(), start + 750);
    }

    #[test]
    fn test_clock_realtime_within_tolerance_of_wall_now() {
        // The live tick() reads the wall clock: two ticks a moment apart advance
        // the virtual clock by roughly the wall delta (1:1, within tolerance).
        let clock = SimClock::realtime();
        let first = clock.tick().get();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let second = clock.tick().get();
        assert!(second >= first, "realtime clock is monotonic");
        assert!(
            second - first < 5_000,
            "realtime tracks wall within tolerance (delta {} ms)",
            second - first
        );
    }

    // ---- monotonicity --------------------------------------------------------

    #[test]
    fn test_clock_never_regresses() {
        let clock = SimClock::realtime();
        clock.track_wall(2_000_000);
        let high = clock.now_ms().get();
        // An earlier wall instant must not move the clock backwards.
        assert_eq!(clock.track_wall(1_000_000).get(), high);
        assert_eq!(clock.now_ms().get(), high);
    }

    // ---- now_ms is a pure atomic read (VenueClock seam) ----------------------

    #[test]
    fn test_clock_now_ms_is_shared_across_clones() {
        let clock = SimClock::stepped(0, 100);
        let clone = clock.clone();
        clock.step();
        // A clone reads the SAME shared instant — one clock, many handles.
        assert_eq!(
            VenueClock::now_ms(&clone),
            VenueClock::now_ms(&clock),
            "clones share the advancing instant"
        );
        assert_eq!(clone.now_ms().get(), 100);
    }

    #[test]
    fn test_mode_token_matches_config_vocabulary() {
        assert_eq!(ClockMode::Realtime.as_token(), "realtime");
        assert_eq!(
            ClockMode::Accelerated { multiplier: 60 }.as_token(),
            "accelerated"
        );
        assert_eq!(ClockMode::Stepped { step_ms: 1 }.as_token(), "stepped");
    }

    #[test]
    fn test_correlation_id_roundtrips_and_displays() {
        let id = CorrelationId::new(7);
        assert_eq!(id.get(), 7);
        assert_eq!(id.to_string(), "corr-7");
    }
}
