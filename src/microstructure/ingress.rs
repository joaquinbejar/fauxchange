//! The deterministic **ingress-reorder buffer** — the live gateway-edge
//! application of the seeded [`LatencyOffset`](crate::microstructure::LatencyOffset)
//! ([03 §6.1](../../../docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering),
//! [05 §3](../../../docs/05-microstructure-config.md#3-latency-injection)).
//!
//! #45 landed the seeded per-`(session_id, msg_seq)` latency draw; this module
//! lands the mechanism that **consumes** it — a bounded, deadline-ordered arrival
//! buffer that sits **before the sequencer** and reshapes the order in which client
//! order-entry commands reach the single-writer actor. A slow client (a large drawn
//! offset) loses the queue race to a later-arriving fast one, exactly the failure
//! mode real venues only exhibit under load.
//!
//! ## Where determinism and replay come from
//!
//! The reorder is a **live ingress transformation**. The per-underlying actor
//! assigns `underlying_sequence` in the order it **receives** commands (post-reorder)
//! and the journal records **that** order. On replay the journal is replayed in
//! `underlying_sequence` order — the reorder is **not** re-run — so replay
//! determinism is automatic (replay uses the journaled post-reorder order). The
//! obligation this module carries is narrower: the **live** reorder is a pure
//! function of its seeded inputs **under a controlled clock and a serialized
//! admission order**, so that the *same run seed, config, and input command stream*
//! yield the *same reorder* and thus the *same journal*. The ordering **rule** never
//! calls `SystemTime` directly:
//!
//! - a command's release **deadline** is `venue_now_at_arrival + LatencyOffset` —
//!   the **venue clock** read at admission plus the #45 seeded draw (never a fresh
//!   RNG here), in microseconds ([`release_deadline_us`]). Under a stepped/seeded
//!   clock that `venue_now` is itself deterministic; under a **realtime** clock it
//!   is wall-fed, so the deadline *value* is wall-influenced and live run-to-run
//!   reproducibility is **not** claimed there (replay stays deterministic
//!   regardless — see below);
//! - commands release in **deadline order**, and an equal-deadline tie breaks on
//!   `(session_id, arrival_sequence)` — a monotonic per-arrival counter, never a
//!   hash-map order or task-scheduling order in the **key** ([`ReleaseKey`]). The
//!   `arrival_sequence` *value* is assigned by the admission-race counter, so under
//!   genuinely concurrent admission two equal-`(deadline, session)` commands take
//!   their relative order from that race — the same off-oracle class as a plain-FIFO
//!   mailbox arrival race, and equally baked into the journal once assigned.
//!
//! ## The release horizon (why we never release too early)
//!
//! An entry is releasable once the venue clock has **strictly passed** its deadline
//! (`now_us > deadline_us`, [`IngressReorderBuffer::drain_below`]). This strict
//! comparison **is** the release horizon: latency offsets are non-negative and the
//! venue clock is monotonic, so any command that has **not yet been admitted** will
//! be admitted at a venue instant `≥ now` and thus carry a deadline `≥ now >
//! deadline_us` of anything we release. So — modulo the small admission window
//! between reading the clock (fixing the deadline) and inserting into the buffer,
//! which only matters under genuinely concurrent admission — every command whose key
//! is `≤ (D, session, seq)` has already been admitted when we release deadline `D`,
//! and draining `deadline < now_us` in key order reproduces the global total order.
//! No separate horizon parameter is needed; it falls out of the invariant. The
//! ordering **rule** is identical under all three clock modes (order by the virtual
//! deadline; only *when* the clock passes it differs). The resulting live global
//! order is run-to-run reproducible under a **controlled clock with serialized
//! admission**; under a realtime clock or concurrent admission it is best-effort and
//! only **replay** is guaranteed — which is the load-bearing property, since the
//! journal records whatever order the live run produced.
//!
//! ## Bounded (a DoS control)
//!
//! Two independent bounds keep a hostile input from growing the buffer without
//! limit ([08 §5](../../../docs/08-threat-model.md#5-resource-exhaustion)): the
//! drawn offset is **clamped** to [`MAX_INGRESS_OFFSET_US`] before it becomes a
//! deadline (a `u64::MAX` offset cannot hold a command forever — it releases within
//! the horizon), and the buffer **depth** is capped ([`IngressReorderBuffer::insert`]
//! returns [`IngressBufferFull`] at capacity, a documented typed drop rather than
//! unbounded growth). Both bounds are deterministic — the same flood is dropped at
//! the same point every run.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::microstructure::LatencyOffset;

/// The maximum latency offset, in **microseconds of virtual time**, the ingress
/// buffer will apply — a hostile draw (a `lognormal` tail, or the non-finite
/// fail-safe [`u64::MAX`]) is clamped to this so a command can never be held past a
/// bounded horizon. One virtual hour: far beyond any plausible latency yet a hard
/// ceiling on hold time, so the buffer drains deterministically once the clock
/// advances ([08 §5](../../../docs/08-threat-model.md#5-resource-exhaustion)).
pub const MAX_INGRESS_OFFSET_US: u64 = 3_600_000_000;

/// The default bounded depth of one per-underlying ingress reorder buffer — a DoS
/// control, never unbounded ([08 §5](../../../docs/08-threat-model.md#5-resource-exhaustion)).
/// A flood beyond this is dropped with a typed [`IngressBufferFull`]. The live value
/// is venue config; this fixes a bounded default well above the actor mailbox so the
/// buffer is not the first thing to shed load under normal pressure.
pub const DEFAULT_INGRESS_BUFFER_CAPACITY: usize = 4_096;

/// The gateway-edge ingress metadata stamped onto an admitted client order so the
/// #45 latency draw is reproducible: the message's stable identity
/// `(session_id, msg_seq)`.
///
/// For FIX this is `(SenderCompID, MsgSeqNum)`; for REST a `(account, request-seq)`
/// pair. It is **ingress-only** metadata — it keys the seeded draw and the
/// tie-break, and is deliberately **not** part of the journaled [`VenueCommand`],
/// so the journal records only the post-reorder order (which is what replay
/// reproduces).
///
/// [`VenueCommand`]: crate::exchange::VenueCommand
#[derive(Debug, Clone)]
pub struct IngressStamp {
    /// The session identity keying the seeded latency sub-stream and the tie-break.
    pub session_id: Arc<str>,
    /// The per-message sequence keying the seeded latency sub-stream.
    pub msg_seq: u64,
}

impl IngressStamp {
    /// Builds an ingress stamp from a session identity and a per-message sequence.
    #[must_use]
    #[inline]
    pub fn new(session_id: impl Into<Arc<str>>, msg_seq: u64) -> Self {
        Self {
            session_id: session_id.into(),
            msg_seq,
        }
    }
}

/// The deterministic total-order release key: a command releases in
/// `(deadline_us, session_id, arrival_sequence)` order.
///
/// The derived [`Ord`] compares fields **in declaration order** — deadline first,
/// then the `(session_id, arrival_sequence)` tie-break the docs mandate
/// ([03 §6.1](../../../docs/03-protocol-surfaces.md#61-deterministic-ingress-ordering)).
/// Because `arrival_sequence` is a venue-wide monotonic counter it is globally
/// unique, so the key is a strict total order — two commands can never collide, and
/// the key comparison never depends on hash-map order or task scheduling. (The
/// `arrival_sequence` *value* is drawn from the admission-race counter, so under
/// concurrent admission two equal-`(deadline, session)` commands take their relative
/// order from that race — deterministic once assigned and journaled, off the replay
/// oracle exactly like a plain-FIFO mailbox race.)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReleaseKey {
    /// The release deadline, in **microseconds on the virtual clock**
    /// (`venue_now_at_arrival + clamped LatencyOffset`).
    pub deadline_us: u64,
    /// The session identity — the first tie-break at equal deadline.
    pub session_id: Arc<str>,
    /// The venue-wide monotonic per-arrival counter — the final tie-break, making
    /// the key a strict total order.
    pub arrival_sequence: u64,
}

impl ReleaseKey {
    /// Builds a release key.
    #[must_use]
    #[inline]
    pub fn new(deadline_us: u64, session_id: Arc<str>, arrival_sequence: u64) -> Self {
        Self {
            deadline_us,
            session_id,
            arrival_sequence,
        }
    }
}

/// The bounded-buffer rejection: a flood or a hostile offset filled the buffer to
/// capacity, so the command is **dropped** rather than growing the buffer without
/// limit ([08 §5](../../../docs/08-threat-model.md#5-resource-exhaustion)). The
/// application layer maps this to a client-facing throttle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressBufferFull;

impl std::fmt::Display for IngressBufferFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ingress reorder buffer at capacity")
    }
}

impl std::error::Error for IngressBufferFull {}

/// The venue instant `now_ms` (or `now_ms·1000 + offset`) exceeded `u64`
/// microseconds — an astronomically-unreachable range violation (a venue clock past
/// ~584 million years). Returned as a **typed range error** rather than a
/// manufactured `u64::MAX` deadline: a `u64::MAX` deadline could never be released
/// (`drain_below` releases only deadlines strictly below `now_us`, and the venue
/// clock also tops out at `u64::MAX`), permanently stranding the admitted command —
/// and a saturated fallback violates the checked-arithmetic rule (#111 review).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("ingress release deadline overflowed u64 microseconds (venue instant out of range)")]
pub struct ReleaseDeadlineOverflow;

/// The delayed **virtual arrival deadline**, in microseconds, for a command that
/// arrived at venue instant `now_ms` (venue-clock milliseconds) carrying the seeded
/// `offset`.
///
/// The offset is **clamped** to [`MAX_INGRESS_OFFSET_US`] first — a hostile draw
/// (including the non-finite fail-safe [`u64::MAX`]) cannot push the deadline past a
/// bounded horizon. The `ms → µs` promotion and the add are **checked**; an overflow
/// is a typed [`ReleaseDeadlineOverflow`] (never a manufactured `u64::MAX` deadline
/// that could never be released, nor a silent wrap).
///
/// # Errors
///
/// [`ReleaseDeadlineOverflow`] if `now_ms·1000 + offset` exceeds `u64` — unreachable
/// for any real venue clock (it would need `now_ms` past ~584 million years).
pub fn release_deadline_us(
    now_ms: u64,
    offset: LatencyOffset,
) -> Result<u64, ReleaseDeadlineOverflow> {
    let offset_us = offset.micros().min(MAX_INGRESS_OFFSET_US);
    now_ms
        .checked_mul(1_000)
        .and_then(|base_us| base_us.checked_add(offset_us))
        .ok_or(ReleaseDeadlineOverflow)
}

/// A bounded, deadline-ordered ingress reorder buffer for **one** underlying,
/// generic over the held payload `T` (the application layer stores the command plus
/// its reply channel).
///
/// It is a pure ordered structure — no clock, no locks, no I/O — so the ordering
/// contract is unit-testable in isolation. The owning [`crate::state::AppState`]
/// wraps one per underlying behind a mutex and drives release from the venue clock.
#[derive(Debug)]
pub struct IngressReorderBuffer<T> {
    /// The held commands, keyed by their deterministic release key. A [`BTreeMap`]
    /// keeps iteration/drain in strict key order (never hash-map order).
    pending: BTreeMap<ReleaseKey, T>,
    /// The bounded depth — a DoS control, never unbounded.
    capacity: usize,
}

impl<T> IngressReorderBuffer<T> {
    /// Builds a buffer bounded at `capacity` (clamped to at least `1`).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            pending: BTreeMap::new(),
            capacity: capacity.max(1),
        }
    }

    /// Builds a buffer at the bounded [`DEFAULT_INGRESS_BUFFER_CAPACITY`].
    #[must_use]
    pub fn with_default_capacity() -> Self {
        Self::new(DEFAULT_INGRESS_BUFFER_CAPACITY)
    }

    /// The bounded depth.
    #[must_use]
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The number of held commands.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Whether the buffer holds no commands.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Holds `payload` under its release `key`, or rejects it when the buffer is at
    /// capacity — the bounded, typed drop that keeps a flood or a hostile offset
    /// from growing the buffer without limit
    /// ([08 §5](../../../docs/08-threat-model.md#5-resource-exhaustion)).
    ///
    /// # Errors
    ///
    /// [`IngressBufferFull`] when the buffer already holds [`Self::capacity`]
    /// commands.
    pub fn insert(&mut self, key: ReleaseKey, payload: T) -> Result<(), IngressBufferFull> {
        if self.pending.len() >= self.capacity {
            return Err(IngressBufferFull);
        }
        self.pending.insert(key, payload);
        Ok(())
    }

    /// Removes and returns every command whose deadline is **strictly before**
    /// `now_us`, in `(deadline_us, session_id, arrival_sequence)` order.
    ///
    /// The strict `<` is the **release horizon** (module docs): because offsets are
    /// non-negative and the clock is monotonic, once the clock has passed an entry's
    /// deadline no not-yet-arrived command can order before it, so the drained batch
    /// is exactly `{ key : deadline < now_us }` in strict key order and successive
    /// batches have non-decreasing deadlines — the concatenation is the global total
    /// order. An entry with `deadline == now_us` is **kept** (a later, equal-deadline,
    /// lower-`(session, seq)` arrival could still be in flight), released only once
    /// the clock strictly passes it.
    #[must_use]
    pub fn drain_below(&mut self, now_us: u64) -> Vec<(ReleaseKey, T)> {
        // The minimum key at `deadline == now_us`: the empty session id is the least
        // `str`, arrival_sequence 0 the least counter, so every key with
        // `deadline < now_us` sorts strictly below it and every key with
        // `deadline >= now_us` sorts at or above it.
        let boundary = ReleaseKey::new(now_us, Arc::from(""), 0);
        // `split_off` keeps `< boundary` in `self.pending` and returns `>= boundary`;
        // swap so `self.pending` retains the not-yet-due remainder and we own the due
        // set (BTreeMap iteration yields it in ascending key order).
        let remainder = self.pending.split_off(&boundary);
        let due = std::mem::replace(&mut self.pending, remainder);
        due.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::microstructure::{LatencyConfig, LatencyOffset};

    const SEED: u64 = 0x1234_5678_9ABC_DEF0;

    fn key(deadline_us: u64, session: &str, arrival: u64) -> ReleaseKey {
        ReleaseKey::new(deadline_us, Arc::from(session), arrival)
    }

    // ---- release-key total order --------------------------------------------

    #[test]
    fn test_release_key_orders_by_deadline_then_session_then_arrival() {
        // Deadline dominates.
        assert!(key(10, "z", 0) < key(20, "a", 0));
        // Equal deadline → session id breaks the tie.
        assert!(key(10, "a", 100) < key(10, "b", 0));
        // Equal deadline + session → arrival sequence breaks the tie.
        assert!(key(10, "a", 1) < key(10, "a", 2));
    }

    // ---- deadline computation: clamp + checked ------------------------------

    #[test]
    fn test_release_deadline_is_arrival_plus_offset() {
        // 1_000 ms → 1_000_000 µs, plus a 250 µs offset.
        assert_eq!(
            release_deadline_us(1_000, LatencyOffset::from_micros(250)).expect("in range"),
            1_000 * 1_000 + 250
        );
        // A zero offset is exactly the arrival instant in µs.
        assert_eq!(
            release_deadline_us(1_000, LatencyOffset::ZERO).expect("in range"),
            1_000_000
        );
    }

    #[test]
    fn test_release_deadline_clamps_a_hostile_offset_to_the_horizon() {
        // A `u64::MAX` offset (the non-finite fail-safe) is clamped to the horizon,
        // not left to hold the command forever.
        let hostile = LatencyOffset::from_micros(u64::MAX);
        let deadline = release_deadline_us(1_000, hostile).expect("in range");
        assert_eq!(deadline, 1_000 * 1_000 + MAX_INGRESS_OFFSET_US);
        assert!(deadline < u64::MAX, "the clamp bounds the deadline");
    }

    #[test]
    fn test_release_deadline_out_of_range_is_a_typed_error_not_a_saturated_deadline() {
        // An absurd virtual instant near the u64 ceiling would overflow the ms→µs
        // promotion. It is a typed `ReleaseDeadlineOverflow` — NEVER a manufactured
        // `u64::MAX` deadline that could never be released (#111 review).
        assert_eq!(
            release_deadline_us(u64::MAX, LatencyOffset::from_micros(1)),
            Err(ReleaseDeadlineOverflow)
        );
    }

    // ---- bounded: a flood is a typed drop, not unbounded growth --------------

    #[test]
    fn test_buffer_is_bounded_and_drops_at_capacity() {
        let mut buffer: IngressReorderBuffer<u32> = IngressReorderBuffer::new(3);
        assert_eq!(buffer.capacity(), 3);
        for seq in 0..3 {
            assert_eq!(buffer.insert(key(100, "s", seq), seq as u32), Ok(()));
        }
        // The 4th insert is rejected — bounded, never growing past capacity.
        assert_eq!(
            buffer.insert(key(100, "s", 3), 3),
            Err(IngressBufferFull),
            "a flood past capacity is a typed drop"
        );
        assert_eq!(buffer.len(), 3);
    }

    #[test]
    fn test_buffer_capacity_is_clamped_to_at_least_one() {
        let mut buffer: IngressReorderBuffer<u32> = IngressReorderBuffer::new(0);
        assert_eq!(buffer.capacity(), 1);
        assert_eq!(buffer.insert(key(1, "s", 0), 0), Ok(()));
        assert_eq!(buffer.insert(key(1, "s", 1), 1), Err(IngressBufferFull));
    }

    // ---- release horizon: strict `<`, drained in key order ------------------

    #[test]
    fn test_drain_below_releases_strictly_before_now_in_key_order() {
        let mut buffer: IngressReorderBuffer<&str> = IngressReorderBuffer::new(16);
        // Insert out of order; the buffer must release in deadline order.
        buffer.insert(key(30, "s", 2), "c").unwrap();
        buffer.insert(key(10, "s", 0), "a").unwrap();
        buffer.insert(key(20, "s", 1), "b").unwrap();
        // now_us = 25: entries with deadline strictly < 25 release (a, b), in order.
        let due = buffer.drain_below(25);
        let released: Vec<&str> = due.iter().map(|(_, v)| *v).collect();
        assert_eq!(released, vec!["a", "b"]);
        // The deadline==30 entry (>= 25) is kept.
        assert_eq!(buffer.len(), 1);
        // Advancing past it releases it.
        let due = buffer.drain_below(31);
        assert_eq!(
            due.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
            vec!["c"]
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_drain_below_keeps_equal_deadline_entries() {
        // An entry whose deadline exactly equals `now_us` is NOT released — a later
        // equal-deadline, lower-tie arrival could still be in flight (strict horizon).
        let mut buffer: IngressReorderBuffer<&str> = IngressReorderBuffer::new(16);
        buffer.insert(key(10, "s", 0), "a").unwrap();
        assert!(buffer.drain_below(10).is_empty(), "deadline == now is kept");
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.drain_below(11).len(), 1, "deadline < now releases");
    }

    #[test]
    fn test_drain_below_tie_breaks_session_before_arrival() {
        // At equal deadline, session id dominates the arrival counter.
        let mut buffer: IngressReorderBuffer<&str> = IngressReorderBuffer::new(16);
        // "b" arrived FIRST (lower arrival) but "a" sorts first on session id.
        buffer.insert(key(10, "b", 0), "b-first-arrival").unwrap();
        buffer.insert(key(10, "a", 1), "a-later-arrival").unwrap();
        let released: Vec<&str> = buffer.drain_below(11).into_iter().map(|(_, v)| v).collect();
        assert_eq!(released, vec!["a-later-arrival", "b-first-arrival"]);
    }

    // ---- the queue-race reorder, at the buffer level ------------------------

    #[test]
    fn test_seeded_offsets_reorder_arrivals_by_deadline() {
        // Two messages arrive at the SAME venue instant; the one with the larger
        // seeded offset gets the later deadline and releases second — the reorder is
        // a pure function of the seeded draw.
        let config = LatencyConfig::Uniform {
            min_us: 0,
            max_us: 1_000_000,
        };
        let now_ms = 1_000u64;
        // Find two identities whose seeded draws differ, then assert the buffer
        // releases them in ascending-offset (deadline) order regardless of insert
        // order.
        let off_a = config.draw(SEED, "sess", 1);
        let off_b = config.draw(SEED, "sess", 2);
        assert_ne!(off_a, off_b, "distinct seq draw distinct offsets");
        let (early, late) = if off_a < off_b {
            (("a", off_a), ("b", off_b))
        } else {
            (("b", off_b), ("a", off_a))
        };
        let mut buffer: IngressReorderBuffer<&str> = IngressReorderBuffer::new(16);
        // Insert the LATE one first (it "arrived" first in wall order).
        buffer
            .insert(
                key(
                    release_deadline_us(now_ms, late.1).expect("in range"),
                    "sess",
                    0,
                ),
                "late-arrival-large-offset",
            )
            .unwrap();
        buffer
            .insert(
                key(
                    release_deadline_us(now_ms, early.1).expect("in range"),
                    "sess",
                    1,
                ),
                "early-deadline-small-offset",
            )
            .unwrap();
        // Advance well past both deadlines and drain: the smaller-offset command
        // reaches the sequencer FIRST despite arriving second.
        let released: Vec<&str> = buffer
            .drain_below(u64::MAX)
            .into_iter()
            .map(|(_, v)| v)
            .collect();
        assert_eq!(
            released,
            vec!["early-deadline-small-offset", "late-arrival-large-offset"],
            "the larger seeded offset loses the queue race"
        );
    }
}
