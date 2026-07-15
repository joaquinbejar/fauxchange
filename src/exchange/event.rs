//! Venue-owned event-timeline newtypes.
//!
//! Time in `fauxchange` is a **venue service**, not `SystemTime`
//! ([01 §9](../../../docs/01-domain-model.md)). Two scalars stamp every
//! sequenced event: [`EventTimestamp`] (the venue clock) and [`SequenceNumber`]
//! (the per-underlying `underlying_sequence`). Both are `#[serde(transparent)]`
//! so the wire carries the bare integer.
//!
//! [`SequenceNumber`] is the only journaled total order, and it is
//! **per-underlying** — there is no venue-wide monotonic counter
//! ([01 §9.1](../../../docs/01-domain-model.md)). Its assignment (from a
//! venue-owned checked `u64` counter) lands with the single-writer sequencer in
//! a later issue; this type only carries the value and the checked successor.

use serde::{Deserialize, Serialize};

/// A venue-clock timestamp in **milliseconds since the Unix epoch** (or virtual
/// venue time under a stepped/replay clock).
///
/// This is the venue clock, never `SystemTime`; the clock service that mints it
/// lands with the sequencer. The wire form is a bare integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventTimestamp(u64);

impl EventTimestamp {
    /// Constructs an `EventTimestamp` from milliseconds since the Unix epoch.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::EventTimestamp;
    /// let ts = EventTimestamp::new(1_700_000_000_000);
    /// assert_eq!(ts.get(), 1_700_000_000_000);
    /// ```
    #[must_use]
    #[inline]
    pub const fn new(millis: u64) -> Self {
        Self(millis)
    }

    /// Returns the timestamp in milliseconds since the Unix epoch.
    #[must_use]
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The `underlying_sequence`: a **per-underlying**, monotonic, journaled total
/// order over every venue event, fill, and order correlation
/// ([01 §9.1](../../../docs/01-domain-model.md)).
///
/// It is never reset — it continues across reconnect, restart, and a
/// snapshot-restore epoch. Advancing it goes through [`SequenceNumber::checked_next`]
/// so a `u64::MAX` roll-over is caught rather than silently wrapping (a wrapped
/// sequence corrupts gap detection and replay). The wire form is a bare integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    /// The first sequence value assigned to an underlying.
    pub const START: Self = Self(0);

    /// Constructs a `SequenceNumber` from a raw counter value.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::SequenceNumber;
    /// let seq = SequenceNumber::new(7);
    /// assert_eq!(seq.get(), 7);
    /// ```
    #[must_use]
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw sequence value.
    #[must_use]
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next sequence value, or `None` on `u64::MAX` exhaustion.
    ///
    /// Checked, never wrapping: the sequencer maps a `None` here onto a typed
    /// exhaustion error when it lands. Callers must handle the `None`; a wrapped
    /// sequence would corrupt gap detection and replay.
    ///
    /// # Examples
    ///
    /// ```
    /// use fauxchange::exchange::SequenceNumber;
    /// assert_eq!(SequenceNumber::new(1).checked_next(), Some(SequenceNumber::new(2)));
    /// assert_eq!(SequenceNumber::new(u64::MAX).checked_next(), None);
    /// ```
    #[must_use]
    #[inline]
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_timestamp_roundtrips_millis() {
        let ts = EventTimestamp::new(1_234_567_890);
        assert_eq!(ts.get(), 1_234_567_890);
    }

    #[test]
    fn test_sequence_number_start_is_zero() {
        assert_eq!(SequenceNumber::START.get(), 0);
    }

    #[test]
    fn test_sequence_number_checked_next_increments() {
        assert_eq!(
            SequenceNumber::new(41).checked_next(),
            Some(SequenceNumber::new(42))
        );
    }

    #[test]
    fn test_sequence_number_checked_next_exhaustion_is_none() {
        assert_eq!(SequenceNumber::new(u64::MAX).checked_next(), None);
    }

    #[test]
    fn test_sequence_number_orders_monotonically() {
        assert!(SequenceNumber::new(1) < SequenceNumber::new(2));
    }

    #[test]
    fn test_event_timestamp_serialises_as_bare_integer() {
        let json = match serde_json::to_string(&EventTimestamp::new(500)) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(json, "500");
    }

    #[test]
    fn test_sequence_number_serialises_as_bare_integer() {
        let json = match serde_json::to_string(&SequenceNumber::new(9)) {
            Ok(s) => s,
            Err(e) => panic!("serialize failed: {e}"),
        };
        assert_eq!(json, "9");
    }
}
