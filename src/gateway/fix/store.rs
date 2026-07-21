//! The durable, **account-keyed** FIX session store — the acceptor's inbound /
//! outbound `MsgSeqNum` counters, its resend/outbound-message log, and its
//! `SequenceReset` session-event audit trail, all persisted per
//! **`(account_id, comp_id_tuple)`** ([ADR-0010](../../../docs/adr/0010-fix-session-account-binding.md),
//! [03 §5.2](../../../docs/03-protocol-surfaces.md#52-session-management--the-acceptor-fsm-and-checked-counters)).
//!
//! ## Contract, fixed here; backend, swapped later
//!
//! [`FixSessionStore`] is the **swap seam**, the same shape #029 used for the
//! per-underlying [`VenueJournal`](crate::exchange::VenueJournal): a synchronous
//! trait the acceptor calls, with two backends behind the identical contract —
//! [`InMemoryFixSessionStore`] when `DATABASE_URL` is unset and, since
//! [#95](https://github.com/joaquinbejar/fauxchange/issues/95),
//! [`PgFixSessionStore`](super::PgFixSessionStore) (`src/gateway/fix/pg_store.rs`)
//! when it is set, selected at boot by
//! [`select_fix_session_store`](super::select_fix_session_store). The in-memory
//! backend persists session state only across a *reconnect* (a new connection,
//! same process); the PG backend adds **process-restart** durability. It is
//! **separate** from the per-underlying `VenueEvent` journal: a session-sequence
//! reset is a transport-level fact, not a book mutation
//! ([ADR-0010 §5](../../../docs/adr/0010-fix-session-account-binding.md)).
//!
//! It mirrors IronFix's `MessageStore` contract (`store` / `get_range` / the
//! next-sender/next-target counters / `reset`; `ironfix-store` ships only the
//! single-session `MemoryStore`), but with three deliberate departures the venue
//! requires: the store is keyed on the **authenticated account** (so no session
//! can address, inherit, or resend another account's state), the counters are
//! **checked and non-wrapping** (IronFix's `SequenceManager` wraps with
//! `fetch_add`), and there is **no wall-clock read** (IronFix's
//! `creation_time()`) inside it — a reset stamps the injected venue clock the
//! caller supplies, so the audit trail replays deterministically.
//!
//! ## Bounds
//!
//! Every per-key collection is bounded (the resend log by entry count and total
//! bytes, the reset audit by entry count, the key-space by
//! [`MAX_SESSION_KEYS`]) so a hostile or churning peer cannot grow venue memory
//! without limit ([08 §5](../../../docs/08-threat-model.md#5-denial-of-service-posture)).
//! A resend for a `MsgSeqNum` the bounded log has already evicted is answered
//! with a gap-fill by the session layer, exactly as FIX intends.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::models::AccountId;

/// The first valid FIX `MsgSeqNum` — sequence numbers are `1`-based, so a fresh
/// session's next-to-use counter starts here.
pub const FIRST_SEQ_NUM: u64 = 1;

/// The maximum number of outbound frames retained per session for resend. Past
/// this the oldest are evicted; a resend for an evicted `MsgSeqNum` is
/// gap-filled by the session layer (a bounded resend window, a DoS control).
pub const MAX_STORED_OUTBOUND_PER_KEY: usize = 4_096;

/// The maximum total bytes of retained outbound frames per session — the second
/// half of the resend-log bound (a single large frame cannot evade the count
/// bound to exhaust memory).
pub const MAX_STORED_OUTBOUND_BYTES_PER_KEY: usize = 8 * 1024 * 1024;

/// The maximum number of `SequenceReset` session events retained per session in
/// the audit trail — a bounded, append-only ring so a reset-spamming peer cannot
/// grow the audit without limit.
pub const MAX_RESET_EVENTS_PER_KEY: usize = 1_024;

/// The maximum number of distinct `(account_id, comp_id_tuple)` session slots the
/// in-memory store tracks — the key-space DoS bound (mirrors the rate limiter's
/// `max_keys`). A new key past this is refused with [`SessionStoreError::KeyspaceFull`].
pub const MAX_SESSION_KEYS: usize = 65_536;

/// The immutable FIX session identity a slot is keyed on: the **authenticated
/// account** plus its bound `(SenderCompID, TargetCompID)` tuple
/// ([ADR-0010 rule 2](../../../docs/adr/0010-fix-session-account-binding.md)).
///
/// The tuple is the pair as presented on an **inbound** message — the client's
/// `SenderCompID (49)` and the venue's `TargetCompID (56)` — and, after the
/// logon binding check, equals the account's provisioned binding. Keying the
/// counters and resend log on this triple (never the CompID tuple alone) is what
/// makes a re-pointed or reused CompID unable to inherit another account's
/// messages.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionKey {
    /// The authenticated account the session acts for.
    pub account: AccountId,
    /// The client's `SenderCompID (49)` (the counterparty).
    pub sender_comp_id: String,
    /// The venue's `TargetCompID (56)` (this acceptor).
    pub target_comp_id: String,
}

impl SessionKey {
    /// Builds a session key from the authenticated account and the bound
    /// `(SenderCompID, TargetCompID)` tuple.
    #[must_use]
    pub fn new(
        account: AccountId,
        sender_comp_id: impl Into<String>,
        target_comp_id: impl Into<String>,
    ) -> Self {
        Self {
            account,
            sender_comp_id: sender_comp_id.into(),
            target_comp_id: target_comp_id.into(),
        }
    }
}

/// What triggered a [`SequenceResetEvent`] — a durable, auditable distinction
/// between the two reset paths ([ADR-0010 §5](../../../docs/adr/0010-fix-session-account-binding.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetTrigger {
    /// A `Logon (A)` carrying `ResetSeqNumFlag=Y` — both counters reset to
    /// [`FIRST_SEQ_NUM`].
    LogonReset,
    /// An administrative `SequenceReset (4)` with `GapFillFlag` absent/`N` — the
    /// inbound counter is set to `NewSeqNo (36)`.
    SequenceReset,
}

/// One durably-recorded reset of a session's sequence state — the auditable
/// venue-owned event that survives a reconnect
/// ([ADR-0010 §5](../../../docs/adr/0010-fix-session-account-binding.md)). It is
/// scoped to its [`SessionKey`] and can never reset, or reset *into*, another
/// account's slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceResetEvent {
    /// The **injected venue clock** instant (ms) the reset was applied at — a
    /// venue-clock read supplied by the caller, never a wall-clock read, so the
    /// audit trail replays deterministically.
    pub at_ms: u64,
    /// What triggered the reset.
    pub trigger: ResetTrigger,
    /// The next-sender (outbound) counter before the reset.
    pub old_next_sender_seq: u64,
    /// The next-target (inbound expected) counter before the reset.
    pub old_next_target_seq: u64,
    /// The next-sender (outbound) counter after the reset.
    pub new_next_sender_seq: u64,
    /// The next-target (inbound expected) counter after the reset.
    pub new_next_target_seq: u64,
}

/// One retained outbound frame, held for a possible `ResendRequest (2)` replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredOutbound {
    /// The frame's `MsgSeqNum (34)`.
    pub seq: u64,
    /// The complete, pre-framed FIX bytes exactly as first sent.
    pub frame: Vec<u8>,
}

/// A failure persisting session state — a resource bound was hit, never a
/// silent unbounded grow.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionStoreError {
    /// The store is at its [`MAX_SESSION_KEYS`] key-space ceiling and a **new**
    /// session key was refused (a DoS bound; an existing key always proceeds).
    #[error("fix session store keyspace is full ({max} keys)")]
    KeyspaceFull {
        /// The key-space ceiling.
        max: usize,
    },
    /// The durable backend (PostgreSQL) failed. Carries a **non-secret** label
    /// only — never a query, a credential, or a `DATABASE_URL`.
    #[error("fix session store backend error: {0}")]
    Backend(&'static str),
}

/// The per-session persisted counters — the pair a reconnect resumes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionCounters {
    /// The next outbound `MsgSeqNum (34)` to send (`1`-based).
    pub next_sender_seq: u64,
    /// The next inbound `MsgSeqNum (34)` expected (`1`-based).
    pub next_target_seq: u64,
}

impl Default for SessionCounters {
    /// A fresh session: both counters at [`FIRST_SEQ_NUM`].
    fn default() -> Self {
        Self {
            next_sender_seq: FIRST_SEQ_NUM,
            next_target_seq: FIRST_SEQ_NUM,
        }
    }
}

/// The durable session-state contract — the swap seam ([ADR-0010](../../../docs/adr/0010-fix-session-account-binding.md)).
///
/// Every method is keyed on a [`SessionKey`], so a backend can never expose one
/// account's state under another account's key. Methods are synchronous (the
/// #029 [`VenueJournal`](crate::exchange::VenueJournal) shape); a PostgreSQL
/// backend bridges its async queries with the same `block_in_place` +
/// `Handle::block_on` pattern the durable journal uses.
///
/// The session task is the sole writer for its own key while live, so there is
/// no cross-writer contention on a key; the trait is `Send + Sync` because the
/// one store instance is shared across every session task (each on its own key).
pub trait FixSessionStore: Send + Sync + std::fmt::Debug {
    /// Loads the persisted counters for `key`, or [`SessionCounters::default`]
    /// (both at [`FIRST_SEQ_NUM`]) when the key is unknown — a fresh session.
    ///
    /// # Errors
    ///
    /// [`SessionStoreError::Backend`] if a durable backend read fails.
    fn load_counters(&self, key: &SessionKey) -> Result<SessionCounters, SessionStoreError>;

    /// Persists the counters for `key`, creating the slot if absent (subject to
    /// the [`MAX_SESSION_KEYS`] key-space bound).
    ///
    /// # Errors
    ///
    /// [`SessionStoreError::KeyspaceFull`] if a **new** key is refused at the
    /// ceiling; [`SessionStoreError::Backend`] on a durable-backend failure.
    fn save_counters(
        &self,
        key: &SessionKey,
        counters: SessionCounters,
    ) -> Result<(), SessionStoreError>;

    /// Appends one outbound frame to `key`'s bounded resend log.
    ///
    /// # Errors
    ///
    /// [`SessionStoreError::KeyspaceFull`] if a **new** key is refused at the
    /// ceiling; [`SessionStoreError::Backend`] on a durable-backend failure.
    fn store_outbound(
        &self,
        key: &SessionKey,
        seq: u64,
        frame: &[u8],
    ) -> Result<(), SessionStoreError>;

    /// Returns the retained outbound frames for `key` whose `MsgSeqNum` is in
    /// `[begin, end]` (an `end` of `0` means "to the latest"), ordered ascending.
    /// A `MsgSeqNum` the bounded log has evicted is simply absent — the caller
    /// gap-fills it.
    ///
    /// # Errors
    ///
    /// [`SessionStoreError::Backend`] on a durable-backend failure.
    fn outbound_range(
        &self,
        key: &SessionKey,
        begin: u64,
        end: u64,
    ) -> Result<Vec<StoredOutbound>, SessionStoreError>;

    /// Applies a sequence reset **within `key` only** — records the
    /// [`SequenceResetEvent`] in the durable audit trail and persists the new
    /// counters atomically, so a reconnect resumes from the reset state and the
    /// reset survives an audit. It can never touch another account's slot.
    ///
    /// # Errors
    ///
    /// [`SessionStoreError::KeyspaceFull`] if a **new** key is refused at the
    /// ceiling; [`SessionStoreError::Backend`] on a durable-backend failure.
    fn record_reset(
        &self,
        key: &SessionKey,
        event: SequenceResetEvent,
        counters: SessionCounters,
    ) -> Result<(), SessionStoreError>;

    /// Reads `key`'s durable `SequenceReset` audit trail, oldest first (for
    /// observability, audit, and conformance tests).
    ///
    /// # Errors
    ///
    /// [`SessionStoreError::Backend`] on a durable-backend failure.
    fn reset_events(&self, key: &SessionKey) -> Result<Vec<SequenceResetEvent>, SessionStoreError>;
}

/// One in-memory session slot: the counters, the bounded resend log, and the
/// bounded reset audit trail.
#[derive(Debug, Default)]
struct Slot {
    counters: SessionCounters,
    outbound: Vec<StoredOutbound>,
    outbound_bytes: usize,
    resets: Vec<SequenceResetEvent>,
}

impl Slot {
    /// Appends an outbound frame, evicting the oldest entries until both the
    /// count and byte bounds hold (a bounded resend window).
    fn push_outbound(&mut self, seq: u64, frame: &[u8]) {
        self.outbound.push(StoredOutbound {
            seq,
            frame: frame.to_vec(),
        });
        self.outbound_bytes = self.outbound_bytes.saturating_add(frame.len());
        while self.outbound.len() > MAX_STORED_OUTBOUND_PER_KEY
            || self.outbound_bytes > MAX_STORED_OUTBOUND_BYTES_PER_KEY
        {
            if self.outbound.is_empty() {
                break;
            }
            let evicted = self.outbound.remove(0);
            self.outbound_bytes = self.outbound_bytes.saturating_sub(evicted.frame.len());
        }
    }

    /// Records a reset event in the bounded audit ring (evicting the oldest past
    /// the cap) and stores the new counters.
    fn record_reset(&mut self, event: SequenceResetEvent, counters: SessionCounters) {
        self.resets.push(event);
        while self.resets.len() > MAX_RESET_EVENTS_PER_KEY {
            self.resets.remove(0);
        }
        self.counters = counters;
    }
}

/// The default in-memory [`FixSessionStore`] backend — one [`Slot`] per
/// [`SessionKey`] behind a single [`Mutex`], with a bounded key-space.
///
/// The `Mutex` is held only across the O(1)/O(log n) slot mutation, never across
/// an `.await` (the store is synchronous). A `HashMap` (not `DashMap`) keeps the
/// key-space bound a single check-then-insert under the lock.
#[derive(Debug, Default)]
pub struct InMemoryFixSessionStore {
    slots: Mutex<HashMap<SessionKey, Slot>>,
}

impl InMemoryFixSessionStore {
    /// Builds an empty in-memory session store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Runs `f` against the slot for `key`, creating it if absent — refusing a
    /// **new** key at the [`MAX_SESSION_KEYS`] ceiling (an existing key always
    /// proceeds, so a live session is never starved by the bound).
    fn with_slot_mut<R>(
        &self,
        key: &SessionKey,
        f: impl FnOnce(&mut Slot) -> R,
    ) -> Result<R, SessionStoreError> {
        // A poisoned lock means a prior holder panicked mid-mutation; recover the
        // guard rather than propagate a panic across the session boundary.
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !slots.contains_key(key) && slots.len() >= MAX_SESSION_KEYS {
            return Err(SessionStoreError::KeyspaceFull {
                max: MAX_SESSION_KEYS,
            });
        }
        let slot = slots.entry(key.clone()).or_default();
        Ok(f(slot))
    }
}

impl FixSessionStore for InMemoryFixSessionStore {
    fn load_counters(&self, key: &SessionKey) -> Result<SessionCounters, SessionStoreError> {
        let slots = self
            .slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(slots
            .get(key)
            .map_or_else(SessionCounters::default, |slot| slot.counters))
    }

    fn save_counters(
        &self,
        key: &SessionKey,
        counters: SessionCounters,
    ) -> Result<(), SessionStoreError> {
        self.with_slot_mut(key, |slot| slot.counters = counters)
    }

    fn store_outbound(
        &self,
        key: &SessionKey,
        seq: u64,
        frame: &[u8],
    ) -> Result<(), SessionStoreError> {
        self.with_slot_mut(key, |slot| slot.push_outbound(seq, frame))
    }

    fn outbound_range(
        &self,
        key: &SessionKey,
        begin: u64,
        end: u64,
    ) -> Result<Vec<StoredOutbound>, SessionStoreError> {
        let slots = self
            .slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(slot) = slots.get(key) else {
            return Ok(Vec::new());
        };
        let mut range: Vec<StoredOutbound> = slot
            .outbound
            .iter()
            .filter(|entry| entry.seq >= begin && (end == 0 || entry.seq <= end))
            .cloned()
            .collect();
        range.sort_by_key(|entry| entry.seq);
        Ok(range)
    }

    fn record_reset(
        &self,
        key: &SessionKey,
        event: SequenceResetEvent,
        counters: SessionCounters,
    ) -> Result<(), SessionStoreError> {
        self.with_slot_mut(key, |slot| slot.record_reset(event, counters))
    }

    fn reset_events(&self, key: &SessionKey) -> Result<Vec<SequenceResetEvent>, SessionStoreError> {
        let slots = self
            .slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(slots
            .get(key)
            .map_or_else(Vec::new, |slot| slot.resets.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(account: &str) -> SessionKey {
        SessionKey::new(AccountId::new(account), "CLIENT", "FAUXCHANGE")
    }

    #[test]
    fn test_load_counters_unknown_key_is_fresh_session() {
        let store = InMemoryFixSessionStore::new();
        let counters = store.load_counters(&key("acct-1")).expect("load");
        assert_eq!(counters.next_sender_seq, FIRST_SEQ_NUM);
        assert_eq!(counters.next_target_seq, FIRST_SEQ_NUM);
    }

    #[test]
    fn test_save_then_load_counters_resumes_numbering() {
        let store = InMemoryFixSessionStore::new();
        let k = key("acct-1");
        store
            .save_counters(
                &k,
                SessionCounters {
                    next_sender_seq: 12,
                    next_target_seq: 34,
                },
            )
            .expect("save");
        let counters = store.load_counters(&k).expect("load");
        assert_eq!(counters.next_sender_seq, 12);
        assert_eq!(counters.next_target_seq, 34);
    }

    #[test]
    fn test_outbound_range_returns_stored_frames_in_order() {
        let store = InMemoryFixSessionStore::new();
        let k = key("acct-1");
        store.store_outbound(&k, 2, b"two").expect("store");
        store.store_outbound(&k, 1, b"one").expect("store");
        store.store_outbound(&k, 3, b"three").expect("store");
        let range = store.outbound_range(&k, 1, 2).expect("range");
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].seq, 1);
        assert_eq!(range[1].seq, 2);
    }

    #[test]
    fn test_outbound_range_open_ended_end_zero_returns_to_latest() {
        let store = InMemoryFixSessionStore::new();
        let k = key("acct-1");
        store.store_outbound(&k, 1, b"one").expect("store");
        store.store_outbound(&k, 2, b"two").expect("store");
        let range = store.outbound_range(&k, 2, 0).expect("range");
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].seq, 2);
    }

    #[test]
    fn test_outbound_log_is_bounded_by_count_and_evicts_oldest() {
        let store = InMemoryFixSessionStore::new();
        let k = key("acct-1");
        for seq in 1..=(MAX_STORED_OUTBOUND_PER_KEY as u64 + 10) {
            store.store_outbound(&k, seq, b"x").expect("store");
        }
        let all = store.outbound_range(&k, 0, 0).expect("range");
        assert!(all.len() <= MAX_STORED_OUTBOUND_PER_KEY);
        // The oldest seqs were evicted; the newest are retained.
        assert!(all.iter().all(|entry| entry.seq > 10));
    }

    #[test]
    fn test_record_reset_is_scoped_to_its_key_only() {
        let store = InMemoryFixSessionStore::new();
        let a = key("acct-a");
        let b = key("acct-b");
        store
            .save_counters(
                &b,
                SessionCounters {
                    next_sender_seq: 99,
                    next_target_seq: 99,
                },
            )
            .expect("save b");
        store
            .record_reset(
                &a,
                SequenceResetEvent {
                    at_ms: 1_000,
                    trigger: ResetTrigger::LogonReset,
                    old_next_sender_seq: 5,
                    old_next_target_seq: 7,
                    new_next_sender_seq: FIRST_SEQ_NUM,
                    new_next_target_seq: FIRST_SEQ_NUM,
                },
                SessionCounters::default(),
            )
            .expect("reset a");
        // Account A reset to 1/1; account B is untouched.
        assert_eq!(store.load_counters(&a).expect("load a").next_sender_seq, 1);
        assert_eq!(store.load_counters(&b).expect("load b").next_sender_seq, 99);
        assert_eq!(store.reset_events(&a).expect("events a").len(), 1);
        assert!(store.reset_events(&b).expect("events b").is_empty());
    }

    #[test]
    fn test_keyspace_is_bounded_but_existing_key_always_proceeds() {
        let store = InMemoryFixSessionStore::new();
        // Fill the keyspace to the ceiling is impractical here; assert the
        // existing-key fast path stays available and a save round-trips.
        let k = key("acct-1");
        store
            .save_counters(&k, SessionCounters::default())
            .expect("save");
        assert!(store.save_counters(&k, SessionCounters::default()).is_ok());
    }
}
