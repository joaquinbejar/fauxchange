//! The venue-wide, account-scoped `(account, ClOrdID) → order_id` correlation
//! index (#098) — the **cross-session** bridge from a client's order id namespace
//! to the venue order id the gateway minted, reachable from every FIX connection
//! and the REST surface through [`crate::state::AppState`].
//!
//! ## Why it exists
//!
//! #039 correlates a FIX `OrigClOrdID (41)` to a [`VenueOrderId`] through a
//! **per-session** map living inside one [`crate::gateway::fix::VenueFixSession`].
//! A cancel/replace/status on a **new** connection (or after a reconnect) finds
//! that map empty and answers `9 Unknown order`. This index lifts the correlation
//! to venue-shared state so an order placed in a prior session is
//! cancel/replace/status-correlatable in a later one, on the **same** account.
//!
//! ## It is a derived, journal-scoped artifact — not a second source of truth
//!
//! The index is a **deterministic function of the journaled `AddOrder` stream**:
//! every placement carries its `(account, client_order_id, order_id)`, so the
//! mapping is fully reconstructable by re-executing the journal. The recovery
//! seam that rebuilds it — [`recover_with_index`](crate::exchange::recover_with_index)
//! — is implemented and tested here; on restart the index **will be rebuilt
//! during #085 boot recovery** from those same commands once #085 wires that
//! recovery into `AppState::new` (until then a restart against a non-empty
//! journal still fails loud, as #085 tracks). There is no separate durable copy
//! to keep in sync. It is never journaled itself, never
//! read on the sequenced decision path, and never affects a book mutation, a fill,
//! or a [`VenueOutcome`](crate::exchange::VenueOutcome) — so it sits **outside**
//! the replay-equality scope exactly like mark prices do. Populating it is a pure
//! side effect the executor performs **after** the outcome is captured.
//!
//! Below the [`DEFAULT_MAX_CLORDID_INDEX_ENTRIES`] ceiling the index **content**
//! is thus a deterministic function of the journal (the same journal rebuilds the
//! same set of entries). The ceiling itself is a memory-DoS backstop whose
//! drop-order across concurrently-sequenced underlyings is not deterministic — the
//! same non-guarantee the per-session tracking map carries — but a dropped entry
//! only costs cross-session correlation for that one order, never correctness of
//! the book.
//!
//! ## Account isolation (security-critical)
//!
//! The key is `(account, ClOrdID)`: a resolution can only ever return an order the
//! **authenticated account** placed. A colliding `ClOrdID` under a different
//! account is a different key, so account B can never resolve or cancel account
//! A's order. A cross-account probe is a plain [`None`] — **indistinguishable** at
//! the client boundary from a genuinely unknown id (the #132 masking), so the
//! index never leaks that another account owns the id.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::exchange::boundary::Side;
use crate::exchange::envelope::{VenueCommand, VenueOutcome};
use crate::exchange::symbol::Symbol;
use crate::models::{AccountId, ClientOrderId, VenueOrderId};

/// The venue-wide ceiling on distinct `(account, ClOrdID)` entries — a memory-DoS
/// bound mirroring the per-session tracking cap
/// ([`crate::gateway::fix`]) and the idempotency map. Once full, a further
/// **new** placement still sequences and reports, but is no longer
/// cross-session-correlatable (a later `OrigClOrdID` for it answers
/// `OrderCancelReject (9)` / an unknown-order status); an **existing** key is
/// still updated (an upsert never trips the ceiling).
pub const DEFAULT_MAX_CLORDID_INDEX_ENTRIES: usize = 1_000_000;

/// The per-**account** ceiling on distinct `(account, ClOrdID)` entries — a
/// **fairness / noisy-neighbor** bound layered under the venue-wide
/// [`DEFAULT_MAX_CLORDID_INDEX_ENTRIES`]. Without it a single account could place
/// enough unique-`ClOrdID` orders to exhaust the shared index for every other
/// account (the index has no eviction), so one account's footprint is capped to a
/// fraction of the global ceiling — many accounts must be busy at once before the
/// venue-wide ceiling can be reached. A new key past a single account's own cap is
/// refused with [`ClOrdIdIndexError::AccountFull`] (the order still sequences and
/// reports; it is only no longer cross-session-correlatable), exactly like the
/// venue-wide `Full` degrade.
pub const DEFAULT_MAX_CLORDID_PER_ACCOUNT: usize = 65_536;

/// The order metadata one `(account, ClOrdID)` resolves to — everything a gateway
/// needs to route a cancel/replace and render its report without re-reading the
/// single-writer book: the venue order id, its contract symbol, side, and the
/// placed quantity. `side` is the protocol-neutral upstream [`Side`] (the value
/// the executor holds), converted to the wire enum at the surface that renders it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClOrdIdRecord {
    /// The venue order id the gateway minted for the original placement.
    pub order_id: VenueOrderId,
    /// The contract symbol the order rests on (the routing key for a cancel).
    pub symbol: Symbol,
    /// The order side (upstream matching-seam [`Side`]).
    pub side: Side,
    /// The placed quantity, in **contracts**.
    pub quantity: u64,
}

/// The typed failure of an index write.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClOrdIdIndexError {
    /// The index is at its `max` distinct-key ceiling and the key is new — the
    /// placement is not cross-session-correlatable (a degraded-path drop, never a
    /// failed order).
    #[error("client-order-id index is full ({max} entries); order not cross-session correlatable")]
    Full {
        /// The ceiling that was hit.
        max: usize,
    },
    /// The **authenticated account** is at its per-account distinct-key ceiling
    /// and the key is new — a fairness bound so one account cannot monopolize the
    /// shared index (a degraded-path drop for that account only, never a failed
    /// order, and never observable to any other account).
    #[error(
        "client-order-id index is full for this account ({max} entries); \
         order not cross-session correlatable"
    )]
    AccountFull {
        /// The per-account ceiling that was hit.
        max: usize,
    },
}

/// The venue-wide, account-scoped `(account, ClOrdID) → order_id` index.
///
/// Shared behind an `Arc` in [`crate::state::AppState`]; the per-underlying
/// executors record into the **same** instance on the sequenced path, and the FIX
/// / REST surfaces resolve from it. The critical section is a single `HashMap`
/// operation under a [`std::sync::Mutex`] — held only for the insert/lookup, never
/// across an `.await`.
#[derive(Debug, Default)]
struct Inner {
    /// The `(account, ClOrdID) → order` map.
    map: HashMap<(AccountId, ClientOrderId), ClOrdIdRecord>,
    /// The number of distinct keys held per account — the per-account fairness
    /// counter, kept atomically with `map` under the one lock. It only ever grows
    /// (the index has no eviction yet), exactly like `map`.
    per_account: HashMap<AccountId, usize>,
}

#[derive(Debug)]
pub struct ClOrdIdIndex {
    inner: Mutex<Inner>,
    max_entries: usize,
    max_per_account: usize,
}

impl ClOrdIdIndex {
    /// Builds an empty index with the given venue-wide distinct-key ceiling and no
    /// tighter per-account bound (the per-account cap equals the venue-wide one, so
    /// the venue-wide ceiling governs). Prefer [`with_default_ceiling`](Self::with_default_ceiling)
    /// for the production path, which layers the [`DEFAULT_MAX_CLORDID_PER_ACCOUNT`]
    /// fairness bound.
    #[must_use]
    pub fn new(max_entries: usize) -> Self {
        Self::with_ceilings(max_entries, max_entries)
    }

    /// Builds an empty index with an explicit venue-wide ceiling AND a per-account
    /// sub-quota (`max_per_account` is clamped to at most `max_entries`).
    #[must_use]
    pub fn with_ceilings(max_entries: usize, max_per_account: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            max_entries,
            max_per_account: max_per_account.min(max_entries),
        }
    }

    /// Builds an empty index with the [`DEFAULT_MAX_CLORDID_INDEX_ENTRIES`]
    /// venue-wide ceiling and the [`DEFAULT_MAX_CLORDID_PER_ACCOUNT`] per-account
    /// fairness bound.
    #[must_use]
    pub fn with_default_ceiling() -> Self {
        Self::with_ceilings(
            DEFAULT_MAX_CLORDID_INDEX_ENTRIES,
            DEFAULT_MAX_CLORDID_PER_ACCOUNT,
        )
    }

    /// Records (or upserts) the order a client placed under `(account, cl_ord_id)`.
    ///
    /// An **existing** key is updated in place (no ceiling check — a same-key
    /// idempotency retry or a re-execution on recovery re-records the identical
    /// value). A **new** key is refused with [`ClOrdIdIndexError::Full`] once the
    /// index is at its ceiling, so the caller degrades (logs, keeps the order)
    /// rather than grow an unbounded map.
    ///
    /// # Errors
    ///
    /// [`ClOrdIdIndexError::Full`] if the key is new and the index is at its
    /// venue-wide `max_entries` ceiling; [`ClOrdIdIndexError::AccountFull`] if the
    /// key is new and the account is at its per-account sub-quota (checked after
    /// the venue-wide ceiling, so a saturated venue reports `Full`).
    pub fn record(
        &self,
        account: AccountId,
        cl_ord_id: ClientOrderId,
        record: ClOrdIdRecord,
    ) -> Result<(), ClOrdIdIndexError> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = (account, cl_ord_id);
        // Existing key: upsert in place — no ceiling/count change (a same-key
        // idempotency retry or a recovery re-execution re-records the identical
        // value, and must never be refused).
        if let Some(slot) = inner.map.get_mut(&key) {
            *slot = record;
            return Ok(());
        }
        // New key: the venue-wide ceiling first (a saturated venue reports `Full`),
        // then the per-account fairness sub-quota.
        if inner.map.len() >= self.max_entries {
            return Err(ClOrdIdIndexError::Full {
                max: self.max_entries,
            });
        }
        let account_count = inner.per_account.get(&key.0).copied().unwrap_or(0);
        if account_count >= self.max_per_account {
            return Err(ClOrdIdIndexError::AccountFull {
                max: self.max_per_account,
            });
        }
        let next = account_count
            .checked_add(1)
            .ok_or(ClOrdIdIndexError::AccountFull {
                max: self.max_per_account,
            })?;
        inner.per_account.insert(key.0.clone(), next);
        inner.map.insert(key, record);
        Ok(())
    }

    /// Resolves the order the **authenticated** `account` placed under `cl_ord_id`,
    /// or [`None`] if the account never placed it (or the id is unknown). A
    /// cross-account id is a different key, so it resolves to [`None`] — the caller
    /// cannot tell a foreign-owned id from an absent one.
    #[must_use]
    pub fn resolve(&self, account: &AccountId, cl_ord_id: &ClientOrderId) -> Option<ClOrdIdRecord> {
        // A borrowing key would need `(&AccountId, &ClientOrderId): Borrow`, which
        // the tuple does not provide; clone the two opaque id strings (cheap) to
        // form the owned lookup key, then clone the small record out under the lock.
        let key = (account.clone(), cl_ord_id.clone());
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .map
            .get(&key)
            .cloned()
    }

    /// The number of distinct `(account, ClOrdID)` entries (tests / observability).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .map
            .len()
    }

    /// Whether the index holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .map
            .is_empty()
    }
}

impl Default for ClOrdIdIndex {
    fn default() -> Self {
        Self::with_default_ceiling()
    }
}

/// Applies the deterministic `(account, ClOrdID)` correlation mutations implied by a
/// **committed** `(command, outcome)` pair to `index` (#098) — the **single** source
/// of truth for how a journaled event maps into the cross-session index.
///
/// It is a pure function of the committed pair, so the two call sites derive the
/// **identical** index state:
///
/// - the live single-writer actor runs it **post-journal** (after the paired
///   [`VenueEvent`](crate::exchange::VenueEvent) durably lands, before fan-out), so
///   an event-append failure never leaves an uncommitted mapping; and
/// - journal recovery ([`reduce_into_executor`](crate::exchange::recovery)) runs it
///   on each re-derived, oracle-verified event — already-journaled, hence also
///   post-journal — so a resumed venue rebuilds byte-for-byte the mapping the live
///   venue exposed.
///
/// A placement is recorded only when it carries a `client_order_id` **and** actually
/// entered the book (`Added` / `Market`, never `Rejected`, and never a `Duplicate`
/// idempotency retry — a retry's freshly-minted order id must never overwrite the
/// canonical mapping, #099/#098). A full index is a **degraded-path drop** (the
/// order still stands, just not cross-session correlatable) — logged `WARN`, never a
/// failed command.
pub(crate) fn apply_committed_correlation(
    index: &ClOrdIdIndex,
    underlying: &str,
    command: &VenueCommand,
    outcome: &VenueOutcome,
) {
    match (command, outcome) {
        // A fresh add that entered the book: record the canonical
        // `(account, ClOrdID) → order_id`. A `Duplicate` retry is NOT `Added`/`Market`,
        // so it never reaches here (fix 1) — the canonical mapping is preserved.
        (
            VenueCommand::AddOrder {
                account,
                client_order_id: Some(cl_ord_id),
                order_id,
                symbol,
                side,
                quantity,
                ..
            },
            VenueOutcome::Added { .. } | VenueOutcome::Market { .. },
        ) => {
            record_or_warn(
                index,
                underlying,
                account,
                cl_ord_id,
                ClOrdIdRecord {
                    order_id: order_id.clone(),
                    symbol: symbol.clone(),
                    side: *side,
                    quantity: *quantity,
                },
            );
        }
        _ => {}
    }
}

/// Records one correlation, logging (never failing) on a degraded full-index drop.
fn record_or_warn(
    index: &ClOrdIdIndex,
    underlying: &str,
    account: &AccountId,
    cl_ord_id: &ClientOrderId,
    record: ClOrdIdRecord,
) {
    if let Err(error) = index.record(account.clone(), cl_ord_id.clone(), record) {
        tracing::warn!(
            %underlying,
            %error,
            "client-order-id index full; order placed but not cross-session correlatable"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sym() -> Symbol {
        match Symbol::parse("BTC-20240329-50000-C") {
            Ok(s) => s,
            Err(e) => panic!("fixture symbol failed to parse: {e:?}"),
        }
    }

    fn record(order_id: &str) -> ClOrdIdRecord {
        ClOrdIdRecord {
            order_id: VenueOrderId::new(order_id),
            symbol: sym(),
            side: Side::Buy,
            quantity: 5,
        }
    }

    #[test]
    fn test_records_and_resolves_within_account() {
        let index = ClOrdIdIndex::with_default_ceiling();
        let account = AccountId::new("acct-a");
        let clid = ClientOrderId::new("cl-1");
        index
            .record(account.clone(), clid.clone(), record("order-1"))
            .expect("first record fits");
        let resolved = index.resolve(&account, &clid).expect("resolves");
        assert_eq!(resolved.order_id, VenueOrderId::new("order-1"));
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_account_isolation_masks_foreign_id_as_absent() {
        let index = ClOrdIdIndex::with_default_ceiling();
        let account_a = AccountId::new("acct-a");
        let account_b = AccountId::new("acct-b");
        // Both accounts use the SAME ClOrdID string — a deliberate collision.
        let clid = ClientOrderId::new("shared-cl-id");
        index
            .record(account_a.clone(), clid.clone(), record("order-a"))
            .expect("account A records");

        // Account B cannot resolve account A's order via the colliding id: the
        // lookup is a plain miss, indistinguishable from an unknown id.
        assert!(
            index.resolve(&account_b, &clid).is_none(),
            "account B must not resolve account A's colliding ClOrdID"
        );
        // Account A still resolves its own.
        assert_eq!(
            index.resolve(&account_a, &clid).map(|r| r.order_id),
            Some(VenueOrderId::new("order-a"))
        );
    }

    #[test]
    fn test_upsert_of_existing_key_never_trips_ceiling() {
        let index = ClOrdIdIndex::new(1);
        let account = AccountId::new("acct-a");
        let clid = ClientOrderId::new("cl-1");
        index
            .record(account.clone(), clid.clone(), record("order-1"))
            .expect("first record fits the ceiling of 1");
        // Re-recording the SAME key (a retry / recovery re-execution) is an upsert,
        // never a Full error, even at a ceiling of 1.
        index
            .record(account.clone(), clid.clone(), record("order-1"))
            .expect("upsert of an existing key does not trip the ceiling");
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_new_key_at_ceiling_is_typed_full_error() {
        let index = ClOrdIdIndex::new(1);
        let account = AccountId::new("acct-a");
        index
            .record(
                account.clone(),
                ClientOrderId::new("cl-1"),
                record("order-1"),
            )
            .expect("first fits");
        let err = index
            .record(account, ClientOrderId::new("cl-2"), record("order-2"))
            .expect_err("second new key is refused at the ceiling");
        assert_eq!(err, ClOrdIdIndexError::Full { max: 1 });
        // The refused placement left the index untouched.
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_per_account_sub_quota_bounds_one_account_and_spares_others() {
        // Venue-wide ceiling 10, per-account 2: one account cannot monopolize.
        let index = ClOrdIdIndex::with_ceilings(10, 2);
        let account_a = AccountId::new("acct-a");
        let account_b = AccountId::new("acct-b");
        index
            .record(account_a.clone(), ClientOrderId::new("a-1"), record("o-a1"))
            .expect("A first fits");
        index
            .record(account_a.clone(), ClientOrderId::new("a-2"), record("o-a2"))
            .expect("A second fits its quota of 2");
        // A third NEW key for account A trips the per-account quota, NOT the
        // venue-wide ceiling (only 2 of 10 global slots are used).
        let err = index
            .record(account_a.clone(), ClientOrderId::new("a-3"), record("o-a3"))
            .expect_err("A third new key hits the per-account sub-quota");
        assert_eq!(err, ClOrdIdIndexError::AccountFull { max: 2 });
        // Account B is unaffected — a noisy account A cannot deny B the shared index.
        index
            .record(account_b.clone(), ClientOrderId::new("b-1"), record("o-b1"))
            .expect("B still records despite A being at its per-account quota");
        // A's existing keys still upsert (never refused).
        index
            .record(account_a, ClientOrderId::new("a-1"), record("o-a1b"))
            .expect("A upsert of an existing key is never refused");
        assert_eq!(
            index
                .resolve(&account_b, &ClientOrderId::new("b-1"))
                .map(|r| r.order_id),
            Some(VenueOrderId::new("o-b1"))
        );
    }
}
